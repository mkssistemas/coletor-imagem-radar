//! Estado híbrido (Fase 3): catálogo durável no **Postgres** (via SeaORM) +
//! **redb** como cache quente local do dedupe.
//!
//! - Boot: [`State::open`] conecta no Postgres, abre o redb e **hidrata** o
//!   cache com as chaves processadas na janela recente (últimas
//!   [`HYDRATE_WINDOW_HOURS`] h) — barato e suficiente, já que o overlap do
//!   poller é de 1 h.
//! - Runtime: [`State::is_done`] consulta só o redb (sem round-trip ao banco);
//!   [`State::mark_done`] grava a linha do catálogo (insert idempotente) e
//!   marca o redb. Marca-se **só após upload OK** (chamado pelo pipeline).

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use redb::{Database as RedbDatabase, ReadableDatabase, TableDefinition};
use sea_orm::sea_query::OnConflict;
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database as SeaDatabase, DatabaseConnection,
    DbErr, EntityTrait, QueryFilter, QuerySelect,
};
use time::{Duration, OffsetDateTime};
use tracing::info;

use crate::config::DatabaseConfig;
use crate::entity;

/// Tabela redb: chave processada → `()` (só presença importa).
const PROCESSED: TableDefinition<&str, ()> = TableDefinition::new("processed");

/// Janela de hidratação do cache no boot.
const HYDRATE_WINDOW_HOURS: i64 = 48;

/// Dados de um frame catalogado (o que o pipeline grava no sucesso).
pub struct FrameRecord {
    pub fonte: String,
    pub produto: String,
    pub canal: Option<String>,
    pub chave_origem: String,
    pub chave_destino: String,
    pub tamanho_bytes: i64,
    pub inicio: OffsetDateTime,
    pub fim: Option<OffsetDateTime>,
}

/// Estado do pipeline: Postgres (durável) + redb (cache de dedupe).
pub struct State {
    db: DatabaseConnection,
    redb: Arc<RedbDatabase>,
}

impl State {
    /// Conecta no Postgres, abre o redb em `state_path` e hidrata o cache.
    pub async fn open(db_cfg: &DatabaseConfig, state_path: &Path) -> Result<Self> {
        let db = connect(db_cfg).await?;

        if let Some(parent) = state_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let redb = RedbDatabase::create(state_path)
            .with_context(|| format!("abrindo redb em {}", state_path.display()))?;
        // Garante que a tabela exista (open_table em write a cria se faltar),
        // pra que os reads de `is_done` nunca falhem com TableDoesNotExist.
        {
            let w = redb.begin_write()?;
            w.open_table(PROCESSED)?;
            w.commit()?;
        }

        let state = Self {
            db,
            redb: Arc::new(redb),
        };
        state.hydrate().await?;
        Ok(state)
    }

    /// Carrega no redb as chaves processadas na janela recente.
    async fn hydrate(&self) -> Result<()> {
        let cutoff = OffsetDateTime::now_utc() - Duration::hours(HYDRATE_WINDOW_HOURS);
        let rows: Vec<(String, String)> = entity::Entity::find()
            .select_only()
            .column(entity::Column::Fonte)
            .column(entity::Column::ChaveOrigem)
            .filter(entity::Column::ProcessadoEm.gt(cutoff))
            .into_tuple()
            .all(&self.db)
            .await
            .context("hidratando estado do catálogo")?;

        let total = rows.len();
        let w = self.redb.begin_write()?;
        {
            let mut t = w.open_table(PROCESSED)?;
            for (fonte, chave) in rows {
                t.insert(cache_key(&fonte, &chave).as_str(), ())?;
            }
        }
        w.commit()?;
        info!(
            carregadas = total,
            janela_horas = HYDRATE_WINDOW_HOURS,
            "estado hidratado do catálogo"
        );
        Ok(())
    }

    /// Já processamos esta chave? (lê só o redb)
    pub fn is_done(&self, fonte: &str, chave_origem: &str) -> Result<bool> {
        let r = self.redb.begin_read()?;
        let t = r.open_table(PROCESSED)?;
        Ok(t.get(cache_key(fonte, chave_origem).as_str())?.is_some())
    }

    /// Grava o frame no catálogo (insert idempotente) e marca o redb.
    pub async fn mark_done(&self, rec: &FrameRecord) -> Result<()> {
        let am = entity::ActiveModel {
            fonte: Set(rec.fonte.clone()),
            produto: Set(rec.produto.clone()),
            canal: Set(rec.canal.clone()),
            chave_origem: Set(rec.chave_origem.clone()),
            chave_destino: Set(rec.chave_destino.clone()),
            tamanho_bytes: Set(rec.tamanho_bytes),
            inicio: Set(rec.inicio),
            fim: Set(rec.fim),
            processado_em: NotSet,
        };

        let res = entity::Entity::insert(am)
            .on_conflict(
                OnConflict::columns([
                    entity::Column::Fonte,
                    entity::Column::ChaveOrigem,
                    entity::Column::Inicio,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec(&self.db)
            .await;
        match res {
            Ok(_) => {}
            // Conflito: a chave já estava no catálogo — ok, é idempotente.
            Err(DbErr::RecordNotInserted) => {}
            Err(e) => return Err(e).context("inserindo no catálogo"),
        }

        let w = self.redb.begin_write()?;
        {
            let mut t = w.open_table(PROCESSED)?;
            t.insert(cache_key(&rec.fonte, &rec.chave_origem).as_str(), ())?;
        }
        w.commit()?;
        Ok(())
    }
}

/// Aplica o DDL do catálogo (subcomando `migrate`): tabela + hypertable + índice
/// **dentro de um schema que já existe** (criado pelo admin; o role do app só
/// opera dentro dele, sem privilégio de criar schema).
///
/// DDL idempotente (`IF NOT EXISTS` / `if_not_exists => TRUE`), executado
/// statement-a-statement (o protocolo do Postgres não aceita múltiplos comandos
/// num `execute`). O schema vem da config e é interpolado já validado.
pub async fn run_migrations(db_cfg: &DatabaseConfig) -> Result<()> {
    let schema = validated_schema(db_cfg)?;
    let db = connect(db_cfg).await?;
    for stmt in ddl_statements(&schema) {
        db.execute_unprepared(&stmt)
            .await
            .with_context(|| format!("aplicando DDL:\n{stmt}"))?;
    }
    info!(%schema, "catálogo migrado (tabela + hypertable + índice)");
    Ok(())
}

/// Statements do DDL do catálogo, na ordem de execução. Não cria o schema (o
/// role não tem permissão p/ isso). `schema` já vem validado como identificador
/// simples, então a interpolação é segura.
fn ddl_statements(schema: &str) -> Vec<String> {
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS \"{schema}\".frames (\n\
             \x20   fonte          text        NOT NULL,\n\
             \x20   produto        text        NOT NULL,\n\
             \x20   canal          text,\n\
             \x20   chave_origem   text        NOT NULL,\n\
             \x20   chave_destino  text        NOT NULL,\n\
             \x20   tamanho_bytes  bigint      NOT NULL,\n\
             \x20   inicio         timestamptz NOT NULL,\n\
             \x20   fim            timestamptz,\n\
             \x20   processado_em  timestamptz NOT NULL DEFAULT now(),\n\
             \x20   PRIMARY KEY (fonte, chave_origem, inicio)\n\
             )"
        ),
        // Hypertable particionada em `inicio`. Função qualificada com `public`
        // porque o `search_path` da conexão aponta só pro schema do catálogo
        // (a API do timescaledb vive em public no Timescale Cloud).
        format!("SELECT public.create_hypertable('{schema}.frames', 'inicio', if_not_exists => TRUE)"),
        format!(
            "CREATE INDEX IF NOT EXISTS frames_produto_inicio_idx \
             ON \"{schema}\".frames (produto, inicio DESC)"
        ),
    ]
}

/// Conexão SeaORM a partir da config. `search_path` aponta pro schema do
/// catálogo (assim a entidade, sem `schema_name` fixo, resolve `frames` nele).
async fn connect(db_cfg: &DatabaseConfig) -> Result<DatabaseConnection> {
    let schema = validated_schema(db_cfg)?;
    let mut opt = ConnectOptions::new(db_cfg.url());
    opt.sqlx_logging(false);
    opt.set_schema_search_path(schema);
    SeaDatabase::connect(opt)
        .await
        .with_context(|| format!("conectando ao Postgres em {}:{}", db_cfg.host, db_cfg.port))
}

/// Valida `database.schema` como identificador Postgres simples e seguro para
/// interpolar no DDL: `[a-z_][a-z0-9_]*` (minúsculo).
fn validated_schema(db_cfg: &DatabaseConfig) -> Result<String> {
    let s = &db_cfg.schema;
    let mut chars = s.chars();
    let head_ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_');
    let tail_ok = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    anyhow::ensure!(
        head_ok && tail_ok,
        "database.schema inválido '{s}': use só [a-z0-9_], começando por letra minúscula ou '_'"
    );
    Ok(s.clone())
}

/// Chave do cache redb: `fonte\0chave_origem` (escopo por fonte, multi-fonte).
fn cache_key(fonte: &str, chave_origem: &str) -> String {
    format!("{fonte}\u{0}{chave_origem}")
}
