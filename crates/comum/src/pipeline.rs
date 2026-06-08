//! Loop genérico de ingest (reto, sem fila — para testes iniciais).
//!
//! A parte comum a todo coletor — poll dos prefixos da hora UTC corrente **e da
//! anterior** (janela de overlap p/ chegadas tardias), dedupe persistente
//! ([`State`], redb + Postgres) e download p/ disco efêmero — vive aqui. A
//! **cauda por objeto** (processar→upload→catálogo no C13; parse→insert no GLM)
//! é fornecida por cada coletor via [`Processor`].

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{Context, Result};
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjPath};
use time::{Duration, OffsetDateTime};
use tokio::io::AsyncWriteExt;
use tracing::{error, info, warn};

use crate::config::{Config, ProductConfig};
use crate::nodd;
use crate::state::State;
use crate::storage;

/// Cauda específica do coletor para um objeto novo da origem.
///
/// O loop genérico cuida de poll/dedupe; o `Processor` recebe uma chave já
/// filtrada (nova) e faz o resto (download via [`ensure_downloaded`], processar,
/// gravar catálogo, delete-on-success). Lançar erro **não** marca como visto →
/// o objeto é retentado no próximo poll.
///
/// O future é aguardado inline no loop (sem `spawn`), então não exigimos `Send`.
#[allow(async_fn_in_trait)]
pub trait Processor {
    async fn process_one(
        &self,
        source: &Arc<dyn ObjectStore>,
        state: &State,
        product: &ProductConfig,
        work_dir: &Path,
        key: &str,
    ) -> Result<()>;
}

/// Setup comum: client de origem, dir de trabalho efêmero e estado persistente
/// (catálogo Postgres + cache redb). O destino (S3) é responsabilidade do
/// coletor que precisar dele (C13).
pub async fn open_common(config: &Config) -> Result<(Arc<dyn ObjectStore>, PathBuf, State)> {
    let source = storage::build_source(&config.source)?;
    let work_dir = Path::new(&config.pipeline.work_dir).to_path_buf();
    tokio::fs::create_dir_all(&work_dir).await?;

    let db_cfg = config
        .database
        .as_ref()
        .context("exige a seção [database] na config (catálogo)")?;
    let state = State::open(db_cfg, Path::new(&config.pipeline.state_path)).await?;

    Ok((source, work_dir, state))
}

/// Roda o pipeline. `once = true` faz uma única passada e sai (para testes).
/// `limit = 0` processa todos os objetos novos por poll; >0 limita por produto.
pub async fn run<P: Processor>(
    config: &Config,
    processor: &P,
    once: bool,
    limit: usize,
) -> Result<()> {
    if config.products.is_empty() {
        warn!("nenhum produto configurado — nada a processar");
        return Ok(());
    }
    let (source, work_dir, state) = open_common(config).await?;

    loop {
        let now = OffsetDateTime::now_utc();
        for product in &config.products {
            // `1` = janela de overlap: hora corrente + a anterior (chegadas tardias).
            if let Err(e) =
                poll_product(&source, product, &work_dir, limit, &state, now, 1, processor).await
            {
                error!(product = %product.name, error = %format!("{e:#}"), "falha no poll");
            }
        }

        if once {
            break;
        }

        let secs = config
            .products
            .iter()
            .map(|p| p.poll_interval_secs)
            .min()
            .unwrap_or(120);
        info!(secs, "aguardando próximo poll");
        tokio::time::sleep(StdDuration::from_secs(secs)).await;
    }

    Ok(())
}

/// Backfill: numa única passada, varre as últimas `hours` horas inteiras (em vez
/// de só a janela de overlap do [`run`]), para popular o catálogo
/// retroativamente. O dedupe persistente evita reprocessar o que já existe.
pub async fn backfill<P: Processor>(
    config: &Config,
    processor: &P,
    hours: i64,
    limit: usize,
) -> Result<()> {
    if config.products.is_empty() {
        warn!("nenhum produto configurado — nada a processar");
        return Ok(());
    }
    let (source, work_dir, state) = open_common(config).await?;

    let now = OffsetDateTime::now_utc();
    info!(hours, "backfill iniciando (últimas N horas)");
    for product in &config.products {
        if let Err(e) =
            poll_product(&source, product, &work_dir, limit, &state, now, hours, processor).await
        {
            error!(product = %product.name, error = %format!("{e:#}"), "falha no backfill");
        }
    }
    info!("backfill concluído");
    Ok(())
}

/// Lista a hora corrente e as `hours_back` anteriores do produto e processa o
/// que for novo. `hours_back = 1` é a janela de overlap do [`run`]; valores
/// maiores fazem backfill.
#[allow(clippy::too_many_arguments)]
async fn poll_product<P: Processor>(
    source: &Arc<dyn ObjectStore>,
    product: &ProductConfig,
    work_dir: &Path,
    limit: usize,
    state: &State,
    now: OffsetDateTime,
    hours_back: i64,
    processor: &P,
) -> Result<()> {
    // Filtro de canal: nome traz "<canal>_G19" (ex.: M6C13_G19). Sem canal = sem filtro.
    let needle = product.channel.as_ref().map(|c| format!("{c}_G19"));

    // Hora corrente + as `hours_back` anteriores. source_hour_prefix recomputa
    // AAAA/DDD/HH a cada hora, então cruzar meia-noite/virada de ano sai de graça.
    let prefixes: Vec<String> = (0..=hours_back)
        .map(|h| nodd::source_hour_prefix(product, now - Duration::hours(h)))
        .collect();

    let mut keys: Vec<String> = Vec::new();
    let mut seen_this_poll: HashSet<String> = HashSet::new();
    for prefix in &prefixes {
        info!(product = %product.name, %prefix, "poll");
        let mut stream = source.list(Some(&ObjPath::from(prefix.clone())));
        while let Some(item) = stream.next().await {
            let meta = item.context("listando origem")?;
            let key = meta.location.to_string();
            let matches = needle.as_ref().map(|n| key.contains(n)).unwrap_or(true);
            if !matches {
                continue;
            }
            // Dedupe persistente (redb): pula o que já foi processado.
            if state.is_done(crate::FONTE, &key)? {
                continue;
            }
            if seen_this_poll.insert(key.clone()) {
                keys.push(key);
            }
        }
    }

    if keys.is_empty() {
        info!(product = %product.name, "nada novo nesta janela");
        return Ok(());
    }
    // Ordena por chave (≈ cronológico, pois o nome começa com s<timestamp>).
    keys.sort();
    if limit > 0 && keys.len() > limit {
        keys.truncate(limit);
    }
    info!(product = %product.name, novos = keys.len(), "objetos a processar");

    for key in keys {
        if let Err(e) = processor
            .process_one(source, state, product, work_dir, &key)
            .await
        {
            // Não cataloga → re-tentado no próximo poll (bruto local permanece,
            // sem re-download se ainda existir em disco).
            error!(key = %key, error = %format!("{e:#}"), "falha ao processar objeto");
        }
    }
    Ok(())
}

/// Garante o `.nc` em disco efêmero, devolvendo seu caminho. Reusa o que já
/// estiver baixado (cache: pula o GET se o arquivo já existe).
pub async fn ensure_downloaded(
    source: &Arc<dyn ObjectStore>,
    work_dir: &Path,
    key: &str,
) -> Result<PathBuf> {
    let filename = key.rsplit('/').next().unwrap_or("frame.nc");
    let local = work_dir.join(filename);
    if tokio::fs::try_exists(&local).await.unwrap_or(false) {
        info!(file = %filename, "bruto já em disco, pulando download");
    } else {
        download(source, key, &local).await.context("download")?;
    }
    Ok(local)
}

/// Stream do GET anônimo direto para arquivo, sem buffer integral em memória.
async fn download(source: &Arc<dyn ObjectStore>, key: &str, dest: &Path) -> Result<()> {
    info!(key = %key, dest = %dest.display(), "baixando");
    let result = source.get(&ObjPath::from(key)).await.context("GET origem")?;
    let mut stream = result.into_stream();
    let mut file = tokio::fs::File::create(dest).await?;
    let mut total: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk.context("lendo chunk")?;
        total += bytes.len() as u64;
        file.write_all(&bytes).await?;
    }
    file.flush().await?;
    info!(bytes = total, "download concluído");
    Ok(())
}

/// Smoke-test de listagem da origem (usado pelo `check` dos coletores): lista
/// até `limit` objetos do prefixo da hora UTC corrente do primeiro produto.
pub async fn smoke_list_source(config: &Config, limit: usize) -> Result<()> {
    let source = storage::build_source(&config.source)?;
    info!("client de origem (anônimo) construído");

    let now = OffsetDateTime::now_utc();
    let prefix = config
        .products
        .first()
        .map(|p| nodd::source_hour_prefix(p, now));
    match &prefix {
        Some(p) => info!(prefix = %p, "listando origem (hora UTC corrente)"),
        None => info!("nenhum produto configurado; listando raiz da origem"),
    }

    let obj_prefix = prefix.as_deref().map(ObjPath::from);
    let mut stream = source.list(obj_prefix.as_ref());
    let mut found = 0usize;
    while let Some(item) = stream.next().await {
        let meta = item.context("listando objetos da origem")?;
        info!(key = %meta.location, bytes = meta.size, "objeto");
        found += 1;
        if found >= limit {
            break;
        }
    }
    info!(found, "list de origem concluído");
    if found == 0 {
        warn!(
            "nenhum objeto no prefixo — pode ser hora UTC ainda sem dados publicados, \
             ou produto/prefixo incorreto"
        );
    }
    Ok(())
}
