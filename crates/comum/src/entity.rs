//! Entidade SeaORM da tabela `imagens_satelite.frames` (o catálogo).
//!
//! Colunas `timestamptz` mapeiam para `time::OffsetDateTime` (feature
//! `with-time` do sea-orm). A PK é natural e composta — `(fonte, chave_origem,
//! inicio)` — porque a tabela é hypertable do TimescaleDB (a coluna de partição
//! `inicio` precisa estar na PK). `processado_em` fica a cargo do banco no
//! insert (`DEFAULT now()`), por isso entra como `NotSet`.
//!
//! Sem `schema_name` fixo: o schema vem da config e é resolvido em runtime pelo
//! `search_path` da conexão (ver [`crate::state`]).

use sea_orm::entity::prelude::*;
use time::OffsetDateTime;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "frames")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub fonte: String,
    pub produto: String,
    pub canal: Option<String>,
    #[sea_orm(primary_key, auto_increment = false)]
    pub chave_origem: String,
    pub chave_destino: String,
    pub tamanho_bytes: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub inicio: OffsetDateTime,
    pub fim: Option<OffsetDateTime>,
    pub processado_em: OffsetDateTime,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
