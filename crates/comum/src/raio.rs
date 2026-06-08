//! Entidades SeaORM do catálogo de **raios** (GLM-L2-LCFA) e o registro
//! intermediário de parse.
//!
//! Dois modelos, cada um em seu submódulo (o `DeriveEntityModel` gera `Entity`/
//! `Column`/`ActiveModel` no escopo do módulo, então não cabem dois no mesmo):
//!
//! - [`raios::Model`] — **um ponto por flash** (nível de flash do LCFA, já
//!   geolocalizado em lat/lon WGS84). Hypertable particionada em `tempo`.
//! - [`arquivos::Model`] — **uma linha por `.nc` processado** (livro-razão,
//!   inclusive arquivos sem flash no BBOX), espelhando o papel de `frames` no
//!   C13: é a base durável do dedupe pós-restart.
//!
//! Sem `schema_name` fixo: o schema vem da config e é resolvido em runtime pelo
//! `search_path` da conexão (ver [`crate::state`]).

use time::OffsetDateTime;

/// Flash já parseado de um `.nc` (saída do coletor-glm, entrada do catálogo).
/// Coordenadas em graus WGS84; `energia`/`area` decodificados (scale+offset),
/// `None` quando `_FillValue`.
#[derive(Debug, Clone)]
pub struct Flash {
    /// Id do flash dentro do arquivo (único por `.nc`).
    pub flash_id: i32,
    /// Início absoluto do flash (s-token do arquivo + offset do 1º evento).
    pub tempo: OffsetDateTime,
    pub lat: f64,
    pub lon: f64,
    /// Energia radiante (J). `None` se `_FillValue`.
    pub energia: Option<f64>,
    /// Área de cobertura (m²). `None` se `_FillValue`.
    pub area: Option<f64>,
    /// `flash_quality_flag` (0 = bom). Filtrado na consulta, não no ingest.
    pub qualidade: i16,
}

/// Tabela `raios`: um ponto por flash.
pub mod raios {
    use sea_orm::entity::prelude::*;
    use time::OffsetDateTime;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "raios")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub fonte: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub chave_origem: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub flash_id: i32,
        #[sea_orm(primary_key, auto_increment = false)]
        pub tempo: OffsetDateTime,
        pub lat: f64,
        pub lon: f64,
        pub energia: Option<f64>,
        pub area: Option<f64>,
        pub qualidade: i16,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

/// Tabela `raios_arquivos`: livro-razão (uma linha por `.nc` processado).
pub mod arquivos {
    use sea_orm::entity::prelude::*;
    use time::OffsetDateTime;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "raios_arquivos")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub fonte: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub chave_origem: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub inicio: OffsetDateTime,
        pub qtd_flashes: i32,
        pub processado_em: OffsetDateTime,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}
