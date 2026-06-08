//! Biblioteca compartilhada pelos binários do Coletor de Imagem de Radar.
//!
//! Reúne o que é comum aos três processos (coletor-c13, coletor-glm, catalogo):
//! configuração, logging, convenções de chave do NODD, construção dos clients
//! S3, catálogo durável (Postgres + cache redb), queries de leitura e o **loop
//! genérico de pipeline** (poll → dedupe → download), parametrizado por um
//! [`pipeline::Processor`] que cada coletor implementa com sua cauda específica.

pub mod config;
pub mod entity;
pub mod logging;
pub mod nodd;
pub mod pipeline;
pub mod query;
pub mod raio;
pub mod state;
pub mod storage;

/// Fonte das imagens. Fixa por ora (GOES-19 via NODD); vira por-fonte quando
/// entrar uma 2ª origem (ex.: EUMETSAT).
pub const FONTE: &str = "noaa-goes-19";

/// BBOX da cobertura `[oeste, sul, leste, norte]` em EPSG:4326 — América do Sul
/// + Atlântico, estendido a Oeste até ~Cidade do México (−100°W) a pedido dos
/// meteorologistas. Compartilhado: recorte do raster C13 e clip dos pontos GLM.
pub const BBOX: [f64; 4] = [-100.0, -56.0, -20.0, 13.0];
