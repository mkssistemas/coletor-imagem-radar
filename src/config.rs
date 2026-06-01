//! Configuração do sincronizador.
//!
//! Carregada de um arquivo TOML (ver `config.example.toml`). As credenciais do
//! destino NÃO ficam aqui — vêm do ambiente (`AWS_ACCESS_KEY_ID` /
//! `AWS_SECRET_ACCESS_KEY`), lidas por `object_store` em [`crate::storage`].

use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Configuração raiz do sincronizador.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub source: SourceConfig,
    pub destination: DestinationConfig,
    /// Produtos a processar. Vazio é válido no esqueleto (Fase 1).
    #[serde(default)]
    pub products: Vec<ProductConfig>,
    /// Parâmetros do pipeline de processamento (Fase 2+).
    #[serde(default)]
    pub pipeline: PipelineConfig,
    /// Conexão com o Postgres do catálogo (Fase 3). Opcional: `check` não
    /// precisa de banco; `run`/`migrate` exigem (erro claro se ausente).
    #[serde(default)]
    pub database: Option<DatabaseConfig>,
    /// Servidor gRPC de consulta ao catálogo (subcomando `serve`).
    #[serde(default)]
    pub grpc: GrpcConfig,
}

/// Parâmetros do servidor gRPC (`serve`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    /// Endereço de bind (`host:porta`). Sobrescrevível por `--listen`.
    #[serde(default = "default_grpc_listen")]
    pub listen: String,
    /// Validade (segundos) das URLs pré-assinadas devolvidas em `Frame.url`.
    #[serde(default = "default_url_ttl_secs")]
    pub url_ttl_secs: u64,
    /// Teto de frames por página em `ListarFrames` (também o default quando o
    /// request pede `limite = 0`).
    #[serde(default = "default_grpc_page_limit")]
    pub limite_pagina: u32,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            listen: default_grpc_listen(),
            url_ttl_secs: default_url_ttl_secs(),
            limite_pagina: default_grpc_page_limit(),
        }
    }
}

/// Parâmetros do pipeline de ingest+processamento.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    /// Diretório de trabalho para o bruto efêmero e artefatos intermediários.
    #[serde(default = "default_work_dir")]
    pub work_dir: String,
    /// Arquivo de rampa de cor (gdaldem color-relief) para o C13. Em °C.
    #[serde(default = "default_c13_ramp")]
    pub c13_color_ramp: String,
    /// Caminho do índice redb (cache quente do dedupe, Fase 3).
    #[serde(default = "default_state_path")]
    pub state_path: String,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            work_dir: default_work_dir(),
            c13_color_ramp: default_c13_ramp(),
            state_path: default_state_path(),
        }
    }
}

/// Conexão com o Postgres do catálogo (campos discretos; a URL é montada em
/// [`DatabaseConfig::url`]). As credenciais ficam aqui no TOML — diferente do
/// destino S3, cujas credenciais vêm do ambiente.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: String,
    pub dbname: String,
    /// Schema do catálogo. O `migrate` cria; a conexão usa via `search_path`.
    /// Identificador simples `[a-z0-9_]` (validado em runtime).
    #[serde(default = "default_schema")]
    pub schema: String,
    /// `sslmode` do libpq (`disable`, `prefer`, `require`, ...). Omitido = default do driver.
    #[serde(default)]
    pub sslmode: Option<String>,
}

impl DatabaseConfig {
    /// Monta a URL `postgres://...` para o sea-orm/sqlx, com `user`/`password`
    /// percent-encoded (suporta caracteres reservados na senha).
    pub fn url(&self) -> String {
        let mut url = format!(
            "postgres://{}:{}@{}:{}/{}",
            pct(&self.user),
            pct(&self.password),
            self.host,
            self.port,
            self.dbname,
        );
        if let Some(mode) = &self.sslmode
            && !mode.is_empty()
        {
            url.push_str("?sslmode=");
            url.push_str(mode);
        }
        url
    }
}

/// Percent-encode de userinfo (RFC 3986 unreserved passa direto; o resto vira `%XX`).
fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Bucket de origem no NOAA NODD — leitura anônima, sem assinatura.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    /// Ex.: `noaa-goes19`.
    pub bucket: String,
    /// Região do bucket público. NODD vive em `us-east-1`.
    #[serde(default = "default_region")]
    pub region: String,
}

/// Bucket de destino: AWS S3 (ou filesystem local via `local_path`, dev/teste).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationConfig {
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    /// Se definido, grava no **filesystem local** sob este caminho (dev/teste),
    /// ignorando o S3. O `prefix` ainda é aplicado às chaves.
    #[serde(default)]
    pub local_path: Option<String>,
    /// Prefixo-raiz das chaves no destino. Ex.: `goes19`.
    /// O layout normalizado do plano é montado a partir daqui.
    #[serde(default)]
    pub prefix: String,
}

/// Um produto GOES-19 a espelhar (ex.: ABI C13 full disk, GLM LCFA).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductConfig {
    /// Identificador interno/legível. Ex.: `abi-l2-cmipf-c13`.
    pub name: String,
    /// Prefixo do produto no bucket de origem. Ex.: `ABI-L2-CMIPF`.
    pub source_prefix: String,
    /// Canal ABI a filtrar (ex.: `C13`). `None` = sem filtro de canal (ex.: GLM).
    #[serde(default)]
    pub channel: Option<String>,
    /// Intervalo de polling em segundos (a cadência real é por produto).
    #[serde(default = "default_poll_secs")]
    pub poll_interval_secs: u64,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_poll_secs() -> u64 {
    120
}

fn default_work_dir() -> String {
    "data".to_string()
}

fn default_c13_ramp() -> String {
    "assets/c13_noaa.txt".to_string()
}

fn default_state_path() -> String {
    "data/state.db".to_string()
}

fn default_pg_port() -> u16 {
    5432
}

fn default_grpc_listen() -> String {
    "0.0.0.0:50051".to_string()
}

fn default_url_ttl_secs() -> u64 {
    3600
}

fn default_grpc_page_limit() -> u32 {
    500
}

fn default_schema() -> String {
    "imagens_satelite".to_string()
}

impl Config {
    /// Lê e valida a configuração de um arquivo TOML.
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("lendo config em {}", path.display()))?;
        let config: Config =
            toml::from_str(&raw).with_context(|| format!("parseando TOML de {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(!self.source.bucket.is_empty(), "source.bucket vazio");
        anyhow::ensure!(
            !self.destination.bucket.is_empty(),
            "destination.bucket vazio"
        );
        for p in &self.products {
            anyhow::ensure!(!p.name.is_empty(), "product.name vazio");
            anyhow::ensure!(
                !p.source_prefix.is_empty(),
                "product.source_prefix vazio para '{}'",
                p.name
            );
        }
        Ok(())
    }
}
