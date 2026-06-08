//! Coletor GLM — GOES-19 (NODD → pontos de raio no catálogo Postgres).
//!
//! Usa o loop genérico de [`comum::pipeline`], fornecendo a cauda de pontos
//! (parse do LCFA → insert em `raios`/`raios_arquivos`) via [`ProcessadorGlm`].
//! Sem S3, sem tiles. CLI: `check`, `run`, `backfill`.

mod glm;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use object_store::ObjectStore;
use time::OffsetDateTime;
use tracing::{info, warn};

use comum::config::{Config, ProductConfig};
use comum::pipeline::{self, Processor};
use comum::state::State;
use comum::{logging, nodd};

#[derive(Parser)]
#[command(name = "coletor-glm", version, about = "Coletor GLM — GOES-19 (NODD → pontos de raio)")]
struct Cli {
    /// Caminho do arquivo de configuração TOML.
    #[arg(short, long, default_value = "config.toml", global = true)]
    config: std::path::PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Valida config/conectividade: lista alguns objetos da origem anônima (dry-run).
    Check {
        #[arg(long, default_value_t = 5)]
        limit: usize,
    },
    /// Loop de ingest: poll → download → parse → insert (raios) → delete.
    Run {
        #[arg(long)]
        once: bool,
        #[arg(long, default_value_t = 0)]
        limit: usize,
    },
    /// Backfill: processa as últimas N horas de uma vez (dedupe evita reprocessar).
    Backfill {
        #[arg(long, default_value_t = 48)]
        hours: i64,
        #[arg(long, default_value_t = 0)]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    logging::init();
    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;
    info!(
        source = %config.source.bucket,
        products = config.products.len(),
        "configuração carregada"
    );

    let proc = ProcessadorGlm;
    match cli.command {
        Command::Check { limit } => pipeline::smoke_list_source(&config, limit).await,
        Command::Run { once, limit } => pipeline::run(&config, &proc, once, limit).await,
        Command::Backfill { hours, limit } => pipeline::backfill(&config, &proc, hours, limit).await,
    }
}

/// Cauda de pontos do GLM: parseia o `.nc` e grava os flashes no catálogo.
struct ProcessadorGlm;

impl Processor for ProcessadorGlm {
    async fn process_one(
        &self,
        source: &Arc<dyn ObjectStore>,
        state: &State,
        _product: &ProductConfig,
        work_dir: &Path,
        key: &str,
    ) -> Result<()> {
        let local_nc = pipeline::ensure_downloaded(source, work_dir, key).await?;
        let filename = key.rsplit('/').next().unwrap_or("frame.nc");

        // Início do arquivo (token `s`): base do tempo absoluto dos flashes e
        // chave de partição do livro-razão.
        let inicio = nodd::parse_frame_times(filename).0.unwrap_or_else(|| {
            warn!(file = %filename, "sem timestamp no nome; usando agora() como início");
            OffsetDateTime::now_utc()
        });

        // Parse é blocking (libnetcdf) → fora do reactor. Arquivos são ~220 KB.
        let nc = local_nc.clone();
        let flashes = tokio::task::spawn_blocking(move || glm::parse_flashes(&nc, inicio, comum::BBOX))
            .await
            .context("join do parse GLM")?
            .context("parse do LCFA")?;

        let n = flashes.len();
        state
            .mark_raios_done(comum::FONTE, key, inicio, &flashes)
            .await
            .context("gravando raios no catálogo")?;
        info!(file = %filename, flashes = n, "raios catalogados");

        // Delete-on-success.
        tokio::fs::remove_file(&local_nc).await.ok();
        Ok(())
    }
}
