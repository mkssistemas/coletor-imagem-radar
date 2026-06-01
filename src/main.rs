//! Coletor de Imagem de Radar — GOES-19 (NODD → PMTiles no nosso S3).
//!
//! CLI: `check` (valida config + lista a origem) e `run` (loop de ingest:
//! poll → download → processa → upload PMTiles → delete-on-success).

mod config;
mod entity;
mod logging;
mod nodd;
mod pipeline;
mod process;
mod state;
mod storage;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::config::Config;

#[derive(Parser)]
#[command(name = "coletor-imagem-radar", version, about = "Coletor de Imagem de Radar — GOES-19 (NODD → PMTiles)")]
struct Cli {
    /// Caminho do arquivo de configuração TOML.
    #[arg(short, long, default_value = "config.toml", global = true)]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Valida a config e a conectividade: constrói os dois clients e lista
    /// alguns objetos da origem anônima (dry-run).
    Check {
        /// Quantos objetos listar no smoke-test da origem.
        #[arg(long, default_value_t = 5)]
        limit: usize,
    },
    /// Loop de ingest+processamento: poll → download → processa → upload PMTiles → delete.
    Run {
        /// Faz uma única passada e sai (útil para testes).
        #[arg(long)]
        once: bool,
        /// Máximo de objetos processados por poll/produto (0 = sem limite).
        #[arg(long, default_value_t = 0)]
        limit: usize,
    },
    /// Aplica as migrations do catálogo no Postgres (schema `imagens_satelite`).
    Migrate,
}

#[tokio::main]
async fn main() -> Result<()> {
    logging::init();

    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;
    info!(
        source = %config.source.bucket,
        destination = %config.destination.bucket,
        products = config.products.len(),
        "configuração carregada"
    );

    match cli.command {
        Command::Check { limit } => check(&config, limit).await,
        Command::Run { once, limit } => pipeline::run(&config, once, limit).await,
        Command::Migrate => {
            let db = config
                .database
                .as_ref()
                .context("subcomando `migrate` exige a seção [database] na config")?;
            state::run_migrations(db).await
        }
    }
}

/// Constrói os clients e faz um list de fumaça na origem para provar a fiação.
async fn check(config: &Config, limit: usize) -> Result<()> {
    let source = storage::build_source(&config.source)?;
    info!("client de origem (anônimo) construído");

    let _destination = storage::build_destination(&config.destination)?;
    info!("client de destino construído");

    // Prefixo a sondar: hora UTC corrente do primeiro produto, se houver;
    // senão a raiz do bucket.
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

    // Destino: não fazemos list (o bucket pode estar vazio/inexistente no
    // esqueleto). A construção do client já valida credenciais/endpoint.
    info!(
        destination = %config.destination.bucket,
        "client de destino pronto (sem list no esqueleto)"
    );

    Ok(())
}
