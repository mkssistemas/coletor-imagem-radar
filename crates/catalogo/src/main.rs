//! Catálogo — servidor gRPC de consulta + migrations do schema.
//!
//! CLI: `migrate` (aplica o DDL de `frames` e `raios`/`raios_arquivos`) e
//! `serve` (servidor gRPC: UltimoFrame/ListarFrames/ListarRaios). Exige a seção
//! `[database]`; o `serve` também precisa das credenciais AWS do destino para
//! pré-assinar as URLs dos `.pmtiles`.

mod grpc;
mod serve;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing::info;

use comum::config::Config;
use comum::{logging, state};

#[derive(Parser)]
#[command(name = "catalogo", version, about = "Catálogo GOES-19 — servidor gRPC + migrations")]
struct Cli {
    /// Caminho do arquivo de configuração TOML.
    #[arg(short, long, default_value = "config.toml", global = true)]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Aplica as migrations do catálogo no Postgres (frames + raios).
    Migrate,
    /// Sobe o servidor gRPC de consulta ao catálogo.
    Serve {
        /// Endereço de bind `host:porta` (sobrescreve `grpc.listen` da config).
        #[arg(long)]
        listen: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    logging::init();
    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;

    match cli.command {
        Command::Migrate => {
            let db = config
                .database
                .as_ref()
                .context("subcomando `migrate` exige a seção [database] na config")?;
            state::run_migrations(db).await
        }
        Command::Serve { listen } => {
            info!("iniciando servidor gRPC do catálogo");
            serve::run(&config, listen).await
        }
    }
}
