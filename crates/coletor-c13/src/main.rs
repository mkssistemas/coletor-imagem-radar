//! Coletor ABI C13 — GOES-19 (NODD → PMTiles no nosso S3 → catálogo `frames`).
//!
//! Usa o loop genérico de [`comum::pipeline`], fornecendo a cauda raster
//! (calibração → reproj/recorte → colormap → MBTiles → PMTiles → upload →
//! catálogo) via [`ProcessadorC13`]. CLI: `check`, `run`, `backfill`.

mod process;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as ObjPath};
use time::OffsetDateTime;
use tracing::{info, warn};

use comum::config::{Config, ProductConfig};
use comum::pipeline::{self, Processor};
use comum::state::{FrameRecord, State};
use comum::{logging, nodd, storage};

#[derive(Parser)]
#[command(name = "coletor-c13", version, about = "Coletor ABI C13 — GOES-19 (NODD → PMTiles)")]
struct Cli {
    /// Caminho do arquivo de configuração TOML.
    #[arg(short, long, default_value = "config.toml", global = true)]
    config: std::path::PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Valida config/conectividade: constrói os clients e lista alguns objetos
    /// da origem anônima (dry-run).
    Check {
        #[arg(long, default_value_t = 5)]
        limit: usize,
    },
    /// Loop de ingest: poll → download → processa → upload PMTiles → catálogo → delete.
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
        destination = %config.destination.bucket,
        products = config.products.len(),
        "configuração carregada"
    );

    match cli.command {
        Command::Check { limit } => {
            // Valida o client de destino (credenciais/endpoint) e lista a origem.
            let _ = storage::build_destination(&config.destination)?;
            info!("client de destino construído");
            pipeline::smoke_list_source(&config, limit).await
        }
        Command::Run { once, limit } => {
            let proc = ProcessadorC13::new(&config)?;
            pipeline::run(&config, &proc, once, limit).await
        }
        Command::Backfill { hours, limit } => {
            let proc = ProcessadorC13::new(&config)?;
            pipeline::backfill(&config, &proc, hours, limit).await
        }
    }
}

/// Cauda raster do C13: processa o `.nc` em PMTiles, sobe no S3 e cataloga.
struct ProcessadorC13 {
    dest: Arc<dyn ObjectStore>,
    dest_prefix: String,
    ramp: std::path::PathBuf,
}

impl ProcessadorC13 {
    fn new(config: &Config) -> Result<Self> {
        let ramp = Path::new(&config.pipeline.c13_color_ramp).to_path_buf();
        // Fail-fast: `gdaldem color-relief` com rampa inexistente SAI 0 e gera um
        // raster 100% transparente → MBTiles vazio → PMTiles vazio publicado em
        // silêncio. Validar a existência aqui evita o footgun.
        anyhow::ensure!(
            ramp.is_file(),
            "rampa de cor não encontrada: '{}' — ajuste `pipeline.c13_color_ramp` \
             (no container é /app/assets; rodando local da raiz use \
             crates/coletor-c13/assets/c13_noaa.txt)",
            ramp.display()
        );
        Ok(Self {
            dest: storage::build_destination(&config.destination)?,
            dest_prefix: config.destination.prefix.clone(),
            ramp,
        })
    }
}

impl Processor for ProcessadorC13 {
    async fn process_one(
        &self,
        source: &Arc<dyn ObjectStore>,
        state: &State,
        product: &ProductConfig,
        work_dir: &Path,
        key: &str,
    ) -> Result<()> {
        let local_nc = pipeline::ensure_downloaded(source, work_dir, key).await?;
        let filename = key.rsplit('/').next().unwrap_or("frame.nc");

        // Processa → PMTiles.
        let job = process::Job {
            product_name: product.name.clone(),
            source_key: key.to_string(),
            local_nc: local_nc.clone(),
        };
        let pmtiles = process::process(&job, work_dir, &self.ramp)
            .await
            .context("processamento")?;

        // Upload do PMTiles → nosso S3.
        let dest_key = nodd::dest_pmtiles_key(product, &self.dest_prefix, key);
        let bytes = tokio::fs::read(&pmtiles).await.context("lendo PMTiles")?;
        let size = bytes.len();
        self.dest
            .put(&ObjPath::from(dest_key.clone()), PutPayload::from(Bytes::from(bytes)))
            .await
            .with_context(|| format!("upload para {dest_key}"))?;
        info!(dest_key = %dest_key, bytes = size, "PMTiles no destino");

        // Catálogo: grava ANTES do delete. Falha aqui mantém o bruto → retry.
        let (inicio, fim) = nodd::parse_frame_times(filename);
        let inicio = inicio.unwrap_or_else(|| {
            warn!(file = %filename, "sem timestamp no nome; usando agora() como início");
            OffsetDateTime::now_utc()
        });
        let record = FrameRecord {
            fonte: comum::FONTE.to_string(),
            produto: product.name.clone(),
            canal: product.channel.clone(),
            chave_origem: key.to_string(),
            chave_destino: dest_key.clone(),
            tamanho_bytes: size as i64,
            inicio,
            fim,
        };
        state.mark_done(&record).await.context("gravando catálogo")?;
        info!(dest_key = %dest_key, "catalogado");

        // Delete-on-success: só agora apaga o bruto e o PMTiles local.
        tokio::fs::remove_file(&local_nc).await.ok();
        tokio::fs::remove_file(&pmtiles).await.ok();
        info!(file = %filename, "bruto descartado (pós-upload+catálogo)");
        Ok(())
    }
}
