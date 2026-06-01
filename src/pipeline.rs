//! Loop de ingest+processamento (reto, sem fila — para testes iniciais).
//!
//! Por produto: poll dos prefixos da hora UTC corrente **e da anterior**
//! (janela de overlap p/ chegadas tardias) → para cada `.nc` ainda não
//! processado: download p/ disco efêmero → processa → upload do PMTiles →
//! **grava o catálogo** → **delete-on-success** do bruto. Dedupe via
//! [`crate::state::State`] (redb + Postgres), persistente entre reinícios.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as ObjPath};
use time::{Duration, OffsetDateTime};
use tokio::io::AsyncWriteExt;
use tracing::{error, info, warn};

use crate::config::{Config, ProductConfig};
use crate::process::{self, Job};
use crate::state::{FrameRecord, State};
use crate::{nodd, storage};

/// Fonte das imagens. Fixa por ora (GOES-19 via NODD); vira por-fonte quando
/// entrar uma 2ª origem (ex.: EUMETSAT) na Fase 4+.
pub(crate) const FONTE: &str = "noaa-goes-19";

/// Setup compartilhado por [`run`] e [`backfill`]: clients de origem/destino,
/// dir de trabalho efêmero, rampa de cor e estado persistente (catálogo
/// Postgres + cache redb).
async fn open_pipeline(
    config: &Config,
) -> Result<(
    Arc<dyn ObjectStore>,
    Arc<dyn ObjectStore>,
    std::path::PathBuf,
    std::path::PathBuf,
    State,
)> {
    let source = storage::build_source(&config.source)?;
    let dest = storage::build_destination(&config.destination)?;

    let work_dir = Path::new(&config.pipeline.work_dir).to_path_buf();
    tokio::fs::create_dir_all(&work_dir).await?;
    let ramp = Path::new(&config.pipeline.c13_color_ramp).to_path_buf();

    // Estado persistente (Fase 3): catálogo Postgres + cache redb.
    let db_cfg = config
        .database
        .as_ref()
        .context("exige a seção [database] na config (catálogo, Fase 3)")?;
    let state = State::open(db_cfg, Path::new(&config.pipeline.state_path)).await?;

    Ok((source, dest, work_dir, ramp, state))
}

/// Roda o pipeline. `once = true` faz uma única passada e sai (para testes).
/// `limit = 0` processa todos os objetos novos por poll; >0 limita.
pub async fn run(config: &Config, once: bool, limit: usize) -> Result<()> {
    if config.products.is_empty() {
        warn!("nenhum produto configurado — nada a processar");
        return Ok(());
    }
    let (source, dest, work_dir, ramp, state) = open_pipeline(config).await?;

    loop {
        let now = OffsetDateTime::now_utc();
        for product in &config.products {
            // `1` = janela de overlap: hora corrente + a anterior (chegadas tardias).
            if let Err(e) = poll_product(
                &source, &dest, config, product, &work_dir, &ramp, limit, &state, now, 1,
            )
            .await
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
/// de só a janela de overlap do [`run`]), para popular catálogo/S3
/// retroativamente. O dedupe persistente evita reprocessar o que já existe.
/// `limit = 0` processa tudo que for novo na janela; >0 limita por produto.
pub async fn backfill(config: &Config, hours: i64, limit: usize) -> Result<()> {
    if config.products.is_empty() {
        warn!("nenhum produto configurado — nada a processar");
        return Ok(());
    }
    let (source, dest, work_dir, ramp, state) = open_pipeline(config).await?;

    let now = OffsetDateTime::now_utc();
    info!(hours, "backfill iniciando (últimas N horas)");
    for product in &config.products {
        if let Err(e) = poll_product(
            &source, &dest, config, product, &work_dir, &ramp, limit, &state, now, hours,
        )
        .await
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
async fn poll_product(
    source: &Arc<dyn ObjectStore>,
    dest: &Arc<dyn ObjectStore>,
    config: &Config,
    product: &ProductConfig,
    work_dir: &Path,
    ramp: &Path,
    limit: usize,
    state: &State,
    now: OffsetDateTime,
    hours_back: i64,
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
            // Dedupe persistente (redb): pula o que já virou PMTiles.
            if state.is_done(FONTE, &key)? {
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
        if let Err(e) = process_one(source, dest, config, product, work_dir, ramp, state, &key).await
        {
            // Não cataloga → re-tentado no próximo poll (bruto local permanece,
            // sem re-download se ainda existir em disco).
            error!(key = %key, error = %format!("{e:#}"), "falha ao processar objeto");
        }
    }
    Ok(())
}

/// Sequência completa de um objeto: download → processa → upload → catálogo → delete.
#[allow(clippy::too_many_arguments)]
async fn process_one(
    source: &Arc<dyn ObjectStore>,
    dest: &Arc<dyn ObjectStore>,
    config: &Config,
    product: &ProductConfig,
    work_dir: &Path,
    ramp: &Path,
    state: &State,
    key: &str,
) -> Result<()> {
    let filename = key.rsplit('/').next().unwrap_or("frame.nc");
    let local_nc = work_dir.join(filename);

    // 1. Download → disco efêmero (cache: pula se já existe).
    if tokio::fs::try_exists(&local_nc).await.unwrap_or(false) {
        info!(file = %filename, "bruto já em disco, pulando download");
    } else {
        download(source, key, &local_nc).await.context("download")?;
    }

    // 2. Processa → PMTiles.
    let job = Job {
        product_name: product.name.clone(),
        source_key: key.to_string(),
        local_nc: local_nc.clone(),
    };
    let pmtiles = process::process(&job, work_dir, ramp)
        .await
        .context("processamento")?;

    // 3. Upload do PMTiles → nosso S3.
    let dest_key = nodd::dest_pmtiles_key(product, &config.destination.prefix, key);
    let bytes = tokio::fs::read(&pmtiles).await.context("lendo PMTiles")?;
    let size = bytes.len();
    dest.put(&ObjPath::from(dest_key.clone()), PutPayload::from(Bytes::from(bytes)))
        .await
        .with_context(|| format!("upload para {dest_key}"))?;
    info!(dest_key = %dest_key, bytes = size, "PMTiles no destino");

    // 4. Catálogo (Fase 3): grava ANTES do delete. Falha aqui mantém o bruto
    //    em disco e não marca como visto → retry no próximo poll.
    let (inicio, fim) = nodd::parse_frame_times(filename);
    let inicio = inicio.unwrap_or_else(|| {
        warn!(file = %filename, "sem timestamp no nome; usando agora() como início");
        OffsetDateTime::now_utc()
    });
    let record = FrameRecord {
        fonte: FONTE.to_string(),
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

    // 5. Delete-on-success: só agora apaga o bruto e o PMTiles local.
    tokio::fs::remove_file(&local_nc).await.ok();
    tokio::fs::remove_file(&pmtiles).await.ok();
    info!(file = %filename, "bruto descartado (pós-upload+catálogo)");

    Ok(())
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
