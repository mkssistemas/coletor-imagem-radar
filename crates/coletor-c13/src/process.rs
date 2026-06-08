//! Processamento por produto. Fase 2: pipeline do C13 (ABI L2 CMIPF, ch13).
//!
//! Reaproveita a cadeia do PoC `goes-nodd-poc` (calibração → recorte/reprojeção
//! → colormap), trocando a cauda COG por **PMTiles**. Tudo via binários GDAL +
//! `pmtiles convert` (mesma abordagem do PoC, que shella `gdal_calc.py`/`gdalwarp`).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use tokio::process::Command;
use tracing::{debug, info};

/// Constantes de calibração do GOES-19 L2 (idênticas ao PoC).
const SCALE: f64 = 0.06145332;
const OFFSET: f64 = 89.620003;
/// BBOX [oeste, sul, leste, norte] em EPSG:4326 — cobertura compartilhada
/// (definida em [`comum::BBOX`]): América do Sul + Atlântico, estendido a Oeste
/// até ~Cidade do México a pedido dos meteorologistas.
const BBOX: [f64; 4] = comum::BBOX;
/// Resolução-alvo em metros (EPSG:3857). ~2 km, equivalente aos 0.018° do PoC.
const TARGET_RES_M: &str = "2000";

/// Unidade de trabalho entregue pelo fetcher ao processador.
#[derive(Debug, Clone)]
pub struct Job {
    /// Nome do produto (ex.: `abi-l2-cmipf-c13`).
    pub product_name: String,
    /// Chave de origem no NODD (para mapear o destino).
    pub source_key: String,
    /// Caminho do `.nc` bruto no disco efêmero.
    pub local_nc: PathBuf,
}

/// Despacha o job para o pipeline do produto. Retorna o caminho do `.pmtiles`.
///
/// Loop reto (sem fila) para testes iniciais: o `match` por produto é o ponto
/// onde, no futuro, entram pipelines distintos (GLM etc.).
pub async fn process(job: &Job, work_dir: &Path, c13_ramp: &Path) -> Result<PathBuf> {
    debug!(product = %job.product_name, source_key = %job.source_key, "despachando job");
    match job.product_name.as_str() {
        "abi-l2-cmipf-c13" => process_c13(job, work_dir, c13_ramp).await,
        other => bail!("sem pipeline de processamento para o produto '{other}'"),
    }
}

/// Pipeline C13: NetCDF CMI → °C → reproj/crop 3857 → colormap → MBTiles → PMTiles.
async fn process_c13(job: &Job, work_dir: &Path, ramp: &Path) -> Result<PathBuf> {
    let nc = job
        .local_nc
        .canonicalize()
        .with_context(|| format!("resolvendo {}", job.local_nc.display()))?;
    let stem = nc
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("frame")
        .to_string();

    let tmp = work_dir.join(format!("tmp_{stem}"));
    // Limpa intermediários de uma tentativa anterior (o tmp só é removido no
    // sucesso): `gdal_calc.py` não tem --overwrite e aborta se o celsius.tif já
    // existir, então um retry precisa de tmp limpo p/ ser idempotente.
    tokio::fs::remove_dir_all(&tmp).await.ok();
    tokio::fs::create_dir_all(&tmp).await?;

    let celsius = tmp.join("celsius.tif");
    let warped = tmp.join("warped.tif");
    let colored = tmp.join("colored.tif");
    let mbtiles = tmp.join("tiles.mbtiles");
    let pmtiles = work_dir.join(format!("{stem}.pmtiles"));

    let p = |path: &Path| path.to_string_lossy().to_string();

    info!(product = %job.product_name, file = %stem, "processando C13");

    // 1. Calibração: conta crua → Kelvin → Celsius.
    let cmi = format!("NETCDF:\"{}\":CMI", p(&nc));
    let formula = format!("((A * {SCALE}) + {OFFSET}) - 273.15");
    run(
        "gdal_calc.py",
        vec![
            "-A".into(), cmi,
            "--outfile".into(), p(&celsius),
            "--calc".into(), formula,
            "--NoDataValue".into(), "-999".into(),
            "--type".into(), "Float32".into(),
            "--quiet".into(),
        ],
    )
    .await
    .context("calibração (gdal_calc.py)")?;

    // 2. Reprojeção p/ Web Mercator + recorte no BBOX (tiles XYZ vivem em 3857).
    run(
        "gdalwarp",
        vec![
            "-t_srs".into(), "EPSG:3857".into(),
            "-te".into(),
            BBOX[0].to_string(), BBOX[1].to_string(),
            BBOX[2].to_string(), BBOX[3].to_string(),
            "-te_srs".into(), "EPSG:4326".into(),
            "-tr".into(), TARGET_RES_M.into(), TARGET_RES_M.into(),
            "-r".into(), "near".into(),
            "-of".into(), "GTiff".into(),
            "-overwrite".into(),
            p(&celsius), p(&warped),
        ],
    )
    .await
    .context("reprojeção/recorte (gdalwarp)")?;

    // 3. Colormap NOAA → RGBA.
    run(
        "gdaldem",
        vec![
            "color-relief".into(),
            p(&warped), p(ramp), p(&colored),
            "-alpha".into(),
        ],
    )
    .await
    .context("colormap (gdaldem color-relief)")?;

    // 4. RGBA → MBTiles (zoom base) + overviews (zooms menores).
    run(
        "gdal_translate",
        vec![
            "-of".into(), "MBTILES".into(),
            "-co".into(), "TILE_FORMAT=PNG".into(),
            p(&colored), p(&mbtiles),
        ],
    )
    .await
    .context("MBTiles (gdal_translate)")?;
    run(
        "gdaladdo",
        vec!["-r".into(), "average".into(), p(&mbtiles), "2".into(), "4".into(), "8".into(), "16".into()],
    )
    .await
    .context("overviews (gdaladdo)")?;

    // 5. MBTiles → PMTiles.
    run("pmtiles", vec!["convert".into(), p(&mbtiles), p(&pmtiles)])
        .await
        .context("MBTiles→PMTiles (pmtiles convert)")?;

    // Limpa os intermediários (o .nc bruto é apagado pelo pipeline pós-upload).
    tokio::fs::remove_dir_all(&tmp).await.ok();

    info!(pmtiles = %pmtiles.display(), "PMTiles gerado");
    Ok(pmtiles)
}

/// Executa um binário externo e falha com o stderr capturado se o status != 0.
async fn run(prog: &str, args: Vec<String>) -> Result<()> {
    debug!(prog, ?args, "exec");
    let out = Command::new(prog)
        .args(&args)
        .output()
        .await
        .with_context(|| format!("não foi possível executar '{prog}' (instalado?)"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        bail!("'{prog}' falhou ({}): {}", out.status, stderr.trim());
    }
    Ok(())
}
