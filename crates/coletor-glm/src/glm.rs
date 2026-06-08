//! Parsing do GLM-L2-LCFA: extrai os **flashes** de um `.nc` e os entrega já
//! geolocalizados e clipados no BBOX.
//!
//! O L2 LCFA já vem clusterizado (events→groups→flashes) e os flashes já vêm em
//! **lat/lon WGS84** (`flash_lat`/`flash_lon` são `NC_FLOAT` em graus) — não há
//! reprojeção/calibração a fazer. As demais grandezas são `short` **unsigned**
//! com `scale_factor`/`add_offset` (decodificadas aqui); `_FillValue` (-1) vira
//! `None`. O tempo absoluto de cada flash é o início do arquivo (token `s` do
//! nome) + `flash_time_offset_of_first_event` (segundos, sub-segundo).

use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use time::{Duration, OffsetDateTime};

use comum::raio::Flash;

/// Lê os flashes de um `.nc` LCFA, mantendo só os que caem no `bbox`
/// (`[oeste, sul, leste, norte]`, graus). `inicio_arquivo` é o token `s` do nome.
pub fn parse_flashes(nc: &Path, inicio_arquivo: OffsetDateTime, bbox: [f64; 4]) -> Result<Vec<Flash>> {
    let file = netcdf::open(nc).with_context(|| format!("abrindo NetCDF {}", nc.display()))?;

    let lat = ler_f64(&file, "flash_lat")?;
    let lon = ler_f64(&file, "flash_lon")?;
    let n = lat.len();
    if lon.len() != n {
        bail!("flash_lat ({}) e flash_lon ({}) divergem", n, lon.len());
    }
    let ids = ler_u16_como_i32(&file, "flash_id")?;
    let qualidade = ler_qualidade(&file, "flash_quality_flag")?;
    let energia = ler_escalado(&file, "flash_energy")?;
    let area = ler_escalado(&file, "flash_area")?;
    let toff = ler_escalado(&file, "flash_time_offset_of_first_event")?;

    let [oeste, sul, leste, norte] = bbox;
    let mut out = Vec::new();
    for i in 0..n {
        let (la, lo) = (lat[i], lon[i]);
        if lo < oeste || lo > leste || la < sul || la > norte {
            continue;
        }
        // Offset ausente/decodificável vira 0 → tempo = início do arquivo.
        let off = toff.get(i).copied().flatten().unwrap_or(0.0);
        out.push(Flash {
            flash_id: ids[i],
            tempo: inicio_arquivo + Duration::seconds_f64(off),
            lat: la,
            lon: lo,
            energia: energia.get(i).copied().flatten(),
            area: area.get(i).copied().flatten(),
            qualidade: qualidade[i],
        });
    }
    Ok(out)
}

fn variavel<'f>(file: &'f netcdf::File, nome: &str) -> Result<netcdf::Variable<'f>> {
    file.variable(nome)
        .ok_or_else(|| anyhow!("variável '{nome}' ausente no NetCDF"))
}

/// Variável `NC_FLOAT` → `Vec<f64>` (lat/lon em graus, sem escala).
fn ler_f64(file: &netcdf::File, nome: &str) -> Result<Vec<f64>> {
    let v = variavel(file, nome)?;
    let raw: Vec<f32> = v
        .get_values(..)
        .with_context(|| format!("lendo '{nome}'"))?;
    Ok(raw.into_iter().map(|x| x as f64).collect())
}

/// Variável `short` **unsigned** sem escala → `Vec<i32>` (reinterpreta o sinal).
fn ler_u16_como_i32(file: &netcdf::File, nome: &str) -> Result<Vec<i32>> {
    let v = variavel(file, nome)?;
    let raw: Vec<i16> = v
        .get_values(..)
        .with_context(|| format!("lendo '{nome}'"))?;
    Ok(raw.into_iter().map(|x| (x as u16) as i32).collect())
}

/// `flash_quality_flag` (unsigned short, valores 0/1/3/5) → `Vec<i16>`.
fn ler_qualidade(file: &netcdf::File, nome: &str) -> Result<Vec<i16>> {
    let v = variavel(file, nome)?;
    let raw: Vec<i16> = v
        .get_values(..)
        .with_context(|| format!("lendo '{nome}'"))?;
    Ok(raw.into_iter().map(|x| (x as u16) as i16).collect())
}

/// Variável `short` unsigned com `scale_factor`/`add_offset` → `Vec<Option<f64>>`
/// (`None` quando o raw é o `_FillValue`). Reinterpreta o unsigned antes da escala.
fn ler_escalado(file: &netcdf::File, nome: &str) -> Result<Vec<Option<f64>>> {
    let v = variavel(file, nome)?;
    let scale = atributo_f64(&v, "scale_factor").unwrap_or(1.0);
    let offset = atributo_f64(&v, "add_offset").unwrap_or(0.0);
    let fill = atributo_f64(&v, "_FillValue");
    let raw: Vec<i16> = v
        .get_values(..)
        .with_context(|| format!("lendo '{nome}'"))?;
    Ok(raw
        .into_iter()
        .map(|x| {
            // _FillValue é comparado no raw com sinal (-1), antes do unsigned.
            if fill.is_some_and(|f| f == x as f64) {
                None
            } else {
                Some((x as u16) as f64 * scale + offset)
            }
        })
        .collect())
}

/// Lê um atributo escalar numérico como `f64` (tolerante ao tipo concreto).
fn atributo_f64(var: &netcdf::Variable, nome: &str) -> Option<f64> {
    use netcdf::AttributeValue as A;
    match var.attribute(nome)?.value().ok()? {
        A::Float(v) => Some(v as f64),
        A::Double(v) => Some(v),
        A::Short(v) => Some(v as f64),
        A::Ushort(v) => Some(v as f64),
        A::Int(v) => Some(v as f64),
        A::Uint(v) => Some(v as f64),
        A::Schar(v) => Some(v as f64),
        A::Uchar(v) => Some(v as f64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    // O decode unsigned+escala é a parte sutil; testamos a aritmética isolada,
    // espelhando exatamente o que `ler_escalado` faz por elemento.
    fn decode(raw: i16, scale: f64, offset: f64, fill: Option<f64>) -> Option<f64> {
        if fill.is_some_and(|f| f == raw as f64) {
            None
        } else {
            Some((raw as u16) as f64 * scale + offset)
        }
    }

    #[test]
    fn fill_value_vira_none() {
        // _FillValue = -1 (short). Raw -1 ⇒ None, mesmo com escala.
        assert_eq!(decode(-1, 9.99996e-16, 2.8515e-16, Some(-1.0)), None);
    }

    #[test]
    fn unsigned_acima_de_32767_nao_fica_negativo() {
        // 40000 cabe em u16 mas estoura i16 (vira negativo). Reinterpretado deve
        // dar 40000, não -25536.
        let raw = 40000u16 as i16; // = -25536
        let v = decode(raw, 1.0, 0.0, None).unwrap();
        assert_eq!(v, 40000.0);
    }

    #[test]
    fn time_offset_negativo_pelo_add_offset() {
        // add_offset = -5: raw 0 ⇒ -5s (buffer de pré-janela do produto).
        let v = decode(0, 0.0003814756, -5.0, None).unwrap();
        assert!((v + 5.0).abs() < 1e-9);
    }

    #[test]
    fn energia_escala_aplicada() {
        let v = decode(100, 9.99996e-16, 2.8515e-16, Some(-1.0)).unwrap();
        assert!((v - (100.0 * 9.99996e-16 + 2.8515e-16)).abs() < 1e-25);
    }

    /// Integração: parseia um `.nc` real (caminho em `GLM_TEST_NC`). Valida a API
    /// do netcdf de ponta a ponta. Ignorado por padrão (precisa do arquivo):
    /// `GLM_TEST_NC=/tmp/OR_GLM-... cargo test -p coletor-glm -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn parse_arquivo_real() {
        use time::macros::datetime;
        let path = std::env::var("GLM_TEST_NC").expect("defina GLM_TEST_NC");
        // s-token do arquivo de exemplo: 2026-05/06-05 12:00:00Z.
        let inicio = datetime!(2026-06-05 12:00:00 UTC);
        let flashes =
            super::parse_flashes(std::path::Path::new(&path), inicio, comum::BBOX).unwrap();
        eprintln!("flashes no BBOX: {}", flashes.len());
        for f in &flashes {
            assert!(f.lon >= -100.0 && f.lon <= -20.0, "lon fora do bbox: {}", f.lon);
            assert!(f.lat >= -56.0 && f.lat <= 13.0, "lat fora do bbox: {}", f.lat);
            assert!(f.tempo >= inicio - time::Duration::seconds(6));
        }
        assert!(!flashes.is_empty(), "esperava ao menos 1 flash no BBOX");
    }
}
