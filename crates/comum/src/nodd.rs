//! Convenções de chave do NODD (`noaa-goes19`).
//!
//! Layout de origem: `<Produto>/<AAAA>/<DDD>/<HH>/<arquivo>.nc`
//! onde DDD = dia juliano (dia-do-ano, 1..=366) e HH = hora UTC.

use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

use crate::config::ProductConfig;

/// Prefixo da hora UTC informada para um produto, no bucket de origem.
///
/// Ex.: `ABI-L2-CMIPF/2026/149/14/`
pub fn source_hour_prefix(product: &ProductConfig, at: OffsetDateTime) -> String {
    let at = at.to_offset(time::UtcOffset::UTC);
    format!(
        "{}/{:04}/{:03}/{:02}/",
        product.source_prefix.trim_end_matches('/'),
        at.year(),
        at.ordinal(),
        at.hour(),
    )
}

/// Chave de destino (PMTiles) a partir da chave de origem.
///
/// Reaproveita os segmentos de data (`AAAA/DDD/HH`) e o nome base do `.nc`,
/// trocando a extensão por `.pmtiles` sob o prefixo normalizado do produto.
///
/// Ex.: origem `ABI-L2-CMIPF/2026/149/14/OR_..._C13_....nc`
///   → `goes19/abi-l2-cmipf-c13/2026/149/14/OR_..._C13_....pmtiles`
pub fn dest_pmtiles_key(product: &ProductConfig, dest_prefix: &str, source_key: &str) -> String {
    let parts: Vec<&str> = source_key.split('/').filter(|s| !s.is_empty()).collect();
    let filename = parts.last().copied().unwrap_or("output.nc");
    let stem = filename.strip_suffix(".nc").unwrap_or(filename);

    // Segmentos de data: os três imediatamente antes do nome do arquivo.
    let date_path = if parts.len() >= 4 {
        parts[parts.len() - 4..parts.len() - 1].join("/")
    } else {
        String::new()
    };

    let root = dest_prefix.trim_matches('/');
    let prefix = if root.is_empty() {
        format!("goes19/{}", product.name)
    } else {
        format!("{}/goes19/{}", root, product.name)
    };

    if date_path.is_empty() {
        format!("{}/{}.pmtiles", prefix, stem)
    } else {
        format!("{}/{}/{}.pmtiles", prefix, date_path, stem)
    }
}

/// Extrai os timestamps de início (`s`) e fim (`e`) do nome de arquivo NODD.
///
/// O nome traz tokens `s<AAAADDDHHMMSSm>` e `e<...>` (DDD = dia juliano, `m` =
/// décimo de segundo). Retorna `(início, fim)`; cada um é `None` se o token
/// não existir ou não parsear.
pub fn parse_frame_times(filename: &str) -> (Option<OffsetDateTime>, Option<OffsetDateTime>) {
    let mut inicio = None;
    let mut fim = None;
    for part in filename.split('_') {
        if let Some(rest) = part.strip_prefix('s') {
            inicio = inicio.or_else(|| parse_nodd_time(rest));
        } else if let Some(rest) = part.strip_prefix('e') {
            fim = fim.or_else(|| parse_nodd_time(rest));
        }
    }
    (inicio, fim)
}

/// Parseia um token de tempo NODD: `AAAADDDHHMMSS` (13 dígitos) + opcional `m`
/// (décimo de segundo). Ex.: `20261491400208` → 2026-05-29T14:00:20.800Z.
fn parse_nodd_time(s: &str) -> Option<OffsetDateTime> {
    let b = s.as_bytes();
    if b.len() < 13 || !b[..13].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let year: i32 = s[0..4].parse().ok()?;
    let doy: u16 = s[4..7].parse().ok()?;
    let hh: u8 = s[7..9].parse().ok()?;
    let mm: u8 = s[9..11].parse().ok()?;
    let ss: u8 = s[11..13].parse().ok()?;
    let milli: u16 = if b.len() >= 14 && b[13].is_ascii_digit() {
        s[13..14].parse::<u16>().ok()? * 100
    } else {
        0
    };
    let date = Date::from_ordinal_date(year, doy).ok()?;
    let time = Time::from_hms_milli(hh, mm, ss, milli).ok()?;
    Some(PrimitiveDateTime::new(date, time).assume_utc())
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn product() -> ProductConfig {
        ProductConfig {
            name: "abi-l2-cmipf-c13".into(),
            source_prefix: "ABI-L2-CMIPF".into(),
            channel: Some("C13".into()),
            poll_interval_secs: 120,
        }
    }

    #[test]
    fn prefix_usa_dia_juliano_e_hora_utc() {
        // 2026-05-29 = dia-do-ano 149.
        let t = datetime!(2026-05-29 14:37:00 UTC);
        assert_eq!(source_hour_prefix(&product(), t), "ABI-L2-CMIPF/2026/149/14/");
    }

    #[test]
    fn prefix_converte_para_utc() {
        // Mesmo instante, expresso em -03:00 → ainda 17h UTC.
        let t = datetime!(2026-01-01 14:00:00 -3);
        assert_eq!(source_hour_prefix(&product(), t), "ABI-L2-CMIPF/2026/001/17/");
    }

    #[test]
    fn dest_key_reaproveita_data_e_troca_extensao() {
        let src = "ABI-L2-CMIPF/2026/149/14/OR_ABI-L2-CMIPF-M6C13_G19_s20261491400208_e20261491409516_c20261491409571.nc";
        assert_eq!(
            dest_pmtiles_key(&product(), "goes19-tiles", src),
            "goes19-tiles/goes19/abi-l2-cmipf-c13/2026/149/14/\
             OR_ABI-L2-CMIPF-M6C13_G19_s20261491400208_e20261491409516_c20261491409571.pmtiles"
        );
    }

    #[test]
    fn dest_key_sem_prefixo_raiz() {
        let src = "ABI-L2-CMIPF/2026/149/14/OR_x_C13_.nc";
        assert_eq!(
            dest_pmtiles_key(&product(), "", src),
            "goes19/abi-l2-cmipf-c13/2026/149/14/OR_x_C13_.pmtiles"
        );
    }

    #[test]
    fn parse_times_extrai_inicio_e_fim() {
        let name = "OR_ABI-L2-CMIPF-M6C13_G19_s20261491400208_e20261491409516_c20261491409571.nc";
        let (inicio, fim) = parse_frame_times(name);
        // dia juliano 149 de 2026 = 29/05; último dígito = décimo de segundo.
        assert_eq!(inicio, Some(datetime!(2026-05-29 14:00:20.800 UTC)));
        assert_eq!(fim, Some(datetime!(2026-05-29 14:09:51.600 UTC)));
    }

    #[test]
    fn parse_times_sem_tokens_retorna_none() {
        let (inicio, fim) = parse_frame_times("arquivo_qualquer.nc");
        assert!(inicio.is_none() && fim.is_none());
    }

    #[test]
    fn parse_nodd_time_rejeita_lixo() {
        assert!(parse_nodd_time("20261491400208").is_some());
        assert!(parse_nodd_time("abc").is_none());
        assert!(parse_nodd_time("2026149").is_none()); // curto demais
    }
}
