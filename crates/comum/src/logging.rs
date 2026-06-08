//! Logging estruturado via `tracing`.
//!
//! Texto legível por padrão; JSON quando `SYNC_LOG_FORMAT=json` (para
//! agregadores em produção). Nível controlado por `RUST_LOG`
//! (ex.: `RUST_LOG=coletor_imagem_radar=debug,info`).

use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Inicializa o subscriber global. Chamar uma única vez no início.
pub fn init() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("coletor_imagem_radar=info,warn"));

    let json = std::env::var("SYNC_LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    let registry = tracing_subscriber::registry().with(filter);
    if json {
        registry.with(fmt::layer().json()).init();
    } else {
        registry.with(fmt::layer().with_target(true)).init();
    }
}
