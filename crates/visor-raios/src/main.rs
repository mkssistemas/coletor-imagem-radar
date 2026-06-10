//! Visor simples de raios (GLM): consulta o catálogo gRPC (`ListarRaios`) pela
//! janela dos últimos N minutos e serve um mapa Leaflet que repinta sozinho.
//!
//! Sem banco, sem S3: é só um cliente do `catalogo serve`. Duas rotas HTTP —
//! `/` (a página) e `/raios.geojson` (os flashes da janela como GeoJSON). O
//! navegador não fala gRPC; este processo faz a ponte gRPC→JSON.

mod grpc;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::State,
    http::{StatusCode, header},
    response::{Html, IntoResponse},
    routing::get,
};
use clap::Parser;
use tonic::transport::Channel;

use grpc::catalogo_client::CatalogoClient;
use grpc::{ListarFramesRequest, ListarRaiosRequest};

/// Página servida em `/` (Leaflet via CDN, embutida no binário).
const INDEX: &str = include_str!("../index.html");

#[derive(Parser)]
#[command(about = "Plota os raios (GLM) dos últimos N minutos num mapa.")]
struct Args {
    /// Catálogo gRPC (fonte dos raios).
    #[arg(long, default_value = "http://10.255.255.4:50051")]
    catalogo: String,
    /// Endereço HTTP local do visor.
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: String,
    /// Janela deslizante, em minutos.
    #[arg(long, default_value_t = 180)]
    minutos: u32,
    /// Máximo de flash_quality_flag aceito (omitido ⇒ servidor usa 0 = só bons).
    #[arg(long)]
    qualidade_max: Option<u32>,
    /// Produto C13 cujos frames (PMTiles) entram de fundo, sincronizados ao playhead.
    #[arg(long, default_value = "abi-l2-cmipf-c13")]
    produto_c13: String,
}

#[derive(Clone)]
struct AppState {
    canal: Channel,
    minutos: u32,
    qualidade_max: Option<u32>,
    produto_c13: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Conexão lazy: o visor sobe mesmo com o catálogo momentaneamente fora, e o
    // tonic reconecta sozinho quando ele volta.
    let canal = Channel::from_shared(args.catalogo.clone())
        .context("endereço do catálogo inválido (use http://host:porta)")?
        .connect_lazy();

    let estado = AppState {
        canal,
        minutos: args.minutos,
        qualidade_max: args.qualidade_max,
        produto_c13: args.produto_c13,
    };

    let app = Router::new()
        .route("/", get(|| async { Html(INDEX) }))
        .route("/raios.geojson", get(raios))
        .route("/frames.json", get(frames))
        .with_state(estado);

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("bind em {}", args.listen))?;
    println!(
        "visor-raios em http://{}  (fonte {}, janela {} min)",
        args.listen, args.catalogo, args.minutos
    );
    axum::serve(listener, app).await.context("servindo HTTP")?;
    Ok(())
}

/// `GET /raios.geojson` — flashes da janela como GeoJSON FeatureCollection.
async fn raios(State(st): State<AppState>) -> impl IntoResponse {
    match coletar(&st).await {
        Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            format!("erro consultando o catálogo: {e:#}"),
        )
            .into_response(),
    }
}

/// Pagina o `ListarRaios` da janela e monta o GeoJSON à mão (campos só
/// numéricos/tempo — sem strings a escapar).
async fn coletar(st: &AppState) -> Result<String> {
    let agora = SystemTime::now();
    let de = (agora - Duration::from_secs(st.minutos as u64 * 60)).duration_since(UNIX_EPOCH)?;
    let de_ts = prost_types::Timestamp {
        seconds: de.as_secs() as i64,
        nanos: de.subsec_nanos() as i32,
    };

    let mut client = CatalogoClient::new(st.canal.clone());
    let mut cursor: Option<String> = None;
    let mut features = String::new();
    let mut total = 0usize;

    // Teto de páginas: trava de segurança contra laço infinito.
    for _ in 0..500 {
        let req = ListarRaiosRequest {
            fonte: None,
            oeste: None,
            sul: None,
            leste: None,
            norte: None,
            de: Some(de_ts.clone()),
            ate: None,
            qualidade_max: st.qualidade_max,
            limite: 0, // default do servidor
            cursor: cursor.clone(),
        };
        let resp = client
            .listar_raios(req)
            .await
            .context("RPC ListarRaios")?
            .into_inner();

        for r in &resp.raios {
            let millis = r
                .tempo
                .as_ref()
                .map(|t| t.seconds * 1000 + (t.nanos as i64) / 1_000_000)
                .unwrap_or(0);
            let (lon, lat, q) = (r.lon, r.lat, r.qualidade);
            // Energia radiante (J): notação científica curta; `null` se ausente.
            let e = r
                .energia
                .map(|v| format!("{v:.3e}"))
                .unwrap_or_else(|| "null".to_string());
            if total > 0 {
                features.push(',');
            }
            // lat/lon a 4 casas (~11 m; a posição do flash já é da ordem de km).
            features.push_str(&format!(
                r#"{{"type":"Feature","geometry":{{"type":"Point","coordinates":[{lon:.4},{lat:.4}]}},"properties":{{"t":{millis},"q":{q},"e":{e}}}}}"#
            ));
            total += 1;
        }

        match resp.proximo_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    let agora_millis = agora.duration_since(UNIX_EPOCH)?.as_millis();
    let min = st.minutos;
    Ok(format!(
        r#"{{"type":"FeatureCollection","gerado":{agora_millis},"janela_min":{min},"total":{total},"features":[{features}]}}"#
    ))
}

/// `GET /frames.json` — frames C13 (PMTiles) cobrindo a janela, com URL assinada.
async fn frames(State(st): State<AppState>) -> impl IntoResponse {
    match coletar_frames(&st).await {
        Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            format!("erro consultando frames: {e:#}"),
        )
            .into_response(),
    }
}

/// Lista os frames C13 de `[agora − (janela + folga)]` até agora. A folga (25 min,
/// > 2 ciclos C13) garante um frame de fundo já no começo do loop, mesmo no "pico
/// do dente de serra" (frame mais novo pode estar ~21 min atrás). `[{t, url}]`.
async fn coletar_frames(st: &AppState) -> Result<String> {
    let agora = SystemTime::now();
    let de = (agora - Duration::from_secs((st.minutos as u64 + 25) * 60))
        .duration_since(UNIX_EPOCH)?;
    let de_ts = prost_types::Timestamp {
        seconds: de.as_secs() as i64,
        nanos: de.subsec_nanos() as i32,
    };

    let mut client = CatalogoClient::new(st.canal.clone());
    let mut cursor: Option<String> = None;
    let mut itens = String::new();
    let mut n = 0usize;

    for _ in 0..50 {
        let req = ListarFramesRequest {
            produto: st.produto_c13.clone(),
            canal: None,
            fonte: None,
            de: Some(de_ts.clone()),
            ate: None,
            limite: 0,
            cursor: cursor.clone(),
        };
        let resp = client
            .listar_frames(req)
            .await
            .context("RPC ListarFrames")?
            .into_inner();

        for f in &resp.frames {
            if f.url.is_empty() {
                continue; // destino local não pré-assina; sem URL não há o que renderizar
            }
            let millis = f
                .inicio
                .as_ref()
                .map(|t| t.seconds * 1000 + (t.nanos as i64) / 1_000_000)
                .unwrap_or(0);
            if n > 0 {
                itens.push(',');
            }
            itens.push_str(&format!(r#"{{"t":{millis},"url":"{}"}}"#, escapar_json(&f.url)));
            n += 1;
        }

        match resp.proximo_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    Ok(format!("[{itens}]"))
}

/// Escapa o mínimo p/ embutir a URL numa string JSON (URLs assinadas não têm
/// aspas/barra-invertida, mas escapamos por segurança).
fn escapar_json(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}
