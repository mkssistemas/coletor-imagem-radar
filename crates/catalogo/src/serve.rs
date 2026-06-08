//! Servidor gRPC do catálogo (subcomando `serve`).
//!
//! Expõe **metadado**, não bytes de tile: cada [`Frame`] devolvido traz uma URL
//! pré-assinada (GET S3) do `.pmtiles`, que o consumidor carrega por HTTP range
//! request direto do bucket. Já os **raios** (flashes GLM) trafegam inline. Lê
//! o catálogo via [`comum::query`] e assina URLs com o client concreto de
//! destino (via [`comum::storage`]).

use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{Context, Result};
use http::Method;
use object_store::aws::AmazonS3;
use object_store::path::Path as ObjPath;
use object_store::signer::Signer;
use sea_orm::DatabaseConnection;
use time::OffsetDateTime;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::grpc::catalogo_server::{Catalogo, CatalogoServer};
use crate::grpc::{
    Frame, ListarFramesRequest, ListarFramesResponse, ListarRaiosRequest, ListarRaiosResponse,
    Raio, UltimoFrameRequest,
};
use comum::config::Config;
use comum::{entity, query, raio, state, storage};

/// Sobe o servidor gRPC. Exige a seção `[database]` (como `run`/`migrate`); as
/// credenciais AWS do destino (ambiente) são necessárias para assinar URLs.
pub async fn run(config: &Config, listen_override: Option<String>) -> Result<()> {
    let db_cfg = config
        .database
        .as_ref()
        .context("subcomando `serve` exige a seção [database] na config")?;
    let db = state::connect(db_cfg).await?;

    let signer = storage::build_destination_signer(&config.destination)?;
    if signer.is_none() {
        warn!("destino local (local_path): sem assinatura — Frame.url virá vazio");
    }

    let svc = CatalogoService {
        db,
        signer,
        url_ttl: StdDuration::from_secs(config.grpc.url_ttl_secs),
        limite_pagina: config.grpc.limite_pagina,
    };

    let listen = listen_override.unwrap_or_else(|| config.grpc.listen.clone());
    let addr = listen
        .parse()
        .with_context(|| format!("endereço de bind inválido '{listen}'"))?;
    info!(%listen, "servidor gRPC do catálogo ouvindo");

    tonic::transport::Server::builder()
        .add_service(CatalogoServer::new(svc))
        .serve(addr)
        .await
        .context("servidor gRPC")?;
    Ok(())
}

struct CatalogoService {
    db: DatabaseConnection,
    /// `None` em modo de destino local (filesystem não pré-assina).
    signer: Option<Arc<AmazonS3>>,
    url_ttl: StdDuration,
    limite_pagina: u32,
}

impl CatalogoService {
    /// Assina um GET pré-assinado para `chave_destino`. Devolve `(url, expira_em)`;
    /// `("", None)` quando não há signer (modo local).
    async fn assinar(
        &self,
        chave_destino: &str,
    ) -> Result<(String, Option<OffsetDateTime>), Status> {
        let Some(signer) = &self.signer else {
            return Ok((String::new(), None));
        };
        let path = ObjPath::from(chave_destino);
        let url = signer
            .signed_url(Method::GET, &path, self.url_ttl)
            .await
            .map_err(|e| Status::internal(format!("assinando URL de '{chave_destino}': {e}")))?;
        let expira = OffsetDateTime::now_utc() + time::Duration::seconds(self.url_ttl.as_secs() as i64);
        Ok((url.to_string(), Some(expira)))
    }

    /// Converte um `entity::Model` em `Frame`, assinando a URL do tile.
    async fn modelo_para_frame(&self, m: entity::Model) -> Result<Frame, Status> {
        let (url, expira) = self.assinar(&m.chave_destino).await?;
        Ok(Frame {
            fonte: m.fonte,
            produto: m.produto,
            canal: m.canal,
            chave_origem: m.chave_origem,
            chave_destino: m.chave_destino,
            tamanho_bytes: m.tamanho_bytes,
            inicio: Some(to_proto_ts(m.inicio)),
            fim: m.fim.map(to_proto_ts),
            processado_em: Some(to_proto_ts(m.processado_em)),
            url,
            url_expira_em: expira.map(to_proto_ts),
        })
    }
}

#[tonic::async_trait]
impl Catalogo for CatalogoService {
    async fn ultimo_frame(
        &self,
        request: Request<UltimoFrameRequest>,
    ) -> Result<Response<Frame>, Status> {
        let req = request.into_inner();
        if req.produto.is_empty() {
            return Err(Status::invalid_argument("produto é obrigatório"));
        }
        let fonte = req.fonte.as_deref().unwrap_or(comum::FONTE);

        let modelo = query::ultimo_frame(&self.db, fonte, &req.produto, req.canal.as_deref())
            .await
            .map_err(|e| Status::internal(format!("{e:#}")))?
            .ok_or_else(|| Status::not_found("nenhum frame para o produto/canal"))?;

        Ok(Response::new(self.modelo_para_frame(modelo).await?))
    }

    async fn listar_frames(
        &self,
        request: Request<ListarFramesRequest>,
    ) -> Result<Response<ListarFramesResponse>, Status> {
        let req = request.into_inner();
        if req.produto.is_empty() {
            return Err(Status::invalid_argument("produto é obrigatório"));
        }
        let fonte = req.fonte.as_deref().unwrap_or(comum::FONTE);

        // 0 = default do servidor; senão, capa no teto configurado.
        let limite = if req.limite == 0 {
            self.limite_pagina
        } else {
            req.limite.min(self.limite_pagina)
        } as u64;

        let cursor = match req.cursor.as_deref() {
            Some(c) => Some(
                decode_cursor(c)
                    .ok_or_else(|| Status::invalid_argument("cursor inválido"))?,
            ),
            None => None,
        };

        let filtro = query::ListarFiltro {
            fonte,
            produto: &req.produto,
            canal: req.canal.as_deref(),
            de: req.de.as_ref().and_then(from_proto_ts),
            ate: req.ate.as_ref().and_then(from_proto_ts),
            cursor,
            limite,
        };
        let modelos = query::listar_frames(&self.db, &filtro)
            .await
            .map_err(|e| Status::internal(format!("{e:#}")))?;

        // Página cheia ⇒ provavelmente há mais; cursor = `inicio` do último.
        let proximo_cursor = (modelos.len() as u64 == limite)
            .then(|| modelos.last().map(|m| encode_cursor(m.inicio)))
            .flatten();

        let mut frames = Vec::with_capacity(modelos.len());
        for m in modelos {
            frames.push(self.modelo_para_frame(m).await?);
        }

        Ok(Response::new(ListarFramesResponse {
            frames,
            proximo_cursor,
        }))
    }

    async fn listar_raios(
        &self,
        request: Request<ListarRaiosRequest>,
    ) -> Result<Response<ListarRaiosResponse>, Status> {
        let req = request.into_inner();
        let fonte = req.fonte.as_deref().unwrap_or(comum::FONTE);

        // 0 = default do servidor; senão, capa no teto configurado.
        let limite = if req.limite == 0 {
            self.limite_pagina
        } else {
            req.limite.min(self.limite_pagina)
        } as u64;

        let cursor = match req.cursor.as_deref() {
            Some(c) => {
                Some(decode_cursor(c).ok_or_else(|| Status::invalid_argument("cursor inválido"))?)
            }
            None => None,
        };

        // BBOX só é aplicado se os quatro lados vierem; senão, toda a cobertura.
        let bbox = match (req.oeste, req.sul, req.leste, req.norte) {
            (Some(o), Some(s), Some(l), Some(n)) => Some([o, s, l, n]),
            _ => None,
        };

        let filtro = query::ListarRaiosFiltro {
            fonte,
            bbox,
            de: req.de.as_ref().and_then(from_proto_ts),
            ate: req.ate.as_ref().and_then(from_proto_ts),
            qualidade_max: req.qualidade_max.unwrap_or(0) as i16,
            cursor,
            limite,
        };
        let modelos = query::listar_raios(&self.db, &filtro)
            .await
            .map_err(|e| Status::internal(format!("{e:#}")))?;

        // Página cheia ⇒ provavelmente há mais; cursor = `tempo` do último.
        let proximo_cursor = (modelos.len() as u64 == limite)
            .then(|| modelos.last().map(|m| encode_cursor(m.tempo)))
            .flatten();

        let raios = modelos.into_iter().map(modelo_para_raio).collect();

        Ok(Response::new(ListarRaiosResponse {
            raios,
            proximo_cursor,
        }))
    }
}

/// Converte um `raio::raios::Model` em `Raio` (sem assinatura — pontos inline).
fn modelo_para_raio(m: raio::raios::Model) -> Raio {
    Raio {
        tempo: Some(to_proto_ts(m.tempo)),
        lat: m.lat,
        lon: m.lon,
        energia: m.energia,
        area: m.area,
        qualidade: m.qualidade as u32,
    }
}

/// `OffsetDateTime` → `google.protobuf.Timestamp`.
fn to_proto_ts(t: OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: t.unix_timestamp(),
        nanos: t.nanosecond() as i32,
    }
}

/// `google.protobuf.Timestamp` → `OffsetDateTime` (None se fora de faixa).
fn from_proto_ts(ts: &prost_types::Timestamp) -> Option<OffsetDateTime> {
    let nanos = ts.seconds as i128 * 1_000_000_000 + ts.nanos as i128;
    OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()
}

/// Cursor opaco = nanossegundos unix de `inicio`, em decimal.
fn encode_cursor(t: OffsetDateTime) -> String {
    t.unix_timestamp_nanos().to_string()
}

fn decode_cursor(s: &str) -> Option<OffsetDateTime> {
    s.parse::<i128>()
        .ok()
        .and_then(|n| OffsetDateTime::from_unix_timestamp_nanos(n).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn timestamp_round_trip() {
        let t = datetime!(2026-05-29 14:00:20.800 UTC);
        let back = from_proto_ts(&to_proto_ts(t)).unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn cursor_round_trip() {
        let t = datetime!(2026-05-29 14:09:51.600 UTC);
        assert_eq!(decode_cursor(&encode_cursor(t)), Some(t));
    }

    #[test]
    fn cursor_invalido_retorna_none() {
        assert_eq!(decode_cursor("abc"), None);
        assert_eq!(decode_cursor(""), None);
    }
}
