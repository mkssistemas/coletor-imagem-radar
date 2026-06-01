//! Construção dos clients S3 via `object_store`.
//!
//! - **Origem** (NODD): anônima, `skip_signature(true)`. Sem credenciais.
//! - **Destino**: AWS S3 com credenciais do ambiente (`AWS_*`), ou filesystem
//!   local (`local_path`) para dev/teste.

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::local::LocalFileSystem;

use crate::config::{DestinationConfig, SourceConfig};

/// Client de leitura anônima do bucket público do NODD.
pub fn build_source(cfg: &SourceConfig) -> Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_bucket_name(&cfg.bucket)
        .with_region(&cfg.region)
        // Bucket público: requisições não assinadas.
        .with_skip_signature(true)
        .build()
        .with_context(|| format!("construindo client de origem para '{}'", cfg.bucket))?;
    Ok(Arc::new(store))
}

/// Client de escrita do bucket de destino.
///
/// Credenciais vêm do ambiente (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
/// opcional `AWS_SESSION_TOKEN`) via [`AmazonS3Builder::from_env`]. A config
/// (bucket/região) sobrescreve o que vier do ambiente.
pub fn build_destination(cfg: &DestinationConfig) -> Result<Arc<dyn ObjectStore>> {
    // Destino local (dev/teste): grava no filesystem sob `local_path`.
    if let Some(path) = &cfg.local_path {
        std::fs::create_dir_all(path)
            .with_context(|| format!("criando diretório de destino local '{path}'"))?;
        let store = LocalFileSystem::new_with_prefix(path)
            .with_context(|| format!("destino local em '{path}'"))?;
        return Ok(Arc::new(store));
    }

    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&cfg.bucket)
        .with_region(&cfg.region)
        .build()
        .with_context(|| format!("construindo client de destino para '{}'", cfg.bucket))?;
    Ok(Arc::new(store))
}

/// Client do destino como tipo **concreto** `AmazonS3`, para **assinar URLs**
/// (o servidor gRPC devolve presigned GET dos `.pmtiles`).
///
/// `dyn ObjectStore` não expõe o trait [`object_store::signer::Signer`], por
/// isso este builder devolve o concreto. Em modo local (`local_path`), o
/// filesystem não pré-assina → `Ok(None)` (o servidor devolve URL vazia).
/// Credenciais vêm do ambiente, como em [`build_destination`].
pub fn build_destination_signer(cfg: &DestinationConfig) -> Result<Option<Arc<AmazonS3>>> {
    if cfg.local_path.is_some() {
        return Ok(None);
    }
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&cfg.bucket)
        .with_region(&cfg.region)
        .build()
        .with_context(|| format!("construindo signer de destino para '{}'", cfg.bucket))?;
    Ok(Some(Arc::new(store)))
}
