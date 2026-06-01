//! Queries de leitura do catálogo (lado consumidor, servido por [`crate::serve`]).
//!
//! Read-only sobre a entidade SeaORM ([`crate::entity`]). Mantém [`crate::state`]
//! focado em dedupe/escrita. Todas as consultas filtram por `(fonte, produto)`
//! e ordenam por `inicio DESC` — casando com o índice
//! `frames_produto_inicio_idx (produto, inicio DESC)`.

use anyhow::{Context, Result};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
};
use time::OffsetDateTime;

use crate::entity;

/// Frame mais recente de `(fonte, produto[, canal])`. `None` se não houver.
pub async fn ultimo_frame(
    db: &DatabaseConnection,
    fonte: &str,
    produto: &str,
    canal: Option<&str>,
) -> Result<Option<entity::Model>> {
    let mut q = entity::Entity::find()
        .filter(entity::Column::Fonte.eq(fonte))
        .filter(entity::Column::Produto.eq(produto));
    if let Some(c) = canal {
        q = q.filter(entity::Column::Canal.eq(c));
    }
    q.order_by_desc(entity::Column::Inicio)
        .one(db)
        .await
        .context("consultando último frame")
}

/// Filtro de [`listar_frames`].
pub struct ListarFiltro<'a> {
    pub fonte: &'a str,
    pub produto: &'a str,
    pub canal: Option<&'a str>,
    /// Janela temporal sobre `inicio` (inclusiva). Lado omitido = sem limite.
    pub de: Option<OffsetDateTime>,
    pub ate: Option<OffsetDateTime>,
    /// Cursor de paginação: só frames com `inicio` **estritamente anterior**
    /// (ordenação DESC). Como `inicio` tem precisão de décimo de segundo e os
    /// frames de um produto são minutos entre si, é único na prática.
    pub cursor: Option<OffsetDateTime>,
    /// Máximo de linhas a devolver.
    pub limite: u64,
}

/// Frames numa janela, do mais novo ao mais antigo, respeitando o cursor.
pub async fn listar_frames(
    db: &DatabaseConnection,
    f: &ListarFiltro<'_>,
) -> Result<Vec<entity::Model>> {
    let mut q = entity::Entity::find()
        .filter(entity::Column::Fonte.eq(f.fonte))
        .filter(entity::Column::Produto.eq(f.produto));
    if let Some(c) = f.canal {
        q = q.filter(entity::Column::Canal.eq(c));
    }
    if let Some(de) = f.de {
        q = q.filter(entity::Column::Inicio.gte(de));
    }
    if let Some(ate) = f.ate {
        q = q.filter(entity::Column::Inicio.lte(ate));
    }
    if let Some(cur) = f.cursor {
        q = q.filter(entity::Column::Inicio.lt(cur));
    }
    q.order_by_desc(entity::Column::Inicio)
        .limit(f.limite)
        .all(db)
        .await
        .context("listando frames")
}
