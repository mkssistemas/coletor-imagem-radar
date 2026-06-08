//! Queries de leitura do catálogo (lado consumidor, servido pelo bin `catalogo`).
//!
//! Read-only sobre as entidades SeaORM ([`crate::entity`] = frames, [`crate::raio`]
//! = raios). Mantém [`crate::state`] focado em dedupe/escrita. As consultas de
//! frames ordenam por `inicio DESC` (índice `frames_produto_inicio_idx`); as de
//! raios por `tempo DESC` (índice `raios_tempo_idx`).

use anyhow::{Context, Result};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
};
use time::OffsetDateTime;

use crate::entity;
use crate::raio;

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

/// Filtro de [`listar_raios`].
pub struct ListarRaiosFiltro<'a> {
    pub fonte: &'a str,
    /// BBOX `[oeste, sul, leste, norte]` (graus). `None` = toda a cobertura.
    pub bbox: Option<[f64; 4]>,
    /// Janela temporal sobre `tempo` (inclusiva). Lado omitido = sem limite.
    pub de: Option<OffsetDateTime>,
    pub ate: Option<OffsetDateTime>,
    /// Máximo `flash_quality_flag` aceito (0 = só bons). Filtro na consulta.
    pub qualidade_max: i16,
    /// Cursor de paginação: só flashes com `tempo` estritamente anterior (DESC).
    pub cursor: Option<OffsetDateTime>,
    pub limite: u64,
}

/// Flashes numa janela (bbox + tempo), do mais novo ao mais antigo, filtrando
/// por qualidade e respeitando o cursor. Casa com o índice `raios_tempo_idx`.
pub async fn listar_raios(
    db: &DatabaseConnection,
    f: &ListarRaiosFiltro<'_>,
) -> Result<Vec<raio::raios::Model>> {
    use raio::raios::Column;
    let mut q = raio::raios::Entity::find()
        .filter(Column::Fonte.eq(f.fonte))
        .filter(Column::Qualidade.lte(f.qualidade_max));
    if let Some([oeste, sul, leste, norte]) = f.bbox {
        q = q
            .filter(Column::Lon.between(oeste, leste))
            .filter(Column::Lat.between(sul, norte));
    }
    if let Some(de) = f.de {
        q = q.filter(Column::Tempo.gte(de));
    }
    if let Some(ate) = f.ate {
        q = q.filter(Column::Tempo.lte(ate));
    }
    if let Some(cur) = f.cursor {
        q = q.filter(Column::Tempo.lt(cur));
    }
    q.order_by_desc(Column::Tempo)
        .limit(f.limite)
        .all(db)
        .await
        .context("listando raios")
}
