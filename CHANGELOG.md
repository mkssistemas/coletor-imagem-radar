# Changelog

Todas as mudanças relevantes deste projeto são documentadas aqui.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/)
e o projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Não lançado]

## [0.3.0] - 2026-06-05

### Adicionado

- **Coletor GLM (raios)** (`coletor-glm`): coleta GLM-L2-LCFA do NODD, parseia os
  **flashes** (lat/lon WGS84, energia, área, qualidade, tempo sub-segundo via crate
  `netcdf`/libnetcdf), clipa no BBOX da cobertura e grava **pontos** no catálogo.
  Sem S3, sem tiles.
- Catálogo de raios: hypertables `raios` (1 linha por flash) e `raios_arquivos`
  (livro-razão, 1 linha por `.nc` — base do dedupe durável, inclusive arquivos sem
  flash no BBOX). `State::mark_raios_done` (transação idempotente) e `hydrate` lendo
  ambos os catálogos.
- RPC gRPC **`ListarRaios`** (`bbox`/`de`/`ate`/`qualidade_max`/cursor): pontos
  inline, do mais novo ao mais antigo; qualidade filtrada na consulta. Habilita o
  "piscar em tempo real" no cliente.

### Mudado

- **Reorganizado em cargo workspace** com lib `comum` + 3 binários
  (`coletor-c13`, `coletor-glm`, `catalogo`), para isolar deps de sistema por
  imagem. O loop de pipeline virou genérico (trait `Processor`); cada coletor traz
  só sua cauda.
- **Três imagens OCI** (`Containerfile.{catalogo,coletor-c13,coletor-glm}`) +
  `compose.yaml` com 3 serviços; só a imagem do GLM carrega libnetcdf. CI de
  container em matriz; `migrate` migrou para o binário `catalogo`.
- `BBOX` da cobertura agora é compartilhado em `comum` (recorte C13 e clip GLM).

## [0.2.1] - 2026-06-01

### Corrigido

- Build do container: instala `protoc`/`protobuf-dev` no stage builder do
  `Containerfile`. O `build.rs` (tonic-prost-build) compila `proto/catalogo.proto`
  em build; sem o `protoc` a imagem da v0.2.0 não compilava.

## [0.2.0] - 2026-06-01

### Adicionado

- **Servidor gRPC `serve`** (`src/serve.rs`, tonic): consulta ao catálogo, só
  metadado. Duas RPCs unárias — `UltimoFrame(produto, canal?)` e
  `ListarFrames(...)` (janela temporal, paginada por cursor sobre `inicio`).
  Devolve uma **URL pré-assinada** (GET S3) do `.pmtiles`; os bytes trafegam por
  HTTP range request direto do bucket, nunca pelo gRPC.
- Contrato gRPC em `proto/catalogo.proto` (pacote `coletor.catalogo.v1`),
  compilado pelo `build.rs` via `tonic-prost-build` e incluído em `src/grpc.rs`.
- Módulo `query.rs`: queries read-only do catálogo (`ultimo_frame`,
  `listar_frames`).
- Seção `[grpc]` na config (`listen`/`url_ttl_secs`/`limite_pagina`).
- Workflow de CI (GitHub Actions) para o projeto Rust.

### Notas

- `serve` exige a seção `[database]` (catálogo Postgres) e as credenciais AWS do
  destino para pré-assinar.
- ⚠️ A identidade IAM que assina precisa de `s3:GetObject` no prefixo — o usuário
  de upload (só `PutObject`) gera URLs que voltam **403** no fetch.

## [0.1.0] - 2026-06-01

### Adicionado

- Esqueleto do binário e pipeline end-to-end C13 (NODD → PMTiles) (Fases 1–2):
  download anônimo do bucket `noaa-goes19`, cadeia GDAL (calibração → reproj/recorte
  EPSG:3857 → colormap NOAA → MBTiles → PMTiles) e upload para o destino S3.
- Catálogo TimescaleDB (hypertable) + dedupe persistente via SeaORM + redb (Fase 3):
  escrita idempotente em `imagens_satelite.frames`, com o redb como cache quente local.
- Subcomando `backfill` (varre as últimas N horas retroativas numa passada).
- Empacotamento OCI: `Containerfile` multi-stage + `compose.yaml`.
- Destino S3-only (AWS S3 ou filesystem local em dev); MinIO removido.

[Não lançado]: https://github.com/henrique-mks/coletor-imagem-radar/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/henrique-mks/coletor-imagem-radar/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/henrique-mks/coletor-imagem-radar/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/henrique-mks/coletor-imagem-radar/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/henrique-mks/coletor-imagem-radar/releases/tag/v0.1.0
