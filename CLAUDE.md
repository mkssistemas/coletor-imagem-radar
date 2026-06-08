# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## O que é

Coletor de produtos GOES-19 do **NOAA NODD** (bucket público `noaa-goes19`,
`us-east-1`, leitura anônima). É um **cargo workspace** com uma lib compartilhada
(`comum`) e **três binários**, um por responsabilidade/container:

- **`coletor-c13`** — ABI C13 (NetCDF) → **PMTiles** no nosso S3 → catálogo `frames`.
- **`coletor-glm`** — GLM-L2-LCFA (raios) → **pontos** (flashes) no Postgres (`raios`).
- **`catalogo`** — servidor gRPC de consulta + `migrate` (DDL do catálogo).

A separação em 3 binários existe para que **cada imagem carregue só suas deps de
sistema** (GDAL+pmtiles no C13; libnetcdf no GLM; nada extra no catálogo).
Comentários e docs em pt-BR.

## Comandos

```sh
cargo build --workspace                          # compila tudo
cargo test --workspace                           # testes (comum/nodd, coletor-glm/glm, catalogo/serve)
cargo test -p coletor-glm                         # testes de um crate
# Teste de integração do parser GLM contra um .nc real (ignorado por padrão):
GLM_TEST_NC=/tmp/OR_GLM-...nc cargo test -p coletor-glm parse_arquivo_real -- --ignored --nocapture

cargo run -p catalogo -- migrate                  # DDL: frames + raios + raios_arquivos
cargo run -p coletor-c13 -- check --limit 5       # valida config + lista a origem (dry-run)
cargo run -p coletor-c13 -- run --once --limit 1  # 1 passada C13 (download→PMTiles→upload→catálogo→delete)
cargo run -p coletor-glm -- run --once --limit 1  # 1 passada GLM (download→parse→insert raios→delete)
cargo run -p coletor-c13 -- run                   # loop contínuo
cargo run -p coletor-glm -- backfill --hours 48   # popula retroativo
cargo run -p catalogo -- serve                    # gRPC (UltimoFrame/ListarFrames/ListarRaios)
```

- Config: `-c/--config` (default `config.toml`). Copie `config.example.toml`.
  **Cada binário tem seu próprio config**, com APENAS o(s) seu(s) `[[products]]`
  (o loop aplica a mesma cauda a todos os produtos listados).
- Credenciais do **destino S3** vêm SÓ do ambiente: `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY` (`AWS_SESSION_TOKEN` opcional). Nunca no TOML.
- Credenciais do **Postgres** (catálogo) vêm da seção `[database]` do TOML.
  Todos os subcomandos que tocam o banco exigem essa seção.
- Logs: `RUST_LOG=coletor_glm=debug,comum=debug` etc.; `SYNC_LOG_FORMAT=json` p/ JSON.

## Dependências externas (não-Rust)

Por binário (faltando, falha em runtime, não em compile):

- **`coletor-c13`**: shella `gdal_calc.py`, `gdalwarp`, `gdaldem`, `gdal_translate`,
  `gdaladdo` (GDAL) e `pmtiles`. Imagem base: `osgeo/gdal:alpine-normal`.
- **`coletor-glm`**: linka a **libnetcdf** (dynlib do sistema) via crate `netcdf`.
  A imagem é **Debian/glibc** (não Alpine/musl como o C13) **de propósito**: o
  `hdf5-metno-sys` (dep do netcdf) infere a versão do HDF5 por `dlopen` no build, e
  binário **musl estático não faz dlopen** ("Dynamic loading not supported") →
  build quebra. Em glibc funciona. Build: `libnetcdf-dev`+`pkg-config`+`clang`;
  runtime: `libnetcdf19` + `ca-certificates`.
  ⚠️ Dev local: HDF5 ≥ 2.x (ex. Manjaro) quebra `hdf5-metno-sys` antigo — por isso
  o crate `netcdf` está em **0.12** (hdf5-metno-sys 0.11, que entende HDF5 2.x).
- **`catalogo`**: nenhuma dep de sistema em runtime; build precisa de `protoc`
  (`build.rs` compila `proto/catalogo.proto` via `tonic-prost-build`).

Todos os que tocam o banco precisam de um **Postgres/TimescaleDB** acessível
(schema `imagens_satelite`). `check`, `build` e `test` NÃO precisam de banco.

O `serve` também precisa das **credenciais AWS do destino** para **pré-assinar**
as URLs GET dos `.pmtiles` (frames). ⚠️ A identidade que assina precisa de
`s3:GetObject` no prefixo — o usuário só-`PutObject` gera URLs que voltam **403**.
(Os **raios** trafegam inline no gRPC, sem URL assinada.)

## Arquitetura

O loop de ingest é **genérico** (`comum::pipeline`): poll → dedupe → download. A
**cauda por objeto** é um `Processor` que cada coletor implementa:

1. **Poll**: lista os prefixos da hora UTC corrente **e da anterior** (overlap p/
   chegadas tardias) na origem (`nodd::source_hour_prefix`, layout NODD
   `<Produto>/<AAAA>/<DDD>/<HH>/`, DDD = dia juliano). Filtra por canal via
   substring `"<canal>_G19"` (C13); produto sem `channel` não filtra (GLM).
   Pula o que o dedupe (`State::is_done`, redb) já marcou.
2. **Download**: GET anônimo em stream → disco efêmero (`pipeline::ensure_downloaded`,
   default `data/`). Pula se o `.nc` já existe.
3. **Cauda (Processor)**:
   - **C13** (`coletor-c13`, `process::process`): calibração CMI→°C (`gdal_calc.py`)
     → reproj/recorte EPSG:3857 no BBOX (`gdalwarp`) → colormap NOAA
     (`gdaldem color-relief` + `assets/c13_noaa.txt`) → MBTiles (`gdal_translate`
     + `gdaladdo`) → PMTiles (`pmtiles convert`) → **upload** S3
     (`nodd::dest_pmtiles_key`) → `State::mark_done` (1 linha em `frames`).
   - **GLM** (`coletor-glm`, `glm::parse_flashes`): lê os arrays `flash_*` do `.nc`
     (lat/lon já em graus WGS84; energia/área são `short` unsigned com
     scale+offset; tempo = s-token + `flash_time_offset_of_first_event`), **clipa
     no `comum::BBOX`** e chama `State::mark_raios_done`.
4. **Delete-on-success**: só após catálogo OK apaga o `.nc` (e o `.pmtiles`).
   Erro em qualquer ponto NÃO cataloga → retentado no próximo poll.

### Catálogo e dedupe (Postgres + redb)

Catálogo durável no **Postgres** (hypertables TimescaleDB) + **redb** como cache
quente de dedupe. Tabelas (schema `imagens_satelite`):

- `frames` — 1 linha por `.nc` C13 (PK `(fonte, chave_origem, inicio)`).
- `raios` — 1 linha por **flash** (PK `(fonte, chave_origem, flash_id, tempo)`);
  colunas `lat`/`lon`/`energia`/`area`/`qualidade`. Hypertable em `tempo`.
- `raios_arquivos` — **livro-razão**: 1 linha por `.nc` GLM processado (inclusive
  com `qtd_flashes = 0`). Espelha o papel de `frames` no dedupe do GLM — é o que o
  `hydrate` lê para não reprocessar arquivos vazios num cold start.

`mark_raios_done` grava livro-razão + pontos numa transação (`ON CONFLICT DO
NOTHING`) e só então marca o redb (mesmo com 0 flashes). No boot, `State::open`
hidrata o redb com as chaves recentes (~48h) de `frames` **e** `raios_arquivos`.
Chave do dedupe no redb: `(fonte, chave_origem)`; `fonte` fixa (`noaa-goes-19`).

### Lado consumidor: servidor gRPC (`catalogo`)

**Sem Kafka**: o consumidor **consulta** (não há push). Contrato em
`crates/catalogo/proto/catalogo.proto` (pacote `coletor.catalogo.v1`). RPCs:

- `UltimoFrame` / `ListarFrames` — metadado dos frames C13 + **URL pré-assinada**
  (GET S3) do `.pmtiles`; os bytes vão por HTTP range direto do bucket.
- `ListarRaios(fonte?, bbox?, de?, ate?, qualidade_max?, cursor?)` — pontos de raio
  **inline** (sem URL), do mais novo ao mais antigo, paginado por cursor sobre
  `tempo`. `qualidade_max` omitido ⇒ 0 (só flashes bons; filtro **na consulta**).
  Uso "tempo real": pollar com `de = último_visto` e animar pelos `tempo` (sub-segundo).

### Módulos

| Crate / módulo | Papel |
|----------------|-------|
| `comum::config` | Structs serde do TOML + `Config::load`/`validate` (`deny_unknown_fields`). |
| `comum::storage` | Clients `object_store`: origem anônima, destino AWS/local, `build_destination_signer`. |
| `comum::nodd` | Convenções de chave NODD + parser de timestamp do nome. Testes. |
| `comum::pipeline` | Loop genérico poll→dedupe→download + trait `Processor`; `ensure_downloaded`, `smoke_list_source`. |
| `comum::state` | Catálogo Postgres (SeaORM) + cache redb: `open`/`is_done`/`mark_done`/`mark_raios_done`/`run_migrations`/`connect`. |
| `comum::entity` | Entidade SeaORM de `frames`. |
| `comum::raio` | Entidades de `raios` e `raios_arquivos` + struct `Flash` (parse). |
| `comum::query` | Queries read-only: `ultimo_frame`, `listar_frames`, `listar_raios`. |
| `comum` (lib.rs) | Consts compartilhadas: `FONTE` (`noaa-goes-19`) e `BBOX`. |
| `coletor-c13::process` | Cadeia GDAL→PMTiles do C13; constantes `SCALE`/`OFFSET`/`TARGET_RES_M`. |
| `coletor-c13::main` | CLI (`check`/`run`/`backfill`) + `ProcessadorC13`. |
| `coletor-glm::glm` | Parse do LCFA (decode unsigned+escala, clip BBOX). Testes (inclui `parse_arquivo_real`). |
| `coletor-glm::main` | CLI (`check`/`run`/`backfill`) + `ProcessadorGlm`. |
| `catalogo::serve` | Servidor gRPC (tonic): impl `Catalogo`, mapeia `Model`→`Frame`/`Raio`, assina URL. Testes. |
| `catalogo::grpc` | Código gerado do `.proto` (`include_proto!`). |
| `catalogo::main` | CLI (`migrate`/`serve`). |

## Constantes geográficas / C13

- `comum::BBOX = [-100, -56, -20, 13]` (EPSG:4326; América do Sul + Atlântico,
  estendido a oeste até ~Cidade do México a pedido dos meteorologistas).
  **Compartilhado**: recorte do raster C13 e clip dos pontos GLM.
- Em `coletor-c13::process`: `SCALE`/`OFFSET` (Kelvin→°C) e `TARGET_RES_M = "2000"`
  (~2 km em 3857).

## Containers

Uma imagem por binário (`Containerfile.catalogo`, `Containerfile.coletor-c13`,
`Containerfile.coletor-glm`); `compose.yaml` com os 3 serviços. Bases: catalogo e
coletor-c13 em **Alpine/musl** (c13 sobre `osgeo/gdal:alpine-normal`), coletor-glm
em **Debian/glibc** (ver acima). Versões pinadas (GDAL/Debian/Alpine pela tag base,
pmtiles por ARG, crates pelo workspace). Migrar com `catalogo migrate` antes de
subir os coletores. Validado localmente com podman (build das 3 imagens + run
end-to-end: migrate, GLM→raios, C13→PMTiles, serve→ListarRaios).

## Notas

- Rust edition 2024. Workspace: deps pinadas em `[workspace.dependencies]`.
- `config*.toml`, `target/`, `data/`, `out-s3/`, `temp/`, `aws.env` são gitignored.
- O C13 reaproveita a cadeia do PoC `goes-nodd-poc`, trocando a cauda COG por PMTiles.
