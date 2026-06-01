# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## O que é

`coletor-imagem-radar` é um binário Rust (tokio) que coleta produtos GOES-19 do **NOAA
NODD** (bucket público `noaa-goes19`, `us-east-1`, leitura anônima) para um S3
nosso (AWS ou filesystem local) e, no caminho, **processa** cada frame
ABI C13 (NetCDF) em **PMTiles** prontos para mapa. Comentários e docs em pt-BR.

## Comandos

```sh
cargo build                         # compila
cargo run -- migrate                # aplica as migrations do catálogo (schema imagens_satelite)
cargo run -- check --limit 5        # valida config + lista a origem (dry-run, sem escrever)
cargo run -- run --once --limit 1   # uma passada do pipeline (download→processa→upload→catálogo→delete)
cargo run -- run                    # loop contínuo (poll por produto)
cargo run -- backfill --hours 48    # popula retroativo: varre as últimas N horas (default 48) numa passada
cargo run -- serve                  # servidor gRPC de consulta ao catálogo (UltimoFrame/ListarFrames)
cargo test                          # testes (src/nodd.rs: chaves + parser; src/serve.rs: timestamp/cursor)
cargo test source_hour_prefix       # roda um teste específico por nome
```

- Config: `-c/--config` (default `config.toml`). Copie `config.example.toml`.
- Credenciais do **destino S3** vêm SÓ do ambiente: `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY` (`AWS_SESSION_TOKEN` opcional). Nunca no TOML.
- Credenciais do **Postgres** (catálogo) vêm da seção `[database]` do TOML
  (campos discretos; ver `config.example.toml`). `run`, `migrate` e `serve` exigem essa seção.
- Logs: `RUST_LOG=coletor_imagem_radar=debug` para verbosidade; `SYNC_LOG_FORMAT=json`
  para saída JSON.

## Dependências externas (não-Rust)

O processamento **shella binários** — precisam estar no `PATH`, senão `run`
falha em runtime (não em compile):
`gdal_calc.py`, `gdalwarp`, `gdaldem`, `gdal_translate`, `gdaladdo` (pacote GDAL)
e `pmtiles` (conversor MBTiles→PMTiles). `cargo build`/`cargo test` NÃO precisam
deles; só `cargo run -- run`.

Além disso, `run`/`migrate`/`serve` precisam de um **Postgres** acessível (catálogo,
schema `imagens_satelite`). `check`, `build` e `test` NÃO precisam de banco.

O `serve` também precisa das **credenciais AWS do destino** (mesmas do `run`) para
**pré-assinar** as URLs GET dos `.pmtiles`. ⚠️ A identidade IAM que assina precisa
de `s3:GetObject` no prefixo — o usuário de upload (`s3-satellite-uploader`, só
`PutObject`) gera URLs que voltam **403** no fetch. O `cargo build`/`compile` do
`.proto` é feito pelo `build.rs` via `tonic-prost-build` (sem dep externa).

## Arquitetura

Fluxo do pipeline (`src/pipeline.rs` → `src/process.rs`), por produto e por poll:

1. **Poll**: lista os prefixos da hora UTC corrente **e da anterior** (janela de
   overlap p/ chegadas tardias) na origem (`nodd::source_hour_prefix`, layout NODD
   `<Produto>/<AAAA>/<DDD>/<HH>/`, onde DDD é dia juliano). Filtra por canal via
   substring `"<canal>_G19"` (ex. `C13_G19`); produto sem `channel` não filtra.
   Pula o que o dedupe (`state::State::is_done`, redb) já marcou como processado.
2. **Download**: GET anônimo em stream → disco efêmero (`pipeline.work_dir`,
   default `data/`). Pula se o `.nc` já existe em disco.
3. **Processa** (`process::process` despacha por `product.name`): só
   `abi-l2-cmipf-c13` tem pipeline. Cadeia GDAL: calibração CMI→°C
   (`gdal_calc.py`) → reproj/recorte EPSG:3857 no BBOX (`gdalwarp`) → colormap
   NOAA (`gdaldem color-relief` + rampa `assets/c13_noaa.txt`) → MBTiles
   (`gdal_translate` + `gdaladdo`) → PMTiles (`pmtiles convert`).
4. **Upload**: PUT do `.pmtiles` no destino, sob a chave de
   `nodd::dest_pmtiles_key` (reaproveita `AAAA/DDD/HH` da origem, troca extensão).
5. **Catálogo**: `state::State::mark_done` grava 1 linha em `imagens_satelite.frames`
   (hypertable TimescaleDB particionada em `inicio`; insert idempotente
   `ON CONFLICT (fonte, chave_origem, inicio) DO NOTHING`) e marca o redb. Timestamps
   `inicio`/`fim` vêm de `nodd::parse_frame_times` (tokens `s`/`e` do nome).
6. **Delete-on-success**: só após upload **e** catálogo OK apaga o `.nc` e o `.pmtiles`
   local. Erro em qualquer ponto NÃO cataloga → retentado no próximo poll.

Dedupe é **persistente** (Fase 3): catálogo durável no **Postgres** (fonte de
verdade, **hypertable** TimescaleDB) + **redb** como cache quente local. No boot,
`State::open` hidrata o redb com as chaves da janela recente (~48h) do catálogo.
Chave do dedupe no redb: `(fonte, chave_origem)` (o `inicio` é determinístico a
partir da chave); `fonte` é fixa (`noaa-goes-19`) até entrar uma 2ª origem.

### Lado consumidor: servidor gRPC (`serve`)

Pós-MVP, **sem Kafka**: o consumidor **consulta** o catálogo via gRPC (não há
push). O servidor (`src/serve.rs`, tonic) é **só metadado** — devolve quais
frames existem e uma **URL pré-assinada** (GET S3) do `.pmtiles`; os bytes
trafegam por HTTP range request direto do bucket (PMTiles), nunca pelo gRPC.
Contrato em `proto/catalogo.proto` (pacote `coletor.catalogo.v1`), compilado pelo
`build.rs` (`tonic-prost-build`) e incluído em `src/grpc.rs` (`include_proto!`).
Duas RPCs unárias: `UltimoFrame(produto, canal?)` e `ListarFrames(...)` (janela
temporal, paginada por cursor sobre `inicio` — casa com o índice
`(produto, inicio DESC)`). Lê o catálogo via `src/query.rs` e assina com o
client concreto `AmazonS3` (`storage::build_destination_signer`).

Módulos:

| Módulo         | Papel |
|----------------|-------|
| `main.rs`      | CLI clap (`check`, `run`, `backfill`, `migrate`, `serve`); `check` faz o smoke-test de list. |
| `config.rs`    | Structs serde do TOML + `Config::load`/`validate`. `deny_unknown_fields`. `DatabaseConfig::url` monta a conn string. `GrpcConfig` (`[grpc]`: listen/url_ttl_secs/limite_pagina). |
| `storage.rs`   | Constrói clients `object_store`: origem anônima (`skip_signature`), destino AWS S3 (`from_env`) ou `LocalFileSystem` (`local_path`). `build_destination_signer` devolve o `AmazonS3` concreto p/ pré-assinar (`None` em modo local). |
| `nodd.rs`      | Convenções de chave NODD (prefixo da hora, chave de destino) + parser de timestamp do nome. Tem os testes. |
| `pipeline.rs`  | Loop poll→processa por produto; janela de overlap; usa o `State` p/ dedupe e catálogo. `FONTE` fixa (`noaa-goes-19`). |
| `process.rs`   | Cadeia GDAL→PMTiles do C13; constantes de calibração/BBOX/resolução. |
| `state.rs`     | Estado híbrido: catálogo Postgres (SeaORM) + cache redb. `open`/`is_done`/`mark_done`/`run_migrations` (DDL idempotente); `connect` (conexão com `search_path`, compartilhada com o `serve`). |
| `query.rs`     | Queries read-only do catálogo p/ o gRPC: `ultimo_frame`, `listar_frames` (filtro + cursor). |
| `serve.rs`     | Servidor gRPC (tonic): impl do serviço `Catalogo`, mapeamento `Model`→`Frame`, assinatura de URL e conversão de timestamp/cursor. Tem testes. |
| `grpc.rs`      | Código gerado do `.proto` (`include_proto!`). |
| `entity.rs`    | Entidade SeaORM da tabela `frames` (schema vem do `search_path`, configurável). |
| `logging.rs`   | Init do `tracing` (texto ou JSON). |

## Constantes do C13 (em `src/process.rs`)

Calibração `SCALE`/`OFFSET` (Kelvin→°C), `BBOX = [-100, -56, -20, 13]`
(EPSG:4326; América do Sul + Atlântico, estendido a oeste até ~Cidade do México
a pedido dos meteorologistas) e `TARGET_RES_M = "2000"` (~2 km em 3857). Mudar
a cobertura/resolução = editar essas constantes.

## Notas

- Rust edition 2024.
- `config.toml`, `target/`, `data/`, `out-s3/`, `temp/` são gitignored.
- O pipeline reaproveita a cadeia do PoC `goes-nodd-poc`, trocando a cauda COG
  por PMTiles.
