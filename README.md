# Coletor de Imagem de Radar

> Cargo workspace. Binários: `coletor-c13`, `coletor-glm`, `catalogo`.

Coleta produtos GOES-19 do **NOAA NODD** (`noaa-goes19`, S3 público, `us-east-1`, leitura
anônima). São **três binários** com responsabilidades (e dependências) separadas:

- **`coletor-c13`** — **processa** cada frame ABI C13 (NetCDF) em **PMTiles** e entrega no
  **nosso S3** (AWS); cataloga em `frames`. O bruto (`.nc`) é efêmero (delete-on-success).
- **`coletor-glm`** — parseia GLM-L2-LCFA (raios) em **pontos** (flashes) e grava no
  **Postgres** (`raios`). Sem S3, sem tiles.
- **`catalogo`** — servidor gRPC de consulta (sem Kafka, sem push) + `migrate`.

Projeto separado da `qualle-control-api`. Stack: **Rust + tokio + `object_store`** (+ GDAL no
C13, + libnetcdf no GLM). Plano completo no Obsidian: `Projetos/Sincronizador GOES-19 NODD/Plano`.

## Status

- **C13 end-to-end** ✅ — poll → download → GDAL→PMTiles → upload → catálogo → delete.
- **Catálogo Postgres + dedupe persistente** ✅ — hypertables TimescaleDB (via SeaORM) +
  cache `redb`, hidratado no boot com a janela recente (~48h).
- **GLM (raios)** ✅ — pontos por flash em `raios`; livro-razão `raios_arquivos` p/ dedupe
  durável (cobre arquivos sem flash no BBOX).
- **Consumidor gRPC** ✅ — `UltimoFrame`/`ListarFrames` (frames + URL pré-assinada do
  `.pmtiles`) e `ListarRaios` (pontos inline; bbox/janela/qualidade; habilita "piscar" no cliente).
- **Empacotamento OCI** ✅ — 3 imagens (`Containerfile.*`) + `compose.yaml`; CI no GitHub Actions.

## Uso

```sh
cp config.example.toml config.coletor-c13.toml   # um config por binário (só o seu produto)
cargo run -p catalogo    -- migrate              # DDL: frames + raios + raios_arquivos
cargo run -p coletor-c13 -- check                # valida config + lista a origem (dry-run)
cargo run -p coletor-c13 -- run --once --limit 1 # 1 passada C13
cargo run -p coletor-glm -- run --once --limit 1 # 1 passada GLM
cargo run -p catalogo    -- serve                # gRPC (UltimoFrame/ListarFrames/ListarRaios)
```

Credenciais do **destino S3** vêm SÓ do ambiente (`coletor-c13` p/ upload, `catalogo` p/ assinar):

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
# AWS_SESSION_TOKEN opcional
```

Credenciais do **Postgres** (catálogo) vêm da seção `[database]` do TOML. Logging:
`RUST_LOG=coletor_glm=debug,comum=debug`; `SYNC_LOG_FORMAT=json` para JSON.

### Dependências externas

- `coletor-c13`: binários GDAL (`gdal_calc.py`, `gdalwarp`, `gdaldem`, `gdal_translate`,
  `gdaladdo`) + `pmtiles` no `PATH`.
- `coletor-glm`: **libnetcdf** (dynlib; build precisa de `netcdf-dev`).
- `catalogo`: `protoc` no build (compila o `.proto`); credenciais AWS p/ pré-assinar
  (a identidade precisa de `s3:GetObject` no prefixo, senão o GET volta **403**).
- Quem toca o banco precisa de um **Postgres/TimescaleDB** acessível.

## Layout (workspace)

| Crate | Papel |
|-------|-------|
| `comum` | Lib compartilhada: config, storage, nodd, `pipeline` (loop genérico + trait `Processor`), `state` (catálogo + dedupe), entidades (`entity`/`raio`), `query`. |
| `coletor-c13` | Cauda raster (GDAL→PMTiles→S3→`frames`). |
| `coletor-glm` | Cauda de pontos (parse LCFA→`raios`). |
| `catalogo` | Servidor gRPC + `migrate` + `.proto`. |
