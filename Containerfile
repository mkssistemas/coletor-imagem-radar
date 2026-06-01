# Imagem do coletor-imagem-radar (Alpine/musl, multi-stage).
#
# O binário Rust é compilado DENTRO da imagem, mirando musl. Motivo: a base de
# runtime é Alpine (musl) e um binário glibc não roda em musl. rust:alpine já é
# musl-nativo, então `cargo build --release` produz um binário estático-musl
# direto — e some o acoplamento de versão de glibc (não importa a libc da base).
#
# Runtime osgeo/gdal alpine-normal (~282 MB) traz tudo que o pipeline shella:
# netCDF + HDF5 (ler o .nc), gdalwarp/gdaldem/gdal_translate/gdaladdo e o
# gdal_calc.py (Python). Falta só o pmtiles (binário Go), instalado abaixo.
# Tag pinada no GDAL testado localmente (3.12.4); NÃO usar -latest, p/ não mudar
# de versão de GDAL sob os pés num bump futuro.

# ---- Stage 1: builder musl ----
FROM rust:alpine AS builder
# ring (cripto do rustls) e libsqlite3-sys compilam C → precisam de toolchain C.
# protoc: o build.rs (tonic-prost-build) compila proto/catalogo.proto em build.
RUN apk add --no-cache build-base protoc protobuf-dev
WORKDIR /src
COPY . .
# Cache mounts (Podman/buildah): registry+git de crates e o dir target/ persistem
# entre builds locais → recompila só o que mudou. ATENÇÃO: target/ vira mount e
# NÃO é commitado na camada, então o binário precisa ser COPIADO pra fora do
# mount (/coletor) aqui, senão o COPY --from do stage 2 não acha nada.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release \
 && cp /src/target/release/coletor-imagem-radar /coletor
# Em rust:alpine o host triple já é x86_64-unknown-linux-musl → binário estático.

# ---- Stage 2: runtime ----
FROM ghcr.io/osgeo/gdal:alpine-normal-3.12.4

# pmtiles: binário Go estático (CGO off) → roda em musl sem problema.
ARG PMTILES_VERSION=1.30.2
ADD https://github.com/protomaps/go-pmtiles/releases/download/v${PMTILES_VERSION}/go-pmtiles_${PMTILES_VERSION}_Linux_x86_64.tar.gz /tmp/pmtiles.tar.gz
RUN tar -xzf /tmp/pmtiles.tar.gz -C /usr/local/bin pmtiles \
 && chmod +x /usr/local/bin/pmtiles \
 && rm /tmp/pmtiles.tar.gz

WORKDIR /app
COPY --from=builder /coletor /usr/local/bin/coletor-imagem-radar
# Rampa de cor do C13. process.rs lê assets/c13_noaa.txt relativo ao cwd (/app).
COPY assets/ /app/assets/
# Diretório de trabalho efêmero do pipeline (pipeline.work_dir, default data/).
RUN mkdir -p /app/data

# Config e credenciais entram em runtime, não na imagem:
#   - config.toml: montado em /app/config.toml — contém [database] do Postgres.
#   - S3 do destino: via env AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (+ TOKEN).
ENTRYPOINT ["coletor-imagem-radar"]
CMD ["run"]
