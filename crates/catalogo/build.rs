//! Compila o `.proto` do catálogo em código Rust (tonic + prost) em tempo de
//! build. O resultado é incluído via `tonic::include_proto!` em `src/grpc.rs`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/catalogo.proto");
    tonic_prost_build::compile_protos("proto/catalogo.proto")?;
    Ok(())
}
