//! Compila o mesmo `.proto` do catálogo (cliente gRPC). Reaproveita o contrato
//! em `crates/catalogo/proto/catalogo.proto` — não duplica o `.proto`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../catalogo/proto/catalogo.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_prost_build::compile_protos(proto)?;
    Ok(())
}
