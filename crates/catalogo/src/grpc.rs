//! Código gerado do `proto/catalogo.proto` (tonic + prost).
//!
//! O `build.rs` compila o `.proto`; aqui só incluímos o módulo gerado do
//! pacote `coletor.catalogo.v1`. A implementação do serviço vive em
//! [`crate::serve`].

tonic::include_proto!("coletor.catalogo.v1");
