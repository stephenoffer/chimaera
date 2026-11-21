fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let files: Vec<_> = std::fs::read_dir(root.join("chimaera/v1"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "proto"))
        .collect();
    // The directory too, so an added .proto file triggers a rebuild.
    println!("cargo:rerun-if-changed={}", root.join("chimaera/v1").display());
    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
    }
    tonic_prost_build::configure()
        .build_server(cfg!(feature = "grpc"))
        .build_client(cfg!(feature = "grpc"))
        .compile_protos(&files, &[root])?;
    Ok(())
}
