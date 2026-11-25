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
    // BUILD_ID: the release build's id (`make wheel` sets CHIMAERA_BUILD_ID for
    // the peer and the extension alike), then a digest of the schemas compiled.
    let mut sorted = files.clone();
    sorted.sort();
    let mut h = std::hash::DefaultHasher::new();
    for f in &sorted {
        std::hash::Hash::hash(&(f.file_name(), std::fs::read(f)?), &mut h);
    }
    println!("cargo:rerun-if-env-changed=CHIMAERA_BUILD_ID");
    let release = std::env::var("CHIMAERA_BUILD_ID").unwrap_or_else(|_| "dev".into());
    println!("cargo:rustc-env=CHIMAERA_BUILD_ID={release}-{:016x}", std::hash::Hasher::finish(&h));
    tonic_prost_build::configure()
        .build_server(cfg!(feature = "grpc"))
        .build_client(cfg!(feature = "grpc"))
        .compile_protos(&files, &[root])?;
    Ok(())
}
