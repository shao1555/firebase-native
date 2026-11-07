use std::path::PathBuf;

fn main() {
    // Use the vendored protoc so neither contributors nor consumers of
    // this crate need a system protoc installation.
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    std::env::set_var("PROTOC", protoc);

    let proto_root = PathBuf::from("proto");
    let protos = [
        "google/firestore/v1/aggregation_result.proto",
        "google/firestore/v1/bloom_filter.proto",
        "google/firestore/v1/common.proto",
        "google/firestore/v1/document.proto",
        "google/firestore/v1/explain_stats.proto",
        "google/firestore/v1/firestore.proto",
        "google/firestore/v1/pipeline.proto",
        "google/firestore/v1/query.proto",
        "google/firestore/v1/query_profile.proto",
        "google/firestore/v1/write.proto",
        "google/api/annotations.proto",
        "google/api/client.proto",
        "google/api/field_behavior.proto",
        "google/api/http.proto",
        "google/api/launch_stage.proto",
        "google/api/routing.proto",
        "google/rpc/status.proto",
        "google/type/latlng.proto",
    ];
    let files: Vec<PathBuf> = protos.iter().map(|p| proto_root.join(p)).collect();

    // Well-known types (google.protobuf.*) are mapped to prost-types by
    // prost-build automatically; registering an extern path for the
    // whole package would collide with those built-in mappings.
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&files, &[proto_root])
        .expect("failed to compile Firestore protos");

    for file in &files {
        println!("cargo:rerun-if-changed={}", file.display());
    }
}
