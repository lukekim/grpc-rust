//! The self-containment guard, the Rust analog of the Go plugin's
//! `TestNoInternalImports`.
//!
//! In Go the proof that the plugin uses only the exported API is the absence
//! of `google.golang.org/grpc/internal/*` imports, enforced by a test that
//! parses every source file. In Rust, crate privacy makes the equivalent
//! violation a compile error — `tonic-shmsc` cannot name tonic's private
//! items at all. What privacy cannot prove is that the engine is genuinely
//! self-contained rather than delegating to the stock HTTP/2 stack, so this
//! test pins the dependency graph: no direct dependency on hyper, h2,
//! hyper-util, tower, or axum — the engine owns framing and flow control
//! itself.

#[test]
fn no_http2_stack_dependencies() {
    let meta = cargo_metadata::MetadataCommand::new()
        .manifest_path(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .exec()
        .expect("cargo metadata");
    let me = meta
        .packages
        .iter()
        .find(|p| p.name.as_str() == "tonic-shmsc")
        .expect("tonic-shmsc in metadata");

    const FORBIDDEN: &[&str] = &[
        "hyper",
        "h2",
        "hyper-util",
        "hyper-timeout",
        "tower",
        "axum",
    ];
    const ALLOWED: &[&str] = &[
        "tonic",
        "bytes",
        "http",
        "http-body",
        "libc", // unix platform layer
        "tokio",
        "tower-service",
        "tracing",
        "windows-sys", // windows platform layer (sections + pipe peer info)
    ];

    for dep in &me.dependencies {
        if dep.kind != cargo_metadata::DependencyKind::Normal {
            continue; // dev/build deps may use anything (tests need a real h2 peer)
        }
        let name = dep.name.as_str();
        assert!(
            !FORBIDDEN.contains(&name),
            "tonic-shmsc must not depend on {name}: the engine owns framing/flow control \
             and must reach gRPC only through tonic's public experimental API"
        );
        assert!(
            ALLOWED.contains(&name),
            "unexpected new dependency {name}; extend the guard deliberately if it is justified"
        );
    }
}
