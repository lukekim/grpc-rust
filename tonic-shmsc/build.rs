//! Generates the test service used by the integration tests (tests/).

fn main() {
    tonic_prost_build::compile_protos("proto/test.proto")
        .unwrap_or_else(|e| panic!("failed to compile test proto: {e}"));
}
