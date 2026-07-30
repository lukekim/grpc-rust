//! Hostile-peer conformance: a peer that completes the handshake and then
//! writes garbage must make the SERVER fail that one connection cleanly —
//! never panic the process, never corrupt other connections. Tests the
//! trust-model boundary the DESIGN claims.
//!
//! Requires the `_test-support` feature (the raw-peer seam):
//!   cargo test -p tonic-shmsc --features _test-support --test hostile

#![cfg(all(unix, feature = "_test-support"))]

use std::pin::Pin;
use std::time::Duration;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tonic_shmsc::test_support::RawPeer;

#[allow(unreachable_pub)]
mod pb {
    tonic::include_proto!("shmsc.test");
}
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};
use pb::{EchoRequest, EchoResponse};

fn sock_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "shmsc-hostile-{}-{}.sock",
        name,
        std::process::id()
    ))
}

async fn wait_for_socket(path: &std::path::Path) {
    for _ in 0..400 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("socket {} never appeared", path.display());
}

#[derive(Default)]
struct EchoSvc;

type RespStream = Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send>>;

#[tonic::async_trait]
impl Echo for EchoSvc {
    async fn unary_echo(
        &self,
        req: Request<EchoRequest>,
    ) -> Result<Response<EchoResponse>, Status> {
        Ok(Response::new(EchoResponse {
            payload: req.into_inner().payload,
        }))
    }
    type ServerStreamingEchoStream = RespStream;
    async fn server_streaming_echo(
        &self,
        _req: Request<EchoRequest>,
    ) -> Result<Response<Self::ServerStreamingEchoStream>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn client_streaming_echo(
        &self,
        _req: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<Response<EchoResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    type BidiEchoStream = RespStream;
    async fn bidi_echo(
        &self,
        _req: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<Response<Self::BidiEchoStream>, Status> {
        Err(Status::unimplemented("n/a"))
    }
}

fn spawn_server(path: &std::path::Path) -> tokio::sync::oneshot::Sender<()> {
    let routes = tonic::service::Routes::new(EchoServer::new(EchoSvc)).prepare();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let p = path.to_owned();
    tokio::spawn(async move {
        tonic_shmsc::serve(p, routes, async {
            let _ = rx.await;
        })
        .await
        .expect("serve");
    });
    tx
}

/// Every flavor of garbage a hostile peer can push must make the server close
/// that connection — and a legitimate client on a fresh connection must keep
/// working afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn garbage_frames_fail_connection_not_process() {
    let path = sock_path("garbage");
    let _stop = spawn_server(&path);
    wait_for_socket(&path).await;

    // Each hostile scenario gets its own connection.
    let scenarios: Vec<(&str, Box<dyn Fn(&mut RawPeer) + Send>)> = vec![
        (
            "unknown-frame-type",
            Box::new(|p: &mut RawPeer| {
                p.send_frame(0xEE, 0, 1, b"whatever");
            }),
        ),
        (
            "oversized-length",
            Box::new(|p: &mut RawPeer| {
                // Header claiming a 4 GiB payload (past the frame cap).
                let mut hdr = [0u8; 12];
                hdr[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
                hdr[4] = 2; // Data
                hdr[8..12].copy_from_slice(&1u32.to_le_bytes());
                p.send_raw(&hdr);
            }),
        ),
        (
            "even-stream-id",
            Box::new(|p: &mut RawPeer| {
                // Client-initiated ids must be odd; an even id is a protocol
                // violation.
                p.send_open(2, "/shmsc.test.Echo/UnaryEcho");
            }),
        ),
        (
            "nonmonotonic-id",
            Box::new(|p: &mut RawPeer| {
                p.send_open(5, "/shmsc.test.Echo/UnaryEcho");
                p.send_open(3, "/shmsc.test.Echo/UnaryEcho"); // goes backwards
            }),
        ),
        (
            "headers-missing-path",
            Box::new(|p: &mut RawPeer| {
                // A valid frame carrying a header block with no :path.
                p.send_frame(1 /* Headers */, 0, 1, &[0u8, 0u8]); // count=0
            }),
        ),
        (
            "pure-random-bytes",
            Box::new(|p: &mut RawPeer| {
                let mut state = 0x9e3779b97f4a7c15u64;
                let mut junk = vec![0u8; 4096];
                for b in junk.iter_mut() {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    *b = (state >> 33) as u8;
                }
                p.send_raw(&junk);
            }),
        ),
    ];

    for (name, hostile) in scenarios {
        let mut peer = RawPeer::connect(&path).await.expect("hostile connect");
        hostile(&mut peer);
        assert!(
            peer.wait_server_closed(Duration::from_secs(5)).await,
            "scenario {name:?}: server must close the connection, not hang or crash"
        );
    }

    // The server survived every hostile connection: a real client still works.
    let channel = tonic_shmsc::connect(&path).await.expect("legit connect");
    let mut client = EchoClient::new(channel);
    let resp = client
        .unary_echo(Request::new(EchoRequest {
            payload: b"still-alive".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("server must still serve after hostile peers");
    assert_eq!(resp.into_inner().payload, b"still-alive");
}

/// A hostile peer must not be able to disturb a CONCURRENT legitimate client:
/// while one connection spews garbage, an honest client's RPCs keep
/// succeeding on their own connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hostile_peer_does_not_disturb_concurrent_client() {
    let path = sock_path("concurrent");
    let _stop = spawn_server(&path);
    wait_for_socket(&path).await;

    let channel = tonic_shmsc::connect(&path).await.expect("legit connect");
    let mut client = EchoClient::new(channel);

    // Warm up the honest connection.
    assert_eq!(
        client
            .unary_echo(Request::new(EchoRequest {
                payload: b"pre".to_vec(),
                ..Default::default()
            }))
            .await
            .expect("pre rpc")
            .into_inner()
            .payload,
        b"pre"
    );

    // A hostile peer floods garbage on its own connection.
    let mut peer = RawPeer::connect(&path).await.expect("hostile connect");
    for _ in 0..50 {
        peer.send_frame(0xEE, 0xFF, 999, b"garbage-garbage-garbage");
    }

    // The honest client is unaffected.
    for i in 0..20u32 {
        let payload = i.to_le_bytes().to_vec();
        let resp = client
            .unary_echo(Request::new(EchoRequest {
                payload: payload.clone(),
                ..Default::default()
            }))
            .await
            .expect("honest rpc during hostile flood");
        assert_eq!(resp.into_inner().payload, payload);
    }
    assert!(peer.wait_server_closed(Duration::from_secs(5)).await);
}
