//! Connection-lifetime regression from the adversarial review: dropping the
//! last client channel must actually CLOSE the connection. Before the fix the
//! demux task held a strong reference to the connection, so `ClientConn::drop`
//! never ran, `io.close` was never called, and the socket + segment fd + three
//! background tasks stayed pinned forever — a per-connection leak that repeated
//! channel churn turns into descriptor/task exhaustion.
//!
//! We can't observe the server-side EOF directly from a test, so we assert the
//! observable consequence: churning many connect → one-RPC → drop cycles does
//! not grow the process's open file-descriptor count. (Client and server share
//! this process, so a leak shows up as several fds per dropped channel.)

#![cfg(unix)]

use std::time::Duration;

use tonic::{Request, Response, Status};

#[allow(unreachable_pub)]
mod pb {
    tonic::include_proto!("shmsc.test");
}
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};
use pb::{EchoRequest, EchoResponse};

fn sock_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("shmsc-life-{}-{}.sock", name, std::process::id()))
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

type RespStream =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<EchoResponse, Status>> + Send>>;

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

fn open_fds() -> usize {
    #[cfg(target_os = "linux")]
    let dir = "/proc/self/fd";
    #[cfg(not(target_os = "linux"))]
    let dir = "/dev/fd";
    std::fs::read_dir(dir).map(|d| d.count()).unwrap_or(0)
}

async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
}

async fn one_rpc(path: &std::path::Path) {
    let ch = tonic_shmsc::connect(path).await.expect("connect");
    let mut c = EchoClient::new(ch);
    let r = c
        .unary_echo(Request::new(EchoRequest {
            payload: b"x".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("rpc");
    assert_eq!(r.into_inner().payload, b"x");
    // `c` (and its channel) drops at the end of scope here.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_channel_closes_connection_no_fd_leak() {
    let path = sock_path("drop");
    let routes = tonic::service::Routes::new(EchoServer::new(EchoSvc)).prepare();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let p = path.clone();
    let server = tokio::spawn(async move {
        tonic_shmsc::serve(p, routes, async {
            let _ = stop_rx.await;
        })
        .await
    });
    wait_for_socket(&path).await;

    // Warm up so one-time fds (registry, lazy runtime bits) are already open.
    for _ in 0..3 {
        one_rpc(&path).await;
    }
    settle().await;
    let baseline = open_fds();

    // Churn: each connection is fully done with after its single RPC, so
    // dropping the channel must tear the whole connection down.
    const CHURN: usize = 40;
    for _ in 0..CHURN {
        one_rpc(&path).await;
    }

    // Allow async teardown to settle, then assert fds returned near baseline.
    let mut fds = open_fds();
    for _ in 0..100 {
        if fds <= baseline + 8 {
            break;
        }
        settle().await;
        fds = open_fds();
    }
    assert!(
        fds <= baseline + 8,
        "open fds grew {baseline} -> {fds} after {CHURN} connect/drop cycles: \
         dropped channels are leaking connections (F2)"
    );

    let _ = stop_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
}
