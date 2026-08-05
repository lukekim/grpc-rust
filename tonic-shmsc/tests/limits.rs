//! Resource-limit and cancellation regressions from the adversarial review:
//!   - `Server::max_concurrent_streams` is actually enforced (excess opens are
//!     refused before any per-stream allocation), and
//!   - cancelling an RPC before the response head arrives aborts the server
//!     handler (no orphaned work), via the client's pending-stream guard.

#![cfg(unix)]

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Notify, oneshot};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::transport::Endpoint;
use tonic::{Request, Response, Status};

#[allow(unreachable_pub)]
mod pb {
    tonic::include_proto!("shmsc.test");
}
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};
use pb::{EchoRequest, EchoResponse};

const NAME: &str = tonic_shmsc::NAME;

fn sock_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("shmsc-limits-{}-{}.sock", name, std::process::id()))
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

fn ensure_registered() {
    static REG: std::sync::Once = std::sync::Once::new();
    REG.call_once(tonic_shmsc::register);
}

/// Signals (via `Drop`) if the handler future it lives in is aborted.
struct AbortSignal(Arc<Notify>);
impl Drop for AbortSignal {
    fn drop(&mut self) {
        self.0.notify_waiters();
    }
}

#[derive(Clone)]
struct LimitsSvc {
    /// Fired when the blocking unary handler begins (before any response head).
    started: Arc<Notify>,
    /// Fired when the blocking unary handler future is dropped (aborted).
    aborted: Arc<Notify>,
}

type RespStream = Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send>>;

#[tonic::async_trait]
impl Echo for LimitsSvc {
    async fn unary_echo(
        &self,
        req: Request<EchoRequest>,
    ) -> Result<Response<EchoResponse>, Status> {
        let r = req.into_inner();
        if r.payload == b"block" {
            // Announce that we started, then block forever holding a guard that
            // fires if this future is dropped (i.e. the stream was cancelled).
            let _guard = AbortSignal(self.aborted.clone());
            self.started.notify_waiters();
            std::future::pending::<()>().await;
            unreachable!();
        }
        Ok(Response::new(EchoResponse { payload: r.payload }))
    }

    type ServerStreamingEchoStream = RespStream;

    async fn server_streaming_echo(
        &self,
        req: Request<EchoRequest>,
    ) -> Result<Response<Self::ServerStreamingEchoStream>, Status> {
        // An effectively unbounded stream: it stays "active" server-side until
        // the client drops it, which is what keeps a stream slot occupied.
        let payload = req.into_inner().payload;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                if tx
                    .send(Ok(EchoResponse {
                        payload: payload.clone(),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
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

/// Spawns a server via the tonic `Server` builder (so `max_concurrent_streams`
/// flows into the transport) and returns its stop handle.
fn spawn(path: &std::path::Path, max_streams: Option<u32>, svc: LimitsSvc) -> oneshot::Sender<()> {
    ensure_registered();
    let ps = path.to_str().unwrap().to_owned();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .max_concurrent_streams(max_streams)
            .add_service(EchoServer::new(svc))
            .serve_with_transport_type_shutdown(NAME, &ps, async {
                let _ = stop_rx.await;
            })
            .await
            .expect("serve");
    });
    stop_tx
}

async fn connect(path: &std::path::Path) -> tonic::transport::Channel {
    Endpoint::from_shared(tonic_shmsc::uri(path.to_str().unwrap()))
        .expect("uri")
        .transport_type(NAME)
        .connect()
        .await
        .expect("connect")
}

/// With the ceiling at 2 and two streams already active on the connection, a
/// third open must be refused (RST Refused → UNAVAILABLE) — before, the option
/// was silently dropped and a client could open unbounded streams.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_concurrent_streams_is_enforced() {
    let path = sock_path("maxstreams");
    let svc = LimitsSvc {
        started: Arc::new(Notify::new()),
        aborted: Arc::new(Notify::new()),
    };
    let _stop = spawn(&path, Some(2), svc);
    wait_for_socket(&path).await;
    let channel = connect(&path).await;

    // Open two long-lived server-streaming RPCs and pull one item from each so
    // both are established (active) server-side.
    async fn open(ch: tonic::transport::Channel) -> tonic::Streaming<EchoResponse> {
        let mut c = EchoClient::new(ch);
        let mut s = c
            .server_streaming_echo(Request::new(EchoRequest {
                payload: b"x".to_vec(),
                ..Default::default()
            }))
            .await
            .expect("open stream")
            .into_inner();
        assert!(
            matches!(s.next().await, Some(Ok(_))),
            "stream must yield its first item"
        );
        s
    }
    let s1 = open(channel.clone()).await;
    let _s2 = open(channel.clone()).await;

    // The third concurrent open must be refused by the ceiling.
    let mut c3 = EchoClient::new(channel.clone());
    let err = c3
        .unary_echo(Request::new(EchoRequest {
            payload: b"nope".to_vec(),
            ..Default::default()
        }))
        .await
        .expect_err("third concurrent stream must be refused");
    assert_eq!(
        err.code(),
        tonic::Code::Unavailable,
        "a refused stream surfaces as UNAVAILABLE, got {err:?}"
    );

    // Freeing an active stream frees a slot: a new RPC then succeeds.
    drop(s1);
    let mut ok = false;
    for _ in 0..100 {
        let mut c = EchoClient::new(channel.clone());
        if c.unary_echo(Request::new(EchoRequest {
            payload: b"ok".to_vec(),
            ..Default::default()
        }))
        .await
        .is_ok()
        {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ok, "a slot must free up after an active stream is dropped");
    drop(_s2);
}

/// Cancelling a call before the response head arrives must abort the server
/// handler (not leave it running as orphaned work). The client installs a
/// pending-stream guard that, on drop, sends an ordered RST the server acts on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_before_headers_aborts_server_handler() {
    let path = sock_path("cancel");
    let started = Arc::new(Notify::new());
    let aborted = Arc::new(Notify::new());
    let _stop = spawn(
        &path,
        None,
        LimitsSvc {
            started: started.clone(),
            aborted: aborted.clone(),
        },
    );
    wait_for_socket(&path).await;
    let channel = connect(&path).await;
    let mut client = EchoClient::new(channel);

    // Register both waiters BEFORE the RPC so no notify is missed.
    let started_fut = started.notified();
    let aborted_fut = aborted.notified();
    tokio::pin!(started_fut, aborted_fut);
    started_fut.as_mut().enable();
    aborted_fut.as_mut().enable();

    // Fire the blocking RPC on a task we can cancel. The handler never sends a
    // response head, so the call is parked awaiting it.
    let call = tokio::spawn(async move {
        let _ = client
            .unary_echo(Request::new(EchoRequest {
                payload: b"block".to_vec(),
                ..Default::default()
            }))
            .await;
    });

    tokio::time::timeout(Duration::from_secs(5), started_fut)
        .await
        .expect("server handler must start");

    // Cancel before any header arrives: dropping the call future must trigger
    // the pending-stream guard → ordered RST → the server aborts the handler.
    call.abort();

    tokio::time::timeout(Duration::from_secs(5), aborted_fut)
        .await
        .expect("server handler must be aborted after a pre-header cancel");
}
