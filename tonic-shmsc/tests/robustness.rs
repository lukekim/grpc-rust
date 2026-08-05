//! Robustness regressions: flow-control pathology, the stream-open race the
//! comparison harness first exposed, reconnect through the registry, and
//! graceful drain with an in-flight stream.

#![cfg(unix)]

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::transport::Endpoint;
use tonic::{Request, Response, Status, Streaming};

#[allow(unreachable_pub)]
mod pb {
    tonic::include_proto!("shmsc.test");
}
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};
use pb::{EchoRequest, EchoResponse};

fn sock_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("shmsc-robust-{}-{}.sock", name, std::process::id()))
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
struct EchoSvc {
    started: Arc<AtomicU32>,
}

type RespStream = Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send>>;

#[tonic::async_trait]
impl Echo for EchoSvc {
    async fn unary_echo(
        &self,
        req: Request<EchoRequest>,
    ) -> Result<Response<EchoResponse>, Status> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let r = req.into_inner();
        if r.payload == b"slow" {
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        Ok(Response::new(EchoResponse { payload: r.payload }))
    }

    type ServerStreamingEchoStream = RespStream;

    async fn server_streaming_echo(
        &self,
        req: Request<EchoRequest>,
    ) -> Result<Response<Self::ServerStreamingEchoStream>, Status> {
        let r = req.into_inner();
        let n = r.repeat.max(1);
        let payload = r.payload;
        let stream = tokio_stream::iter((0..n).map(move |_| {
            Ok(EchoResponse {
                payload: payload.clone(),
            })
        }));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn client_streaming_echo(
        &self,
        req: Request<Streaming<EchoRequest>>,
    ) -> Result<Response<EchoResponse>, Status> {
        let mut stream = req.into_inner();
        let mut all = Vec::new();
        while let Some(m) = stream.next().await {
            all.extend_from_slice(&m?.payload);
        }
        Ok(Response::new(EchoResponse { payload: all }))
    }

    type BidiEchoStream = RespStream;

    async fn bidi_echo(
        &self,
        req: Request<Streaming<EchoRequest>>,
    ) -> Result<Response<Self::BidiEchoStream>, Status> {
        let mut stream = req.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(m) = stream.next().await {
                let out = m.map(|m| EchoResponse { payload: m.payload });
                if tx.send(out).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

const NAME: &str = tonic_shmsc::NAME;

/// Serves via the registry path with a specific config, returning a stop
/// handle and the server's started-RPC counter.
fn spawn_registry_server(
    path: &std::path::Path,
    cfg: tonic_shmsc::ShmscConfig,
) -> (tokio::sync::oneshot::Sender<()>, Arc<AtomicU32>) {
    static REG: std::sync::Once = std::sync::Once::new();
    let path_str = path.to_str().unwrap().to_owned();
    let started = Arc::new(AtomicU32::new(0));
    let svc = EchoServer::new(EchoSvc {
        started: started.clone(),
    })
    .max_decoding_message_size(64 << 20)
    .max_encoding_message_size(64 << 20);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    REG.call_once(|| {
        tonic::transport::experimental::server::register(
            NAME,
            tonic_shmsc::server_builder_with_config(cfg.clone()),
        );
        tonic::transport::experimental::client::register(
            NAME,
            tonic_shmsc::client_builder_with_config(cfg),
        );
    });
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_transport_type_shutdown(NAME, &path_str, async {
                let _ = stop_rx.await;
            })
            .await
            .expect("serve");
    });
    (stop_tx, started)
}

/// A message far larger than a deliberately tiny window AND ring must still
/// complete: this is the Go engine's documented WINDOW_UPDATE deadlock, and
/// the regression test for our `wu_threshold` floor. With a 4 KiB ring and
/// 4 KiB window, a 2 MiB echo forces hundreds of chunk / park / window-update
/// cycles in both directions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tiny_window_and_ring_large_message() {
    let path = sock_path("tinywin");
    let cfg = tonic_shmsc::ShmscConfig {
        ring_size: 4096,
        stream_window: 4096,
        max_frame: 4096,
        spin_us: 0,
    };
    let (_stop, _started) = spawn_registry_server(&path, cfg);
    wait_for_socket(&path).await;

    let channel = Endpoint::from_shared(tonic_shmsc::uri(path.to_str().unwrap()))
        .expect("uri")
        .transport_type(NAME)
        .connect()
        .await
        .expect("connect");
    let mut client = EchoClient::new(channel)
        .max_decoding_message_size(64 << 20)
        .max_encoding_message_size(64 << 20);

    let payload: Vec<u8> = (0..2 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        client.unary_echo(Request::new(EchoRequest {
            payload: payload.clone(),
            ..Default::default()
        })),
    )
    .await
    .expect("must not deadlock under a tiny window")
    .expect("rpc");
    assert_eq!(resp.into_inner().payload, payload);
}

/// Many streams opened SIMULTANEOUSLY on one channel: their client stream ids
/// must reach the server monotonically or the server (correctly) kills the
/// connection. Regression for the race between `next_id` and the HEADERS
/// enqueue that the comparison harness first exposed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_opens_on_one_channel() {
    let path = sock_path("openrace");
    let _stop = {
        let routes = tonic::service::Routes::new(EchoServer::new(EchoSvc::default())).prepare();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let p = path.clone();
        tokio::spawn(async move {
            tonic_shmsc::serve(p, routes, async {
                let _ = rx.await;
            })
            .await
            .expect("serve");
        });
        tx
    };
    wait_for_socket(&path).await;
    let channel = tonic_shmsc::connect(&path).await.expect("connect");

    // Release a burst of opens at once through a barrier so the id-assignment
    // and enqueue genuinely interleave.
    let barrier = Arc::new(tokio::sync::Barrier::new(200));
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..200u32 {
        let mut client = EchoClient::new(channel.clone());
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            let payload = i.to_le_bytes().to_vec();
            let resp = client
                .unary_echo(Request::new(EchoRequest {
                    payload: payload.clone(),
                    ..Default::default()
                }))
                .await
                .unwrap_or_else(|e| panic!("open {i} failed: {e}"));
            assert_eq!(resp.into_inner().payload, payload);
        });
    }
    while let Some(r) = tasks.join_next().await {
        r.expect("task");
    }
}

/// A lazily-connected endpoint must transparently reconnect through the
/// registered builder after the server restarts — the tonic `Reconnect`
/// machinery driving a custom transport, a path no other test exercises.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_after_server_restart() {
    let path = sock_path("reconnect");
    let cfg = tonic_shmsc::ShmscConfig::default();

    let (stop1, _s1) = spawn_registry_server(&path, cfg.clone());
    wait_for_socket(&path).await;

    let channel = Endpoint::from_shared(tonic_shmsc::uri(path.to_str().unwrap()))
        .expect("uri")
        .transport_type(NAME)
        .connect_lazy();
    let mut client = EchoClient::new(channel.clone());
    let r1 = client
        .unary_echo(Request::new(EchoRequest {
            payload: b"first".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("first rpc");
    assert_eq!(r1.into_inner().payload, b"first");

    // Drop server 1 and stand up server 2 on the same path.
    let _ = stop1.send(());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (_stop2, _s2) = spawn_registry_server(&path, cfg);
    wait_for_socket(&path).await;

    // The same channel must recover: Reconnect asks the builder for a fresh
    // transport. Allow a few attempts for the reconnect to land.
    let mut ok = false;
    for _ in 0..50 {
        match client
            .unary_echo(Request::new(EchoRequest {
                payload: b"second".to_vec(),
                ..Default::default()
            }))
            .await
        {
            Ok(resp) => {
                assert_eq!(resp.into_inner().payload, b"second");
                ok = true;
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    assert!(ok, "channel never reconnected after server restart");
}

/// Graceful drain must let an in-flight RPC finish while refusing new work:
/// a slow call is dispatched, shutdown is signaled, and the slow call still
/// returns its correct response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_drain_completes_inflight() {
    let path = sock_path("drain");
    let routes = tonic::service::Routes::new(EchoServer::new(EchoSvc::default())).prepare();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let p = path.clone();
    let server = tokio::spawn(async move {
        tonic_shmsc::serve(p, routes, async {
            let _ = stop_rx.await;
        })
        .await
    });
    wait_for_socket(&path).await;

    let channel = tonic_shmsc::connect(&path).await.expect("connect");
    let mut client = EchoClient::new(channel);

    // Fire a slow RPC (400 ms server sleep), let it reach the handler, then
    // trigger graceful shutdown while it is in flight.
    let mut slow = client.clone();
    let inflight = tokio::spawn(async move {
        slow.unary_echo(Request::new(EchoRequest {
            payload: b"slow".to_vec(),
            ..Default::default()
        }))
        .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = stop_tx.send(());

    // The in-flight call still completes correctly despite the drain.
    let resp = tokio::time::timeout(Duration::from_secs(5), inflight)
        .await
        .expect("inflight must finish")
        .expect("join")
        .expect("inflight rpc");
    assert_eq!(resp.into_inner().payload, b"slow");

    let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
}
