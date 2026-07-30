//! End-to-end tests: real gRPC clients and servers over the shared-memory
//! transport, exercising every selection path the PR feature defines —
//! plugin-direct, `Endpoint::transport_type`, and
//! `Router::serve_with_transport_type`.

#![cfg(unix)]

use std::pin::Pin;
use std::sync::Once;
use std::time::Duration;

use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;
use tonic::{Code, Request, Response, Status, Streaming};

#[allow(unreachable_pub)]
mod pb {
    tonic::include_proto!("shmsc.test");
}
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};
use pb::{EchoRequest, EchoResponse};

fn register_once() {
    static ONCE: Once = Once::new();
    ONCE.call_once(tonic_shmsc::register);
}

fn sock_path(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir();
    dir.join(format!("shmsc-e2e-{}-{}.sock", name, std::process::id()))
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
        let echoed = req.metadata().get("x-echo").cloned();
        let r = req.into_inner();
        if r.fail_with_code != 0 {
            let mut md = tonic::metadata::MetadataMap::new();
            md.insert("x-fail-detail", MetadataValue::from_static("boom"));
            return Err(Status::with_metadata(
                Code::from_i32(r.fail_with_code),
                "requested failure",
                md,
            ));
        }
        if r.payload == b"sleep" {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        let mut resp = Response::new(EchoResponse { payload: r.payload });
        if let Some(v) = echoed {
            resp.metadata_mut().insert("x-echo", v);
        }
        Ok(resp)
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
        while let Some(msg) = stream.next().await {
            all.extend_from_slice(&msg?.payload);
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
            while let Some(msg) = stream.next().await {
                let out = match msg {
                    Ok(m) => Ok(EchoResponse { payload: m.payload }),
                    Err(e) => Err(e),
                };
                if tx.send(out).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Serves the echo service plugin-direct at `path` until the returned guard
/// drops.
fn spawn_plugin_direct(path: std::path::PathBuf) -> tokio::sync::oneshot::Sender<()> {
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let routes = tonic::service::Routes::new(EchoServer::new(EchoSvc)).prepare();
    tokio::spawn(async move {
        tonic_shmsc::serve(path, routes, async {
            let _ = stop_rx.await;
        })
        .await
        .expect("shmsc serve failed");
    });
    stop_tx
}

async fn wait_for_socket(path: &std::path::Path) {
    for _ in 0..200 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("server socket {} never appeared", path.display());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unary_roundtrip_plugin_direct_with_metadata() {
    let path = sock_path("unary-direct");
    let _stop = spawn_plugin_direct(path.clone());
    wait_for_socket(&path).await;

    let channel = tonic_shmsc::connect(&path).await.expect("connect");
    let mut client = EchoClient::new(channel);

    let mut req = Request::new(EchoRequest {
        payload: b"hello over shared memory".to_vec(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("x-echo", MetadataValue::from_static("round-trip"));
    let resp = client.unary_echo(req).await.expect("rpc");
    assert_eq!(
        resp.metadata().get("x-echo").map(|v| v.as_bytes()),
        Some(&b"round-trip"[..])
    );
    assert_eq!(resp.into_inner().payload, b"hello over shared memory");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unary_via_endpoint_transport_type() {
    register_once();
    let path = sock_path("endpoint");
    let _stop = spawn_plugin_direct(path.clone());
    wait_for_socket(&path).await;

    let channel = Endpoint::from_shared(tonic_shmsc::uri(path.to_str().unwrap()))
        .expect("uri")
        .transport_type(tonic_shmsc::NAME)
        .connect()
        .await
        .expect("connect via registry");
    let mut client = EchoClient::new(channel);
    let resp = client
        .unary_echo(Request::new(EchoRequest {
            payload: b"selected by transport type".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("rpc");
    assert_eq!(resp.into_inner().payload, b"selected by transport type");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unary_via_router_serve_with_transport_type() {
    register_once();
    let path = sock_path("router");
    let path_str = path.to_str().unwrap().to_owned();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EchoServer::new(EchoSvc))
            .serve_with_transport_type_shutdown(tonic_shmsc::NAME, &path_str, async {
                let _ = stop_rx.await;
            })
            .await
    });
    wait_for_socket(&path).await;

    let channel = Endpoint::from_shared(tonic_shmsc::uri(path.to_str().unwrap()))
        .expect("uri")
        .transport_type(tonic_shmsc::NAME)
        .connect()
        .await
        .expect("connect");
    let mut client = EchoClient::new(channel);
    let resp = client
        .unary_echo(Request::new(EchoRequest {
            payload: b"through the Router".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("rpc");
    assert_eq!(resp.into_inner().payload, b"through the Router");

    // Graceful shutdown: serve returns cleanly and removes the socket.
    drop(client);
    let _ = stop_tx.send(());
    let served = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("server did not shut down")
        .expect("join");
    served.expect("serve_with_transport_type failed");
    assert!(!path.exists(), "socket file should be removed on shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_messages_exceed_window_and_ring() {
    // 40 MiB payload with default 16 MiB rings and 8 MiB stream windows:
    // exercises chunking, ring-full parking, and WINDOW_UPDATE cycles in
    // both directions.
    let path = sock_path("large");
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let routes = tonic::service::Routes::new(
        EchoServer::new(EchoSvc).max_decoding_message_size(64 * 1024 * 1024),
    )
    .prepare();
    let p2 = path.clone();
    tokio::spawn(async move {
        tonic_shmsc::serve(p2, routes, async {
            let _ = stop_rx.await;
        })
        .await
        .expect("serve");
    });
    wait_for_socket(&path).await;

    let channel = tonic_shmsc::connect(&path).await.expect("connect");
    let mut client = EchoClient::new(channel).max_decoding_message_size(64 * 1024 * 1024);

    let payload: Vec<u8> = (0..40 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let resp = client
        .unary_echo(Request::new(EchoRequest {
            payload: payload.clone(),
            ..Default::default()
        }))
        .await
        .expect("large rpc");
    assert_eq!(resp.into_inner().payload, payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_all_shapes() {
    let path = sock_path("streams");
    let _stop = spawn_plugin_direct(path.clone());
    wait_for_socket(&path).await;
    let channel = tonic_shmsc::connect(&path).await.expect("connect");
    let mut client = EchoClient::new(channel);

    // Server streaming.
    let mut stream = client
        .server_streaming_echo(Request::new(EchoRequest {
            payload: b"repeat-me".to_vec(),
            repeat: 17,
            ..Default::default()
        }))
        .await
        .expect("server streaming")
        .into_inner();
    let mut count = 0;
    while let Some(item) = stream.next().await {
        assert_eq!(item.expect("stream item").payload, b"repeat-me");
        count += 1;
    }
    assert_eq!(count, 17);

    // Client streaming.
    let reqs = tokio_stream::iter((0..5u8).map(|i| EchoRequest {
        payload: vec![b'a' + i; 3],
        ..Default::default()
    }));
    let resp = client
        .client_streaming_echo(reqs)
        .await
        .expect("client streaming");
    assert_eq!(resp.into_inner().payload, b"aaabbbcccdddeee");

    // Bidi: interleaved request/response pairs.
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let mut bidi = client
        .bidi_echo(ReceiverStream::new(rx))
        .await
        .expect("bidi")
        .into_inner();
    for i in 0..10u32 {
        let payload = format!("ping-{i}").into_bytes();
        tx.send(EchoRequest {
            payload: payload.clone(),
            ..Default::default()
        })
        .await
        .expect("send");
        let echoed = bidi.next().await.expect("bidi item").expect("bidi ok");
        assert_eq!(echoed.payload, payload);
    }
    drop(tx);
    assert!(bidi.next().await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rich_error_status_with_metadata() {
    let path = sock_path("errors");
    let _stop = spawn_plugin_direct(path.clone());
    wait_for_socket(&path).await;
    let channel = tonic_shmsc::connect(&path).await.expect("connect");
    let mut client = EchoClient::new(channel);

    let err = client
        .unary_echo(Request::new(EchoRequest {
            fail_with_code: Code::FailedPrecondition as i32,
            ..Default::default()
        }))
        .await
        .expect_err("must fail");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(err.message(), "requested failure");
    assert_eq!(
        err.metadata().get("x-fail-detail").map(|v| v.as_bytes()),
        Some(&b"boom"[..])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_timeout_enforced_server_side() {
    register_once();
    let path = sock_path("timeout");
    let path_str = path.to_str().unwrap().to_owned();
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EchoServer::new(EchoSvc))
            .serve_with_transport_type_shutdown(tonic_shmsc::NAME, &path_str, async {
                let _ = stop_rx.await;
            })
            .await
            .expect("serve");
    });
    wait_for_socket(&path).await;

    let channel = Endpoint::from_shared(tonic_shmsc::uri(path.to_str().unwrap()))
        .expect("uri")
        .transport_type(tonic_shmsc::NAME)
        .connect()
        .await
        .expect("connect");
    let mut client = EchoClient::new(channel);

    // The handler sleeps 30s; the grpc-timeout header must cut it short via
    // the server-side GrpcTimeout layer baked into RouteService.
    let mut req = Request::new(EchoRequest {
        payload: b"sleep".to_vec(),
        ..Default::default()
    });
    req.set_timeout(Duration::from_millis(300));
    let start = std::time::Instant::now();
    let err = tokio::time::timeout(Duration::from_secs(10), client.unary_echo(req))
        .await
        .expect("rpc must finish quickly")
        .expect_err("must time out");
    assert!(
        matches!(err.code(), Code::Cancelled | Code::DeadlineExceeded),
        "unexpected code {:?} ({})",
        err.code(),
        err.message()
    );
    assert!(start.elapsed() < Duration::from_secs(10));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_streams() {
    let path = sock_path("concurrent");
    let _stop = spawn_plugin_direct(path.clone());
    wait_for_socket(&path).await;
    let channel = tonic_shmsc::connect(&path).await.expect("connect");

    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..64u32 {
        let mut client = EchoClient::new(channel.clone());
        tasks.spawn(async move {
            let payload: Vec<u8> = (0..(i * 1013) % 50_000).map(|j| (j % 256) as u8).collect();
            let resp = client
                .unary_echo(Request::new(EchoRequest {
                    payload: payload.clone(),
                    ..Default::default()
                }))
                .await
                .expect("rpc");
            assert_eq!(resp.into_inner().payload, payload);
        });
    }
    while let Some(res) = tasks.join_next().await {
        res.expect("task");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_propagates() {
    let path = sock_path("cancel");
    let _stop = spawn_plugin_direct(path.clone());
    wait_for_socket(&path).await;
    let channel = tonic_shmsc::connect(&path).await.expect("connect");
    let mut client = EchoClient::new(channel);

    // Drop the in-flight call: the transport must RST the stream; the
    // connection stays healthy for the next call.
    let call = client.unary_echo(Request::new(EchoRequest {
        payload: b"sleep".to_vec(),
        ..Default::default()
    }));
    let _ = tokio::time::timeout(Duration::from_millis(200), call).await;

    let resp = client
        .unary_echo(Request::new(EchoRequest {
            payload: b"alive".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("connection must survive cancellation");
    assert_eq!(resp.into_inner().payload, b"alive");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_listener_refused_and_stale_socket_reclaimed() {
    let path = sock_path("dup");
    let _stop = spawn_plugin_direct(path.clone());
    wait_for_socket(&path).await;

    // A live listener must be refused, never hijacked.
    let routes = tonic::service::Routes::new(EchoServer::new(EchoSvc)).prepare();
    let err = tonic_shmsc::serve(path.clone(), routes.clone(), std::future::pending())
        .await
        .expect_err("duplicate listener must be refused");
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse, "{err}");

    // A stale socket file (dead process) is reclaimed.
    let stale = sock_path("stale");
    {
        let l = std::os::unix::net::UnixListener::bind(&stale).expect("bind stale");
        drop(l); // closes the fd but leaves the file behind
    }
    assert!(stale.exists());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let stale2 = stale.clone();
    let handle = tokio::spawn(async move {
        tonic_shmsc::serve(stale2, routes, async {
            let _ = stop_rx.await;
        })
        .await
    });
    // The stale file exists the whole time, so poll by connecting until the
    // reclaimed listener actually accepts.
    let channel = {
        let mut attempt = 0;
        loop {
            match tonic_shmsc::connect(&stale).await {
                Ok(ch) => break ch,
                Err(_) if attempt < 200 => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(e) => panic!("connect after reclaim: {e}"),
            }
        }
    };
    let mut client = EchoClient::new(channel);
    let resp = client
        .unary_echo(Request::new(EchoRequest {
            payload: b"reclaimed".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("rpc");
    assert_eq!(resp.into_inner().payload, b"reclaimed");
    let _ = stop_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_info_extension_present() {
    // The server should see our pid/uid via the connect-info extension.
    // (Asserted through a service that copies it into the response.)
    #[derive(Default)]
    struct PeerSvc;
    #[tonic::async_trait]
    impl Echo for PeerSvc {
        async fn unary_echo(
            &self,
            req: Request<EchoRequest>,
        ) -> Result<Response<EchoResponse>, Status> {
            let info = req
                .extensions()
                .get::<tonic_shmsc::ShmscConnectInfo>()
                .copied()
                .ok_or_else(|| Status::internal("missing ShmscConnectInfo"))?;
            let text = format!(
                "{}:{}",
                info.peer_pid.unwrap_or(0),
                info.peer_uid.unwrap_or(0)
            );
            Ok(Response::new(EchoResponse {
                payload: text.into_bytes(),
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
            _req: Request<Streaming<EchoRequest>>,
        ) -> Result<Response<EchoResponse>, Status> {
            Err(Status::unimplemented("n/a"))
        }
        type BidiEchoStream = RespStream;
        async fn bidi_echo(
            &self,
            _req: Request<Streaming<EchoRequest>>,
        ) -> Result<Response<Self::BidiEchoStream>, Status> {
            Err(Status::unimplemented("n/a"))
        }
    }

    let path = sock_path("peer");
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let routes = tonic::service::Routes::new(EchoServer::new(PeerSvc)).prepare();
    let p2 = path.clone();
    tokio::spawn(async move {
        tonic_shmsc::serve(p2, routes, async {
            let _ = stop_rx.await;
        })
        .await
        .expect("serve");
    });
    wait_for_socket(&path).await;
    let channel = tonic_shmsc::connect(&path).await.expect("connect");
    let mut client = EchoClient::new(channel);
    let resp = client
        .unary_echo(Request::new(EchoRequest::default()))
        .await
        .expect("rpc");
    let text = String::from_utf8(resp.into_inner().payload).unwrap();
    let (pid, uid) = text.split_once(':').unwrap();
    let my_pid = std::process::id().to_string();
    // pid is reported on both platforms we support; uid at least on one.
    assert!(
        pid == my_pid || uid == unsafe { libc::getuid() }.to_string(),
        "unexpected peer info {text:?} (my pid {my_pid})"
    );
}
