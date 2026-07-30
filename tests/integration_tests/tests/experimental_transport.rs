//! Conformance test for tonic's EXPERIMENTAL pluggable-transport API using a
//! FAKE, transport-independent transport.
//!
//! The Go PR this API is ported from sets its stability bar at "one
//! independent transport and one fake transport pass the conformance suite".
//! `tonic-shmsc` is the independent transport; this is the fake one: an
//! in-process loopback that carries real gRPC (unary + server-streaming) end
//! to end through tonic's full client and server stacks — encode → Body →
//! transport → Body → decode — proving the exported API is sufficient without
//! any framing, sockets, or shared memory.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use integration_tests::pb::{Input1, Output1, test1_client::Test1Client, test1_server};
use tonic::transport::experimental::client::{
    ClientBuildOptions, ClientTransport, ClientTransportBuilder,
};
use tonic::transport::experimental::server::{
    RouteService, ServerBuildOptions, ServerTransport, ServerTransportBuilder,
};
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status};
use tokio_stream::StreamExt;

// ---- the fake transport ------------------------------------------------

/// Process-global registry of bound fake servers: target string → the
/// server's prepared `RouteService`. This is the "wire" — a client transport
/// finds the peer here and calls it directly.
static FAKE_WIRE: LazyLock<Mutex<HashMap<String, RouteService>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const FAKE: &str = "fake-inproc";

struct FakeClientBuilder;

impl ClientTransportBuilder for FakeClientBuilder {
    fn build(
        &self,
        target: http::Uri,
        _opts: ClientBuildOptions,
    ) -> Pin<Box<dyn Future<Output = Result<ClientTransport, tonic::transport::experimental::BoxError>> + Send>>
    {
        Box::pin(async move {
            let key = target.path().to_owned();
            let svc = FAKE_WIRE
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .ok_or_else(|| format!("fake transport: no server bound at {key:?}"))?;
            Ok(ClientTransport::new(FakeClient { svc }))
        })
    }
}

/// The client transport: hands each request straight to the peer's
/// `RouteService` (which is routes + grpc-timeout + error recovery) and
/// returns its response. A real transport would serialize to bytes here; the
/// fake one proves the API shape by short-circuiting the wire.
#[derive(Clone)]
struct FakeClient {
    svc: RouteService,
}

impl tower_service::Service<http::Request<tonic::body::Body>> for FakeClient {
    type Response = http::Response<tonic::body::Body>;
    type Error = tonic::transport::experimental::BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // RouteService is infallible; map its Infallible error into BoxError.
        tower_service::Service::poll_ready(&mut self.svc, cx)
            .map_err(|e| -> Self::Error { match e {} })
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let mut svc = self.svc.clone();
        Box::pin(async move {
            // Infallible: the response is always Ok, so surface it directly.
            let resp = tower_service::Service::call(&mut svc, req).await.unwrap_or_else(|e| match e {});
            Ok(resp)
        })
    }
}

struct FakeServerBuilder;

impl ServerTransportBuilder for FakeServerBuilder {
    fn bind(
        &self,
        target: &str,
        _opts: ServerBuildOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn ServerTransport>, tonic::transport::experimental::BoxError>> + Send>>
    {
        let target = target.to_owned();
        Box::pin(async move { Ok(Box::new(FakeServer { target }) as Box<dyn ServerTransport>) })
    }
}

struct FakeServer {
    target: String,
}

impl ServerTransport for FakeServer {
    fn serve(
        self: Box<Self>,
        service: RouteService,
        shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), tonic::transport::experimental::BoxError>> + Send>>
    {
        Box::pin(async move {
            FAKE_WIRE.lock().unwrap().insert(self.target.clone(), service);
            shutdown.await;
            FAKE_WIRE.lock().unwrap().remove(&self.target);
            Ok(())
        })
    }
}

fn register_fake() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        tonic::transport::experimental::client::register(FAKE, FakeClientBuilder);
        tonic::transport::experimental::server::register(FAKE, FakeServerBuilder);
    });
}

// ---- a test service ----------------------------------------------------

#[derive(Default)]
struct StreamSvc;

type OutStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<Output1, Status>> + Send>>;

#[tonic::async_trait]
impl test1_server::Test1 for StreamSvc {
    async fn unary_call(&self, req: Request<Input1>) -> Result<Response<Output1>, Status> {
        // Echo, or fail on a magic marker so error propagation is covered.
        let buf = req.into_inner().buf;
        if buf == b"fail" {
            return Err(Status::failed_precondition("requested failure"));
        }
        Ok(Response::new(Output1 { buf }))
    }

    type StreamCallStream = OutStream;

    async fn stream_call(
        &self,
        req: Request<Input1>,
    ) -> Result<Response<Self::StreamCallStream>, Status> {
        let buf = req.into_inner().buf;
        let stream = tokio_stream::iter((0..5).map(move |_| Ok(Output1 { buf: buf.clone() })));
        Ok(Response::new(Box::pin(stream)))
    }
}

fn spawn_fake_server(target: &str) -> tokio::sync::oneshot::Sender<()> {
    let target = target.to_owned();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        Server::builder()
            .add_service(test1_server::Test1Server::new(StreamSvc))
            .serve_with_transport_type_shutdown(FAKE, &target, async {
                let _ = rx.await;
            })
            .await
            .expect("fake serve");
    });
    tx
}

async fn fake_channel(target: &str) -> tonic::transport::Channel {
    Endpoint::from_shared(format!("fake://localhost{target}"))
        .expect("uri")
        .transport_type(FAKE)
        .connect()
        .await
        .expect("connect over fake transport")
}

// ---- conformance tests -------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unary_over_fake_transport() {
    register_fake();
    let target = "/conf-unary";
    let _stop = spawn_fake_server(target);
    // Bind is synchronous in the fake serve, but the spawn is not; wait for it.
    wait_bound(target).await;

    let mut client = Test1Client::new(fake_channel(target).await);
    let resp = client
        .unary_call(Request::new(Input1 { buf: b"hello-fake".to_vec() }))
        .await
        .expect("unary");
    assert_eq!(resp.into_inner().buf, b"hello-fake");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_streaming_over_fake_transport() {
    register_fake();
    let target = "/conf-stream";
    let _stop = spawn_fake_server(target);
    wait_bound(target).await;

    let mut client = Test1Client::new(fake_channel(target).await);
    let mut stream = client
        .stream_call(Request::new(Input1 { buf: b"s".to_vec() }))
        .await
        .expect("stream open")
        .into_inner();
    let mut n = 0;
    while let Some(item) = stream.next().await {
        assert_eq!(item.expect("item").buf, b"s");
        n += 1;
    }
    assert_eq!(n, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_status_propagates_over_fake_transport() {
    register_fake();
    let target = "/conf-error";
    let _stop = spawn_fake_server(target);
    wait_bound(target).await;

    let mut client = Test1Client::new(fake_channel(target).await);
    let err = client
        .unary_call(Request::new(Input1 { buf: b"fail".to_vec() }))
        .await
        .expect_err("must fail");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(err.message(), "requested failure");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fail_closed_unregistered_type() {
    register_fake();
    // A registered type with nothing bound, and an entirely unknown type,
    // must both fail — never silently fall back to HTTP/2.
    let err = Endpoint::from_shared("fake://localhost/nothing-here".to_string())
        .expect("uri")
        .transport_type("totally-unregistered")
        .connect()
        .await
        .expect_err("unregistered type must fail closed");
    assert!(
        format!("{err:?}").contains("totally-unregistered"),
        "error must name the unregistered type: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_timeout_is_honored() {
    // A builder that never completes must be cut off by connect_timeout
    // rather than hanging forever.
    struct SlowBuilder;
    impl ClientTransportBuilder for SlowBuilder {
        fn build(
            &self,
            _t: http::Uri,
            _o: ClientBuildOptions,
        ) -> Pin<Box<dyn Future<Output = Result<ClientTransport, tonic::transport::experimental::BoxError>> + Send>>
        {
            Box::pin(std::future::pending())
        }
    }
    tonic::transport::experimental::client::register("slow-builder", SlowBuilder);

    let start = std::time::Instant::now();
    let res = Endpoint::from_shared("fake://localhost/slow".to_string())
        .expect("uri")
        .transport_type("slow-builder")
        .connect_timeout(Duration::from_millis(200))
        .connect()
        .await;
    assert!(res.is_err(), "a never-completing build must time out");
    assert!(start.elapsed() < Duration::from_secs(5), "must not hang");
}

async fn wait_bound(target: &str) {
    for _ in 0..400 {
        if FAKE_WIRE.lock().unwrap().contains_key(target) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("fake server never bound at {target}");
}
