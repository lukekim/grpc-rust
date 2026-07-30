//! Fail-closed transport selection, ported from the Go PR's dispatch tests:
//! dialing a REAL, working HTTP/2 gRPC server with an unregistered transport
//! type must fail — the RPC must never silently succeed over the default
//! HTTP/2 fallback. Symmetric on the server side.

#![cfg(unix)]

use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status};

#[allow(unreachable_pub)]
mod pb {
    tonic::include_proto!("shmsc.test");
}
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};
use pb::{EchoRequest, EchoResponse};

#[derive(Default)]
struct EchoSvc;

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
    type ServerStreamingEchoStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<EchoResponse, Status>> + Send>>;
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
    type BidiEchoStream = Self::ServerStreamingEchoStream;
    async fn bidi_echo(
        &self,
        _req: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<Response<Self::BidiEchoStream>, Status> {
        Err(Status::unimplemented("n/a"))
    }
}

/// Starts a real HTTP/2 gRPC server on localhost TCP, returning its address.
async fn spawn_h2_server() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        Server::builder()
            .add_service(EchoServer::new(EchoSvc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("h2 serve");
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unregistered_type_fails_closed_against_real_h2_server() {
    let addr = spawn_h2_server().await;

    // Control: the same endpoint WITHOUT a transport type works over HTTP/2.
    let ok = Endpoint::from_shared(format!("http://{addr}"))
        .expect("uri")
        .connect()
        .await
        .expect("default path must work");
    let mut client = EchoClient::new(ok);
    let resp = client
        .unary_echo(Request::new(EchoRequest {
            payload: b"h2".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("h2 rpc");
    assert_eq!(resp.into_inner().payload, b"h2");

    // Eager connect with an unregistered type: hard error naming the type.
    let err = Endpoint::from_shared(format!("http://{addr}"))
        .expect("uri")
        .transport_type("definitely-not-registered")
        .connect()
        .await
        .expect_err("must fail closed");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("definitely-not-registered") && msg.contains("not registered"),
        "error must name the unregistered type: {msg}"
    );

    // Lazy connect: the error surfaces on the first RPC instead — and the
    // RPC must fail rather than silently succeed over HTTP/2.
    let lazy = Endpoint::from_shared(format!("http://{addr}"))
        .expect("uri")
        .transport_type("definitely-not-registered")
        .connect_lazy();
    let mut client = EchoClient::new(lazy);
    let rpc_err = client
        .unary_echo(Request::new(EchoRequest {
            payload: b"nope".to_vec(),
            ..Default::default()
        }))
        .await
        .expect_err("lazy fail-closed");
    assert!(
        rpc_err.message().contains("definitely-not-registered")
            || format!("{rpc_err:?}").contains("definitely-not-registered"),
        "lazy error must name the type: {rpc_err:?}"
    );

    // An explicitly provided connector must NOT override an explicit
    // transport type (that would silently change the wire protocol).
    let err = Endpoint::from_shared(format!("http://{addr}"))
        .expect("uri")
        .transport_type("definitely-not-registered")
        .connect_with_connector(tower::service_fn(|_uri: tonic::transport::Uri| async {
            Err::<hyper_util::rt::TokioIo<tokio::net::TcpStream>, std::io::Error>(
                std::io::Error::other("connector must not be used"),
            )
        }))
        .await
        .expect_err("must fail closed");
    assert!(format!("{err:?}").contains("definitely-not-registered"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_side_unregistered_type_fails_closed() {
    let err = Server::builder()
        .add_service(EchoServer::new(EchoSvc))
        .serve_with_transport_type("nope-not-registered", "/tmp/never-bound.sock")
        .await
        .expect_err("must fail closed before binding");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("nope-not-registered") && msg.contains("not registered"),
        "error must name the type: {msg}"
    );
    assert!(
        !std::path::Path::new("/tmp/never-bound.sock").exists(),
        "nothing may be bound for an unregistered type"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registered_transport_rejects_tls_fail_closed() {
    use tonic::transport::experimental::client::{ClientBuildOptions, ClientTransportBuilder};

    // Exercise the plugin's fail-closed credential check directly at the
    // builder seam (the Endpoint plumbing sets tls_configured from its TLS
    // config).
    let builder = tonic_shmsc::ShmscClientBuilder::default();
    let mut opts = ClientBuildOptions::default();
    opts.tls_configured = true;
    let err = builder
        .build("shmsc://localhost/tmp/whatever.sock".parse().unwrap(), opts)
        .await
        .expect_err("TLS must be rejected, never silently downgraded");
    assert!(err.to_string().contains("transport security"), "{err}");
}
