//! Compares the shmsc transport against tonic's stock HTTP/2 transport over
//! a Unix socket and over TCP loopback — the Rust port of the Go PR's
//! `benchmark/shmsccmp` harness (latency + throughput, same service, same
//! process pair).
//!
//! Run with: `cargo run -p tonic-shmsc --example bench --release`

// Everything below main is unix-only (see the non-unix main stub).
#![cfg_attr(not(unix), allow(unused, dead_code))]

use std::pin::Pin;
use std::time::{Duration, Instant};

use tokio_stream::Stream;
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

const MAX_MSG: usize = 128 * 1024 * 1024;

fn big_server() -> EchoServer<EchoSvc> {
    EchoServer::new(EchoSvc)
        .max_decoding_message_size(MAX_MSG)
        .max_encoding_message_size(MAX_MSG)
}

async fn bench_client<T>(name: &str, mut client: EchoClient<T>)
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    <T::ResponseBody as http_body::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    client = client
        .max_decoding_message_size(MAX_MSG)
        .max_encoding_message_size(MAX_MSG);

    // Latency: 1 KiB unary round trips.
    let payload = vec![0xA5u8; 1024];
    let warmup = 200;
    let iters = 2000;
    let mut lat = Vec::with_capacity(iters);
    for i in 0..(warmup + iters) {
        let t0 = Instant::now();
        let resp = client
            .unary_echo(Request::new(EchoRequest {
                payload: payload.clone(),
                ..Default::default()
            }))
            .await
            .expect("rpc");
        assert_eq!(resp.into_inner().payload.len(), payload.len());
        if i >= warmup {
            lat.push(t0.elapsed());
        }
    }
    lat.sort();
    let p50 = lat[lat.len() / 2];
    let p99 = lat[lat.len() * 99 / 100];

    // Throughput: 16 MiB unary echoes (payload travels both directions).
    let big: Vec<u8> = vec![0x5Au8; 16 * 1024 * 1024];
    let big_iters = 24;
    let t0 = Instant::now();
    for _ in 0..big_iters {
        let resp = client
            .unary_echo(Request::new(EchoRequest {
                payload: big.clone(),
                ..Default::default()
            }))
            .await
            .expect("big rpc");
        assert_eq!(resp.into_inner().payload.len(), big.len());
    }
    let elapsed = t0.elapsed();
    // Bytes moved: payload out + payload back, per iteration.
    let gib = (2.0 * big_iters as f64 * big.len() as f64) / (1u64 << 30) as f64;
    let gib_s = gib / elapsed.as_secs_f64();

    println!(
        "{name:<28} unary 1KiB p50 {p50:>9.1?}  p99 {p99:>9.1?}   16MiB echo {gib_s:>6.2} GiB/s"
    );
}

#[cfg(not(unix))]
fn main() {
    // The shmsc leg works on Windows, but the UDS comparison leg does not;
    // keep the example unix-only for now.
    eprintln!("the bench example compares against a Unix-socket baseline and is unix-only");
}

#[cfg(unix)]
#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = std::env::temp_dir();
    let pid = std::process::id();

    // shmsc.
    {
        let path = dir.join(format!("shmsc-bench-{pid}.sock"));
        let routes = tonic::service::Routes::new(big_server()).prepare();
        let p2 = path.clone();
        let (_stop, stop_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            tonic_shmsc::serve(p2, routes, async {
                let _ = stop_rx.await;
            })
            .await
            .expect("shmsc serve");
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let channel = tonic_shmsc::connect(&path).await.expect("shmsc connect");
        bench_client("shmsc (shared memory)", EchoClient::new(channel)).await;
    }

    // Stock tonic HTTP/2 over a Unix socket.
    {
        let path = dir.join(format!("shmsc-bench-uds-{pid}.sock"));
        let _ = std::fs::remove_file(&path);
        let uds = tokio::net::UnixListener::bind(&path).expect("bind uds");
        tokio::spawn(async move {
            Server::builder()
                .add_service(big_server())
                .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(uds))
                .await
                .expect("uds serve");
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let channel = Endpoint::try_from(format!("unix://{}", path.display()))
            .expect("uds uri")
            .connect()
            .await
            .expect("uds connect");
        bench_client("hyper/h2 over unix socket", EchoClient::new(channel)).await;
        let _ = std::fs::remove_file(&path);
    }

    // Stock tonic HTTP/2 over TCP loopback.
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind tcp");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            Server::builder()
                .add_service(big_server())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("tcp serve");
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let channel = Endpoint::try_from(format!("http://{addr}"))
            .expect("tcp uri")
            .connect()
            .await
            .expect("tcp connect");
        bench_client("hyper/h2 over tcp loopback", EchoClient::new(channel)).await;
    }
}
