//! Crash-safety: a peer that dies uncleanly (SIGKILL) must not hang, corrupt,
//! or strand the survivor, and must leave nothing behind that blocks reuse.
//!
//! These are the tests behind the README's "process death reclaims
//! everything" claim, which no in-process test can make: a dropped `Arc` runs
//! destructors, a `kill -9` does not. Each test spawns a REAL second process
//! (this test binary, re-executed into a server/client role via an env var)
//! and kills it mid-RPC.

#![cfg(unix)]

use std::process::{Command, Stdio};
use std::time::Duration;

use tokio_stream::StreamExt;
use tonic::{Request, Status};

#[allow(unreachable_pub)]
mod pb {
    tonic::include_proto!("shmsc.test");
}
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};
use pb::{EchoRequest, EchoResponse};

const ROLE_ENV: &str = "SHMSC_CRASH_ROLE";
const SOCK_ENV: &str = "SHMSC_CRASH_SOCK";

fn sock_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("shmsc-crash-{}-{}.sock", name, std::process::id()))
}

async fn wait_for_socket(path: &std::path::Path) {
    for _ in 0..400 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("child socket {} never appeared", path.display());
}

/// Connects with retry, tolerating a stale socket file left by a crashed
/// server that a fresh listener is still in the middle of reclaiming.
async fn connect_retry(path: &std::path::Path) -> tonic_shmsc::ShmscChannel {
    for _ in 0..400 {
        if let Ok(ch) = tonic_shmsc::connect(path).await {
            return ch;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("could not connect to reclaimed path {}", path.display());
}

/// Spawns this test binary re-executed into `role`, serving/using `sock`.
fn spawn_child(role: &str, sock: &std::path::Path) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        // Run only the role entrypoint test in the child, single-threaded and
        // quiet so its harness chatter does not intermix with ours.
        .args([
            "--exact",
            "child_role_entrypoint",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(ROLE_ENV, role)
        .env(SOCK_ENV, sock)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child")
}

// ---- server side of the child ------------------------------------------

/// A streaming echo whose server-stream drips slowly and whose unary sleeps
/// on a magic payload, so the parent can reliably kill the child mid-RPC.
#[derive(Default)]
struct SlowEcho;

type RespStream =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<EchoResponse, Status>> + Send>>;

#[tonic::async_trait]
impl Echo for SlowEcho {
    async fn unary_echo(
        &self,
        req: Request<EchoRequest>,
    ) -> Result<tonic::Response<EchoResponse>, Status> {
        Ok(tonic::Response::new(EchoResponse {
            payload: req.into_inner().payload,
        }))
    }

    type ServerStreamingEchoStream = RespStream;

    async fn server_streaming_echo(
        &self,
        req: Request<EchoRequest>,
    ) -> Result<tonic::Response<Self::ServerStreamingEchoStream>, Status> {
        let payload = req.into_inner().payload;
        // Effectively unbounded, one message per 20 ms — the parent kills us
        // partway through.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                if tx
                    .send(Ok(EchoResponse {
                        payload: payload.clone(),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn client_streaming_echo(
        &self,
        _req: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<tonic::Response<EchoResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }

    type BidiEchoStream = RespStream;

    async fn bidi_echo(
        &self,
        _req: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<tonic::Response<Self::BidiEchoStream>, Status> {
        Err(Status::unimplemented("n/a"))
    }
}

/// The child's role entrypoint. In a normal `cargo test` run (no role env) it
/// is a no-op and passes instantly; when spawned by a parent test with the
/// role env set, it becomes a server or client that runs until killed.
#[test]
fn child_role_entrypoint() {
    let Ok(role) = std::env::var(ROLE_ENV) else {
        return; // ordinary test run: nothing to do
    };
    let sock = std::path::PathBuf::from(std::env::var(SOCK_ENV).unwrap());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        match role.as_str() {
            "server" => {
                let routes = tonic::service::Routes::new(EchoServer::new(SlowEcho)).prepare();
                // Serve forever; the parent SIGKILLs us.
                tonic_shmsc::serve(sock, routes, std::future::pending::<()>())
                    .await
                    .expect("child serve");
            }
            "client" => {
                let channel = tonic_shmsc::connect(&sock).await.expect("child connect");
                let mut client = EchoClient::new(channel);
                // Open a long server-stream and hold it open forever.
                let mut stream = client
                    .server_streaming_echo(Request::new(EchoRequest {
                        payload: b"hold".to_vec(),
                        ..Default::default()
                    }))
                    .await
                    .expect("child stream open")
                    .into_inner();
                while stream.next().await.is_some() {}
            }
            other => panic!("unknown child role {other}"),
        }
    });
}

// ---- parent tests -------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_survives_server_sigkill() {
    let sock = sock_path("srv");
    let _ = std::fs::remove_file(&sock);
    let mut child = spawn_child("server", &sock);
    wait_for_socket(&sock).await;

    let channel = tonic_shmsc::connect(&sock).await.expect("connect");
    let mut client = EchoClient::new(channel);

    // Start consuming a slow server-stream so a stream is genuinely in
    // flight when the server dies.
    let mut stream = client
        .server_streaming_echo(Request::new(EchoRequest {
            payload: b"inflight".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("open stream")
        .into_inner();
    assert!(stream.next().await.expect("first item").is_ok());

    // Hard-kill the server mid-stream.
    child.kill().expect("kill server");
    child.wait().ok();

    // The in-flight stream must FAIL PROMPTLY, not hang: peer death arrives
    // as socket EOF, which tears the connection down.
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match stream.next().await {
                Some(Ok(_)) => continue,
                Some(Err(status)) => break Some(status),
                None => break None,
            }
        }
    })
    .await
    .expect("stream must resolve within 5s, not hang");
    if let Some(status) = ended {
        assert_eq!(
            status.code(),
            tonic::Code::Unavailable,
            "peer death should surface as UNAVAILABLE, got {status:?}"
        );
    }

    // Nothing stranded: a fresh listener can reclaim the path (the dead
    // server left a stale socket file; bind must reclaim it).
    let routes = tonic::service::Routes::new(EchoServer::new(SlowEcho)).prepare();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let s2 = sock.clone();
    let h = tokio::spawn(async move {
        tonic_shmsc::serve(s2, routes, async {
            let _ = stop_rx.await;
        })
        .await
    });
    // The dead server left a stale socket file, so file-existence can't
    // signal readiness of the NEW listener; retry-connect until it accepts.
    let ch2 = connect_retry(&sock).await;
    let mut c2 = EchoClient::new(ch2);
    let resp = c2
        .unary_echo(Request::new(EchoRequest {
            payload: b"alive".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("rpc on reclaimed path");
    assert_eq!(resp.into_inner().payload, b"alive");
    let _ = stop_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_survives_client_sigkill() {
    let sock = sock_path("cli");
    let _ = std::fs::remove_file(&sock);

    // Parent is the server.
    let routes = tonic::service::Routes::new(EchoServer::new(SlowEcho)).prepare();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let s = sock.clone();
    let server = tokio::spawn(async move {
        tonic_shmsc::serve(s, routes, async {
            let _ = stop_rx.await;
        })
        .await
    });
    wait_for_socket(&sock).await;

    // A child client opens a stream and holds it; we then SIGKILL it.
    let mut child = spawn_child("client", &sock);
    // Give it time to connect and open the stream.
    tokio::time::sleep(Duration::from_millis(400)).await;
    child.kill().expect("kill client");
    child.wait().ok();

    // The server must still serve: a fresh client succeeds after the peer
    // vanished mid-stream (the abandoned stream's handler is torn down, the
    // connection reclaimed).
    let channel = tonic_shmsc::connect(&sock)
        .await
        .expect("connect after client crash");
    let mut client = EchoClient::new(channel);
    let resp = tokio::time::timeout(
        Duration::from_secs(5),
        client.unary_echo(Request::new(EchoRequest {
            payload: b"still-here".to_vec(),
            ..Default::default()
        })),
    )
    .await
    .expect("server must respond within 5s")
    .expect("rpc after client crash");
    assert_eq!(resp.into_inner().payload, b"still-here");

    let _ = stop_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
}
