//! Server-side listener, per-connection demux, and per-stream dispatch into
//! the tonic service.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use http::{HeaderMap, Request, Response};
use http_body::Body as HttpBody;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::{Notify, mpsc};
use tonic::Status;
use tonic::body::Body;

use super::client::DATA_QUEUE_DEPTH;
use super::conduit::Conduit;
use super::conn::{
    BodyEvent, CloseReason, Config, ConnIo, RecvCredit, SendWindow, ShmBody, SpinBudget,
    WriteHandle, WriteOp, read_frame, server_handshake, writer_loop,
};
use super::frame::{self, FLAG_ACK, FLAG_END_STREAM, Frame, FrameType, ProtocolError, RstCode};
use super::ring::{Consumer, Producer};

/// How long a dialer may take to complete the segment handshake.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The tower service type served over this transport (cloned per stream).
pub(crate) trait ServeService:
    tower_service::Service<
        Request<Body>,
        Response = Response<Body>,
        Error = std::convert::Infallible,
        Future: Send,
    > + Clone
    + Send
    + 'static
{
}

impl<T> ServeService for T where
    T: tower_service::Service<
            Request<Body>,
            Response = Response<Body>,
            Error = std::convert::Infallible,
            Future: Send,
        > + Clone
        + Send
        + 'static
{
}

/// Identity of the shared-memory peer, inserted into request extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShmscConnectInfo {
    /// The peer's process id, if the platform reports it.
    pub peer_pid: Option<u32>,
    /// The peer's effective uid, if the platform reports it.
    pub peer_uid: Option<u32>,
}

#[cfg(unix)]
fn peer_info(sock: &Conduit) -> ShmscConnectInfo {
    use std::os::fd::AsRawFd;
    let fd = sock.as_raw_fd();
    #[cfg(target_os = "linux")]
    {
        // SAFETY: getsockopt with a properly sized ucred out-param.
        unsafe {
            let mut cred: libc::ucred = std::mem::zeroed();
            let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
            if libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&raw mut cred).cast(),
                &mut len,
            ) == 0
            {
                return ShmscConnectInfo {
                    peer_pid: Some(cred.pid as u32),
                    peer_uid: Some(cred.uid),
                };
            }
        }
        ShmscConnectInfo {
            peer_pid: None,
            peer_uid: None,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let mut info = ShmscConnectInfo {
            peer_pid: None,
            peer_uid: None,
        };
        // SAFETY: getpeereid / getsockopt(LOCAL_PEERPID) with properly sized
        // out-params; both are macOS/BSD-standard for AF_UNIX sockets.
        unsafe {
            let mut uid: libc::uid_t = 0;
            let mut gid: libc::gid_t = 0;
            if libc::getpeereid(fd, &mut uid, &mut gid) == 0 {
                info.peer_uid = Some(uid);
            }
            #[cfg(target_os = "macos")]
            {
                const LOCAL_PEERPID: libc::c_int = 0x002;
                let mut pid: libc::pid_t = 0;
                let mut len = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
                if libc::getsockopt(
                    fd,
                    libc::SOL_LOCAL,
                    LOCAL_PEERPID,
                    (&raw mut pid).cast(),
                    &mut len,
                ) == 0
                {
                    info.peer_pid = Some(pid as u32);
                }
            }
        }
        info
    }
}

#[cfg(windows)]
fn peer_info(sock: &Conduit) -> ShmscConnectInfo {
    use std::os::windows::io::AsRawHandle;
    let mut info = ShmscConnectInfo {
        peer_pid: None,
        peer_uid: None,
    };
    if let Conduit::Server(pipe) = sock {
        let mut pid: u32 = 0;
        // SAFETY: GetNamedPipeClientProcessId on a live server-end handle
        // with a properly sized out-param.
        let ok = unsafe {
            windows_sys::Win32::System::Pipes::GetNamedPipeClientProcessId(
                pipe.as_raw_handle().cast(),
                &mut pid,
            )
        };
        if ok != 0 {
            info.peer_pid = Some(pid);
        }
    }
    info
}

/// Only a peer with the same effective uid as the server process may connect.
/// Enforced at accept — before any segment is created or handed out — so the
/// unavoidable window between binding the rendezvous socket and tightening it
/// to mode 0600 cannot be used by another local user to obtain a segment or
/// invoke RPCs. This is the authoritative owner-only boundary; the socket mode
/// is defense-in-depth. Fails closed if the peer uid cannot be determined.
#[cfg(unix)]
fn peer_authorized(sock: &Conduit) -> bool {
    // SAFETY: geteuid is always safe and never fails.
    let me = unsafe { libc::geteuid() };
    matches!(peer_info(sock).peer_uid, Some(uid) if uid == me)
}

/// The platform accept source: a bound Unix listener, or the named-pipe
/// instance chain on Windows (one pending instance is always kept so a
/// connecting client never races an instance-less name).
#[cfg(unix)]
struct Acceptor {
    listener: UnixListener,
}

#[cfg(windows)]
struct Acceptor {
    pipe_name: String,
    next: tokio::net::windows::named_pipe::NamedPipeServer,
}

impl Acceptor {
    #[cfg(unix)]
    async fn accept(&mut self) -> io::Result<Conduit> {
        let (sock, _addr) = self.listener.accept().await?;
        Ok(sock)
    }

    #[cfg(windows)]
    async fn accept(&mut self) -> io::Result<Conduit> {
        use tokio::net::windows::named_pipe::ServerOptions;
        self.next.connect().await?;
        // Create the successor instance BEFORE handing the connected one out,
        // so the name always has an instance accepting.
        let fresh = ServerOptions::new()
            .reject_remote_clients(true)
            .create(&self.pipe_name)?;
        let connected = std::mem::replace(&mut self.next, fresh);
        Ok(Conduit::Server(connected))
    }
}

/// A bound shmsc listener.
pub(crate) struct ShmscListener {
    /// `Some` until `serve` takes it (the `Drop` impl needs the struct
    /// partially movable).
    acceptor: Option<Acceptor>,
    path: PathBuf,
    cfg: Config,
    /// (dev, ino) of the socket we bound, so cleanup only ever removes *our*
    /// socket — never a replacement listener's that rebound the same path.
    #[cfg(unix)]
    sock_id: Option<(u64, u64)>,
    /// Cleared once `serve` has done its explicit unlink, so `Drop` does not
    /// unlink a second time and delete a replacement bound during our drain.
    #[cfg(unix)]
    cleanup: bool,
}

impl std::fmt::Debug for ShmscListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmscListener")
            .field("path", &self.path)
            .finish()
    }
}

impl ShmscListener {
    /// Binds the rendezvous address for `path`.
    ///
    /// Unix: a Unix socket at `path` with `0600` permissions. A live
    /// listener on the same path is refused (never hijacked); a stale socket
    /// file left by a dead process is reclaimed. This mirrors the Go
    /// engine's duplicate-listener refusal.
    ///
    /// Windows: the named pipe derived from `path` (see
    /// `conduit::pipe_name`), created with `first_pipe_instance` so an
    /// existing pipe name is refused, and with remote clients rejected. Pipe
    /// names cannot go stale — the kernel drops them with their last handle.
    #[cfg(unix)]
    pub(crate) fn bind(path: &Path, cfg: Config) -> io::Result<Self> {
        let cfg = cfg.validated()?;
        let listener = match UnixListener::bind(path) {
            Ok(l) => l,
            Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
                // Live listener or stale file? Only a refused connection
                // proves staleness.
                match std::os::unix::net::UnixStream::connect(path) {
                    Ok(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AddrInUse,
                            format!(
                                "shmsc: {} is already in use by another listener",
                                path.display()
                            ),
                        ));
                    }
                    Err(ce) if ce.kind() == io::ErrorKind::ConnectionRefused => {
                        std::fs::remove_file(path)?;
                        UnixListener::bind(path)?
                    }
                    Err(_) => return Err(e),
                }
            }
            Err(e) => return Err(e),
        };
        // The socket is the security boundary (as the segment file is in the
        // Go engine): only the owner may connect.
        std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
        let sock_id = sock_ident(path);
        Ok(ShmscListener {
            acceptor: Some(Acceptor { listener }),
            path: path.to_owned(),
            cfg,
            sock_id,
            cleanup: true,
        })
    }

    /// See the Unix variant's doc comment.
    #[cfg(windows)]
    pub(crate) fn bind(path: &Path, cfg: Config) -> io::Result<Self> {
        use tokio::net::windows::named_pipe::ServerOptions;
        let cfg = cfg.validated()?;
        let pipe_name = super::conduit::pipe_name(path);
        let first = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .create(&pipe_name)
            .map_err(|e| {
                if e.kind() == io::ErrorKind::PermissionDenied {
                    // ERROR_ACCESS_DENIED with first_pipe_instance means the
                    // name is already served: the duplicate-listener refusal.
                    io::Error::new(
                        io::ErrorKind::AddrInUse,
                        format!("shmsc: {pipe_name} is already in use by another listener"),
                    )
                } else {
                    e
                }
            })?;
        Ok(ShmscListener {
            acceptor: Some(Acceptor {
                pipe_name,
                next: first,
            }),
            path: path.to_owned(),
            cfg,
        })
    }

    /// Serves `service` until `shutdown` resolves, then drains gracefully:
    /// stops accepting, sends GOAWAY on every connection, and waits for
    /// in-flight streams to finish.
    pub(crate) async fn serve<S>(
        mut self,
        service: S,
        shutdown: impl Future<Output = ()> + Send,
    ) -> io::Result<()>
    where
        S: ServeService,
    {
        let mut acceptor = self.acceptor.take().expect("serve called twice");
        let state = Arc::new(ServeState::default());
        let mut conn_tasks = tokio::task::JoinSet::new();
        let mut shutdown = std::pin::pin!(shutdown);
        loop {
            tokio::select! {
                accepted = acceptor.accept() => {
                    let sock = accepted?;
                    // Authorize the peer (same uid) before creating or passing a
                    // segment — this closes the bind→chmod authorization race,
                    // regardless of the socket's transient permissions.
                    #[cfg(unix)]
                    if !peer_authorized(&sock) {
                        tracing::debug!("shmsc: refused connection from a different uid");
                        continue;
                    }
                    let cfg = self.cfg.clone();
                    let svc = service.clone();
                    let state = state.clone();
                    conn_tasks.spawn(async move {
                        match tokio::time::timeout(HANDSHAKE_TIMEOUT, server_handshake(&sock, &cfg)).await {
                            Ok(Ok(seg)) => {
                                run_conn(sock, seg, cfg, svc, state).await;
                            }
                            Ok(Err(e)) => {
                                tracing::debug!("shmsc handshake failed: {e}");
                            }
                            Err(_) => {
                                tracing::debug!("shmsc handshake timed out");
                            }
                        }
                    });
                }
                _ = &mut shutdown => break,
            }
        }
        // Graceful drain: no new connections (the accept source closes on
        // drop), GOAWAY on every live connection, then wait for the conn
        // tasks.
        state.begin_drain();
        drop(acceptor);
        // Unlink our socket (only if the path still resolves to it) and disarm
        // the Drop cleanup, so a replacement listener that rebinds the path
        // while we drain is never deleted by this listener's teardown.
        #[cfg(unix)]
        {
            unlink_if_ours(&self.path, self.sock_id);
            self.cleanup = false;
        }
        while conn_tasks.join_next().await.is_some() {}
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for ShmscListener {
    fn drop(&mut self) {
        // Only if `serve` did not already unlink, and only if the path still
        // resolves to *our* socket (never a replacement's).
        if self.cleanup {
            unlink_if_ours(&self.path, self.sock_id);
        }
    }
}

/// (dev, ino) identifying the socket file just bound, for later
/// match-guarded cleanup.
#[cfg(unix)]
fn sock_ident(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    std::fs::symlink_metadata(path)
        .ok()
        .map(|m| (m.dev(), m.ino()))
}

/// Removes `path` only if it still names the exact socket identified by `id`
/// (same device + inode), so a rolling restart's replacement listener — which
/// may have rebound the same path — is never deleted by this listener's
/// teardown.
#[cfg(unix)]
fn unlink_if_ours(path: &Path, id: Option<(u64, u64)>) {
    use std::os::unix::fs::MetadataExt;
    if let Some((dev, ino)) = id
        && let Ok(m) = std::fs::symlink_metadata(path)
        && m.dev() == dev
        && m.ino() == ino
    {
        let _ = std::fs::remove_file(path);
    }
}

/// Serve-wide state: live connections for GOAWAY fan-out on drain.
#[derive(Default)]
struct ServeState {
    inner: std::sync::Mutex<ServeInner>,
}

#[derive(Default)]
struct ServeInner {
    conns: Vec<std::sync::Weak<ServerConn>>,
    draining: bool,
}

impl ServeState {
    /// Registers a live connection and reports whether the server is ALREADY
    /// draining. Registration and the drain transition share one lock, so a
    /// connection is either captured by `begin_drain`'s snapshot or told here
    /// to drain itself — never missed. (Previously a handshake finishing just
    /// after the snapshot would send GOAWAY but never be closed, wedging
    /// shutdown behind an otherwise-idle client that held its socket open.)
    #[must_use]
    fn register(&self, conn: &Arc<ServerConn>) -> bool {
        let mut g = self.inner.lock().unwrap();
        g.conns.retain(|w| w.strong_count() > 0);
        g.conns.push(Arc::downgrade(conn));
        g.draining
    }

    /// Sends GOAWAY on every live connection and closes each once its
    /// in-flight streams finish (immediately if idle) — the same shape as
    /// hyper's graceful shutdown, which does not wait for clients to hang
    /// up on their own.
    fn begin_drain(&self) {
        let conns: Vec<_> = {
            let mut g = self.inner.lock().unwrap();
            g.draining = true;
            g.conns.drain(..).filter_map(|w| w.upgrade()).collect()
        };
        for conn in conns {
            tokio::spawn(conn.drain_and_close());
        }
    }
}

struct ServerEntry {
    body_tx: Option<mpsc::UnboundedSender<Result<BodyEvent, Status>>>,
    credit: Arc<RecvCredit>,
    window: Arc<SendWindow>,
    task: Option<tokio::task::AbortHandle>,
}

/// One accepted shared-memory connection on the server.
struct ServerConn {
    io: Arc<ConnIo>,
    write: WriteHandle,
    streams: std::sync::Mutex<HashMap<u32, ServerEntry>>,
    /// Highest stream id accepted so far (for GOAWAY).
    last_accepted: AtomicU32,
    draining: AtomicBool,
    goaway_sent: AtomicBool,
    active: AtomicUsize,
    drained: Notify,
    window: u32,
    max_frame: usize,
    /// Operator ceiling on concurrent streams (`Server::max_concurrent_streams`);
    /// `None` is unlimited. Excess opens are refused before any allocation.
    max_streams: Option<u32>,
    /// Pre-park spin budget (µs) for the demux path.
    spin_us: u32,
    info: ShmscConnectInfo,
}

impl ServerConn {
    fn send_goaway(&self, msg: &str) {
        self.draining.store(true, Ordering::Release);
        if !self.goaway_sent.swap(true, Ordering::AcqRel) {
            self.write.send_ctrl(WriteOp::Frame {
                typ: FrameType::GoAway,
                flags: 0,
                stream_id: 0,
                payload: frame::encode_goaway(self.last_accepted.load(Ordering::Acquire), msg),
            });
        }
    }

    /// GOAWAY, wait for in-flight streams to finish, then close the
    /// connection.
    async fn drain_and_close(self: Arc<Self>) {
        self.send_goaway("server shutting down");
        // Let the writer flush the GOAWAY before tearing the socket down
        // (best effort; a slow peer just sees the close).
        tokio::task::yield_now().await;
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                break;
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = self.io.on_close.notified() => return,
            }
            if self.io.is_closed() {
                return;
            }
        }
        // All handlers finished and their responses (trailers included) are
        // queued; wait for the writer to push them into the ring and the
        // client to consume them before tearing the connection down, so a
        // final response is never lost to an abrupt close.
        self.io
            .graceful_flush(std::time::Duration::from_secs(5))
            .await;
        self.io.close(CloseReason::Local);
    }

    fn finish_stream(&self, id: u32) {
        let removed = self.streams.lock().unwrap().remove(&id);
        if removed.is_some() {
            let left = self.active.fetch_sub(1, Ordering::AcqRel) - 1;
            if left == 0 {
                self.drained.notify_waiters();
            }
        }
    }
}

async fn run_conn<S>(
    sock: Conduit,
    seg: super::seg::Segment,
    cfg: Config,
    service: S,
    state: Arc<ServeState>,
) where
    S: ServeService,
{
    let seg = Arc::new(seg);
    let info = peer_info(&sock);
    let io = ConnIo::new(sock);
    let l = seg.layout;
    // Server produces into s2c, consumes from c2s.
    // SAFETY: validated layout over a mapping kept alive by the Arc<Segment>
    // clones captured by each task; one owner per ring side per process.
    let producer = unsafe {
        Producer::new(
            seg.base().add(l.s2c_hdr),
            seg.base().add(l.s2c_data),
            l.s2c_cap,
        )
    };
    // SAFETY: as above.
    let consumer = unsafe {
        Consumer::new(
            seg.base().add(l.c2s_hdr),
            seg.base().add(l.c2s_data),
            l.c2s_cap,
        )
    };

    let (data_tx, data_rx) = mpsc::channel(DATA_QUEUE_DEPTH);
    let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel();
    let write = WriteHandle { data_tx, ctrl_tx };

    let conn = Arc::new(ServerConn {
        io: io.clone(),
        write: write.clone(),
        streams: std::sync::Mutex::new(HashMap::new()),
        last_accepted: AtomicU32::new(0),
        draining: AtomicBool::new(false),
        goaway_sent: AtomicBool::new(false),
        active: AtomicUsize::new(0),
        drained: Notify::new(),
        window: cfg.stream_window,
        max_frame: cfg.max_frame,
        max_streams: cfg.max_streams,
        spin_us: cfg.spin_us,
        info,
    });
    if state.register(&conn) {
        // Registered after the drain began: drain this late connection ourselves
        // (GOAWAY, then close once its streams finish) so an idle late arrival
        // cannot wedge graceful shutdown.
        tokio::spawn(conn.clone().drain_and_close());
    }

    {
        let io = io.clone();
        let seg = seg.clone();
        tokio::spawn(async move {
            io.doorbell_loop().await;
            drop(seg);
        });
    }
    {
        let io = io.clone();
        let seg = seg.clone();
        let spin = cfg.spin_us;
        tokio::spawn(async move {
            writer_loop(producer, io, data_rx, ctrl_rx, spin).await;
            drop(seg);
        });
    }

    // The demux runs on this task; when it exits the connection is over.
    demux_loop(conn.clone(), consumer, service).await;

    // Ensure the connection is closed however the demux exited (a frame-level
    // protocol error, a read error, or peer EOF): this shuts the socket so
    // the peer observes the teardown and the writer/doorbell tasks exit.
    conn.io.close(CloseReason::Local);

    // Tear down: abort in-flight handlers, drop stream state.
    let entries: Vec<ServerEntry> = {
        let mut t = conn.streams.lock().unwrap();
        t.drain().map(|(_, e)| e).collect()
    };
    for e in &entries {
        if let Some(task) = &e.task {
            task.abort();
        }
    }
    drop(seg);
}

async fn demux_loop<S>(conn: Arc<ServerConn>, mut consumer: Consumer, service: S)
where
    S: ServeService,
{
    let io = conn.io.clone();
    let mut bell = io.bell();
    let mut spin = SpinBudget::new(conn.spin_us);
    let mut arena = BytesMut::new();
    loop {
        match read_frame(&mut consumer, &mut bell, &io, &mut spin, &mut arena).await {
            Ok(frame) => {
                if let Err(e) = handle_frame(&conn, frame, &service) {
                    io.close(CloseReason::Protocol(e.0));
                    return;
                }
            }
            // A frame-level protocol error (bad header, unknown type,
            // oversized length) already arrives as CloseReason::Protocol;
            // close with it so the peer sees the teardown rather than a
            // silently half-open connection.
            Err(reason) => {
                io.close(reason);
                return;
            }
        }
    }
}

fn handle_frame<S>(conn: &Arc<ServerConn>, frame: Frame, service: &S) -> Result<(), ProtocolError>
where
    S: ServeService,
{
    match frame.typ {
        FrameType::Headers => {
            let id = frame.stream_id;
            let request_ended = frame.end_stream();
            if id % 2 != 1 {
                return Err(ProtocolError(format!(
                    "client-initiated stream id {id} must be odd"
                )));
            }
            if id <= conn.last_accepted.load(Ordering::Acquire) {
                return Err(ProtocolError(format!("stream id {id} not increasing")));
            }
            if conn.draining.load(Ordering::Acquire) {
                // Refused before any processing: safe for the client to retry
                // on a new connection.
                conn.write.send_ctrl(WriteOp::Frame {
                    typ: FrameType::RstStream,
                    flags: 0,
                    stream_id: id,
                    payload: frame::encode_rst(RstCode::Refused),
                });
                return Ok(());
            }
            // Enforce the operator's concurrent-stream ceiling BEFORE allocating
            // any per-stream state or spawning a handler, so a client cannot
            // exhaust memory past a configured bound. RST(Refused) is retry-safe.
            // `active` is incremented only by this single-threaded demux and
            // decremented by handler completion, so the load is an accurate gate.
            if let Some(max) = conn.max_streams
                && conn.active.load(Ordering::Acquire) >= max as usize
            {
                conn.write.send_ctrl(WriteOp::Frame {
                    typ: FrameType::RstStream,
                    flags: 0,
                    stream_id: id,
                    payload: frame::encode_rst(RstCode::Refused),
                });
                return Ok(());
            }
            let block = frame::decode_header_block(frame.payload)?;
            let Some(path) = block.path else {
                return Err(ProtocolError("request HEADERS missing :path".into()));
            };
            conn.last_accepted.store(id, Ordering::Release);

            let (body_tx, body_rx) = mpsc::unbounded_channel();
            let credit = RecvCredit::new(id, conn.window, conn.write.ctrl_tx.clone());
            let window = SendWindow::new(conn.window);

            let mut builder = Request::builder()
                .method(http::Method::POST)
                .version(http::Version::HTTP_2);
            let uri = if let Some(auth) = &block.authority {
                format!("http://{auth}{path}")
            } else {
                path.clone()
            };
            builder = builder.uri(uri);
            let Ok(mut req) = builder.body(()) else {
                return Err(ProtocolError(
                    "request HEADERS produce an invalid request".into(),
                ));
            };
            *req.headers_mut() = block.headers;
            req.extensions_mut().insert(conn.info);

            let req_body = if request_ended {
                Body::new(ShmBody::empty(body_rx, credit.clone()))
            } else {
                // Deliberately no RST when the handler drops the request body
                // early (e.g. unary handlers drop it right after decoding the
                // message): an RST here can overtake the response frames and
                // cancel a perfectly good RPC. Unlike TCP-based HTTP/2 there
                // is no bandwidth to save — an early-finishing stream's
                // remaining inbound DATA is bounded by the flow-control
                // window and simply ignored, and the client stops pumping
                // when the response finishes the stream.
                Body::new(ShmBody::new(body_rx, credit.clone(), Box::new(|| {})))
            };
            let req = req.map(|()| req_body);

            conn.active.fetch_add(1, Ordering::AcqRel);
            // Insert BEFORE spawning: the handler looks its window up by id,
            // and a fast handler must find the entry.
            conn.streams.lock().unwrap().insert(
                id,
                ServerEntry {
                    body_tx: if request_ended { None } else { Some(body_tx) },
                    credit,
                    window,
                    task: None,
                },
            );
            let task = tokio::spawn(handle_stream(conn.clone(), id, req, service.clone()));
            let mut table = conn.streams.lock().unwrap();
            if let Some(entry) = table.get_mut(&id) {
                entry.task = Some(task.abort_handle());
            } else {
                // Reset while we were spawning.
                task.abort();
            }
            Ok(())
        }
        FrameType::Data => {
            let end = frame.end_stream();
            let mut table = conn.streams.lock().unwrap();
            let Some(entry) = table.get_mut(&frame.stream_id) else {
                return Ok(()); // stream already finished/reset
            };
            if !frame.payload.is_empty() {
                if !entry.credit.on_data(frame.payload.len()) {
                    return Err(ProtocolError(
                        "peer overran stream flow-control window".into(),
                    ));
                }
                if let Some(tx) = &entry.body_tx {
                    let _ = tx.send(Ok(BodyEvent::Data(frame.payload)));
                }
            }
            if end {
                // Half-close: end the request body stream.
                entry.body_tx = None;
            }
            Ok(())
        }
        FrameType::RstStream => {
            let _code = frame::decode_rst(frame.payload)?;
            let entry = conn.streams.lock().unwrap().remove(&frame.stream_id);
            if let Some(e) = entry {
                if let Some(task) = &e.task {
                    task.abort();
                }
                let left = conn.active.fetch_sub(1, Ordering::AcqRel) - 1;
                if left == 0 {
                    conn.drained.notify_waiters();
                }
            }
            Ok(())
        }
        FrameType::WindowUpdate => {
            let incr = frame::decode_window_update(frame.payload)?;
            let table = conn.streams.lock().unwrap();
            if let Some(entry) = table.get(&frame.stream_id) {
                entry.window.add(incr);
            }
            Ok(())
        }
        FrameType::Trailers => {
            // gRPC clients do not send trailers; tolerate and ignore.
            Ok(())
        }
        FrameType::GoAway => {
            // A client drain notice; nothing to do (the client just stops
            // opening streams).
            Ok(())
        }
        FrameType::Ping => {
            frame.validate_ping()?;
            if frame.flags & FLAG_ACK == 0 {
                // Fail the connection on a PING flood (peer keeps sending while
                // refusing to drain our outbound ring) instead of pinning an
                // unbounded queue of acks.
                if !conn.io.queue_ping_ack() {
                    return Err(ProtocolError("peer flooded unacknowledged PINGs".into()));
                }
                conn.write.send_ctrl(WriteOp::Frame {
                    typ: FrameType::Ping,
                    flags: FLAG_ACK,
                    stream_id: 0,
                    payload: frame.payload,
                });
            }
            Ok(())
        }
    }
}

/// Runs one stream: calls the service and pumps the response back.
async fn handle_stream<S>(conn: Arc<ServerConn>, id: u32, req: Request<Body>, mut service: S)
where
    S: ServeService,
{
    let resp = match service.call(req).await {
        Ok(resp) => resp,
        Err(never) => match never {},
    };
    let (parts, body) = resp.into_parts();
    let window = {
        let table = conn.streams.lock().unwrap();
        match table.get(&id) {
            Some(e) => e.window.clone(),
            None => return, // reset while the handler ran
        }
    };

    let mut body = std::pin::pin!(body);
    let immediate_end = body.is_end_stream();
    let head = frame::encode_header_block(None, None, Some(parts.status.as_u16()), &parts.headers);
    let head_flags = if immediate_end { FLAG_END_STREAM } else { 0 };
    if conn
        .write
        .data_tx
        .send(WriteOp::Frame {
            typ: FrameType::Headers,
            flags: head_flags,
            stream_id: id,
            payload: head,
        })
        .await
        .is_err()
    {
        conn.finish_stream(id);
        return;
    }
    if immediate_end {
        conn.finish_stream(id);
        return;
    }

    loop {
        let item = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await;
        match item {
            Some(Ok(f)) => {
                if f.is_data() {
                    let mut data: Bytes = f.into_data().unwrap_or_default();
                    while !data.is_empty() {
                        let want = data.len().min(conn.max_frame);
                        let granted = match window.acquire(want, &conn.io).await {
                            Ok(g) => g,
                            Err(_) => {
                                conn.finish_stream(id);
                                return;
                            }
                        };
                        let piece = data.split_to(granted);
                        if conn
                            .write
                            .data_tx
                            .send(WriteOp::Data {
                                stream_id: id,
                                payload: piece,
                                end_stream: false,
                            })
                            .await
                            .is_err()
                        {
                            conn.finish_stream(id);
                            return;
                        }
                    }
                } else if let Ok(trailers) = f.into_trailers() {
                    send_trailers(&conn, id, &trailers).await;
                    conn.finish_stream(id);
                    return;
                }
            }
            Some(Err(status)) => {
                // The response body failed (a tonic Status): deliver it as
                // trailers, exactly as tonic's HTTP/2 server path would.
                let trailers = status_trailers(&status);
                send_trailers(&conn, id, &trailers).await;
                conn.finish_stream(id);
                return;
            }
            None => {
                // Body ended without trailers (non-gRPC response, e.g. an
                // axum fallback 404): plain end-of-stream.
                let _ = conn
                    .write
                    .data_tx
                    .send(WriteOp::Data {
                        stream_id: id,
                        payload: Bytes::new(),
                        end_stream: true,
                    })
                    .await;
                conn.finish_stream(id);
                return;
            }
        }
    }
}

async fn send_trailers(conn: &Arc<ServerConn>, id: u32, trailers: &HeaderMap) {
    let payload = frame::encode_header_block(None, None, None, trailers);
    // Trailers ride the ordered data queue so they can never overtake this
    // stream's queued DATA (the Go engine's "trailer sentinel" lesson).
    let _ = conn
        .write
        .data_tx
        .send(WriteOp::Frame {
            typ: FrameType::Trailers,
            flags: FLAG_END_STREAM,
            stream_id: id,
            payload,
        })
        .await;
}

fn status_trailers(status: &Status) -> HeaderMap {
    let mut hm = HeaderMap::new();
    if status.add_header(&mut hm).is_err() {
        hm.insert("grpc-status", http::HeaderValue::from_static("13"));
    }
    hm
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// Cleanup must remove only the exact socket this listener bound (by
    /// dev+inode), never a replacement listener's file that rebound the same
    /// path during a rolling restart — the shutdown-unlink hazard from the
    /// adversarial review.
    #[test]
    fn unlink_if_ours_matches_by_inode() {
        let path = std::env::temp_dir().join(format!("shmsc-unlink-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);

        // Bind a real socket so the path names one; capture its identity.
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let ours = sock_ident(&path);
        assert!(ours.is_some());

        // Simulate a replacement: a different file (different inode) at the path.
        drop(listener);
        let _ = std::fs::remove_file(&path);
        std::fs::write(&path, b"replacement").unwrap();

        // Cleanup keyed on the OLD identity must NOT delete the replacement.
        unlink_if_ours(&path, ours);
        assert!(
            path.exists(),
            "must not delete a different inode at the path"
        );

        // Cleanup keyed on the CURRENT identity removes exactly that file.
        let cur = sock_ident(&path);
        unlink_if_ours(&path, cur);
        assert!(!path.exists(), "must remove the file it identifies");

        let _ = std::fs::remove_file(&path);
    }
}
