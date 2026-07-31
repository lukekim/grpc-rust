//! Client-side connection: dial + handshake, the demux loop, and the tower
//! `Service` that tonic drives.
//!
//! Frame ordering rule (learned the hard way by the Go engine): everything
//! that must stay ordered *within a stream* — HEADERS, DATA, TRAILERS — goes
//! through the bounded, FIFO data queue. Only stream-independent control
//! (WINDOW_UPDATE, RST_STREAM, PING, GOAWAY) may take the priority control
//! queue, otherwise a trailer could overtake its own stream's parked DATA.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use http::{HeaderMap, Request, Response, StatusCode};
use http_body::Body as HttpBody;
use tokio::sync::{mpsc, oneshot};
use tonic::Status;
use tonic::body::Body;

use super::conn::{
    BodyEvent, CloseReason, Config, ConnIo, RecvCredit, SendWindow, ShmBody, SpinBudget,
    WriteHandle, WriteOp, client_handshake, read_frame, writer_loop,
};
use super::frame::{self, FLAG_ACK, Frame, FrameType, ProtocolError, RstCode};
use super::ring::{Consumer, Producer};

/// Highest usable stream id; beyond it the connection must be replaced.
const MAX_STREAM_ID: u32 = i32::MAX as u32;
/// Capacity of the bounded data queue into the writer task.
pub(crate) const DATA_QUEUE_DEPTH: usize = 128;

/// Response head delivered to the RPC future when server HEADERS arrive.
#[derive(Debug)]
struct RespHead {
    status: StatusCode,
    headers: HeaderMap,
    end_stream: bool,
}

struct ClientEntry {
    resp_tx: Option<oneshot::Sender<Result<RespHead, Status>>>,
    body_tx: mpsc::UnboundedSender<Result<BodyEvent, Status>>,
    credit: Arc<RecvCredit>,
    window: Arc<SendWindow>,
    pump: Option<tokio::task::AbortHandle>,
}

#[derive(Default)]
struct StreamTable {
    map: HashMap<u32, ClientEntry>,
}

/// Client connection state shared by the service handle, demux and pumps.
pub(crate) struct ClientConn {
    io: Arc<ConnIo>,
    write: WriteHandle,
    streams: std::sync::Mutex<StreamTable>,
    next_id: AtomicU32,
    /// Serializes stream-id assignment with the HEADERS enqueue so ids appear
    /// on the wire in increasing order — the invariant the server (like the
    /// Go engine and HTTP/2) enforces. Without it, two concurrent calls can
    /// race between `next_id` and the FIFO queue and arrive inverted.
    open_mu: tokio::sync::Mutex<()>,
    draining: AtomicBool,
    /// Peer-granted per-stream window (from the hello), both directions.
    window: u32,
    max_frame: usize,
}

impl std::fmt::Debug for ClientConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConn")
            .field("closed", &self.io.is_closed())
            .finish()
    }
}

impl ClientConn {
    fn remove_entry(&self, id: u32) -> Option<ClientEntry> {
        let entry = self.streams.lock().unwrap().map.remove(&id);
        if let Some(e) = &entry
            && let Some(p) = &e.pump
        {
            p.abort();
        }
        entry
    }

    /// Fails every live stream with `status` (connection-fatal path).
    fn fail_all(&self, status: &Status) {
        let entries: Vec<ClientEntry> = {
            let mut t = self.streams.lock().unwrap();
            t.map.drain().map(|(_, e)| e).collect()
        };
        for mut e in entries {
            if let Some(p) = &e.pump {
                p.abort();
            }
            if let Some(tx) = e.resp_tx.take() {
                let _ = tx.send(Err(status.clone()));
            } else {
                let _ = e.body_tx.send(Err(status.clone()));
            }
        }
    }

    fn status_for_close(reason: &CloseReason) -> Status {
        Status::unavailable(format!("shmsc connection lost: {reason}"))
    }
}

impl Drop for ClientConn {
    fn drop(&mut self) {
        self.io.close(CloseReason::Local);
    }
}

/// RAII cleanup for a stream that has been inserted into the table but whose
/// RPC future may still be dropped before `ShmBody` takes over cancellation —
/// i.e. while awaiting queue capacity for HEADERS or while awaiting the
/// response head. On drop while armed it removes the table entry (which aborts
/// the request pump) and sends RST_STREAM(Cancel) *after* HEADERS, so the
/// server cancels the stream rather than running an orphaned handler. Disarmed
/// once the head has arrived and `ShmBody` (streaming) or a clean END_STREAM
/// (unary) owns cancellation.
struct PendingStreamGuard {
    conn: Arc<ClientConn>,
    id: u32,
    armed: bool,
}

impl PendingStreamGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingStreamGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let conn = self.conn.clone();
        let id = self.id;
        // Drop cannot await, and the RST must follow HEADERS on the *ordered*
        // data queue: a control-queue RST could overtake a still-queued HEADERS
        // and reach the server first, which would then open and orphan the
        // handler. So run the ordered cleanup on a detached task; fall back to
        // the priority queue only if there is no runtime to spawn on.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    conn.remove_entry(id);
                    let _ = conn
                        .write
                        .data_tx
                        .send(WriteOp::Frame {
                            typ: FrameType::RstStream,
                            flags: 0,
                            stream_id: id,
                            payload: frame::encode_rst(RstCode::Cancel),
                        })
                        .await;
                });
            }
            Err(_) => {
                conn.remove_entry(id);
                conn.write.send_ctrl(WriteOp::Frame {
                    typ: FrameType::RstStream,
                    flags: 0,
                    stream_id: id,
                    payload: frame::encode_rst(RstCode::Cancel),
                });
            }
        }
    }
}

/// Dials the platform rendezvous address (Unix socket path / named pipe).
#[cfg(unix)]
async fn dial(path: &std::path::Path) -> io::Result<super::conduit::Conduit> {
    tokio::net::UnixStream::connect(path).await
}

#[cfg(windows)]
async fn dial(path: &std::path::Path) -> io::Result<super::conduit::Conduit> {
    use tokio::net::windows::named_pipe::ClientOptions;
    const ERROR_PIPE_BUSY: i32 = 231;
    let name = super::conduit::pipe_name(path);
    // All pipe instances may momentarily be taken between a client connecting
    // and the server creating the next instance; retry briefly on BUSY only
    // (a missing pipe means no server — surface that to the caller).
    let mut attempts = 0u32;
    loop {
        match ClientOptions::new().open(&name) {
            Ok(client) => return Ok(super::conduit::Conduit::Client(client)),
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) && attempts < 100 => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Dials an shmsc server at the given rendezvous address and returns a
/// connected channel (a cloneable tower `Service`).
pub(crate) async fn connect(path: &std::path::Path, cfg: Config) -> io::Result<ShmscChannel> {
    let cfg = cfg.validated()?;
    let sock = dial(path).await?;
    let (seg, hello) = client_handshake(&sock).await?;
    let seg = Arc::new(seg);
    let io = ConnIo::new(sock);

    // Client produces into c2s, consumes from s2c.
    let l = seg.layout;
    // SAFETY: offsets come from the validated layout of a mapping that the
    // producer/consumer keep alive via the Arc<Segment> captured below; each
    // side of each ring has exactly one owner (us or the peer process).
    let producer = unsafe {
        Producer::new(
            seg.base().add(l.c2s_hdr),
            seg.base().add(l.c2s_data),
            l.c2s_cap,
        )
    };
    // SAFETY: as above, for the consuming side of s2c.
    let consumer = unsafe {
        Consumer::new(
            seg.base().add(l.s2c_hdr),
            seg.base().add(l.s2c_data),
            l.s2c_cap,
        )
    };

    let (data_tx, data_rx) = mpsc::channel(DATA_QUEUE_DEPTH);
    let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel();
    let write = WriteHandle { data_tx, ctrl_tx };

    let conn = Arc::new(ClientConn {
        io: io.clone(),
        write: write.clone(),
        streams: std::sync::Mutex::new(StreamTable::default()),
        next_id: AtomicU32::new(1),
        open_mu: tokio::sync::Mutex::new(()),
        draining: AtomicBool::new(false),
        window: hello.stream_window,
        max_frame: cfg.max_frame,
    });

    // Doorbell task.
    {
        let io = io.clone();
        let seg = seg.clone();
        tokio::spawn(async move {
            io.doorbell_loop().await;
            drop(seg);
        });
    }
    // Writer task.
    {
        let io = io.clone();
        let seg = seg.clone();
        let spin = cfg.spin_us;
        tokio::spawn(async move {
            writer_loop(producer, io, data_rx, ctrl_rx, spin).await;
            drop(seg);
        });
    }
    // Demux task. It holds only a *weak* reference to the connection: the
    // externally owned lifetime is the channel handle plus any in-flight RPC
    // (call future, request pump, live response body). When all of those drop,
    // `ClientConn::drop` runs `io.close`, which wakes this loop — otherwise a
    // demux parked waiting for peer traffic would pin the connection (socket,
    // mapping, writer/doorbell tasks, and the server-side peer) forever after
    // the user is done with it.
    {
        let weak = Arc::downgrade(&conn);
        let io = io.clone();
        let seg = seg.clone();
        let spin_us = cfg.spin_us;
        tokio::spawn(async move {
            demux_loop(weak, io, consumer, spin_us).await;
            drop(seg);
        });
    }

    Ok(ShmscChannel { conn })
}

async fn demux_loop(conn: Weak<ClientConn>, io: Arc<ConnIo>, mut consumer: Consumer, spin_us: u32) {
    let mut bell = io.bell();
    let mut spin = SpinBudget::new(spin_us);
    let mut arena = BytesMut::new();
    let reason = loop {
        match read_frame(&mut consumer, &mut bell, &io, &mut spin, &mut arena).await {
            Ok(frame) => {
                // Upgrade per frame: once every external owner has dropped there
                // is nothing left to dispatch to, so close and stop.
                let Some(conn) = conn.upgrade() else {
                    io.close(CloseReason::Local);
                    break io.close_reason();
                };
                if let Err(e) = handle_frame(&conn, frame) {
                    io.close(CloseReason::Protocol(e.0));
                    break io.close_reason();
                }
            }
            Err(reason) => break reason,
        }
    };
    if let Some(conn) = conn.upgrade() {
        conn.fail_all(&ClientConn::status_for_close(&reason));
    }
}

fn handle_frame(conn: &Arc<ClientConn>, frame: Frame) -> Result<(), ProtocolError> {
    match frame.typ {
        FrameType::Headers => {
            let end = frame.end_stream();
            let stream_id = frame.stream_id;
            let block = frame::decode_header_block(frame.payload)?;
            let mut table = conn.streams.lock().unwrap();
            let Some(entry) = table.map.get_mut(&stream_id) else {
                return Ok(()); // stream already reset locally
            };
            let Some(tx) = entry.resp_tx.take() else {
                return Err(ProtocolError("duplicate HEADERS for stream".into()));
            };
            let status = block
                .status
                .and_then(|s| StatusCode::from_u16(s).ok())
                .unwrap_or(StatusCode::OK);
            let _ = tx.send(Ok(RespHead {
                status,
                headers: block.headers,
                end_stream: end,
            }));
            if end {
                drop(table);
                conn.remove_entry(stream_id);
            }
            Ok(())
        }
        FrameType::Data => {
            let end = frame.end_stream();
            let stream_id = frame.stream_id;
            let mut table = conn.streams.lock().unwrap();
            let Some(entry) = table.map.get_mut(&stream_id) else {
                return Ok(());
            };
            if !frame.payload.is_empty() {
                if !entry.credit.on_data(frame.payload.len()) {
                    return Err(ProtocolError(
                        "peer overran stream flow-control window".into(),
                    ));
                }
                let _ = entry.body_tx.send(Ok(BodyEvent::Data(frame.payload)));
            }
            if end {
                drop(table);
                conn.remove_entry(stream_id);
            }
            Ok(())
        }
        FrameType::Trailers => {
            let block = frame::decode_header_block(frame.payload)?;
            let Some(entry) = conn.remove_entry(frame.stream_id) else {
                return Ok(());
            };
            if entry.resp_tx.is_some() {
                return Err(ProtocolError("TRAILERS before HEADERS".into()));
            }
            let _ = entry.body_tx.send(Ok(BodyEvent::Trailers(block.headers)));
            Ok(())
        }
        FrameType::RstStream => {
            let code = frame::decode_rst(frame.payload)?;
            let Some(mut entry) = conn.remove_entry(frame.stream_id) else {
                return Ok(());
            };
            let status = match code {
                RstCode::Cancel => Status::cancelled("stream reset by server"),
                RstCode::Refused => Status::unavailable("stream refused by server"),
                RstCode::Internal => Status::internal("stream failed on server transport"),
            };
            if let Some(tx) = entry.resp_tx.take() {
                let _ = tx.send(Err(status));
            } else {
                let _ = entry.body_tx.send(Err(status));
            }
            Ok(())
        }
        FrameType::WindowUpdate => {
            let incr = frame::decode_window_update(frame.payload)?;
            let table = conn.streams.lock().unwrap();
            if let Some(entry) = table.map.get(&frame.stream_id) {
                entry.window.add(incr);
            }
            Ok(())
        }
        FrameType::GoAway => {
            let (last, msg) = frame::decode_goaway(frame.payload)?;
            conn.draining.store(true, Ordering::Release);
            // Streams the server will never process fail immediately; they
            // are safe to retry on another connection.
            let doomed: Vec<u32> = {
                let t = conn.streams.lock().unwrap();
                t.map.keys().copied().filter(|id| *id > last).collect()
            };
            let status = Status::unavailable(if msg.is_empty() {
                "connection draining (GOAWAY)".to_string()
            } else {
                format!("connection draining (GOAWAY): {msg}")
            });
            for id in doomed {
                if let Some(mut e) = conn.remove_entry(id) {
                    if let Some(tx) = e.resp_tx.take() {
                        let _ = tx.send(Err(status.clone()));
                    } else {
                        let _ = e.body_tx.send(Err(status.clone()));
                    }
                }
            }
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

/// A connected shmsc client channel: a cloneable tower service speaking
/// `http::Request<tonic::body::Body>` → `http::Response<tonic::body::Body>`
/// over the shared-memory connection.
#[derive(Clone, Debug)]
pub(crate) struct ShmscChannel {
    conn: Arc<ClientConn>,
}

impl tower_service::Service<Request<Body>> for ShmscChannel {
    type Response = Response<Body>;
    type Error = crate::BoxError;
    type Future = std::pin::Pin<
        Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.conn.io.is_closed() {
            return Poll::Ready(Err(Box::new(ClientConn::status_for_close(
                &self.conn.io.close_reason(),
            ))));
        }
        if self.conn.draining.load(Ordering::Acquire) {
            return Poll::Ready(Err(Box::new(Status::unavailable(
                "shmsc connection is draining (GOAWAY)",
            ))));
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let conn = self.conn.clone();
        Box::pin(async move { call_inner(conn, req).await })
    }
}

async fn call_inner(
    conn: Arc<ClientConn>,
    req: Request<Body>,
) -> Result<Response<Body>, crate::BoxError> {
    if conn.io.is_closed() {
        return Err(Box::new(ClientConn::status_for_close(
            &conn.io.close_reason(),
        )));
    }
    if conn.draining.load(Ordering::Acquire) {
        return Err(Box::new(Status::unavailable(
            "shmsc connection is draining (GOAWAY)",
        )));
    }
    let (parts, body) = req.into_parts();
    let path = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str().to_owned())
        .unwrap_or_else(|| parts.uri.path().to_owned());
    let authority = parts.uri.authority().map(|a| a.as_str().to_owned());
    let block = frame::encode_header_block(Some(&path), authority.as_deref(), None, &parts.headers);

    let (resp_tx, resp_rx) = oneshot::channel();
    let (body_tx, body_rx) = mpsc::unbounded_channel();

    // Assign the id and enqueue HEADERS under the open lock: stream ids must
    // hit the wire in increasing order (the server, like the Go engine and
    // HTTP/2, rejects a non-increasing id). Only stream OPENS serialize here;
    // the writer task drains the queue independently, so this cannot
    // deadlock against a full queue.
    let (id, credit, window, mut guard) = {
        let _open = conn.open_mu.lock().await;
        let id = conn.next_id.fetch_add(2, Ordering::AcqRel);
        if id > MAX_STREAM_ID {
            conn.io.close(CloseReason::Local);
            return Err(Box::new(Status::unavailable(
                "shmsc connection exhausted stream ids",
            )));
        }
        let credit = RecvCredit::new(id, conn.window, conn.write.ctrl_tx.clone());
        let window = SendWindow::new(conn.window);
        {
            let mut table = conn.streams.lock().unwrap();
            table.map.insert(
                id,
                ClientEntry {
                    resp_tx: Some(resp_tx),
                    body_tx,
                    credit: credit.clone(),
                    window: window.clone(),
                    pump: None,
                },
            );
        }
        // Arm cleanup now: from here until the response head arrives, dropping
        // this future (deadline/cancel) must remove the entry, abort the pump,
        // and RST the stream so the server does not run an orphaned handler.
        let mut guard = PendingStreamGuard {
            conn: conn.clone(),
            id,
            armed: true,
        };
        // HEADERS first (FIFO with this stream's DATA); the request pump is
        // spawned after the lock drops. Dropping the future during this await
        // (queue full) triggers the guard.
        if conn
            .write
            .data_tx
            .send(WriteOp::Frame {
                typ: FrameType::Headers,
                flags: 0,
                stream_id: id,
                payload: block,
            })
            .await
            .is_err()
        {
            // HEADERS never reached the wire and the connection is gone, so the
            // guard's ordered RST would be pointless; clean up directly.
            guard.disarm();
            conn.remove_entry(id);
            return Err(Box::new(ClientConn::status_for_close(
                &conn.io.close_reason(),
            )));
        }
        (id, credit, window, guard)
    };

    let pump = tokio::spawn(pump_request_body(conn.clone(), id, body, window));
    if let Some(entry) = conn.streams.lock().unwrap().map.get_mut(&id) {
        entry.pump = Some(pump.abort_handle());
    } else {
        // The stream already finished (fast server) — stop the pump.
        pump.abort();
    }

    // Await the response head. Until it arrives the guard stays armed, so a
    // dropped or timed-out call future is cleaned up and the stream is RST.
    let head = match resp_rx.await {
        Ok(Ok(head)) => head,
        // Server error/RST (entry already removed by the demux) or a closed
        // connection: the still-armed guard cleans up on drop.
        Ok(Err(status)) => return Err(Box::new(status)),
        Err(_) => {
            return Err(Box::new(ClientConn::status_for_close(
                &conn.io.close_reason(),
            )));
        }
    };
    // Head received: ShmBody (streaming) or a clean END_STREAM (unary) now owns
    // cancellation, so stand the pre-header guard down.
    guard.disarm();

    let body = if head.end_stream {
        Body::new(ShmBody::empty(body_rx, credit))
    } else {
        let rst_conn = conn.clone();
        Body::new(ShmBody::new(
            body_rx,
            credit,
            Box::new(move || {
                rst_conn.remove_entry(id);
                rst_conn.write.send_ctrl(WriteOp::Frame {
                    typ: FrameType::RstStream,
                    flags: 0,
                    stream_id: id,
                    payload: frame::encode_rst(RstCode::Cancel),
                });
            }),
        ))
    };
    let mut resp = Response::new(body);
    *resp.status_mut() = head.status;
    *resp.headers_mut() = head.headers;
    *resp.version_mut() = http::Version::HTTP_2;
    Ok(resp)
}

/// Pumps the request body into the writer queue under the stream's send
/// window, ending with an empty END_STREAM DATA frame (the half-close).
async fn pump_request_body(conn: Arc<ClientConn>, id: u32, body: Body, window: Arc<SendWindow>) {
    let mut body = std::pin::pin!(body);
    loop {
        let frame_item = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await;
        match frame_item {
            Some(Ok(f)) => {
                if let Ok(mut data) = f.into_data() {
                    while !data.is_empty() {
                        let want = data.len().min(conn.max_frame);
                        let granted = match window.acquire(want, &conn.io).await {
                            Ok(g) => g,
                            Err(_) => return,
                        };
                        let piece: Bytes = data.split_to(granted);
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
                            return;
                        }
                    }
                }
                // Trailer frames from a client body are not part of gRPC;
                // ignore them (tonic clients never produce any).
            }
            Some(Err(status)) => {
                // The request stream failed locally: reset so the server
                // stops waiting, and surface nothing further.
                let _ = status;
                conn.remove_entry(id);
                conn.write.send_ctrl(WriteOp::Frame {
                    typ: FrameType::RstStream,
                    flags: 0,
                    stream_id: id,
                    payload: frame::encode_rst(RstCode::Cancel),
                });
                return;
            }
            None => {
                let _ = conn
                    .write
                    .data_tx
                    .send(WriteOp::Data {
                        stream_id: id,
                        payload: Bytes::new(),
                        end_stream: true,
                    })
                    .await;
                return;
            }
        }
    }
}
