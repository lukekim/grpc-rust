//! Shared per-connection machinery: handshake, doorbells, the writer task,
//! flow-control windows, receive credit, and the stream body type.
//!
//! # Task model (per connection, per process)
//!
//! - a **doorbell task** owns the read side of the rendezvous socket. Every
//!   inbound byte means "re-check your rings"; it bumps a `watch` counter that
//!   the writer and demux tasks wait on. Socket EOF or error closes the
//!   connection — the socket doubles as the liveness channel, so a crashed
//!   peer can never strand a parked task.
//! - a **writer task** owns the outbound ring producer. It drains two queues —
//!   an unbounded control queue (WINDOW_UPDATE, RST_STREAM, PING, GOAWAY;
//!   these must never be dropped or the peer stalls) and a bounded data queue
//!   (HEADERS and DATA) — control first, and parks on the doorbell when the
//!   ring is full.
//! - a **demux task** (in `client.rs` / `server.rs`) owns the inbound ring
//!   consumer, parses frames, and dispatches them to per-stream queues. It
//!   never blocks on a stream: per-stream buffering is bounded by the
//!   flow-control window the peer was granted, so draining the ring promptly
//!   can never be head-of-line blocked by a slow stream.
//!
//! # Flow control
//!
//! Per-stream credit windows over DATA payload bytes, HTTP/2-style. The
//! connection level has no explicit window — the ring itself is the
//! connection-level backpressure (as in the Go engine, which sets the
//! connection window to 2^31-1 for the same reason). WINDOW_UPDATE emission
//! follows the Go engine's hard-learned formula: grant when consumed credit
//! reaches `window / 4`, floored at 1 KiB and capped at `window / 2` —
//! see `wu_threshold`.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use bytes::{Bytes, BytesMut};
use tokio::io::Interest;
use tokio::sync::{Notify, mpsc, watch};

use super::conduit::{self, Conduit};
use super::frame::{FRAME_HDR_SIZE, Frame, FrameType, encode_frame_header};
use super::ring::{Consumer, Producer};
use super::seg::{Layout, MAX_RING_CAP, MIN_RING_CAP, Segment};

/// Tuning knobs for one connection / listener.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    /// Capacity of each ring (power of two).
    pub(crate) ring_size: usize,
    /// Initial per-stream flow-control window (DATA payload bytes).
    pub(crate) stream_window: u32,
    /// Largest DATA payload emitted in one frame; larger messages are chunked.
    pub(crate) max_frame: usize,
    /// Microseconds to busy-spin re-checking the ring before parking on the
    /// doorbell. Under back-to-back traffic the peer's next action usually
    /// lands within this budget, eliding the parked flag, the doorbell
    /// syscall, and the task wake entirely (and, on hardware with deep
    /// cpuidle states, the idle-exit latency those wakes pay). 0 disables.
    pub(crate) spin_us: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            // The Go engine defaults to 64 MiB rings and a 32 MiB stream
            // window, tuned for peak throughput benchmarks. These defaults
            // are a quarter of that — plenty for multi-GB/s pipes — and
            // configurable upward.
            ring_size: 16 * 1024 * 1024,
            stream_window: 8 * 1024 * 1024,
            max_frame: 1024 * 1024,
            spin_us: 0,
        }
    }
}

impl Config {
    /// Clamps and validates the configuration.
    pub(crate) fn validated(mut self) -> io::Result<Self> {
        if !self.ring_size.is_power_of_two()
            || !(MIN_RING_CAP..=MAX_RING_CAP).contains(&self.ring_size)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "shmsc: ring_size {} must be a power of two in [4 KiB, 1 GiB]",
                    self.ring_size
                ),
            ));
        }
        if self.stream_window == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shmsc: zero stream window",
            ));
        }
        self.max_frame = self.max_frame.clamp(4096, super::frame::MAX_FRAME_PAYLOAD);
        Ok(self)
    }

    /// The WINDOW_UPDATE emission threshold for `window`: `window / 4`,
    /// floored at 1 KiB and capped at `window / 2`. The floor and cap matter:
    /// a small window inheriting a large absolute threshold would never emit
    /// an update and the sender would park forever (a real deadlock the Go
    /// engine documents at length).
    pub(crate) fn wu_threshold(window: u32) -> u32 {
        (window / 4).clamp(1024.min(window / 2).max(1), (window / 2).max(1))
    }
}

/// The 32-byte hello message sent by the server alongside the segment fd.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Hello {
    pub(crate) c2s_cap: u64,
    pub(crate) s2c_cap: u64,
    pub(crate) stream_window: u32,
}

pub(crate) const HELLO_SIZE: usize = 32;
const HELLO_ACK: u8 = 0xA5;

impl Hello {
    pub(crate) fn encode(&self) -> [u8; HELLO_SIZE] {
        let mut b = [0u8; HELLO_SIZE];
        b[0..8].copy_from_slice(&super::seg::MAGIC.to_le_bytes());
        b[8..12].copy_from_slice(&super::seg::VERSION.to_le_bytes());
        b[12..16].copy_from_slice(&self.stream_window.to_le_bytes());
        b[16..24].copy_from_slice(&self.c2s_cap.to_le_bytes());
        b[24..32].copy_from_slice(&self.s2c_cap.to_le_bytes());
        b
    }

    pub(crate) fn decode(b: &[u8]) -> io::Result<Self> {
        if b.len() != HELLO_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shmsc: bad hello size",
            ));
        }
        let magic = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let version = u32::from_le_bytes(b[8..12].try_into().unwrap());
        if magic != super::seg::MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shmsc: bad hello magic",
            ));
        }
        if version != super::seg::VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("shmsc: protocol version {version} not supported"),
            ));
        }
        let stream_window = u32::from_le_bytes(b[12..16].try_into().unwrap());
        let c2s_cap = u64::from_le_bytes(b[16..24].try_into().unwrap());
        let s2c_cap = u64::from_le_bytes(b[24..32].try_into().unwrap());
        if stream_window == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shmsc: zero window in hello",
            ));
        }
        Ok(Hello {
            c2s_cap,
            s2c_cap,
            stream_window,
        })
    }
}

/// Server side of the handshake: create the segment, share it (fd on Unix,
/// section name on Windows), await the ack.
#[cfg(unix)]
pub(crate) async fn server_handshake(sock: &Conduit, cfg: &Config) -> io::Result<Segment> {
    use std::os::fd::AsRawFd;
    let layout = Layout::new(cfg.ring_size, cfg.ring_size)?;
    let seg = Segment::create(layout)?;
    let hello = Hello {
        c2s_cap: layout.c2s_cap as u64,
        s2c_cap: layout.s2c_cap as u64,
        stream_window: cfg.stream_window,
    };
    let payload = hello.encode();
    sock.async_io(Interest::WRITABLE, || {
        super::seg::send_with_fd(sock.as_raw_fd(), &payload, seg.fd())
    })
    .await?;
    read_ack(sock).await?;
    Ok(seg)
}

/// Client side of the handshake: receive the shared segment (fd on Unix,
/// section name on Windows), map and validate it, send the ack.
#[cfg(unix)]
pub(crate) async fn client_handshake(sock: &Conduit) -> io::Result<(Segment, Hello)> {
    use std::os::fd::AsRawFd;
    let (payload, fd) = sock
        .async_io(Interest::READABLE, || {
            super::seg::recv_with_fd(sock.as_raw_fd(), HELLO_SIZE)
        })
        .await?;
    let hello = Hello::decode(&payload)?;
    // Layout::new re-validates the peer-supplied capacities (power of two,
    // bounded); from_fd validates the fd's actual size and the stamped header.
    let layout = Layout::new(hello.c2s_cap as usize, hello.s2c_cap as usize)?;
    let seg = Segment::from_fd(fd, layout)?;
    conduit::write_all(sock, &[HELLO_ACK]).await?;
    Ok((seg, hello))
}

/// Windows: the segment is a randomly named section; the hello is followed by
/// `name_len: u16 LE` and the UTF-8 section name, which the client opens.
#[cfg(windows)]
pub(crate) async fn server_handshake(sock: &Conduit, cfg: &Config) -> io::Result<Segment> {
    let layout = Layout::new(cfg.ring_size, cfg.ring_size)?;
    let (seg, name) = Segment::create_named(layout)?;
    let hello = Hello {
        c2s_cap: layout.c2s_cap as u64,
        s2c_cap: layout.s2c_cap as u64,
        stream_window: cfg.stream_window,
    };
    let mut msg = Vec::with_capacity(HELLO_SIZE + 2 + name.len());
    msg.extend_from_slice(&hello.encode());
    msg.extend_from_slice(&(name.len() as u16).to_le_bytes());
    msg.extend_from_slice(name.as_bytes());
    conduit::write_all(sock, &msg).await?;
    read_ack(sock).await?;
    Ok(seg)
}

/// Windows client side: see [`server_handshake`].
#[cfg(windows)]
pub(crate) async fn client_handshake(sock: &Conduit) -> io::Result<(Segment, Hello)> {
    let mut hello_buf = [0u8; HELLO_SIZE];
    conduit::read_exact(sock, &mut hello_buf).await?;
    let hello = Hello::decode(&hello_buf)?;
    let mut len_buf = [0u8; 2];
    conduit::read_exact(sock, &mut len_buf).await?;
    let name_len = usize::from(u16::from_le_bytes(len_buf));
    if name_len == 0 || name_len > super::seg::MAX_SECTION_NAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "shmsc: bad section name length in hello",
        ));
    }
    let mut name_buf = vec![0u8; name_len];
    conduit::read_exact(sock, &mut name_buf).await?;
    let name = String::from_utf8(name_buf)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "shmsc: non-utf8 section name"))?;
    let layout = Layout::new(hello.c2s_cap as usize, hello.s2c_cap as usize)?;
    let seg = Segment::open_named(&name, layout)?;
    conduit::write_all(sock, &[HELLO_ACK]).await?;
    Ok((seg, hello))
}

async fn read_ack(sock: &Conduit) -> io::Result<()> {
    let mut b = [0u8; 1];
    conduit::read_exact(sock, &mut b).await?;
    if b[0] != HELLO_ACK {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "shmsc: bad handshake ack",
        ));
    }
    Ok(())
}

/// Why a connection terminated.
#[derive(Debug, Clone)]
pub(crate) enum CloseReason {
    /// Locally initiated (transport dropped, server shutdown).
    Local,
    /// The rendezvous socket reached EOF: the peer went away.
    PeerGone,
    /// The peer violated the protocol.
    Protocol(String),
    /// An I/O error on the socket or segment.
    Io(String),
}

impl std::fmt::Display for CloseReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloseReason::Local => write!(f, "connection closed"),
            CloseReason::PeerGone => write!(f, "peer closed the connection"),
            CloseReason::Protocol(m) => write!(f, "protocol error: {m}"),
            CloseReason::Io(m) => write!(f, "i/o error: {m}"),
        }
    }
}

/// Shared close state + doorbell plumbing for one connection.
pub(crate) struct ConnIo {
    pub(crate) sock: Arc<Conduit>,
    closed: AtomicBool,
    reason: std::sync::Mutex<Option<CloseReason>>,
    bell_tx: watch::Sender<u64>,
    bell_rx: watch::Receiver<u64>,
    /// Notified once when the connection closes (any reason).
    pub(crate) on_close: Notify,
    /// Set when a graceful flush was requested: the writer must push every
    /// queued frame into the ring and wait for the peer to consume it before
    /// the connection is torn down, so a final response (its trailers in
    /// particular) is never lost to an abrupt close.
    graceful: AtomicBool,
    /// Wakes the (possibly idle) writer to observe `graceful`.
    graceful_wake: Notify,
    /// Notified once the writer has flushed everything and the outbound ring
    /// is empty.
    flushed: Notify,
}

impl std::fmt::Debug for ConnIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnIo")
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl ConnIo {
    pub(crate) fn new(sock: Conduit) -> Arc<Self> {
        let (bell_tx, bell_rx) = watch::channel(0u64);
        Arc::new(ConnIo {
            sock: Arc::new(sock),
            closed: AtomicBool::new(false),
            reason: std::sync::Mutex::new(None),
            bell_tx,
            bell_rx,
            on_close: Notify::new(),
            graceful: AtomicBool::new(false),
            graceful_wake: Notify::new(),
            flushed: Notify::new(),
        })
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn is_graceful(&self) -> bool {
        self.graceful.load(Ordering::Acquire)
    }

    /// Requests a graceful flush and waits (bounded) for the writer to push
    /// every queued frame into the ring and the peer to consume it, then
    /// returns. Called on the server's drain path before `close`.
    pub(crate) async fn graceful_flush(&self, timeout: std::time::Duration) {
        // Register interest before setting the flag so the writer's
        // notify_waiters after an immediate flush is never missed.
        let flushed = self.flushed.notified();
        tokio::pin!(flushed);
        flushed.as_mut().enable();
        self.graceful.store(true, Ordering::Release);
        self.graceful_wake.notify_waiters();
        let _ = tokio::time::timeout(timeout, flushed).await;
    }

    pub(crate) fn close_reason(&self) -> CloseReason {
        self.reason
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(CloseReason::Local)
    }

    /// Closes the connection once; later calls keep the first reason.
    pub(crate) fn close(&self, reason: CloseReason) {
        {
            let mut r = self.reason.lock().unwrap();
            if self.closed.swap(true, Ordering::AcqRel) {
                return;
            }
            *r = Some(reason);
        }
        // Wake every parked task: local waiters via the bell watch, the peer
        // via conduit shutdown (their doorbell task sees EOF / broken pipe).
        self.bell_tx.send_modify(|v| *v = v.wrapping_add(1));
        conduit::shutdown(&self.sock);
        self.on_close.notify_waiters();
    }

    /// A receiver for doorbell edges. Clone one per waiting task.
    pub(crate) fn bell(&self) -> watch::Receiver<u64> {
        self.bell_rx.clone()
    }

    /// Rings the peer's doorbell (best effort). `WouldBlock` is fine: the
    /// socket buffer holds unread doorbells, so the peer will wake anyway.
    pub(crate) fn ring_peer(&self) {
        let _ = self.sock.try_write(&[1]);
    }

    /// Runs the doorbell read loop until close. Bumps the local bell watch on
    /// every inbound byte batch; closes the connection on EOF or error.
    pub(crate) async fn doorbell_loop(self: &Arc<Self>) {
        let mut buf = [0u8; 64];
        loop {
            if self.is_closed() {
                return;
            }
            let ready = tokio::select! {
                r = self.sock.ready(Interest::READABLE) => r,
                _ = self.on_close.notified() => return,
            };
            match ready {
                Ok(_) => loop {
                    match self.sock.try_read(&mut buf) {
                        Ok(0) => {
                            self.close(CloseReason::PeerGone);
                            return;
                        }
                        Ok(_) => continue,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) => {
                            self.close(CloseReason::Io(e.to_string()));
                            return;
                        }
                    }
                },
                Err(e) => {
                    self.close(CloseReason::Io(e.to_string()));
                    return;
                }
            }
            self.bell_tx.send_modify(|v| *v = v.wrapping_add(1));
        }
    }
}

/// Adaptive pre-park spin state, one per waiting loop (writer or demux).
///
/// Spinning elides the park/doorbell/task-wake chain (and any cpuidle exit
/// latency behind it) — but only pays off when the peer's next action lands
/// within the budget. Request/response traffic has inter-message gaps of a
/// full RTT, where spinning is pure loss that lands exactly on the
/// latency-critical send path; streaming refills the ring within
/// microseconds, where spinning wins big. So the budget self-arms:
///
///   - a spin that HITS keeps spinning armed;
///   - a wait that had to PARK disarms it (the traffic is gappy);
///   - a park that resolved FASTER than the budget re-arms it (the gap was
///     small enough that spinning would have caught it).
///
/// The Go engine's adaptive EMA spin cutoffs are the smooth version of the
/// same idea.
#[derive(Debug)]
pub(crate) struct SpinBudget {
    us: u32,
    armed: bool,
}

impl SpinBudget {
    pub(crate) fn new(us: u32) -> Self {
        SpinBudget { us, armed: us > 0 }
    }
}

/// Spins for up to `spin_us` microseconds while `ready()` is false,
/// YIELDING to the runtime between short spin slices. Returns true if the
/// condition became true within the budget.
///
/// The yields are load-bearing, not politeness: a task the caller just woke
/// (e.g. the RPC future receiving the frame the demux dispatched a moment
/// ago) sits in THIS worker's LIFO slot, which other workers cannot steal —
/// a hard busy-spin here would hold that task hostage for the full budget
/// and show up directly as added RPC latency. (The Go engine's post-message
/// `runtime.Gosched()` exists for exactly the same reason.)
async fn spin_for(spin_us: u32, mut ready: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_micros(u64::from(spin_us));
    loop {
        for _ in 0..16 {
            if ready() {
                return true;
            }
            std::hint::spin_loop();
        }
        tokio::task::yield_now().await;
        if ready() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
    }
}

/// Waits until the outbound ring has free space (or the connection closes).
pub(crate) async fn wait_for_space(
    prod: &mut Producer,
    bell: &mut watch::Receiver<u64>,
    io: &ConnIo,
    spin: &mut SpinBudget,
) -> Result<(), CloseReason> {
    if spin.armed && spin_for(spin.us, || prod.free_space() > 0).await {
        return Ok(());
    }
    spin.armed = false;
    let parked_at = std::time::Instant::now();
    loop {
        if io.is_closed() {
            return Err(io.close_reason());
        }
        // Mark the current bell edge as seen BEFORE parking: any later ring
        // resolves `changed()`, so a wakeup can never be lost.
        bell.mark_unchanged();
        if prod.park_self() {
            prod.unpark_self();
            rearm_after_park(spin, parked_at);
            return Ok(());
        }
        if io.is_closed() {
            prod.unpark_self();
            return Err(io.close_reason());
        }
        let _ = bell.changed().await;
        prod.unpark_self();
        if prod.free_space() > 0 {
            rearm_after_park(spin, parked_at);
            return Ok(());
        }
    }
}

/// Waits until the inbound ring has data (or the connection closes).
pub(crate) async fn wait_for_data(
    cons: &mut Consumer,
    bell: &mut watch::Receiver<u64>,
    io: &ConnIo,
    spin: &mut SpinBudget,
) -> Result<(), CloseReason> {
    if spin.armed && spin_for(spin.us, || cons.available() > 0).await {
        return Ok(());
    }
    spin.armed = false;
    let parked_at = std::time::Instant::now();
    loop {
        if io.is_closed() {
            return Err(io.close_reason());
        }
        bell.mark_unchanged();
        if cons.park_self() {
            cons.unpark_self();
            rearm_after_park(spin, parked_at);
            return Ok(());
        }
        if io.is_closed() {
            cons.unpark_self();
            return Err(io.close_reason());
        }
        let _ = bell.changed().await;
        cons.unpark_self();
        if cons.available() > 0 {
            rearm_after_park(spin, parked_at);
            return Ok(());
        }
    }
}

/// Re-arms the spin iff the park resolved faster than the spin budget —
/// i.e. the gap was small enough that spinning would have caught it.
fn rearm_after_park(spin: &mut SpinBudget, parked_at: std::time::Instant) {
    if spin.us > 0 && parked_at.elapsed() < std::time::Duration::from_micros(u64::from(spin.us)) {
        spin.armed = true;
    }
}

/// Reads one complete frame directly from the ring: the 12-byte header is
/// validated first, then the payload is copied ONCE into `arena` (the
/// single copy on the receive path) and split off as the frame's `Bytes`.
/// Frames may be arbitrarily split across ring wraps and park/wake cycles.
///
/// `arena` is the demux loop's persistent receive buffer. `reserve`
/// reclaims the underlying allocation once downstream consumers drop their
/// payload `Bytes`, so steady-state traffic recycles ONE hot allocation
/// instead of paying a fresh (page-faulting) allocation per frame — the
/// same recv-arena shape hyper/h2 use, and worth >2x on large-message
/// throughput on cores where per-frame mmap faults are expensive.
pub(crate) async fn read_frame(
    cons: &mut Consumer,
    bell: &mut watch::Receiver<u64>,
    io: &ConnIo,
    spin: &mut SpinBudget,
    arena: &mut BytesMut,
) -> Result<Frame, CloseReason> {
    let mut hdr = [0u8; FRAME_HDR_SIZE];
    pop_exact(cons, bell, io, spin, &mut hdr).await?;
    let (typ, flags, stream_id, len) =
        super::frame::parse_frame_header(&hdr).map_err(|e| CloseReason::Protocol(e.0))?;
    let payload = if len == 0 {
        Bytes::new()
    } else {
        debug_assert!(arena.is_empty());
        arena.reserve(len);
        // SAFETY: set_len within just-reserved capacity is sound for u8 (no
        // invalid representations, no Drop), and pop_exact overwrites every
        // byte in 0..len before the buffer is exposed; on an error return
        // the bytes are truncated away. Skipping the zeroing avoids a full
        // extra write pass per payload.
        unsafe { arena.set_len(len) };
        match pop_exact(cons, bell, io, spin, arena).await {
            Ok(()) => arena.split_to(len).freeze(),
            Err(e) => {
                arena.clear();
                return Err(e);
            }
        }
    };
    Ok(Frame {
        typ,
        flags,
        stream_id,
        payload,
    })
}

/// Pops exactly `dst.len()` bytes from the ring, waking a space-parked peer
/// producer after each drained batch and parking for data as needed.
async fn pop_exact(
    cons: &mut Consumer,
    bell: &mut watch::Receiver<u64>,
    io: &ConnIo,
    spin: &mut SpinBudget,
    dst: &mut [u8],
) -> Result<(), CloseReason> {
    // Copy the ring out in bounded windows rather than one huge pop: the
    // consumer then trails the producer closely enough that ring bytes are
    // still cache-resident when loaded, which is worth ~2x on large-message
    // throughput on cores with modest last-level caches. (The producing side
    // needs no cap: its stores stream to memory either way.)
    const POP_WINDOW: usize = 64 * 1024;
    let mut off = 0;
    while off < dst.len() {
        let end = (off + POP_WINDOW).min(dst.len());
        let n = cons.pop(&mut dst[off..end]);
        if n == 0 {
            // Before sleeping, wake a peer producer waiting on the space we
            // freed so far (it may be parked while we park for data).
            if cons.take_producer_parked() {
                io.ring_peer();
            }
            wait_for_data(cons, bell, io, spin).await?;
            continue;
        }
        off += n;
        if cons.take_producer_parked() {
            io.ring_peer();
        }
    }
    Ok(())
}

/// One outbound operation for the writer task.
#[derive(Debug)]
pub(crate) enum WriteOp {
    /// An already-encoded non-DATA frame (headers, trailers, control).
    Frame {
        typ: FrameType,
        flags: u8,
        stream_id: u32,
        payload: Bytes,
    },
    /// Message bytes for a stream; flow-control quota was already acquired.
    Data {
        stream_id: u32,
        payload: Bytes,
        end_stream: bool,
    },
}

/// Sender half of the writer queues.
#[derive(Debug, Clone)]
pub(crate) struct WriteHandle {
    /// Bounded: HEADERS and DATA (backpressures callers).
    pub(crate) data_tx: mpsc::Sender<WriteOp>,
    /// Unbounded: control frames that must not be dropped (WINDOW_UPDATE,
    /// RST_STREAM, PING, GOAWAY, trailers). Bounded in practice by stream
    /// count and window arithmetic.
    pub(crate) ctrl_tx: mpsc::UnboundedSender<WriteOp>,
}

impl WriteHandle {
    pub(crate) fn send_ctrl(&self, op: WriteOp) {
        let _ = self.ctrl_tx.send(op);
    }
}

/// Runs the writer task: drains the control queue first, then the data queue,
/// serializing frames into the outbound ring and ringing the peer's doorbell
/// when it parked. The header is encoded into a 12-byte scratch and the
/// payload is written to the ring STRAIGHT from its own buffer — nothing is
/// staged, so the send path performs exactly one copy (buffer → ring).
/// Exits when the connection closes or both queues close.
pub(crate) async fn writer_loop(
    mut prod: Producer,
    io: Arc<ConnIo>,
    mut data_rx: mpsc::Receiver<WriteOp>,
    mut ctrl_rx: mpsc::UnboundedReceiver<WriteOp>,
    spin_us: u32,
) {
    let mut bell = io.bell();
    let mut spin = SpinBudget::new(spin_us);
    'outer: loop {
        // Collect the next op, control queue first.
        let op = {
            match ctrl_rx.try_recv() {
                Ok(op) => Some(op),
                Err(_) => match data_rx.try_recv() {
                    Ok(op) => Some(op),
                    Err(_) => {
                        // Nothing queued: flush the doorbell for everything
                        // written so far, then wait for more work.
                        if prod.take_consumer_parked() {
                            io.ring_peer();
                        }
                        // On a graceful drain, every frame is already in the
                        // ring; wait for the peer to consume it, then signal
                        // flushed and exit so the connection can close without
                        // dropping a final response.
                        if io.is_graceful() {
                            graceful_flush_wait(&mut prod, &io).await;
                            return;
                        }
                        tokio::select! {
                            biased;
                            op = ctrl_rx.recv() => op,
                            op = data_rx.recv() => op,
                            _ = io.graceful_wake.notified() => continue,
                            _ = io.on_close.notified() => return,
                        }
                    }
                },
            }
        };
        let Some(op) = op else { return };
        if io.is_closed() {
            return;
        }
        let (typ, flags, stream_id, payload) = match op {
            WriteOp::Frame {
                typ,
                flags,
                stream_id,
                payload,
            } => (typ, flags, stream_id, payload),
            WriteOp::Data {
                stream_id,
                payload,
                end_stream,
            } => {
                let flags = if end_stream {
                    super::frame::FLAG_END_STREAM
                } else {
                    0
                };
                (FrameType::Data, flags, stream_id, payload)
            }
        };
        let mut hdr = [0u8; FRAME_HDR_SIZE];
        encode_frame_header(&mut hdr, typ, flags, stream_id, payload.len());
        // Header then payload, each pushed fully before the next op; the
        // ring is a byte stream, so a frame may straddle parks and wraps.
        for chunk in [&hdr[..], &payload[..]] {
            let mut off = 0;
            while off < chunk.len() {
                let n = prod.push(&chunk[off..]);
                off += n;
                if n == 0 {
                    // Ring full: let the peer know there is data pending (it
                    // may be parked for data while we park for space).
                    if prod.take_consumer_parked() {
                        io.ring_peer();
                    }
                    if wait_for_space(&mut prod, &mut bell, &io, &mut spin)
                        .await
                        .is_err()
                    {
                        break 'outer;
                    }
                }
            }
        }
    }
}

/// The graceful-flush tail of the writer loop: nudge the peer and wait until
/// it has drained the outbound ring (every queued frame consumed), then mark
/// the connection flushed. Bounded by the connection close so a peer that
/// stops reading cannot wedge shutdown forever.
async fn graceful_flush_wait(prod: &mut Producer, io: &ConnIo) {
    loop {
        if io.is_closed() {
            return;
        }
        if prod.is_drained() {
            io.flushed.notify_waiters();
            return;
        }
        // The peer may be parked on data; ring its doorbell and give it a
        // beat to consume, then re-check.
        let _ = prod.take_consumer_parked();
        io.ring_peer();
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
            _ = io.on_close.notified() => return,
        }
    }
}

/// Per-stream send window (credit granted by the peer).
#[derive(Debug)]
pub(crate) struct SendWindow {
    avail: AtomicI64,
    notify: Notify,
}

impl SendWindow {
    pub(crate) fn new(initial: u32) -> Arc<Self> {
        Arc::new(SendWindow {
            avail: AtomicI64::new(i64::from(initial)),
            notify: Notify::new(),
        })
    }

    /// Adds credit (WINDOW_UPDATE received) and wakes the sender.
    pub(crate) fn add(&self, n: u32) {
        self.avail.fetch_add(i64::from(n), Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    /// Acquires up to `want` bytes of quota, waiting for credit if none is
    /// available. Returns the granted amount (min(want, available), at least
    /// 1 byte) or an error when the connection closes. At most one task per
    /// stream may call this (the stream's pump task).
    pub(crate) async fn acquire(&self, want: usize, io: &ConnIo) -> Result<usize, CloseReason> {
        loop {
            if io.is_closed() {
                return Err(io.close_reason());
            }
            // Register interest before checking so a concurrent `add` between
            // the check and the await cannot be lost.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let cur = self.avail.load(Ordering::Acquire);
            if cur > 0 {
                let take = (cur as u64).min(want as u64) as i64;
                if self
                    .avail
                    .compare_exchange(cur, cur - take, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Ok(take as usize);
                }
                continue;
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = io.on_close.notified() => {}
            }
        }
    }
}

/// Receive-side credit ledger for one stream. The demux task adds inbound
/// DATA bytes; the body consumer marks them consumed, and consumed credit is
/// granted back to the peer via WINDOW_UPDATE once it crosses the threshold.
#[derive(Debug)]
pub(crate) struct RecvCredit {
    stream_id: u32,
    window: u32,
    threshold: u32,
    /// Bytes delivered to the stream buffer but not yet consumed.
    outstanding: AtomicI64,
    /// Consumed bytes not yet granted back to the peer.
    pending_grant: AtomicI64,
    writer: mpsc::UnboundedSender<WriteOp>,
}

impl RecvCredit {
    pub(crate) fn new(
        stream_id: u32,
        window: u32,
        writer: mpsc::UnboundedSender<WriteOp>,
    ) -> Arc<Self> {
        Arc::new(RecvCredit {
            stream_id,
            window,
            threshold: Config::wu_threshold(window),
            outstanding: AtomicI64::new(0),
            pending_grant: AtomicI64::new(0),
            writer,
        })
    }

    /// Records `n` inbound payload bytes. Returns `false` if the peer overran
    /// its window (only a broken/hostile peer can; treat as protocol error).
    #[must_use]
    pub(crate) fn on_data(&self, n: usize) -> bool {
        let out = self.outstanding.fetch_add(n as i64, Ordering::AcqRel) + n as i64;
        // Allow one max-frame of slack over the window for chunk boundaries.
        out <= i64::from(self.window) + super::frame::MAX_FRAME_PAYLOAD as i64
    }

    /// Marks `n` bytes consumed by the application and grants credit back to
    /// the peer once the threshold is crossed.
    pub(crate) fn on_consumed(&self, n: usize) {
        self.outstanding.fetch_sub(n as i64, Ordering::AcqRel);
        let pending = self.pending_grant.fetch_add(n as i64, Ordering::AcqRel) + n as i64;
        if pending >= i64::from(self.threshold) {
            let grant = self.pending_grant.swap(0, Ordering::AcqRel);
            if grant > 0 {
                let mut payload = BytesMut::with_capacity(4);
                bytes::BufMut::put_u32_le(&mut payload, grant.min(i64::from(u32::MAX)) as u32);
                let _ = self.writer.send(WriteOp::Frame {
                    typ: FrameType::WindowUpdate,
                    flags: 0,
                    stream_id: self.stream_id,
                    payload: payload.freeze(),
                });
            }
        }
    }
}

/// Events delivered to a stream's inbound queue by the demux task.
#[derive(Debug)]
pub(crate) enum BodyEvent {
    /// Message bytes (a chunk of gRPC-framed data).
    Data(Bytes),
    /// Trailer block; always the last event.
    Trailers(http::HeaderMap),
}

/// The inbound half of a stream, surfaced as an `http_body::Body`.
///
/// Yields the DATA chunks and, if the peer sent one, a trailers frame.
/// Dropping the body before the stream finished sends RST_STREAM so the peer
/// stops transmitting (and releases its resources). Errors are delivered as
/// `tonic::Status` so tonic's status classification sees them unchanged.
pub(crate) struct ShmBody {
    rx: mpsc::UnboundedReceiver<Result<BodyEvent, tonic::Status>>,
    credit: Arc<RecvCredit>,
    /// Set when the stream ended cleanly (trailers or END_STREAM).
    ended: bool,
    /// Sends the RST on drop; None once ended or already reset.
    rst: Option<Box<dyn Fn() + Send + Sync>>,
    empty: bool,
}

impl std::fmt::Debug for ShmBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmBody")
            .field("ended", &self.ended)
            .finish()
    }
}

impl ShmBody {
    pub(crate) fn new(
        rx: mpsc::UnboundedReceiver<Result<BodyEvent, tonic::Status>>,
        credit: Arc<RecvCredit>,
        rst: Box<dyn Fn() + Send + Sync>,
    ) -> Self {
        ShmBody {
            rx,
            credit,
            ended: false,
            rst: Some(rst),
            empty: false,
        }
    }

    /// A body that is already at end-of-stream (for END_STREAM headers).
    pub(crate) fn empty(
        rx: mpsc::UnboundedReceiver<Result<BodyEvent, tonic::Status>>,
        credit: Arc<RecvCredit>,
    ) -> Self {
        ShmBody {
            rx,
            credit,
            ended: true,
            rst: None,
            empty: true,
        }
    }
}

impl Drop for ShmBody {
    fn drop(&mut self) {
        if let Some(rst) = self.rst.take()
            && !self.ended
        {
            rst();
        }
    }
}

impl http_body::Body for ShmBody {
    type Data = Bytes;
    type Error = crate::BoxError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.empty {
            return std::task::Poll::Ready(None);
        }
        match std::task::ready!(self.rx.poll_recv(cx)) {
            Some(Ok(BodyEvent::Data(b))) => {
                self.credit.on_consumed(b.len());
                std::task::Poll::Ready(Some(Ok(http_body::Frame::data(b))))
            }
            Some(Ok(BodyEvent::Trailers(t))) => {
                self.ended = true;
                self.rst = None;
                std::task::Poll::Ready(Some(Ok(http_body::Frame::trailers(t))))
            }
            Some(Err(e)) => {
                self.ended = true;
                self.rst = None;
                std::task::Poll::Ready(Some(Err(e.into())))
            }
            None => {
                // Queue closed by the demux task: clean END_STREAM.
                self.ended = true;
                self.rst = None;
                std::task::Poll::Ready(None)
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.empty
    }
}
