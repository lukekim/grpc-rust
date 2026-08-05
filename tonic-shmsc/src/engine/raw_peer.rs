//! A raw, protocol-hostile client peer for conformance tests.
//!
//! It completes the real handshake (so the server accepts it as a peer) and
//! then writes whatever bytes a test dictates into the client→server ring —
//! valid frames, malformed frames, or pure garbage — and can observe when the
//! server tears the connection down in response. This is how the trust-model
//! boundary is tested from the outside: a broken/hostile peer must make the
//! server FAIL THE CONNECTION, never panic the process or corrupt other
//! connections.
//!
//! Only compiled under the `_test-support` feature.

use std::io;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::Interest;

use super::conn::client_handshake;
use super::frame::{self, FrameType};
use super::ring::Producer;
use super::seg::Segment;

/// A hostile raw client. Not `Send` across the doorbell concern by design —
/// tests drive it from one task.
#[derive(Debug)]
pub struct RawPeer {
    sock: tokio::net::UnixStream,
    prod: Producer,
    _seg: Arc<Segment>,
}

impl RawPeer {
    /// Dials the shmsc server at `path` and completes the handshake, then
    /// returns a peer that can write arbitrary bytes to the c2s ring.
    pub async fn connect(path: &std::path::Path) -> io::Result<Self> {
        let sock = tokio::net::UnixStream::connect(path).await?;
        let conduit = sock;
        let (seg, _hello) = client_handshake(&conduit).await?;
        let seg = Arc::new(seg);
        let l = seg.layout;
        // Client produces into c2s.
        // SAFETY: validated layout of a live mapping kept alive by `_seg`;
        // this peer is the sole producer on c2s.
        let prod = unsafe {
            Producer::new(
                seg.base().add(l.c2s_hdr),
                seg.base().add(l.c2s_data),
                l.c2s_cap,
            )
        };
        Ok(RawPeer {
            sock: conduit,
            prod,
            _seg: seg,
        })
    }

    /// Pushes raw bytes into the c2s ring and rings the doorbell. Returns the
    /// number of bytes accepted by the ring (may be short if the ring fills).
    pub fn send_raw(&mut self, bytes: &[u8]) -> usize {
        let mut off = 0;
        while off < bytes.len() {
            let n = self.prod.push(&bytes[off..]);
            if n == 0 {
                break;
            }
            off += n;
        }
        let _ = self.prod.take_consumer_parked();
        let _ = self.sock.try_write(&[1]);
        off
    }

    /// Encodes and sends a well-formed frame with an arbitrary type byte,
    /// flags, stream id and payload — the type byte may be invalid on
    /// purpose.
    pub fn send_frame(&mut self, typ: u8, flags: u8, stream_id: u32, payload: &[u8]) -> usize {
        let mut buf = BytesMut::with_capacity(frame::FRAME_HDR_SIZE + payload.len());
        let mut hdr = [0u8; frame::FRAME_HDR_SIZE];
        // Reuse the header encoder, then override the type byte so tests can
        // inject unknown types.
        frame::encode_frame_header(&mut hdr, FrameType::Data, flags, stream_id, payload.len());
        hdr[4] = typ;
        buf.extend_from_slice(&hdr);
        buf.extend_from_slice(payload);
        self.send_raw(&buf)
    }

    /// Sends a valid client HEADERS frame opening `stream_id` for `path`, so
    /// a test can establish a real stream before misbehaving on it.
    pub fn send_open(&mut self, stream_id: u32, path: &str) -> usize {
        let block =
            frame::encode_header_block(Some(path), Some("localhost"), None, &Default::default());
        let mut buf = BytesMut::new();
        let mut hdr = [0u8; frame::FRAME_HDR_SIZE];
        frame::encode_frame_header(&mut hdr, FrameType::Headers, 0, stream_id, block.len());
        buf.extend_from_slice(&hdr);
        buf.extend_from_slice(&block);
        self.send_raw(&buf)
    }

    /// Waits up to `timeout` for the server to close the connection (observed
    /// as socket EOF / error). Returns true if the server closed us.
    pub async fn wait_server_closed(&self, timeout: std::time::Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut buf = [0u8; 64];
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let ready = tokio::time::timeout(remaining, self.sock.ready(Interest::READABLE)).await;
            match ready {
                Ok(Ok(r)) if r.is_readable() => match self.sock.try_read(&mut buf) {
                    Ok(0) => return true, // EOF: server closed
                    Ok(_) => continue,    // doorbell byte; keep waiting
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(_) => return true, // broken pipe: closed
                },
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => return true,
                Err(_) => return false, // timed out
            }
        }
    }
}
