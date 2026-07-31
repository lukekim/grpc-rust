//! Transport frame codec.
//!
//! The rings carry a byte stream of length-prefixed frames. The layering is
//! deliberately HTTP/2-shaped — HEADERS / DATA / TRAILERS / RST_STREAM /
//! WINDOW_UPDATE / GOAWAY / PING with an END_STREAM flag — because that is the
//! stream model gRPC needs, but the encoding is a simpler explicit
//! little-endian format rather than HTTP/2 wire framing + HPACK (see
//! DESIGN.md for the rationale and the delta from the Go engine).
//!
//! ```text
//! FrameHeader (12 bytes, little-endian):
//!   len       u32   payload length (excludes this header)
//!   type      u8
//!   flags     u8
//!   reserved  u16   must be 0
//!   stream_id u32
//! ```
//!
//! Header/trailer blocks are flat name/value lists:
//!
//! ```text
//!   count u16, then per entry: name_len u16, name, value_len u32, value
//! ```
//!
//! Pseudo-entries use HTTP/2 names (`:path`, `:authority`, `:status`). Every
//! length decoded from the ring is bounds-checked before use: a corrupt peer
//! yields a protocol error, never a panic or an out-of-bounds access.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::{HeaderMap, HeaderName, HeaderValue};

/// Size of the fixed frame header.
pub(crate) const FRAME_HDR_SIZE: usize = 12;

/// Hard cap on any inbound frame payload (defense against a corrupt length).
pub(crate) const MAX_FRAME_PAYLOAD: usize = 16 * 1024 * 1024;
/// Cap on a decoded header block (mirrors the Go engine's 8 MiB header cap).
pub(crate) const MAX_HEADER_BLOCK: usize = 8 * 1024 * 1024;
/// Cap on entries in one header block.
const MAX_HEADER_ENTRIES: usize = 4096;

/// Frame types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum FrameType {
    /// Request or response header block.
    Headers = 1,
    /// Message bytes (already gRPC length-prefix framed by tonic's codec).
    Data = 2,
    /// Trailer block (always ends the stream).
    Trailers = 3,
    /// Abnormal stream termination.
    RstStream = 4,
    /// Per-stream flow-control credit (`stream_id != 0`).
    WindowUpdate = 5,
    /// Connection drain: no new streams after `last_stream_id`.
    GoAway = 6,
    /// Liveness probe; flags bit 1 marks the ack.
    Ping = 7,
}

impl FrameType {
    fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => FrameType::Headers,
            2 => FrameType::Data,
            3 => FrameType::Trailers,
            4 => FrameType::RstStream,
            5 => FrameType::WindowUpdate,
            6 => FrameType::GoAway,
            7 => FrameType::Ping,
            _ => return None,
        })
    }
}

/// Flag bit: last frame of the stream in this direction.
pub(crate) const FLAG_END_STREAM: u8 = 0x1;
/// Flag bit (PING only): this is an acknowledgement.
pub(crate) const FLAG_ACK: u8 = 0x2;

/// Maximum PING payload (opaque data), matching HTTP/2's fixed 8-byte ping.
/// A PING — or the ACK that echoes it — larger than this, or carried on a
/// non-zero stream, is a protocol violation. Bounding it stops a hostile peer
/// from amplifying a flood of unacked PINGs into unbounded queued payload
/// allocations.
pub(crate) const PING_MAX_PAYLOAD: usize = 8;

/// Stream error codes carried by RST_STREAM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub(crate) enum RstCode {
    /// The peer cancelled the stream (deadline, dropped call, ...).
    Cancel = 1,
    /// The stream was refused before any processing (safe to retry).
    Refused = 2,
    /// The peer hit an internal error on this stream.
    Internal = 3,
}

impl RstCode {
    pub(crate) fn from_u32(v: u32) -> Self {
        match v {
            1 => RstCode::Cancel,
            2 => RstCode::Refused,
            _ => RstCode::Internal,
        }
    }
}

/// A parsed inbound frame.
#[derive(Debug)]
pub(crate) struct Frame {
    pub(crate) typ: FrameType,
    pub(crate) flags: u8,
    pub(crate) stream_id: u32,
    pub(crate) payload: Bytes,
}

impl Frame {
    pub(crate) fn end_stream(&self) -> bool {
        self.flags & FLAG_END_STREAM != 0
    }

    /// Validates an inbound PING: it must be connection-level (`stream_id 0`)
    /// with an opaque payload no larger than [`PING_MAX_PAYLOAD`]. A peer that
    /// sends an oversized or misdirected PING is failed, which (together with
    /// the bounded ack queue) caps the memory a flood of PINGs can pin.
    pub(crate) fn validate_ping(&self) -> Result<(), ProtocolError> {
        if self.stream_id != 0 {
            return Err(ProtocolError(format!(
                "PING must be connection-level (stream_id 0), got {}",
                self.stream_id
            )));
        }
        if self.payload.len() > PING_MAX_PAYLOAD {
            return Err(ProtocolError(format!(
                "PING payload {} exceeds {PING_MAX_PAYLOAD} bytes",
                self.payload.len()
            )));
        }
        Ok(())
    }
}

/// Encodes just the fixed frame header into `dst` (the payload is written
/// separately, straight from its own buffer, so it is never staged/copied).
pub(crate) fn encode_frame_header(
    dst: &mut [u8; FRAME_HDR_SIZE],
    typ: FrameType,
    flags: u8,
    stream_id: u32,
    payload_len: usize,
) {
    debug_assert!(payload_len <= MAX_FRAME_PAYLOAD);
    dst[0..4].copy_from_slice(&(payload_len as u32).to_le_bytes());
    dst[4] = typ as u8;
    dst[5] = flags;
    dst[6..8].copy_from_slice(&0u16.to_le_bytes());
    dst[8..12].copy_from_slice(&stream_id.to_le_bytes());
}

/// Validates and decodes a fixed frame header: (type, flags, stream_id,
/// payload_len). Every length is bounds-checked here, so a corrupt peer
/// yields a protocol error, never an oversized allocation.
pub(crate) fn parse_frame_header(
    hdr: &[u8; FRAME_HDR_SIZE],
) -> Result<(FrameType, u8, u32, usize), ProtocolError> {
    let len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
    let typ_raw = hdr[4];
    let flags = hdr[5];
    let reserved = u16::from_le_bytes(hdr[6..8].try_into().unwrap());
    let stream_id = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
    if len > MAX_FRAME_PAYLOAD {
        return Err(ProtocolError(format!("frame payload {len} exceeds cap")));
    }
    if reserved != 0 {
        return Err(ProtocolError(
            "nonzero reserved bits in frame header".into(),
        ));
    }
    let Some(typ) = FrameType::from_u8(typ_raw) else {
        return Err(ProtocolError(format!("unknown frame type {typ_raw:#x}")));
    };
    Ok((typ, flags, stream_id, len))
}

/// Appends an encoded frame to `dst` (test/support path; the writer task
/// writes header and payload to the ring separately).
#[cfg(test)]
pub(crate) fn encode_frame(
    dst: &mut BytesMut,
    typ: FrameType,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) {
    let mut hdr = [0u8; FRAME_HDR_SIZE];
    encode_frame_header(&mut hdr, typ, flags, stream_id, payload.len());
    dst.reserve(FRAME_HDR_SIZE + payload.len());
    dst.put_slice(&hdr);
    dst.put_slice(payload);
}

/// A protocol violation by the peer. Fails the connection (the peer shares
/// the segment and is trusted; a malformed frame means a broken or hostile
/// peer, and there is no way to resynchronize a byte stream).
#[derive(Debug)]
pub(crate) struct ProtocolError(pub(crate) String);

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shmsc protocol error: {}", self.0)
    }
}

impl std::error::Error for ProtocolError {}

/// Incremental frame parser over a byte stream. The demux path reads frames
/// directly from the ring (single payload copy); this parser remains as the
/// reference implementation exercised by the codec tests.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct FrameParser {
    buf: BytesMut,
}

#[cfg(test)]
impl FrameParser {
    /// Buffer to append newly popped ring bytes into.
    pub(crate) fn buf_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    /// Extracts the next complete frame, if any.
    pub(crate) fn next_frame(&mut self) -> Result<Option<Frame>, ProtocolError> {
        if self.buf.len() < FRAME_HDR_SIZE {
            return Ok(None);
        }
        let hdr: [u8; FRAME_HDR_SIZE] = self.buf[0..FRAME_HDR_SIZE].try_into().unwrap();
        let (typ, flags, stream_id, len) = parse_frame_header(&hdr)?;
        if self.buf.len() < FRAME_HDR_SIZE + len {
            return Ok(None);
        }
        self.buf.advance(FRAME_HDR_SIZE);
        let payload = self.buf.split_to(len).freeze();
        Ok(Some(Frame {
            typ,
            flags,
            stream_id,
            payload,
        }))
    }
}

/// One decoded header block: HTTP/2-style pseudo entries plus regular headers.
#[derive(Debug, Default)]
pub(crate) struct HeaderBlock {
    pub(crate) path: Option<String>,
    pub(crate) authority: Option<String>,
    pub(crate) status: Option<u16>,
    pub(crate) headers: HeaderMap,
}

/// Encodes a header block. Pseudo entries first, mirroring HTTP/2 ordering.
pub(crate) fn encode_header_block(
    path: Option<&str>,
    authority: Option<&str>,
    status: Option<u16>,
    headers: &HeaderMap,
) -> Bytes {
    let mut out = BytesMut::with_capacity(256);
    let mut count: u16 = 0;
    let mut body = BytesMut::with_capacity(256);
    let put = |body: &mut BytesMut, count: &mut u16, name: &[u8], value: &[u8]| {
        body.put_u16_le(name.len() as u16);
        body.put_slice(name);
        body.put_u32_le(value.len() as u32);
        body.put_slice(value);
        *count += 1;
    };
    if let Some(p) = path {
        put(&mut body, &mut count, b":path", p.as_bytes());
    }
    if let Some(a) = authority {
        put(&mut body, &mut count, b":authority", a.as_bytes());
    }
    if let Some(s) = status {
        put(&mut body, &mut count, b":status", s.to_string().as_bytes());
    }
    // HeaderMap iteration yields each value with its name; multi-valued
    // headers round-trip as repeated entries.
    for (name, value) in headers {
        put(
            &mut body,
            &mut count,
            name.as_str().as_bytes(),
            value.as_bytes(),
        );
    }
    out.put_u16_le(count);
    out.extend_from_slice(&body);
    out.freeze()
}

/// Decodes a header block, validating every length and each name/value.
pub(crate) fn decode_header_block(mut buf: Bytes) -> Result<HeaderBlock, ProtocolError> {
    if buf.len() > MAX_HEADER_BLOCK {
        return Err(ProtocolError("header block exceeds cap".into()));
    }
    if buf.len() < 2 {
        return Err(ProtocolError("short header block".into()));
    }
    let count = buf.get_u16_le() as usize;
    if count > MAX_HEADER_ENTRIES {
        return Err(ProtocolError("too many header entries".into()));
    }
    let mut block = HeaderBlock::default();
    for _ in 0..count {
        if buf.len() < 2 {
            return Err(ProtocolError("truncated header name length".into()));
        }
        let nlen = buf.get_u16_le() as usize;
        if buf.len() < nlen + 4 {
            return Err(ProtocolError("truncated header name".into()));
        }
        let name = buf.split_to(nlen);
        let vlen = buf.get_u32_le() as usize;
        if buf.len() < vlen {
            return Err(ProtocolError("truncated header value".into()));
        }
        let value = buf.split_to(vlen);
        match &name[..] {
            b":path" => {
                block.path = Some(
                    std::str::from_utf8(&value)
                        .map_err(|_| ProtocolError("non-utf8 :path".into()))?
                        .to_owned(),
                );
            }
            b":authority" => {
                block.authority = Some(
                    std::str::from_utf8(&value)
                        .map_err(|_| ProtocolError("non-utf8 :authority".into()))?
                        .to_owned(),
                );
            }
            b":status" => {
                let s = std::str::from_utf8(&value)
                    .ok()
                    .and_then(|s| s.parse::<u16>().ok())
                    .ok_or_else(|| ProtocolError("bad :status".into()))?;
                block.status = Some(s);
            }
            _ => {
                let hn = HeaderName::from_bytes(&name)
                    .map_err(|_| ProtocolError("invalid header name".into()))?;
                let hv = HeaderValue::from_bytes(&value)
                    .map_err(|_| ProtocolError("invalid header value".into()))?;
                block.headers.append(hn, hv);
            }
        }
    }
    if !buf.is_empty() {
        return Err(ProtocolError("trailing bytes after header block".into()));
    }
    Ok(block)
}

/// Encodes a GOAWAY payload.
pub(crate) fn encode_goaway(last_stream_id: u32, msg: &str) -> Bytes {
    let mut b = BytesMut::with_capacity(8 + msg.len());
    b.put_u32_le(last_stream_id);
    b.put_u32_le(0); // error code, reserved
    b.put_slice(msg.as_bytes());
    b.freeze()
}

/// Decodes a GOAWAY payload into (last_stream_id, message).
pub(crate) fn decode_goaway(mut payload: Bytes) -> Result<(u32, String), ProtocolError> {
    if payload.len() < 8 {
        return Err(ProtocolError("short GOAWAY".into()));
    }
    let last = payload.get_u32_le();
    let _code = payload.get_u32_le();
    let msg = String::from_utf8_lossy(&payload).into_owned();
    Ok((last, msg))
}

/// Encodes an RST_STREAM payload.
pub(crate) fn encode_rst(code: RstCode) -> Bytes {
    let mut b = BytesMut::with_capacity(4);
    b.put_u32_le(code as u32);
    b.freeze()
}

/// Decodes an RST_STREAM payload.
pub(crate) fn decode_rst(mut payload: Bytes) -> Result<RstCode, ProtocolError> {
    if payload.len() < 4 {
        return Err(ProtocolError("short RST_STREAM".into()));
    }
    Ok(RstCode::from_u32(payload.get_u32_le()))
}

/// Decodes a WINDOW_UPDATE payload.
pub(crate) fn decode_window_update(mut payload: Bytes) -> Result<u32, ProtocolError> {
    if payload.len() != 4 {
        return Err(ProtocolError("bad WINDOW_UPDATE length".into()));
    }
    let incr = payload.get_u32_le();
    if incr == 0 {
        return Err(ProtocolError("zero WINDOW_UPDATE increment".into()));
    }
    Ok(incr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip_incremental() {
        let mut out = BytesMut::new();
        encode_frame(&mut out, FrameType::Data, FLAG_END_STREAM, 7, b"hello");
        encode_frame(
            &mut out,
            FrameType::Ping,
            FLAG_ACK,
            0,
            &[1, 2, 3, 4, 5, 6, 7, 8],
        );
        let mut parser = FrameParser::default();
        // Feed one byte at a time to exercise partial-frame handling.
        let mut frames = Vec::new();
        for b in out.iter() {
            parser.buf_mut().put_u8(*b);
            while let Some(f) = parser.next_frame().unwrap() {
                frames.push(f);
            }
        }
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].typ, FrameType::Data);
        assert!(frames[0].end_stream());
        assert_eq!(frames[0].stream_id, 7);
        assert_eq!(&frames[0].payload[..], b"hello");
        assert_eq!(frames[1].typ, FrameType::Ping);
        assert_eq!(frames[1].flags, FLAG_ACK);
    }

    #[test]
    fn rejects_bad_frames() {
        // Unknown type.
        let mut parser = FrameParser::default();
        let mut out = BytesMut::new();
        out.put_u32_le(0);
        out.put_u8(0xEE);
        out.put_u8(0);
        out.put_u16_le(0);
        out.put_u32_le(1);
        parser.buf_mut().extend_from_slice(&out);
        assert!(parser.next_frame().is_err());

        // Oversized length.
        let mut parser = FrameParser::default();
        let mut out = BytesMut::new();
        out.put_u32_le(u32::MAX);
        out.put_u8(FrameType::Data as u8);
        out.put_u8(0);
        out.put_u16_le(0);
        out.put_u32_le(1);
        parser.buf_mut().extend_from_slice(&out);
        assert!(parser.next_frame().is_err());
    }

    #[test]
    fn header_block_roundtrip() {
        let mut hm = HeaderMap::new();
        hm.append("content-type", HeaderValue::from_static("application/grpc"));
        hm.append("x-multi", HeaderValue::from_static("a"));
        hm.append("x-multi", HeaderValue::from_static("b"));
        hm.append("x-bin-bin", HeaderValue::from_bytes(b"AAEC").unwrap());
        let enc = encode_header_block(Some("/pkg.Svc/Method"), Some("localhost"), None, &hm);
        let dec = decode_header_block(enc).unwrap();
        assert_eq!(dec.path.as_deref(), Some("/pkg.Svc/Method"));
        assert_eq!(dec.authority.as_deref(), Some("localhost"));
        assert_eq!(dec.status, None);
        assert_eq!(dec.headers.get("content-type").unwrap(), "application/grpc");
        let multi: Vec<_> = dec.headers.get_all("x-multi").iter().collect();
        assert_eq!(multi.len(), 2);
    }

    #[test]
    fn header_block_rejects_truncation_and_bad_names() {
        // Claim one entry but truncate the value.
        let mut b = BytesMut::new();
        b.put_u16_le(1);
        b.put_u16_le(3);
        b.put_slice(b"abc");
        b.put_u32_le(100);
        b.put_slice(b"short");
        assert!(decode_header_block(b.freeze()).is_err());

        // Invalid header name bytes.
        let mut b = BytesMut::new();
        b.put_u16_le(1);
        b.put_u16_le(3);
        b.put_slice(b"a\x00c");
        b.put_u32_le(1);
        b.put_slice(b"v");
        assert!(decode_header_block(b.freeze()).is_err());
    }

    #[test]
    fn goaway_rst_wu_roundtrip() {
        let (last, msg) = decode_goaway(encode_goaway(41, "draining")).unwrap();
        assert_eq!(last, 41);
        assert_eq!(msg, "draining");
        assert_eq!(
            decode_rst(encode_rst(RstCode::Refused)).unwrap(),
            RstCode::Refused
        );
        let mut wu = BytesMut::new();
        wu.put_u32_le(65536);
        assert_eq!(decode_window_update(wu.freeze()).unwrap(), 65536);
        let mut zero = BytesMut::new();
        zero.put_u32_le(0);
        assert!(decode_window_update(zero.freeze()).is_err());
    }

    #[test]
    fn status_pseudo_header() {
        let enc = encode_header_block(None, None, Some(200), &HeaderMap::new());
        let dec = decode_header_block(enc).unwrap();
        assert_eq!(dec.status, Some(200));
    }

    /// Component micro-bench: frame-header + header-block codec throughput.
    /// `cargo test -p tonic-shmsc bench_codec -- --ignored --nocapture`.
    /// Guards against reintroducing allocations/copies in the hot codec.
    #[test]
    #[ignore = "micro-benchmark; run explicitly with --ignored --nocapture"]
    fn bench_frame_codec() {
        // Frame header encode + parse.
        let iters = 20_000_000u64;
        let start = std::time::Instant::now();
        let mut acc = 0u64;
        for i in 0..iters {
            let mut hdr = [0u8; FRAME_HDR_SIZE];
            encode_frame_header(&mut hdr, FrameType::Data, FLAG_END_STREAM, i as u32, 4096);
            let (_, _, sid, len) = parse_frame_header(&hdr).unwrap();
            acc = acc.wrapping_add(sid as u64 + len as u64);
        }
        let el = start.elapsed();
        assert_ne!(acc, u64::MAX);
        println!(
            "frame header codec: {:.1} M ops/s ({iters} encode+parse in {:.3}s)",
            iters as f64 / el.as_secs_f64() / 1e6,
            el.as_secs_f64()
        );

        // Header block encode + decode with a realistic gRPC header set.
        let mut hm = HeaderMap::new();
        hm.append("content-type", HeaderValue::from_static("application/grpc"));
        hm.append("grpc-encoding", HeaderValue::from_static("identity"));
        hm.append("user-agent", HeaderValue::from_static("grpc-rust/0"));
        let block_iters = 2_000_000u64;
        let start = std::time::Instant::now();
        for _ in 0..block_iters {
            let enc = encode_header_block(Some("/pkg.Svc/Method"), Some("localhost"), None, &hm);
            let dec = decode_header_block(enc).unwrap();
            assert_eq!(dec.headers.len(), 3);
        }
        let el = start.elapsed();
        println!(
            "header block codec: {:.1} M roundtrips/s ({block_iters} in {:.3}s)",
            block_iters as f64 / el.as_secs_f64() / 1e6,
            el.as_secs_f64()
        );
    }

    /// A cheap deterministic PRNG so the fuzz tests need no dev-dependency
    /// and reproduce identically across runs/platforms.
    fn lcg(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *state
    }

    // The peer is trusted for CONTENT but not for SAFETY: a broken or
    // hostile peer can put any bytes on the ring, and the decoders must
    // answer every input with Ok or a ProtocolError — never a panic, an
    // out-of-bounds slice, or an unbounded allocation. These tests assert
    // exactly that across adversarial and random inputs.

    #[test]
    fn frame_header_parse_total_over_all_type_bytes() {
        // Every possible type byte, with a maximal length field: parse must
        // classify (Ok for known types, Err for unknown) and never allocate
        // based on the unchecked length.
        for typ in 0u8..=255 {
            let mut hdr = [0u8; FRAME_HDR_SIZE];
            hdr[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
            hdr[4] = typ;
            hdr[8..12].copy_from_slice(&7u32.to_le_bytes());
            // Ok for known types, Err for unknown/oversized — never a panic.
            if let Ok((_, _, _, len)) = parse_frame_header(&hdr) {
                assert!(len <= MAX_FRAME_PAYLOAD);
            }
        }
    }

    #[test]
    fn header_block_decode_never_panics_on_random_bytes() {
        let mut state = 0x1234_5678_9abc_def0u64;
        for _ in 0..20_000 {
            let len = (lcg(&mut state) % 64) as usize;
            let mut buf = BytesMut::with_capacity(len);
            for _ in 0..len {
                buf.put_u8((lcg(&mut state) & 0xff) as u8);
            }
            // Any random byte string is either a valid block or a clean
            // protocol error — never a panic.
            let _ = decode_header_block(buf.freeze());
        }
    }

    #[test]
    fn header_block_rejects_absurd_counts_and_lengths() {
        // Claims 65535 entries in a 2-byte buffer: must error, not
        // pre-allocate 65535 slots or read past the end.
        let mut b = BytesMut::new();
        b.put_u16_le(u16::MAX);
        assert!(decode_header_block(b.freeze()).is_err());

        // One entry claiming a 4 GiB value: must error on the length check,
        // never attempt the allocation.
        let mut b = BytesMut::new();
        b.put_u16_le(1);
        b.put_u16_le(1);
        b.put_slice(b"k");
        b.put_u32_le(u32::MAX);
        b.put_slice(b"short");
        assert!(decode_header_block(b.freeze()).is_err());

        // A block larger than the cap is rejected before any parsing.
        let huge = BytesMut::zeroed(MAX_HEADER_BLOCK + 1);
        assert!(decode_header_block(huge.freeze()).is_err());
    }

    #[test]
    fn control_frame_payloads_never_panic_on_random_bytes() {
        let mut state = 0xdead_beef_cafe_babeu64;
        for _ in 0..10_000 {
            let len = (lcg(&mut state) % 20) as usize;
            let mut buf = BytesMut::with_capacity(len);
            for _ in 0..len {
                buf.put_u8((lcg(&mut state) & 0xff) as u8);
            }
            let b = buf.freeze();
            let _ = decode_goaway(b.clone());
            let _ = decode_rst(b.clone());
            let _ = decode_window_update(b);
        }
    }

    #[test]
    fn frame_parser_never_panics_on_random_stream() {
        // Feed a long random byte stream through the incremental parser: it
        // must consume it as a mix of valid/invalid frames without ever
        // panicking, and stop cleanly once a byte forces a protocol error.
        let mut state = 0x0f0f_0f0f_0f0f_0f0fu64;
        for _ in 0..2_000 {
            let mut parser = FrameParser::default();
            let n = (lcg(&mut state) % 256) as usize;
            for _ in 0..n {
                parser.buf_mut().put_u8((lcg(&mut state) & 0xff) as u8);
            }
            loop {
                match parser.next_frame() {
                    Ok(Some(f)) => assert!(f.payload.len() <= MAX_FRAME_PAYLOAD),
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
    }
}
