//! SPSC byte rings over shared memory, with cross-process parking flags.
//!
//! Each ring is a byte pipe: a producer in one process, a consumer in the
//! other. Positions are monotonically increasing `u64`s; the data index is
//! `pos & (cap - 1)` (capacity is a power of two). The producer publishes with
//! a `Release` store of `tail`; the consumer acknowledges with a `Release`
//! store of `head`. Frame boundaries are a concern of the layer above — the
//! ring behaves like a TCP stream, so a partially written frame is simply not
//! yet parseable.
//!
//! # Wakeups (parking protocol)
//!
//! The ring carries two parking flags so the doorbell byte over the rendezvous
//! socket is only paid when the other side is actually asleep:
//!
//! - the **consumer** parks when the ring is empty: it stores
//!   `consumer_parked = 1` (`SeqCst`), then re-checks `tail`; only if the ring
//!   is still empty does it await the doorbell. The producer, after publishing,
//!   does `consumer_parked.swap(0)` and rings the doorbell if it was set.
//! - the **producer** parks when the ring is full, symmetrically against
//!   `producer_parked` and `head`.
//!
//! The `SeqCst` flag operations plus the re-check make the classic lost-wakeup
//! race impossible: either the publisher observes the parked flag, or the
//! parker's re-check observes the published position.
//!
//! # Trust model
//!
//! The peer process writes the shared header, so every value loaded from it is
//! untrusted. Positions are only used after masking and range-clamping, so a
//! corrupt or hostile peer can produce garbage bytes (which the frame layer
//! rejects) but can never make this code read or write outside the mapped
//! ring.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Byte offsets of the fields inside a ring header (see `seg.rs` for the
/// segment layout). `head`/`consumer_parked` are written by the consumer and
/// live on one cache line; `tail`/`producer_parked` are written by the
/// producer and live on another, so the two sides never false-share.
const OFF_HEAD: usize = 0;
const OFF_CONSUMER_PARKED: usize = 8;
const OFF_TAIL: usize = 64;
const OFF_PRODUCER_PARKED: usize = 72;

/// Raw accessor over one mapped ring (header + data region).
struct RawRing {
    hdr: *mut u8,
    data: *mut u8,
    cap: usize,
}

impl RawRing {
    /// # Safety
    /// `hdr` must point at a live `RING_HDR_SIZE` header and `data` at `cap`
    /// bytes of ring data, both inside a mapping that outlives the ring (the
    /// callers tie this to an `Arc<Segment>`).
    unsafe fn new(hdr: *mut u8, data: *mut u8, cap: usize) -> Self {
        debug_assert!(cap.is_power_of_two());
        RawRing { hdr, data, cap }
    }

    fn head(&self) -> &AtomicU64 {
        // SAFETY: in-bounds, 8-aligned field of the live mapping; AtomicU64
        // over shared memory is the supported cross-process idiom.
        unsafe { AtomicU64::from_ptr(self.hdr.add(OFF_HEAD).cast()) }
    }
    fn tail(&self) -> &AtomicU64 {
        // SAFETY: as above.
        unsafe { AtomicU64::from_ptr(self.hdr.add(OFF_TAIL).cast()) }
    }
    fn consumer_parked(&self) -> &AtomicU32 {
        // SAFETY: as above.
        unsafe { AtomicU32::from_ptr(self.hdr.add(OFF_CONSUMER_PARKED).cast()) }
    }
    fn producer_parked(&self) -> &AtomicU32 {
        // SAFETY: as above.
        unsafe { AtomicU32::from_ptr(self.hdr.add(OFF_PRODUCER_PARKED).cast()) }
    }

    /// Copies `src` into the ring at position `pos`, splitting at the wrap.
    fn copy_in(&self, pos: u64, src: &[u8]) {
        let idx = (pos & (self.cap as u64 - 1)) as usize;
        let first = src.len().min(self.cap - idx);
        // SAFETY: idx < cap and both segments are clamped to the data region;
        // the produce/consume protocol guarantees the target bytes are not
        // concurrently read by a correct peer, and a hostile peer racing here
        // can only observe garbage bytes, not cause out-of-bounds access.
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), self.data.add(idx), first);
            if first < src.len() {
                std::ptr::copy_nonoverlapping(
                    src.as_ptr().add(first),
                    self.data,
                    src.len() - first,
                );
            }
        }
    }

    /// Copies `dst.len()` bytes out of the ring at position `pos`.
    fn copy_out(&self, pos: u64, dst: &mut [u8]) {
        let idx = (pos & (self.cap as u64 - 1)) as usize;
        let first = dst.len().min(self.cap - idx);
        // SAFETY: mirror of `copy_in`.
        unsafe {
            std::ptr::copy_nonoverlapping(self.data.add(idx), dst.as_mut_ptr(), first);
            if first < dst.len() {
                std::ptr::copy_nonoverlapping(
                    self.data,
                    dst.as_mut_ptr().add(first),
                    dst.len() - first,
                );
            }
        }
    }
}

// SAFETY: RawRing is a set of raw pointers into a shared mapping; all access
// is via atomics or raw copies with explicit synchronization. Single-owner
// (Send) usage is enforced by the Producer/Consumer wrappers.
unsafe impl Send for RawRing {}

/// The producing side of one ring. Owned by exactly one task.
pub(crate) struct Producer {
    ring: RawRing,
    /// Local copy of our own tail (authoritative; the shared one mirrors it).
    tail: u64,
    /// Cached consumer position, refreshed when space runs out.
    head_cache: u64,
}

impl std::fmt::Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("tail", &self.tail)
            .finish()
    }
}

impl Producer {
    /// # Safety
    /// See [`RawRing::new`]; additionally there must be at most one `Producer`
    /// for this ring across both processes.
    pub(crate) unsafe fn new(hdr: *mut u8, data: *mut u8, cap: usize) -> Self {
        // SAFETY: forwarded contract.
        let ring = unsafe { RawRing::new(hdr, data, cap) };
        let tail = ring.tail().load(Ordering::Acquire);
        let head_cache = ring.head().load(Ordering::Acquire);
        Producer {
            ring,
            tail,
            head_cache,
        }
    }

    /// Bytes that can be pushed right now (refreshes the consumer position).
    pub(crate) fn free_space(&mut self) -> usize {
        self.head_cache = self.ring.head().load(Ordering::Acquire);
        // Clamp against a corrupt peer-written head: pretend full rather than
        // computing a bogus (possibly huge) free count.
        let used = self.tail.wrapping_sub(self.head_cache);
        if used > self.ring.cap as u64 {
            0
        } else {
            self.ring.cap - used as usize
        }
    }

    /// Whether the consumer has drained everything this producer published
    /// (refreshes the consumer position). Used by the graceful-flush path to
    /// know the peer has read the final response before the connection closes.
    pub(crate) fn is_drained(&mut self) -> bool {
        self.head_cache = self.ring.head().load(Ordering::Acquire);
        self.tail == self.head_cache
    }

    /// Pushes as much of `src` as fits, returning the number of bytes taken.
    /// The bytes are published (visible to the consumer) before this returns.
    pub(crate) fn push(&mut self, src: &[u8]) -> usize {
        let mut space = {
            let used = self.tail.wrapping_sub(self.head_cache);
            if used > self.ring.cap as u64 {
                0
            } else {
                self.ring.cap - used as usize
            }
        };
        if space < src.len() {
            space = self.free_space();
        }
        let n = src.len().min(space);
        if n == 0 {
            return 0;
        }
        self.ring.copy_in(self.tail, &src[..n]);
        self.tail = self.tail.wrapping_add(n as u64);
        self.ring.tail().store(self.tail, Ordering::Release);
        n
    }

    /// Marks the producer as parked (about to wait for space). Returns `true`
    /// if space appeared during the race window, in which case the caller must
    /// NOT wait and should clear the flag with [`Self::unpark_self`].
    pub(crate) fn park_self(&mut self) -> bool {
        self.ring.producer_parked().store(1, Ordering::SeqCst);
        // Full fence so the flag store and the position re-check below cannot
        // reorder against the peer's publish + flag-check (Dekker pattern; the
        // peer's side is the fence in `take_producer_parked`).
        std::sync::atomic::fence(Ordering::SeqCst);
        self.free_space() > 0
    }

    /// Clears this side's parked flag after a wakeup or an aborted park.
    pub(crate) fn unpark_self(&self) {
        self.ring.producer_parked().store(0, Ordering::SeqCst);
    }

    /// Consumes the consumer's parked flag; `true` means the peer consumer is
    /// (or was about to be) asleep and needs a doorbell. Call after publishing.
    pub(crate) fn take_consumer_parked(&self) -> bool {
        // Pairs with the fence in `Consumer::park_self`: either this swap sees
        // the parked flag (and we ring the doorbell), or the parker's re-check
        // sees our published tail. Both missing is impossible.
        std::sync::atomic::fence(Ordering::SeqCst);
        self.ring.consumer_parked().swap(0, Ordering::SeqCst) == 1
    }
}

/// The consuming side of one ring. Owned by exactly one task.
pub(crate) struct Consumer {
    ring: RawRing,
    /// Local copy of our own head (authoritative).
    head: u64,
    /// Cached producer position, refreshed when data runs out.
    tail_cache: u64,
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("head", &self.head)
            .finish()
    }
}

impl Consumer {
    /// # Safety
    /// See [`RawRing::new`]; additionally there must be at most one `Consumer`
    /// for this ring across both processes.
    pub(crate) unsafe fn new(hdr: *mut u8, data: *mut u8, cap: usize) -> Self {
        // SAFETY: forwarded contract.
        let ring = unsafe { RawRing::new(hdr, data, cap) };
        let head = ring.head().load(Ordering::Acquire);
        let tail_cache = ring.tail().load(Ordering::Acquire);
        Consumer {
            ring,
            head,
            tail_cache,
        }
    }

    /// Bytes available to pop right now (refreshes the producer position).
    pub(crate) fn available(&mut self) -> usize {
        self.tail_cache = self.ring.tail().load(Ordering::Acquire);
        // Clamp against a corrupt peer-written tail: never report more than
        // the ring can hold, so callers can never copy out of bounds.
        let n = self.tail_cache.wrapping_sub(self.head);
        (n.min(self.ring.cap as u64)) as usize
    }

    /// Pops up to `dst.len()` bytes, returning how many were copied. The
    /// consumed space is released to the producer before this returns.
    pub(crate) fn pop(&mut self, dst: &mut [u8]) -> usize {
        let mut avail =
            (self.tail_cache.wrapping_sub(self.head)).min(self.ring.cap as u64) as usize;
        if avail == 0 {
            avail = self.available();
        }
        let n = dst.len().min(avail);
        if n == 0 {
            return 0;
        }
        self.ring.copy_out(self.head, &mut dst[..n]);
        self.head = self.head.wrapping_add(n as u64);
        self.ring.head().store(self.head, Ordering::Release);
        n
    }

    /// Marks the consumer as parked (about to wait for data). Returns `true`
    /// if data appeared during the race window, in which case the caller must
    /// NOT wait and should clear the flag with [`Self::unpark_self`].
    pub(crate) fn park_self(&mut self) -> bool {
        self.ring.consumer_parked().store(1, Ordering::SeqCst);
        // See Producer::park_self: pairs with the fence in
        // `take_consumer_parked` on the producing side.
        std::sync::atomic::fence(Ordering::SeqCst);
        self.available() > 0
    }

    /// Clears this side's parked flag after a wakeup or an aborted park.
    pub(crate) fn unpark_self(&self) {
        self.ring.consumer_parked().store(0, Ordering::SeqCst);
    }

    /// Consumes the producer's parked flag; `true` means the peer producer is
    /// (or was about to be) asleep waiting for space and needs a doorbell.
    /// Call after releasing space (advancing `head`).
    pub(crate) fn take_producer_parked(&self) -> bool {
        // Pairs with the fence in `Producer::park_self`.
        std::sync::atomic::fence(Ordering::SeqCst);
        self.ring.producer_parked().swap(0, Ordering::SeqCst) == 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::seg::{Layout, RING_HDR_SIZE, Segment};
    use std::sync::Arc;

    fn ring_pair(cap: usize) -> (Arc<Segment>, Producer, Consumer) {
        let layout = Layout::new(cap, cap).unwrap();
        #[cfg(unix)]
        let seg = Arc::new(Segment::create(layout).unwrap());
        #[cfg(windows)]
        let seg = Arc::new(Segment::create_named(layout).unwrap().0);
        let hdr = unsafe { seg.base().add(layout.c2s_hdr) };
        let data = unsafe { seg.base().add(layout.c2s_data) };
        let p = unsafe { Producer::new(hdr, data, cap) };
        let c = unsafe { Consumer::new(hdr, data, cap) };
        (seg, p, c)
    }

    #[test]
    fn roundtrip_with_wrap() {
        let (_seg, mut p, mut c) = ring_pair(4096);
        let msg: Vec<u8> = (0..1500u32).map(|i| (i % 251) as u8).collect();
        let mut out = vec![0u8; 1500];
        // Push/pop enough times to wrap the ring several times.
        for _ in 0..10 {
            assert_eq!(p.push(&msg), 1500);
            assert_eq!(c.available(), 1500);
            assert_eq!(c.pop(&mut out), 1500);
            assert_eq!(out, msg);
        }
    }

    #[test]
    fn partial_push_when_full() {
        let (_seg, mut p, mut c) = ring_pair(4096);
        let big = vec![0xABu8; 6000];
        assert_eq!(p.push(&big), 4096);
        assert_eq!(p.push(&big[4096..]), 0); // full
        assert!(!p.park_self()); // full, and no space appeared during the park
        p.unpark_self();
        let mut out = vec![0u8; 1000];
        assert_eq!(c.pop(&mut out), 1000);
        assert!(out.iter().all(|&b| b == 0xAB));
        // Space freed; producer can continue.
        assert_eq!(p.push(&big[4096..]), 1000);
    }

    #[test]
    fn parking_flags_handshake() {
        let (_seg, mut p, mut c) = ring_pair(4096);
        // Consumer parks on empty; producer push must observe it.
        assert!(!c.park_self()); // empty: park succeeds (no data appeared)
        assert_eq!(p.push(b"x"), 1);
        assert!(p.take_consumer_parked());
        assert!(!p.take_consumer_parked()); // consumed
        c.unpark_self();
        // Producer parks on full; consumer pop must observe it.
        let fill = vec![0u8; 4095];
        assert_eq!(p.push(&fill), 4095); // ring now holds 4096 bytes total
        assert!(!p.park_self()); // full: park succeeds (no space appeared)
        let mut out = vec![0u8; 16];
        assert_eq!(c.pop(&mut out), 16);
        assert!(c.take_producer_parked());
        assert!(!c.take_producer_parked());
    }

    #[test]
    fn park_race_detected() {
        let (_seg, mut p, mut c) = ring_pair(4096);
        // Data arrives between "empty" observation and the park: park_self
        // must report it so the caller does not sleep.
        assert_eq!(c.available(), 0);
        assert_eq!(p.push(b"race"), 4);
        assert!(c.park_self());
        c.unpark_self();
    }

    #[test]
    fn cross_thread_stress() {
        let (seg, mut p, mut c) = ring_pair(4096);
        let n: u64 = 2_000_000;
        let seg_producer = seg.clone();
        let producer = std::thread::spawn(move || {
            let _seg = seg_producer; // each side keeps the mapping alive
            let mut sent = 0u64;
            let mut buf = [0u8; 8];
            while sent < n {
                buf.copy_from_slice(&sent.to_le_bytes());
                let mut off = 0;
                while off < 8 {
                    let k = p.push(&buf[off..]);
                    off += k;
                    if k == 0 {
                        std::hint::spin_loop();
                    }
                }
                sent += 1;
            }
        });
        let mut expect = 0u64;
        let mut buf = [0u8; 8];
        while expect < n {
            let mut off = 0;
            while off < 8 {
                let k = c.pop(&mut buf[off..]);
                off += k;
                if k == 0 {
                    std::hint::spin_loop();
                }
            }
            assert_eq!(u64::from_le_bytes(buf), expect);
            expect += 1;
        }
        producer.join().unwrap();
        let _ = RING_HDR_SIZE; // silence unused import in cfg permutations
    }

    /// Writes `v` to a `u64` field in the ring header, simulating a hostile
    /// or corrupt peer scribbling on the shared index it "owns".
    fn poke_u64(seg: &Segment, hdr_off: usize, field_off: usize, v: u64) {
        // SAFETY: test-only write to an in-bounds 8-aligned header field.
        unsafe {
            AtomicU64::from_ptr(seg.base().add(hdr_off + field_off).cast())
                .store(v, Ordering::Release);
        }
    }

    // The peer owns the OTHER end's index (tail for a consumer, head for a
    // producer). These tests corrupt that shared value to every dangerous
    // shape and assert the local side clamps rather than reading/writing out
    // of bounds — the trust-boundary property the DESIGN claims ("a hostile
    // peer can corrupt gRPC traffic, never cause OOB access in this
    // process"). `cargo test` under Miri/ASan would make the OOB explicit;
    // here we assert the clamps hold and nothing panics.

    #[test]
    fn consumer_clamps_corrupt_tail() {
        let cap = 4096usize;
        for bogus_tail in [
            u64::MAX,
            u64::MAX - 10,
            cap as u64 * 1000,
            1u64 << 63,
            (1u64 << 63) + 5,
        ] {
            let (seg, _p, mut c) = ring_pair(cap);
            // Consumer head is 0; peer claims a wildly-ahead tail.
            poke_u64(&seg, seg.layout.c2s_hdr, OFF_TAIL, bogus_tail);
            let avail = c.available();
            assert!(
                avail <= cap,
                "available {avail} must never exceed cap {cap}"
            );
            // Popping the clamped amount must not read out of bounds.
            let mut out = vec![0u8; cap];
            let n = c.pop(&mut out);
            assert!(n <= cap);
        }
    }

    #[test]
    fn producer_clamps_corrupt_head() {
        let cap = 4096usize;
        for bogus_head in [u64::MAX, 1u64 << 63, cap as u64 + 1, cap as u64 * 999] {
            let (seg, mut p, _c) = ring_pair(cap);
            // Producer tail is 0; peer claims a head that would imply
            // negative or absurd occupancy.
            poke_u64(&seg, seg.layout.c2s_hdr, OFF_HEAD, bogus_head);
            let free = p.free_space();
            assert!(free <= cap, "free_space {free} must never exceed cap {cap}");
            // Pushing the clamped amount must not write out of bounds.
            let src = vec![0x5Au8; cap];
            let n = p.push(&src);
            assert!(n <= cap);
        }
    }

    /// Component micro-bench: raw ring push/pop bandwidth. Not run by
    /// default; `cargo test -p tonic-shmsc bench_ring -- --ignored --nocapture`
    /// prints GiB/s. Guards the single-copy ring path against regressions the
    /// end-to-end harness cannot isolate.
    #[test]
    #[ignore = "micro-benchmark; run explicitly with --ignored --nocapture"]
    fn bench_ring_throughput() {
        let cap = 1 << 20; // 1 MiB ring
        let (seg, mut p, mut c) = ring_pair(cap);
        let total: u64 = 8 << 30; // move 8 GiB
        let chunk = 64 * 1024usize;
        let seg_p = seg.clone();
        let producer = std::thread::spawn(move || {
            let _seg = seg_p;
            let src = vec![0xABu8; chunk];
            let mut sent = 0u64;
            while sent < total {
                let mut off = 0;
                while off < chunk {
                    let n = p.push(&src[off..]);
                    off += n;
                    if n == 0 {
                        std::hint::spin_loop();
                    }
                }
                sent += chunk as u64;
            }
        });
        let start = std::time::Instant::now();
        let mut dst = vec![0u8; chunk];
        let mut got = 0u64;
        while got < total {
            let mut off = 0;
            while off < chunk {
                let n = c.pop(&mut dst[off..]);
                off += n;
                if n == 0 {
                    std::hint::spin_loop();
                }
            }
            got += chunk as u64;
        }
        let el = start.elapsed();
        producer.join().unwrap();
        let gib = total as f64 / (1u64 << 30) as f64;
        println!(
            "ring throughput: {:.2} GiB/s ({gib:.0} GiB in {:.3}s, {cap}-byte ring, {chunk}-byte chunks)",
            gib / el.as_secs_f64(),
            el.as_secs_f64()
        );
    }

    #[test]
    fn corrupt_indices_never_panic_under_traffic() {
        // Interleave real pops with a peer that keeps rewriting the tail to
        // garbage: the consumer must keep clamping, never panic, never OOB.
        let cap = 8192usize;
        let (seg, mut p, mut c) = ring_pair(cap);
        assert_eq!(p.push(&vec![7u8; 4096]), 4096);
        let mut out = vec![0u8; cap];
        for (i, bogus) in [u64::MAX, 1u64 << 62, 3, cap as u64 * 5, 0]
            .into_iter()
            .enumerate()
        {
            poke_u64(&seg, seg.layout.c2s_hdr, OFF_TAIL, bogus);
            let avail = c.available();
            assert!(avail <= cap, "iter {i}: {avail} > {cap}");
            let take = avail.min(out.len());
            let _ = c.pop(&mut out[..take]);
        }
    }
}
