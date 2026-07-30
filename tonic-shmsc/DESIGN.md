# tonic-shmsc — Detailed Design

This document describes how `tonic-shmsc` is built and why, and how it maps
to the Go original (`plugin/shmsc` from grpc-go-shmem PR #17). For usage and
the security model, see [README.md](README.md).

## 1. Goal

Prove that the exported pluggable-transport API in
`tonic::transport::experimental` is sufficient to implement a non-trivial,
high-performance transport **outside** the tonic core.

The proof obligation, ported from the Go PR: the plugin must not reach tonic
internals. In Go that is enforced by an import-scan guard test; in Rust the
compiler enforces it — `tonic-shmsc` cannot name any non-`pub` tonic item.
What privacy cannot prove is that the engine is genuinely self-contained
rather than delegating to the stock HTTP/2 stack, so a guard test
(`tests/guard.rs`) pins the dependency graph: no hyper, no h2, no tower/axum.

## 2. How tonic drives a transport

tonic does not speak "shared memory"; it speaks a stream contract. The
contract was already byte-based and public at the HTTP layer — a client
transport is a tower `Service<http::Request<Body>> → http::Response<Body>`
where bodies stream already-framed gRPC messages as ref-counted
`bytes::Bytes` plus a trailers frame (`Bytes` is the Rust analog of grpc-go's
ref-counted `mem.BufferSlice`).

What tonic lacked — and what the Go PR had to build for grpc-go — was the
**selection machinery**:

- a registry of transport builders keyed by type name, on both sides;
- client selection on `Endpoint::transport_type`, wired through `Channel`'s
  layer stack, reconnect machinery, and `balance_list`;
- server selection via `Router::serve_with_transport_type` handing the
  transport a prepared `RouteService` (routes + error recovery +
  `grpc-timeout` enforcement);
- **fail-closed semantics** everywhere: an explicitly selected transport
  type that is not registered is a hard error, never a silent fallback to
  HTTP/2. That is a correctness *and* a security property: an explicit
  transport selector must never quietly change the protocol on the wire.

## 3. The seam

```
tonic core (Endpoint / Channel / Router)
     |
     |  Endpoint::transport_type / Router::serve_with_transport_type select a builder
     v
tonic::transport::experimental::{client, server}     <-- the public contract
     |
     v
tonic-shmsc (this crate)  ->  src/engine (owned SHM engine)
```

Differences from grpc-go's layering: grpc-go needed an adapter
(`d1_adapter.go`) translating the public byte contract into its internal
stream types, because core drove concrete `internal/transport` structs. tonic
already drives tower services, so no adapter layer exists — the registered
builder's service **is** the connection, wrapped in the same per-endpoint
layers (origin, user-agent, timeout, limits) and `Reconnect` machinery as the
hyper path.

## 4. The engine

One connection = one Unix-socket rendezvous + one anonymous shared-memory
segment holding two SPSC byte rings.

### Rendezvous and segment lifecycle

The Go engine rendezvouses through *named* segments (`/dev/shm/grpc_shm_<name>`,
a control segment serialized by `flock`, per-connection data segments the
listener must unlink on close). This port replaces all of that with the
socket the platform already gives us:

- the server listens on a UDS path (`0600`); a live listener is refused,
  a stale socket file reclaimed (the Go engine's duplicate-listener refusal);
- per connection the server creates an **anonymous** segment
  (`memfd_create` on Linux, `shm_open`+immediate `shm_unlink` elsewhere) and
  passes the fd over the socket (`SCM_RIGHTS`) with a validated hello;
- the segment has no name, so nothing can leak and nothing can be squatted:
  the kernel reclaims it with the last holder. The Go DESIGN's §8 problem —
  "without an onClose hook every completed connection leaks a segment" —
  does not exist by construction.

The socket then stays open as the doorbell and liveness channel (EOF = peer
gone). This is also what makes the port portable: the Go engine's wakeups
are Linux futex/eventfd and Windows named events with *no* portable fallback
(the plugin does not build on darwin at all).

### Rings and wakeups

Each ring is an SPSC byte pipe: monotonically increasing `u64` head/tail,
power-of-two capacity, masked indices, wrap-splitting copies — the same shape
as the Go ring minus its zero-copy machinery (§7). Producers publish with a
`Release` store of `tail`; consumers acknowledge with `Release` stores of
`head`.

Parking uses per-ring flags (`consumer_parked` / `producer_parked`) with
`SeqCst` fences forming the classic Dekker/eventcount pattern: a parker sets
its flag, re-checks the position, and only then awaits the doorbell; a
publisher publishes, then swaps the flag and rings one byte on the socket if
it was set. Either the publisher sees the flag or the parker's re-check sees
the position — a lost wakeup is impossible, and the doorbell byte is only
paid when the peer actually sleeps. (The Go engine's `dataSeq`/`spaceSeq`
futex words plus waiter counters serve the same role.)

### Framing

Length-prefixed little-endian frames, HTTP/2-shaped in *semantics* —
HEADERS / DATA / TRAILERS / RST_STREAM / WINDOW_UPDATE / GOAWAY / PING with
END_STREAM — but not HTTP/2 in *encoding*. The Go engine writes real HTTP/2
wire framing with HPACK into the rings; this port uses explicit frames with
a flat name/value header block. Both choices are private to the transport
(the peer is always the same implementation); the simpler encoding drops the
HPACK/CONTINUATION/padding surface entirely. Every inbound length is
bounds-checked; violations fail the connection.

Stream ids are client-assigned, odd, monotonically increasing (server
rejects violations). The server sends HEADERS → DATA* → TRAILERS; trailers
ride the same FIFO queue as the stream's DATA so they can never overtake it —
the Go engine's "trailer sentinel" lesson, inherited directly.

### Flow control

Per-stream credit windows over DATA payload bytes, granted by the server for
both directions in the hello (default 8 MiB). WINDOW_UPDATE emission uses
the Go engine's hard-learned formula: grant at `window / 4`, floored at
1 KiB and capped at `window / 2` — the floor is what prevents the documented
small-window deadlock. There is no connection-level window (the Go engine
sets it to 2^31−1 for the same reason): the ring is the connection-level
backpressure. Large messages are chunked (default 1 MiB) so credit and ring
occupancy interleave across streams; the demux task never blocks on a slow
stream because per-stream buffering is bounded by the window the peer was
granted (an overrun is a protocol error).

One deliberate behavioral choice, learned from a real race: the server does
**not** RST a stream whose request body is dropped early (unary handlers
drop it right after decoding). Over TCP that RST saves bandwidth; over
shared memory it can only overtake the response and cancel a good RPC. An
early-finishing stream's residual inbound DATA is window-bounded and simply
ignored, and the client stops pumping when the response finishes the stream.

### Task model (per connection, per process)

- **doorbell task**: owns the socket read side; bumps a `watch` counter that
  parked tasks wait on; closes the connection on EOF/error;
- **writer task**: owns the outbound ring; drains an unbounded control queue
  (WINDOW_UPDATE, RST, PING, GOAWAY — must never be dropped) before a
  bounded FIFO data queue (HEADERS/DATA/TRAILERS — per-stream ordered);
- **demux task**: owns the inbound ring; parses frames and dispatches to
  per-stream queues, granting doorbells for freed space.

The client service (`ShmscChannel`) is a cheaply cloneable handle; each RPC
registers a stream entry, sends HEADERS, pumps the request body under the
send window, and resolves the response head + body from demuxed events.
Dropping a response body mid-stream sends RST (cancellation); GOAWAY fails
streams above the last-accepted id (safe to retry) and drains the rest.

## 5. Selection

Client: `Endpoint::transport_type(NAME)` — consulted per connection attempt
(so `Reconnect` redials through the builder), overriding even an explicitly
provided connector (honoring it would silently change the wire protocol).
Server: `Router::serve_with_transport_type` /
`experimental::server::serve_routes`. Both fail closed; the client-side test
dials a **real, working HTTP/2 server** with an unregistered type and
asserts the RPC fails rather than succeeding over a fallback — ported
verbatim in intent from the Go PR's dispatch tests.

## 6. Credentials

Insecure-only, fail-closed, like the Go plugin: `tls_configured` arrives in
the build options on both sides and is rejected with an explicit error, so
tonic is never left believing a channel is secure while bytes travel over an
unauthenticated segment. (tonic has no per-RPC credential machinery to
enforce beyond this; auth interceptors compose above the seam.)

## 7. Deltas from the Go plugin

Deliberate scope differences, not oversights:

| Go plugin | This port |
|---|---|
| Linux + Windows; futex/eventfd/named events; no portable fallback | Linux + macOS + Windows; rendezvous-channel doorbells with shared-memory parking flags everywhere (Unix sockets / named pipes) |
| Named segments + control segment + `flock` dial lock + unlink-on-close | Unix: UDS rendezvous + anonymous fd-passed segments (nothing to unlink or squat). Windows: named-pipe rendezvous + randomly named sections opened by name (kernel refcounting reclaims them) |
| Real HTTP/2 wire framing + HPACK in the rings | Simpler explicit framing, same stream semantics |
| Read-side zero-copy (`mem.BufferSlice` over ring memory, anchor FIFO, deferred `ridx` publish) | Exactly one copy per side: the writer pushes header and payload straight from their own buffers (nothing staged), and the demux parses in place and copies each payload once into a reusable receive arena (`BytesMut` reserve/split, the hyper/h2 shape), popping in 64 KiB windows so the consumer trails the producer cache-warm. Full ZC (a `Bytes`-with-release-hook over ring memory) remains the next step |
| Adaptive spin (EMA cutoffs, OFF by default, `ConfigureShmSpinIterations`) | Adaptive pre-park spin (`ShmscConfig::spin_us`, OFF by default): arms itself only while spins are hitting, disarms on a park, re-arms when parks resolve inside the budget. Worth ~3x on ping-pong p50 on quiet dedicated hosts (11µs on a 20-core Grace with 25µs); costs multiplexed workloads on busy shared runtimes, hence the 0 default — matching the Go engine's own off-by-default spin |
| INLINE_TX (`ProtoWriteStream`) marshal-into-ring | Not ported; tonic's codec owns marshalling above the seam. The Go API itself makes this optional — a transport MAY implement it — so its absence does not change the contract |
| BDP estimation, adaptive spin, keepalive pings | Static windows; liveness via socket EOF; PING frames reserved |
| `resolver.Address.TransportType` participates in `AddressMap` identity | tonic has no address map; `transport_type` lives on `Endpoint`, and `balance_list` keys endpoints explicitly |
| grpc-go server honors max-streams from the segment header | `max_concurrent_streams` is passed as a build option hint (not yet enforced) |
| Optional symmetric nonce handshake | Not ported (it is advisory-only in Go: the nonce is not challenge-bound) |

## 8. Known limitations

- Insecure-only (see §6).
- `ClientBuildOptions` window hints are not honored on the dialing side —
  the server assigns both directions' windows in the hello.
- `ServerBuildOptions::max_concurrent_streams` is not yet enforced.
- Malformed frames fail the connection (the Go engine fails the stream);
  with a trusted same-host peer either is defensible, and failing the
  connection is the safer default for a byte stream that cannot resync.
- On NVIDIA Grace (GB10), very-large unary messages (16 MiB) run at about
  half the throughput of the previous double-copy demux, while every
  latency metric improved and the same build shows large 16 MiB gains on
  Apple silicon (native and Linux VM). The interaction is Grace's memory
  subsystem, not the protocol (fresh-allocation, zeroing, publish-
  granularity and pop-window hypotheses were each tested); profiling on
  Grace is the open follow-up.
- Windows support is compile-verified only (no Windows CI yet); the
  client-end pipe has no early shutdown, so a client-initiated close is
  observed by the server when the handle drops (promptly, once tasks exit).

## 9. Test suites

Beyond the selection-path e2e (`tests/e2e.rs`) and fail-closed dispatch
(`tests/fail_closed.rs`), the crate carries:

- **`tests/crash.rs`** — crash safety. Spawns a real second process
  (this test binary re-executed into a server/client role) and `kill -9`s
  it mid-RPC, asserting the survivor fails promptly (never hangs), the path
  is reclaimable, and the peer keeps serving. This is the evidence behind
  "process death reclaims everything" — a claim no in-process test can make.
- **`tests/hostile.rs`** (feature `_test-support`) — the trust boundary. A
  raw peer completes the handshake then writes every flavor of garbage
  (unknown frame types, oversized lengths, even/backwards stream ids, pure
  random bytes); the server must fail *that connection* cleanly and keep
  serving others. Found and fixed a real bug: a frame-header protocol error
  left the connection half-open instead of closing it (§ demux teardown).
- **`tests/robustness.rs`** — the tiny-window/tiny-ring large-message
  deadlock regression (the Go engine's documented WINDOW_UPDATE hazard, our
  `wu_threshold` floor's test), the concurrent-stream-open race the
  comparison harness first exposed, reconnect-through-registry, and
  graceful-drain-completes-inflight (which found and fixed the graceful
  flush gap in § lifecycle).
- **codec + ring fuzz/adversarial unit tests** — random and adversarial
  bytes through the frame and header-block decoders never panic or allocate
  unboundedly; a peer scribbling corrupt ring indices is always clamped,
  never OOB. These test the "hostile peer can corrupt data, never cause UB"
  property directly (and would light up under Miri/ASan).
- **micro-benches** (`bench_ring_throughput`, `bench_frame_codec`, run with
  `--ignored --nocapture`) — component-level regression guards for the
  single-copy ring path and the codec, which the end-to-end harness cannot
  isolate.
- **fake-transport conformance** (`tests/experimental_transport.rs` in the
  `integration-tests` crate) — an in-process loopback transport carrying
  real unary + streaming gRPC through tonic's full stacks purely via the
  experimental API, plus fail-closed and connect-timeout checks. This is the
  Go PR's "one fake transport passes conformance" bar, met independently of
  shmsc.

## 10. Open questions

Mirroring the Go DESIGN's list where it still applies:

- Should the experimental API grow a way for transports to surface
  transport-level stats/events?
- What belongs in a reusable conformance suite before the API can be called
  stable? (The e2e suite here — selection paths, fail-closed dispatch,
  streaming shapes, flow-control saturation, cancellation, drain — is a
  first draft of that list.)
- Whether `Channel`'s balance path should expose per-endpoint transport
  health beyond `poll_ready` errors.
