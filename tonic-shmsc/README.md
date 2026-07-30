# tonic-shmsc

A **self-contained shared-memory (SHM) gRPC transport plugin for tonic**, the
Rust port of grpc-go's `plugin/shmsc` (grpc-go-shmem PR #17).

This crate owns its entire SHM engine — segments, rings, framing, flow
control, wakeups — and reaches gRPC **only** through tonic's public,
experimental pluggable-transport API (`tonic::transport::experimental`). It
does not depend on hyper or h2. It is a working proof that the exported API
is sufficient for a non-trivial transport implemented outside the tonic core.

It registers under the transport type **`shmsc`** on both sides.

## Usage

Server (through tonic's `Server`, symmetric with the default path):

```rust,ignore
tonic_shmsc::register();
tonic::transport::Server::builder()
    .add_service(GreeterServer::new(MyGreeter))
    .serve_with_transport_type(tonic_shmsc::NAME, "/tmp/greeter.shmsc")
    .await?;
```

Client (selection by transport type, fail-closed):

```rust,ignore
tonic_shmsc::register();
let channel = Endpoint::from_shared(tonic_shmsc::uri("/tmp/greeter.shmsc"))?
    .transport_type(tonic_shmsc::NAME)
    .connect()
    .await?;
let mut client = GreeterClient::new(channel);
```

Plugin-direct (no registry, no tonic transport machinery):

```rust,ignore
let channel = tonic_shmsc::connect("/tmp/greeter.shmsc").await?;
let mut client = GreeterClient::new(channel);
// and: tonic_shmsc::serve(path, Routes::new(svc).prepare(), shutdown).await?;
```

## How it connects

1. The server binds a Unix-domain **rendezvous socket** (mode `0600`) at the
   target path. A live listener at the same path is refused, never hijacked;
   a stale socket file left by a dead process is reclaimed.
2. Per accepted connection the server creates an **anonymous** shared-memory
   segment (`memfd_create` on Linux, `shm_open`+immediate `shm_unlink`
   elsewhere), and passes the file descriptor to the client over the socket
   (`SCM_RIGHTS`) together with a validated hello (magic, version, ring
   capacities, stream window).
3. Both sides map the segment: two SPSC byte rings (client→server and
   server→client) carrying length-prefixed frames — HEADERS / DATA /
   TRAILERS / RST_STREAM / WINDOW_UPDATE / GOAWAY / PING.
4. The socket stays open as the **doorbell + liveness channel**: parked
   readers/writers advertise themselves in shared memory and are woken by a
   single byte; process death surfaces as EOF and tears the connection down,
   so a crashed peer can never strand the other side, and the anonymous
   segment is reclaimed by the kernel with the last process.

Flow control is HTTP/2-shaped: per-stream credit windows over DATA bytes
(default 8 MiB) with WINDOW_UPDATE granted at the Go engine's hard-learned
threshold (`window / 4`, floored at 1 KiB, capped at `window / 2`); the ring
itself (default 16 MiB per direction) is the connection-level backpressure.

## Security model

- **Insecure-only, fail-closed**: a TLS-configured endpoint or server is
  rejected rather than silently downgraded, so tonic is never left believing
  a channel is secure while bytes travel over an unauthenticated segment.
- The boundary is Unix file permissions: the rendezvous socket is `0600`,
  and the segment is anonymous — there is no name to squat on or open.
- The peer is trusted (both endpoints share the segment), but every length,
  offset, and position read from shared memory is bounds-checked: a hostile
  peer can corrupt gRPC traffic, never cause out-of-bounds access in this
  process. Protocol violations fail the connection.
- The server inserts `ShmscConnectInfo` (peer pid/uid where the platform
  reports them) into request extensions.

## Platform scope

Linux, macOS, and Windows.

- **Unix**: rendezvous over a Unix socket; the segment is anonymous
  (`memfd_create` / unlinked `shm_open`) and shared by `SCM_RIGHTS` fd
  passing.
- **Windows**: rendezvous over a named pipe (`\\.\pipe\...`, derived from
  the target string, `first_pipe_instance` + local-only); the segment is a
  randomly named pagefile-backed section opened by name, with the same
  no-leak lifecycle via kernel-object refcounting.

Doorbell wakeups ride the rendezvous channel on every platform, gated by
shared-memory parking flags. (The Go plugin uses futex/eventfd on Linux and
named events on Windows, with no macOS support.) The Unix paths are
exercised by this repo's test suite; Windows support is compile-verified
(`cargo check --target x86_64-pc-windows-msvc --all-targets`) pending
Windows CI. See [DESIGN.md](DESIGN.md) for the full delta list.

## Benchmarks

`cargo run -p tonic-shmsc --example bench --release` compares the same echo
service over shmsc and over tonic's stock hyper/h2 transport (the port of
the Go PR's `benchmark/shmsccmp` harness). Baseline on an Apple Silicon
laptop (macOS, one process pair, release build):

| transport | unary 1 KiB p50 | p99 | 16 MiB echo |
|---|---|---|---|
| shmsc (shared memory) | 23.8 µs | 37.3 µs | 5.73 GiB/s |
| hyper/h2 over unix socket | 35.2 µs | 51.0 µs | 1.60 GiB/s |
| hyper/h2 over tcp loopback | 61.4 µs | 141.0 µs | 3.16 GiB/s |

## Status

EXPERIMENTAL. This crate exists to prove out
`tonic::transport::experimental` and evolves with it. See the known
limitations in [DESIGN.md](DESIGN.md) §9.
