//! `tonic-shmsc` — a SELF-CONTAINED shared-memory (SHM) gRPC transport
//! plugin for tonic.
//!
//! This crate owns its ENTIRE SHM engine and depends on tonic ONLY through
//! the public, experimental pluggable-transport API
//! ([`tonic::transport::experimental`]) plus public types — it does not
//! depend on hyper or h2, and it cannot reach tonic internals (Rust's crate
//! privacy enforces at compile time what grpc-go's `shmsc` plugin enforces
//! with an import-guard test). That independence makes this crate a working
//! proof that the exported API is sufficient for a non-trivial transport
//! implemented outside the tonic core. A guard test additionally asserts the
//! dependency graph.
//!
//! It registers under the transport type [`NAME`] (`"shmsc"`) on both sides.
//!
//! # Usage
//!
//! Server:
//!
//! ```ignore
//! tonic_shmsc::register();
//! tonic::transport::Server::builder()
//!     .add_service(GreeterServer::new(MyGreeter))
//!     .serve_with_transport_type(tonic_shmsc::NAME, "/tmp/greeter.shmsc")
//!     .await?;
//! ```
//!
//! Client:
//!
//! ```ignore
//! tonic_shmsc::register();
//! let channel = tonic::transport::Endpoint::from_shared(tonic_shmsc::uri("/tmp/greeter.shmsc"))?
//!     .transport_type(tonic_shmsc::NAME)
//!     .connect()
//!     .await?;
//! let mut client = GreeterClient::new(channel);
//! ```
//!
//! Or bypass `Endpoint` entirely and hand the transport straight to a
//! generated client: [`connect`] returns a tower service accepted by
//! `GreeterClient::new`.
//!
//! # Platform scope
//!
//! Linux, macOS, and Windows. On Unix the engine rendezvouses over a Unix
//! socket and shares the segment by `SCM_RIGHTS` fd passing (`memfd` /
//! anonymous `shm_open`). On Windows the rendezvous is a named pipe
//! (`\\.\pipe\...`, derived from the target string) and the segment is a
//! randomly named pagefile-backed section opened by name; kernel-object
//! refcounting gives the same no-leak lifecycle. Doorbell wakeups ride the
//! rendezvous channel on every platform. (The Go `plugin/shmsc` engine uses
//! futex/eventfd on Linux and named events on Windows, with no macOS
//! support; see DESIGN.md for the full delta list.) The Unix paths are
//! exercised by this repo's test suite; Windows support is compile-verified
//! (`cargo check --target x86_64-pc-windows-msvc`) pending Windows CI.
//!
//! # Security model
//!
//! The transport is insecure-only and fail-closed about it: an endpoint or
//! server with TLS configured is REJECTED rather than silently downgraded.
//! The security boundary is Unix file permissions — the rendezvous socket is
//! created `0600`, and the shared-memory segment is anonymous (created
//! unlinked, passed by fd), so it has no name to attack and dies with the
//! processes holding it. The peer is otherwise trusted, exactly like the Go
//! engine: a broken or hostile peer can corrupt gRPC traffic, but every
//! length and offset read from shared memory is bounds-checked, so it cannot
//! make this process read or write outside the mapped segment.
//!
//! # Stability
//!
//! EXPERIMENTAL — this crate exists to prove out
//! [`tonic::transport::experimental`] and evolves with it.

mod engine;

/// Test-only seam for hostile-peer conformance tests. Gated behind the
/// `_test-support` feature so it is never part of a normal build; the crate's
/// own integration tests enable it. NOT a stable API.
#[cfg(feature = "_test-support")]
#[doc(hidden)]
pub mod test_support {
    pub use crate::engine::raw_peer::RawPeer;
}

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use http::Uri;
use tonic::transport::experimental::client::{
    ClientBuildOptions, ClientTransport, ClientTransportBuilder,
};
use tonic::transport::experimental::server::{
    RouteService, ServerBuildOptions, ServerTransport, ServerTransportBuilder,
};

pub use engine::server::ShmscConnectInfo;

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The transport type name this plugin registers under, on both the client
/// and the server side.
pub const NAME: &str = "shmsc";

/// Builds an endpoint URI for an shmsc target path, e.g.
/// `shmsc://localhost/tmp/greeter.shmsc`.
///
/// (`http::Uri` requires an authority after `//`, so a placeholder host is
/// included; the transport only reads the path.)
pub fn uri(path: &str) -> String {
    format!("shmsc://localhost{path}")
}

fn target_path(uri: &Uri) -> Result<PathBuf, BoxError> {
    let path = uri.path();
    if path.is_empty() || path == "/" {
        return Err(format!("shmsc: endpoint URI {uri} carries no socket path").into());
    }
    Ok(PathBuf::from(path))
}

/// Tuning knobs for shmsc connections and listeners.
#[derive(Debug, Clone)]
pub struct ShmscConfig {
    /// Capacity of each per-connection ring (power of two, 4 KiB – 1 GiB).
    /// Default 16 MiB. Set on the server; the client learns it in the
    /// handshake.
    pub ring_size: usize,
    /// Initial per-stream flow-control window in bytes, both directions.
    /// Default 8 MiB. Chosen by the server for the whole connection.
    pub stream_window: u32,
    /// Largest DATA payload written as one frame; larger messages are
    /// chunked. Default 1 MiB.
    pub max_frame: usize,
    /// Microseconds the reader/writer tasks busy-spin re-checking the ring
    /// before parking on the doorbell. Under back-to-back traffic this
    /// elides the doorbell syscall and task wake entirely (and any cpuidle
    /// exit latency behind them), at the cost of up to this much CPU per
    /// empty/full transition. Default 25; 0 disables spinning.
    pub spin_us: u32,
}

impl Default for ShmscConfig {
    fn default() -> Self {
        let d = engine::conn::Config::default();
        ShmscConfig {
            ring_size: d.ring_size,
            stream_window: d.stream_window,
            max_frame: d.max_frame,
            spin_us: d.spin_us,
        }
    }
}

impl ShmscConfig {
    fn into_engine(self) -> engine::conn::Config {
        engine::conn::Config {
            ring_size: self.ring_size,
            stream_window: self.stream_window,
            max_frame: self.max_frame,
            spin_us: self.spin_us,
        }
    }
}

/// Registers the shmsc transport with tonic's experimental client and server
/// transport registries under [`NAME`].
///
/// Call once at process startup (the Rust analog of the Go plugin's
/// `init()`); a second call panics, as does a conflicting registration under
/// the same name, to surface wiring mistakes immediately.
pub fn register() {
    tonic::transport::experimental::client::register(NAME, ShmscClientBuilder::default());
    tonic::transport::experimental::server::register(NAME, ShmscServerBuilder::default());
}

/// The client-side transport builder registered under [`NAME`].
///
/// tonic selects it when an `Endpoint` carries
/// `.transport_type(tonic_shmsc::NAME)`; it dials the Unix socket named by
/// the endpoint URI's path.
#[derive(Debug, Default, Clone)]
pub struct ShmscClientBuilder {
    config: Option<ShmscConfig>,
}

impl ClientTransportBuilder for ShmscClientBuilder {
    fn build(
        &self,
        target: Uri,
        opts: ClientBuildOptions,
    ) -> Pin<Box<dyn Future<Output = Result<ClientTransport, BoxError>> + Send>> {
        let config = self.config.clone();
        Box::pin(async move {
            // Fail-closed: this transport cannot provide transport security.
            // Never silently downgrade a TLS-configured endpoint.
            if opts.tls_configured {
                return Err(
                    "shmsc: transport security is not supported; remove the TLS configuration \
                     or use a different transport (refusing to silently downgrade)"
                        .into(),
                );
            }
            let path = target_path(&target)?;
            // The server assigns the connection's windows in the handshake;
            // the endpoint's requested window applies only to targets this
            // process serves (documented delta from the Go engine, which
            // honors WithInitialWindowSize on the dialing side).
            let cfg = config.unwrap_or_default().into_engine();
            let channel = engine::client::connect(&path, cfg).await.map_err(|e| {
                // ConnectError gives tonic the correct UNAVAILABLE mapping.
                BoxError::from(tonic::ConnectError(Box::new(e)))
            })?;
            Ok(ClientTransport::new(channel))
        })
    }
}

/// Connects directly to an shmsc server at `path`, returning a cloneable
/// tower service that generated tonic clients accept (`GreeterClient::new`).
///
/// This bypasses `Endpoint`/`Channel` entirely — useful when you want the
/// transport without the registry/selection machinery.
pub async fn connect(path: impl Into<PathBuf>) -> Result<ShmscChannel, std::io::Error> {
    connect_with_config(path, ShmscConfig::default()).await
}

/// [`connect`] with explicit tuning.
pub async fn connect_with_config(
    path: impl Into<PathBuf>,
    config: ShmscConfig,
) -> Result<ShmscChannel, std::io::Error> {
    let inner = engine::client::connect(&path.into(), config.into_engine()).await?;
    Ok(ShmscChannel { inner })
}

/// A connected shmsc channel: a cheaply cloneable tower `Service` from
/// `http::Request<tonic::body::Body>` to `http::Response<tonic::body::Body>`.
#[derive(Clone, Debug)]
pub struct ShmscChannel {
    inner: engine::client::ShmscChannel,
}

impl tower_service::Service<http::Request<tonic::body::Body>> for ShmscChannel {
    type Response = http::Response<tonic::body::Body>;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        tower_service::Service::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        tower_service::Service::call(&mut self.inner, req)
    }
}

/// The server-side transport builder registered under [`NAME`]. Binds the
/// rendezvous socket at the given target path.
#[derive(Debug, Default, Clone)]
pub struct ShmscServerBuilder {
    config: Option<ShmscConfig>,
}

impl ServerTransportBuilder for ShmscServerBuilder {
    fn bind(
        &self,
        target: &str,
        opts: ServerBuildOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn ServerTransport>, BoxError>> + Send>> {
        let target = target.to_owned();
        let config = self.config.clone();
        Box::pin(async move {
            if opts.tls_configured {
                return Err(
                    "shmsc: transport security is not supported; remove the TLS configuration \
                     or use a different transport (refusing to silently downgrade)"
                        .into(),
                );
            }
            let mut cfg = config.unwrap_or_default().into_engine();
            if let Some(w) = opts.init_stream_window_size {
                cfg.stream_window = w;
            }
            let listener = engine::server::ShmscListener::bind(std::path::Path::new(&target), cfg)?;
            Ok(Box::new(ShmscServerTransport { listener }) as Box<dyn ServerTransport>)
        })
    }
}

struct ShmscServerTransport {
    listener: engine::server::ShmscListener,
}

impl ServerTransport for ShmscServerTransport {
    fn serve(
        self: Box<Self>,
        service: RouteService,
        shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>> {
        Box::pin(async move {
            self.listener
                .serve(service, shutdown)
                .await
                .map_err(BoxError::from)
        })
    }
}

/// Serves a set of tonic services over shmsc at `path` until `shutdown`
/// resolves — the plugin-direct server entry point (no tonic `Server`
/// machinery, mirroring [`connect`]).
pub async fn serve<S>(
    path: impl Into<PathBuf>,
    service: S,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), std::io::Error>
where
    S: tower_service::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
            Future: Send,
        > + Clone
        + Send
        + 'static,
{
    let listener =
        engine::server::ShmscListener::bind(&path.into(), ShmscConfig::default().into_engine())?;
    listener.serve(service, shutdown).await
}

/// An `Arc`'d [`ShmscClientBuilder`] with a specific configuration, for
/// registering under a custom name.
pub fn client_builder_with_config(config: ShmscConfig) -> ShmscClientBuilder {
    ShmscClientBuilder {
        config: Some(config),
    }
}

/// A [`ShmscServerBuilder`] with a specific configuration, for registering
/// under a custom name.
pub fn server_builder_with_config(config: ShmscConfig) -> ShmscServerBuilder {
    ShmscServerBuilder {
        config: Some(config),
    }
}
