//! EXPERIMENTAL pluggable server-side transport API, the peer of
//! [`client`](super::client).
//!
//! A [`ServerTransportBuilder`] registered under a transport-type name binds
//! a listener for a target and returns a [`ServerTransport`], which tonic
//! then drives with the prepared [`RouteService`] — the same routed gRPC
//! services the default hyper server would serve, with tonic's error
//! recovery and `grpc-timeout` enforcement already applied. The transport
//! owns everything below that seam: accepting connections, framing, flow
//! control, and per-stream dispatch into clones of the service.
//!
//! Selection is fail-closed: [`serve_routes`] with an unregistered transport
//! type is an error before anything is bound — a tagged listener can never
//! silently fall back to the HTTP/2 server path.

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use http::{Request, Response};

use super::BoxError;
use crate::body::Body;
use crate::service::Routes;
use crate::transport::service::GrpcTimeout;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Per-listener inputs tonic supplies to a server transport builder.
///
/// Purpose-built and passed by value; concerns above the transport seam
/// (per-request timeout enforcement, error recovery) are already baked into
/// the [`RouteService`] handed to [`ServerTransport::serve`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ServerBuildOptions {
    /// Bound on concurrent streams per connection, or `None` for the
    /// transport default.
    pub max_concurrent_streams: Option<u32>,
    /// Initial per-stream flow-control window, or `None` for the transport
    /// default.
    pub init_stream_window_size: Option<u32>,
    /// Whether the server has transport security (TLS) configured. A
    /// transport that cannot provide it MUST fail closed rather than
    /// silently serve unprotected traffic.
    pub tls_configured: bool,
}

/// The routed gRPC service a [`ServerTransport`] dispatches streams into.
///
/// Cloneable and infallible: tonic has already applied error recovery (any
/// service error becomes a proper gRPC status response) and `grpc-timeout`
/// header enforcement. A transport clones it per stream (or per connection)
/// and calls it with `http::Request<Body>` values it constructs from its own
/// framing.
#[derive(Clone)]
pub struct RouteService {
    inner: tower::util::BoxCloneService<Request<Body>, Response<Body>, Infallible>,
}

impl std::fmt::Debug for RouteService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteService").finish()
    }
}

impl RouteService {
    /// Wraps prepared routes with tonic's above-the-seam server behavior:
    /// `grpc-timeout` enforcement and recovery of service errors into gRPC
    /// status responses.
    pub(crate) fn new(routes: Routes, timeout: Option<Duration>) -> Self {
        let svc = Recovered {
            inner: GrpcTimeout::new(routes.prepare(), timeout),
        };
        RouteService {
            inner: tower::util::BoxCloneService::new(svc),
        }
    }
}

impl tower_service::Service<Request<Body>> for RouteService {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Result<Response<Body>, Infallible>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        self.inner.call(req)
    }
}

/// Turns any residual service error (e.g. an expired `grpc-timeout`) into a
/// gRPC status response, making the stack infallible for the transport.
#[derive(Clone)]
struct Recovered<S> {
    inner: S,
}

impl<S> tower_service::Service<Request<Body>> for Recovered<S>
where
    S: tower_service::Service<Request<Body>, Response = Response<Body>>,
    S::Error: Into<BoxError>,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Result<Response<Body>, Infallible>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Route errors surface per call; readiness itself cannot fail here.
        match self.inner.poll_ready(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(_)) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let fut = self.inner.call(req);
        Box::pin(async move {
            match fut.await {
                Ok(resp) => Ok(resp),
                Err(e) => Ok(crate::Status::from_error(e.into()).into_http()),
            }
        })
    }
}

/// A bound server-side transport, ready to serve streams.
pub trait ServerTransport: Send + 'static {
    /// Serves `service` until `shutdown` resolves or the transport fails,
    /// then drains gracefully. Analogous to grpc-go's
    /// `ServerTransport.HandleStreams`: the transport accepts connections
    /// and streams, constructs an `http::Request<Body>` per stream, and
    /// drives a clone of `service` for it.
    fn serve(
        self: Box<Self>,
        service: RouteService,
        shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;
}

/// Builds server transports: binds a listener for `target`.
pub trait ServerTransportBuilder: Send + Sync + 'static {
    /// Binds the transport's listener at `target` (the transport defines the
    /// target syntax, e.g. a filesystem path).
    #[allow(clippy::type_complexity)]
    fn bind(
        &self,
        target: &str,
        opts: ServerBuildOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn ServerTransport>, BoxError>> + Send>>;
}

static REGISTRY: LazyLock<RwLock<HashMap<String, Arc<dyn ServerTransportBuilder>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Registers a server transport builder under `name`.
///
/// Expected to be called once at process startup; panics on a duplicate or
/// empty name to surface wiring mistakes immediately.
pub fn register<B>(name: impl Into<String>, builder: B)
where
    B: ServerTransportBuilder,
{
    let name = name.into();
    if name.is_empty() {
        panic!("tonic experimental transport: register called with an empty name");
    }
    // See the client registry: poisoning is ignored (no invariants to
    // protect) and the panic happens outside the guard.
    let duplicate = {
        let mut reg = REGISTRY
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reg.contains_key(&name) {
            true
        } else {
            reg.insert(name.clone(), Arc::new(builder));
            false
        }
    };
    if duplicate {
        panic!("tonic experimental transport: register called twice for {name:?}");
    }
}

/// Returns the builder registered under `name`, or `None`. See the client
/// registry's [`get`](super::client::get) for the fail-closed contract.
pub fn get(name: &str) -> Option<Arc<dyn ServerTransportBuilder>> {
    REGISTRY
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(name)
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NopBuilder;
    impl ServerTransportBuilder for NopBuilder {
        fn bind(
            &self,
            _target: &str,
            _opts: ServerBuildOptions,
        ) -> Pin<Box<dyn Future<Output = Result<Box<dyn ServerTransport>, BoxError>> + Send>>
        {
            Box::pin(async { Err("nop".into()) })
        }
    }

    #[test]
    fn register_and_get() {
        assert!(get("server-reg-absent").is_none());
        register("server-reg-present", NopBuilder);
        assert!(get("server-reg-present").is_some());
    }

    #[test]
    #[should_panic(expected = "register called twice")]
    fn duplicate_registration_panics() {
        register("server-reg-dup", NopBuilder);
        register("server-reg-dup", NopBuilder);
    }

    #[test]
    #[should_panic(expected = "empty name")]
    fn empty_name_panics() {
        register("", NopBuilder);
    }

    #[tokio::test]
    async fn serve_routes_fails_closed_for_unregistered_type() {
        let err = serve_routes(
            "server-reg-never",
            "/tmp/never.sock",
            Routes::default(),
            ServerBuildOptions::default(),
            None,
            std::future::pending(),
        )
        .await
        .expect_err("must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("server-reg-never") && msg.contains("not registered"),
            "{msg}"
        );
    }
}

/// Serves `routes` over the transport registered under `transport_type`,
/// bound at `target`, until `shutdown` resolves.
///
/// Fail-closed: an unregistered `transport_type` is an error before anything
/// is bound. `timeout` (if set) is enforced per request alongside the
/// client's `grpc-timeout` header.
pub async fn serve_routes(
    transport_type: &str,
    target: &str,
    routes: Routes,
    opts: ServerBuildOptions,
    timeout: Option<Duration>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), BoxError> {
    let Some(builder) = get(transport_type) else {
        return Err(format!(
            "transport type {transport_type:?} is not registered with \
             tonic::transport::experimental::server"
        )
        .into());
    };
    let transport = builder.bind(target, opts).await?;
    let service = RouteService::new(routes, timeout);
    transport.serve(service, Box::pin(shutdown)).await
}
