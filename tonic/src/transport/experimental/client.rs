//! EXPERIMENTAL pluggable client-side transport API.
//!
//! A client transport is built per connection attempt by a
//! [`ClientTransportBuilder`] registered under a transport-type name. tonic
//! selects the builder when an [`Endpoint`](crate::transport::Endpoint) names
//! that type via `Endpoint::transport_type`; selection is fail-closed (see
//! the [module docs](super)).
//!
//! The built [`ClientTransport`] participates in the regular `Channel`
//! machinery: per-endpoint layers (origin, user agent, timeout, limits) and
//! reconnect-on-failure apply exactly as they do to the default HTTP/2
//! transport, and `Channel::balance_list` balances across custom-transport
//! endpoints like any other.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use http::{Request, Response, Uri};

use super::BoxError;
use crate::body::Body;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Per-connection inputs tonic supplies to a client transport builder.
///
/// Purpose-built and passed by value: it carries only what a transport
/// legitimately consumes, deliberately not aliasing any internal option
/// struct (aliasing would freeze internal layout as a public compatibility
/// obligation). Concerns above the transport seam — per-request timeouts,
/// concurrency limits, load balancing — stay in tonic's layers.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ClientBuildOptions {
    /// The `User-Agent` value configured on the endpoint, if any.
    pub user_agent: Option<http::HeaderValue>,
    /// Bound on connection establishment. tonic also enforces it around the
    /// builder's future, so a transport may simply ignore it.
    pub connect_timeout: Option<Duration>,
    /// Initial per-stream flow-control window requested on the endpoint, or
    /// `None` for the transport default.
    pub init_stream_window_size: Option<u32>,
    /// Initial connection-level flow-control window requested on the
    /// endpoint, or `None` for the transport default.
    pub init_connection_window_size: Option<u32>,
    /// Whether the endpoint has transport security (TLS) configured.
    ///
    /// A transport that cannot provide transport security MUST fail closed
    /// when this is set — never silently downgrade — so tonic is never left
    /// believing a channel is secure while bytes travel unprotected.
    pub tls_configured: bool,
}

/// A client transport produced by a [`ClientTransportBuilder`]: a boxed tower
/// service from `http::Request<Body>` to `http::Response<Body>` over one
/// established connection.
///
/// The transport owns everything below the gRPC framing layer. Request and
/// response bodies stream framed gRPC messages; the response body ends with
/// an `http_body` trailers frame carrying `grpc-status` (or the status
/// appears in the response headers for trailers-only responses).
///
/// An erroring `poll_ready` marks the connection broken; tonic's reconnect
/// machinery then asks the builder for a fresh transport on the next request.
pub struct ClientTransport {
    #[allow(clippy::type_complexity)]
    inner: Box<
        dyn tower_service::Service<
                Request<Body>,
                Response = Response<Body>,
                Error = BoxError,
                Future = BoxFuture<Result<Response<Body>, BoxError>>,
            > + Send,
    >,
}

impl std::fmt::Debug for ClientTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTransport").finish()
    }
}

impl ClientTransport {
    /// Boxes a tower service as a client transport.
    pub fn new<S>(svc: S) -> Self
    where
        S: tower_service::Service<Request<Body>, Response = Response<Body>> + Send + 'static,
        S::Error: Into<BoxError> + Send,
        S::Future: Send + 'static,
    {
        struct Adapt<S>(S);
        impl<S> tower_service::Service<Request<Body>> for Adapt<S>
        where
            S: tower_service::Service<Request<Body>, Response = Response<Body>>,
            S::Error: Into<BoxError> + Send,
            S::Future: Send + 'static,
        {
            type Response = Response<Body>;
            type Error = BoxError;
            type Future = BoxFuture<Result<Response<Body>, BoxError>>;

            fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                self.0.poll_ready(cx).map_err(Into::into)
            }

            fn call(&mut self, req: Request<Body>) -> Self::Future {
                let fut = self.0.call(req);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
        }
        ClientTransport {
            inner: Box::new(Adapt(svc)),
        }
    }
}

impl tower_service::Service<Request<Body>> for ClientTransport {
    type Response = Response<Body>;
    type Error = BoxError;
    type Future = BoxFuture<Result<Response<Body>, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        self.inner.call(req)
    }
}

/// Builds client transports for endpoints that select this transport type.
pub trait ClientTransportBuilder: Send + Sync + 'static {
    /// Establishes a connection to `target` and returns the transport for it.
    ///
    /// `target` is the endpoint URI (the transport defines how to interpret
    /// it). Called once per connection attempt; tonic's reconnect machinery
    /// calls it again after a transport failure.
    fn build(
        &self,
        target: Uri,
        opts: ClientBuildOptions,
    ) -> Pin<Box<dyn Future<Output = Result<ClientTransport, BoxError>> + Send>>;
}

static REGISTRY: LazyLock<RwLock<HashMap<String, Arc<dyn ClientTransportBuilder>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Registers a client transport builder under `name`. `name` is matched
/// against `Endpoint::transport_type` during connection establishment.
///
/// Expected to be called once at process startup (a plugin's explicit
/// registration call); panics on a duplicate or empty name to surface wiring
/// mistakes immediately.
pub fn register<B>(name: impl Into<String>, builder: B)
where
    B: ClientTransportBuilder,
{
    let name = name.into();
    if name.is_empty() {
        panic!("tonic experimental transport: register called with an empty name");
    }
    // The registry map holds no invariants a poisoned lock could break, and
    // the panics below must not wedge the registry for the rest of the
    // process, so poisoning is ignored and the panic happens outside the
    // guard.
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

/// Returns the builder registered under `name`, or `None`.
///
/// Selection is fail-closed: a non-empty `Endpoint::transport_type` that is
/// not registered here is a hard connection error rather than a silent
/// fallback to the default HTTP/2 transport, so an explicit transport
/// selector can never silently change the protocol used for a connection.
pub fn get(name: &str) -> Option<Arc<dyn ClientTransportBuilder>> {
    REGISTRY
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(name)
        .cloned()
}

/// The `Service<Uri>` that tonic's reconnect layer drives for a custom
/// transport type: each call looks the builder up (fail-closed) and builds a
/// fresh transport for the target.
pub(crate) struct MakeCustomTransport {
    transport_type: String,
    opts: ClientBuildOptions,
}

impl MakeCustomTransport {
    pub(crate) fn new(transport_type: String, opts: ClientBuildOptions) -> Self {
        MakeCustomTransport {
            transport_type,
            opts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NopBuilder;
    impl ClientTransportBuilder for NopBuilder {
        fn build(
            &self,
            _target: Uri,
            _opts: ClientBuildOptions,
        ) -> Pin<Box<dyn Future<Output = Result<ClientTransport, BoxError>> + Send>> {
            Box::pin(async { Err("nop".into()) })
        }
    }

    #[test]
    fn register_and_get() {
        assert!(get("client-reg-absent").is_none());
        register("client-reg-present", NopBuilder);
        assert!(get("client-reg-present").is_some());
    }

    #[test]
    #[should_panic(expected = "register called twice")]
    fn duplicate_registration_panics() {
        register("client-reg-dup", NopBuilder);
        register("client-reg-dup", NopBuilder);
    }

    #[test]
    #[should_panic(expected = "empty name")]
    fn empty_name_panics() {
        register("", NopBuilder);
    }

    #[test]
    fn concurrent_get_is_safe() {
        register("client-reg-conc", NopBuilder);
        let handles: Vec<_> = (0..8)
            .map(|i| {
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        assert!(get("client-reg-conc").is_some());
                        assert!(get(&format!("client-reg-missing-{i}")).is_none());
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }
}

impl tower_service::Service<Uri> for MakeCustomTransport {
    type Response = ClientTransport;
    type Error = BoxError;
    type Future = BoxFuture<Result<ClientTransport, BoxError>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, target: Uri) -> Self::Future {
        let transport_type = self.transport_type.clone();
        let opts = self.opts.clone();
        Box::pin(async move {
            // Fail-closed: an explicitly selected transport type with no
            // registered builder is a connection error, never a fallback.
            let Some(builder) = get(&transport_type) else {
                return Err(format!(
                    "endpoint {target:?} requests transport type {transport_type:?}, which is \
                     not registered with tonic::transport::experimental::client"
                )
                .into());
            };
            let build = builder.build(target, opts.clone());
            match opts.connect_timeout {
                Some(t) => match tokio::time::timeout(t, build).await {
                    Ok(res) => res,
                    Err(_) => Err(crate::ConnectError(Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "transport connect timed out",
                    )))
                    .into()),
                },
                None => build.await,
            }
        })
    }
}
