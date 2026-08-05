//! EXPERIMENTAL pluggable transport extension point.
//!
//! This module defines a public contract that lets an out-of-tree transport —
//! for example a shared-memory transport — be selected and driven by tonic
//! WITHOUT depending on tonic internals, hyper, or h2. It is the tonic port
//! of grpc-go's `experimental/transport` pluggable-transport API.
//!
//! # Stability
//!
//! EXPERIMENTAL. The shapes here are deliberately minimal and tonic-native.
//! No compatibility is promised until at least one independent transport and
//! one fake transport pass a conformance suite. Do NOT depend on this from
//! production code.
//!
//! # Design
//!
//! The contract is byte-based at the gRPC framing level: a client transport
//! is a tower `Service` carrying `http::Request<Body>` to
//! `http::Response<Body>`, where the bodies stream already-framed gRPC
//! messages as ref-counted [`bytes::Bytes`] chunks plus a trailers frame.
//! Everything below that — wire framing, flow control, the byte carrier — is
//! owned by the transport. This mirrors the grpc-go API, where the mandatory
//! send path takes already-framed bytes and the receive path hands ref-counted
//! buffers upward.
//!
//! # Selection is fail-closed
//!
//! A transport type is selected explicitly: on the client via
//! [`Endpoint::transport_type`](crate::transport::Endpoint::transport_type),
//! on the server via [`server::serve_routes`] (or
//! `Router::serve_with_transport_type`). A non-empty transport type with no
//! registered builder is a hard error — never a silent fallback to the
//! default HTTP/2 path — so an explicit transport selector can never quietly
//! change the protocol on the wire. An unset transport type takes the default
//! hyper-based path, unchanged.

#[cfg(feature = "channel")]
pub mod client;
#[cfg(all(feature = "server", feature = "router"))]
pub mod server;

/// The boxed error type used across the experimental transport API.
///
/// Structurally identical to the boxed error tonic uses internally, spelled
/// out here so external transports can name it.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
