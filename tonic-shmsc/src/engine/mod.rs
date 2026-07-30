//! The self-contained shared-memory engine: segment, rings, framing,
//! connection machinery, and the client/server halves.

pub(crate) mod client;
pub(crate) mod conduit;
pub(crate) mod conn;
pub(crate) mod frame;
#[cfg(feature = "_test-support")]
pub(crate) mod raw_peer;
pub(crate) mod ring;
pub(crate) mod seg;
pub(crate) mod server;
