//! Hostile-peer tests: the trust-model boundary, enforced.
//!
//! The documented model is "the peer is trusted, but every length, offset,
//! and position read from shared memory is bounds-checked: a hostile peer
//! can corrupt gRPC traffic, never cause out-of-bounds access, and a
//! protocol violation fails the connection". These tests act as that
//! hostile peer — completing a real handshake through the public listener,
//! then writing malformed bytes straight into the ring — and assert three
//! things: the hostile connection dies, the process does not, and a
//! well-behaved connection on the same listener keeps working.
//!
//! They live inside the crate (not tests/) because a convincing attacker
//! needs the engine's ring/frame internals.

#![cfg(all(test, unix))]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use http::{Request, Response};
use tonic::body::Body;

use super::conduit::{self, Conduit};
use super::conn::{Config, client_handshake};
use super::frame::{FRAME_HDR_SIZE, FrameType, encode_frame_header};
use super::ring::{Consumer, Producer};
use super::seg::Segment;
use super::server::ShmscListener;

/// A trivial always-OK gRPC-ish service (unary echo of raw body bytes is
/// not needed — the hostile tests only need the server