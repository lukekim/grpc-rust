//! The rendezvous byte channel, per platform.
//!
//! On Unix it is a `tokio::net::UnixStream`; on Windows a tokio named pipe
//! (server or client end). Both expose the same readiness API
//! (`ready`/`try_read`/`try_write`), which is everything the engine needs:
//! the channel carries only the handshake, doorbell bytes, and liveness
//! (EOF/broken-pipe when the peer goes away).

use std::io;

use tokio::io::Interest;
#[cfg(windows)]
use tokio::io::Ready;

#[cfg(unix)]
pub(crate) type Conduit = tokio::net::UnixStream;

#[cfg(windows)]
pub(crate) enum Conduit {
    /// The accepting end of one connection (one pipe instance).
    Server(tokio::net::windows::named_pipe::NamedPipeServer),
    /// The dialing end.
    Client(tokio::net::windows::named_pipe::NamedPipeClient),
}

#[cfg(windows)]
impl Conduit {
    pub(crate) async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        match self {
            Conduit::Server(p) => p.ready(interest).await,
            Conduit::Client(p) => p.ready(interest).await,
        }
    }

    pub(crate) fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Conduit::Server(p) => p.try_read(buf),
            Conduit::Client(p) => p.try_read(buf),
        }
    }

    pub(crate) fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Conduit::Server(p) => p.try_write(buf),
            Conduit::Client(p) => p.try_write(buf),
        }
    }
}

#[cfg(windows)]
impl std::fmt::Debug for Conduit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Conduit::Server(_) => f.write_str("Conduit::Server"),
            Conduit::Client(_) => f.write_str("Conduit::Client"),
        }
    }
}

/// Tears the channel down so the PEER wakes promptly (the local side is woken
/// through the close flag + bell).
pub(crate) fn shutdown(conduit: &Conduit) {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: plain shutdown(2) on the live fd owned by the conduit.
        unsafe {
            libc::shutdown(conduit.as_raw_fd(), libc::SHUT_RDWR);
        }
    }
    #[cfg(windows)]
    {
        if let Conduit::Server(p) = conduit {
            let _ = p.disconnect();
        }
        // The client end has no shutdown; the handle closes when the Arc
        // drops (tasks exit promptly on the local close flag), and the
        // server then observes a broken pipe.
    }
}

/// Writes all of `buf` using the readiness API (works on both platforms).
pub(crate) async fn write_all(conduit: &Conduit, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        conduit.ready(Interest::WRITABLE).await?;
        match conduit.try_write(buf) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => buf = &buf[n..],
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Reads exactly `buf.len()` bytes using the readiness API.
pub(crate) async fn read_exact(conduit: &Conduit, buf: &mut [u8]) -> io::Result<()> {
    let mut off = 0;
    while off < buf.len() {
        conduit.ready(Interest::READABLE).await?;
        match conduit.try_read(&mut buf[off..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "shmsc: peer closed during handshake",
                ));
            }
            Ok(n) => off += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Maps a user-supplied target string to the platform rendezvous address.
///
/// Unix: the string is a filesystem path for the Unix socket, unchanged.
/// Windows: the string is mapped into the named-pipe namespace — a target
/// already starting with `\\.\pipe\` is used as-is, anything else becomes
/// `\\.\pipe\<target with path separators and colons replaced by '-'>`, so
/// the same harness/target string works on both platforms.
#[cfg(windows)]
pub(crate) fn pipe_name(target: &std::path::Path) -> String {
    let s = target.to_string_lossy();
    if s.starts_with(r"\\.\pipe\") || s.starts_with("//./pipe/") {
        return s.replace("//./pipe/", r"\\.\pipe\");
    }
    let sanitized: String = s
        .chars()
        .map(|c| {
            if c == '/' || c == '\\' || c == ':' {
                '-'
            } else {
                c
            }
        })
        .collect();
    format!(r"\\.\pipe\{}", sanitized.trim_start_matches('-'))
}
