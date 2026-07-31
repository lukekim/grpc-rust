//! Shared-memory segment creation, mapping, layout, and fd passing.
//!
//! A connection is backed by one anonymous shared-memory segment created by the
//! server, passed to the client as a file descriptor over the Unix-domain
//! rendezvous socket (`SCM_RIGHTS`). The segment is *anonymous*: on Linux it is
//! a `memfd`, on other Unix platforms it is `shm_open`ed and immediately
//! `shm_unlink`ed, so no name outlives the handshake and segments can never
//! leak past the processes holding them — process death reclaims everything.
//!
//! # Layout
//!
//! ```text
//! offset 0                SegmentHeader (64 bytes)
//! offset 64               c2s RingHeader (128 bytes)
//! offset 192              c2s ring data  (c2s_cap bytes, power of two)
//! offset 192 + c2s_cap    s2c RingHeader (128 bytes)
//! offset 320 + c2s_cap    s2c ring data  (s2c_cap bytes, power of two)
//! ```
//!
//! All headers are 64-byte aligned; ring capacities are powers of two of at
//! least one page, so every field below keeps its natural alignment.

use std::io;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// Magic value identifying an shmsc segment ("SHMSC1").
pub(crate) const MAGIC: u64 = u64::from_le_bytes(*b"SHMSC1\0\0");
/// Protocol version spoken over the segment.
pub(crate) const VERSION: u32 = 1;
/// Size of the segment header at offset 0.
pub(crate) const SEG_HDR_SIZE: usize = 64;
/// Size of one ring header.
pub(crate) const RING_HDR_SIZE: usize = 128;
/// Smallest permitted ring capacity (one page).
pub(crate) const MIN_RING_CAP: usize = 4096;
/// Largest permitted ring capacity (1 GiB); an upper bound keeps a corrupt or
/// hostile hello from making us map an absurd segment.
pub(crate) const MAX_RING_CAP: usize = 1 << 30;

/// Computed byte offsets for a segment with the given ring capacities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Layout {
    pub(crate) c2s_hdr: usize,
    pub(crate) c2s_data: usize,
    pub(crate) c2s_cap: usize,
    pub(crate) s2c_hdr: usize,
    pub(crate) s2c_data: usize,
    pub(crate) s2c_cap: usize,
    pub(crate) total: usize,
}

impl Layout {
    /// Computes the layout, validating capacities. Capacities must be powers of
    /// two within [`MIN_RING_CAP`], [`MAX_RING_CAP`].
    pub(crate) fn new(c2s_cap: usize, s2c_cap: usize) -> io::Result<Self> {
        for cap in [c2s_cap, s2c_cap] {
            if !cap.is_power_of_two() || !(MIN_RING_CAP..=MAX_RING_CAP).contains(&cap) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("shmsc: invalid ring capacity {cap}"),
                ));
            }
        }
        let c2s_hdr = SEG_HDR_SIZE;
        let c2s_data = c2s_hdr + RING_HDR_SIZE;
        let s2c_hdr = c2s_data + c2s_cap;
        let s2c_data = s2c_hdr + RING_HDR_SIZE;
        let total = s2c_data + s2c_cap;
        Ok(Layout {
            c2s_hdr,
            c2s_data,
            c2s_cap,
            s2c_hdr,
            s2c_data,
            s2c_cap,
            total,
        })
    }
}

/// Stamps the identifying header of a freshly created segment mapping.
///
/// # Safety
/// `p` must point at a live mapping of at least `SEG_HDR_SIZE` bytes that no
/// other process observes yet.
unsafe fn stamp_header(p: *mut u8, layout: &Layout) {
    // SAFETY: forwarded contract; offsets are in bounds of the header.
    unsafe {
        (p.add(0).cast::<u64>()).write(MAGIC);
        (p.add(8).cast::<u32>()).write(VERSION);
        (p.add(12).cast::<u32>()).write(0); // flags
        (p.add(16).cast::<u64>()).write(layout.c2s_cap as u64);
        (p.add(24).cast::<u64>()).write(layout.s2c_cap as u64);
    }
}

/// Validates the stamped header of a mapping received from the peer.
fn validate_stamp(p: *mut u8, layout: &Layout) -> io::Result<()> {
    // SAFETY: callers map at least `layout.total >= SEG_HDR_SIZE` bytes;
    // reads are of plain integers.
    let (magic, version, c2s, s2c) = unsafe {
        (
            (p.add(0).cast::<u64>()).read(),
            (p.add(8).cast::<u32>()).read(),
            (p.add(16).cast::<u64>()).read(),
            (p.add(24).cast::<u64>()).read(),
        )
    };
    if magic != MAGIC || version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "shmsc: segment header mismatch (magic/version)",
        ));
    }
    if c2s != layout.c2s_cap as u64 || s2c != layout.s2c_cap as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "shmsc: segment ring capacities disagree with hello",
        ));
    }
    Ok(())
}

/// An mmap'ed shared-memory segment. Unmapped on drop; the backing object is
/// anonymous, so the last unmap (in either process) frees the memory.
#[cfg(unix)]
pub(crate) struct Segment {
    ptr: *mut u8,
    len: usize,
    /// Keeps the backing fd alive for the mapping's lifetime; the server also
    /// passes it to the client during the handshake.
    fd: OwnedFd,
    pub(crate) layout: Layout,
}

// SAFETY: the segment is raw shared memory; all access goes through raw
// pointers and atomics with explicit synchronization. No thread-affine state.
unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment")
            .field("len", &self.len)
            .field("layout", &self.layout)
            .finish()
    }
}

#[cfg(unix)]
impl Drop for Segment {
    fn drop(&mut self) {
        // SAFETY: ptr/len came from a successful mmap and are unmapped once.
        unsafe {
            libc::munmap(self.ptr.cast(), self.len);
        }
    }
}

#[cfg(unix)]
impl Segment {
    /// Creates a new anonymous segment sized for `layout` and maps it.
    pub(crate) fn create(layout: Layout) -> io::Result<Self> {
        let fd = create_anon_fd(layout.total)?;
        let seg = Self::map(fd, layout)?;
        // Stamp the header so the peer can validate what it mapped.
        // SAFETY: fresh, exclusively-owned mapping of at least a header.
        unsafe {
            stamp_header(seg.ptr, &layout);
        }
        Ok(seg)
    }

    /// Maps an existing segment fd received from the peer and validates its
    /// header against `layout` (which was independently validated from the
    /// hello message).
    pub(crate) fn from_fd(fd: OwnedFd, layout: Layout) -> io::Result<Self> {
        // Never trust the peer's size claim: validate against the fd itself.
        // SAFETY: fstat on a valid owned fd with a zeroed out-param.
        let actual = unsafe {
            let mut st: libc::stat = std::mem::zeroed();
            if libc::fstat(fd.as_raw_fd(), &mut st) != 0 {
                return Err(io::Error::last_os_error());
            }
            st.st_size
        };
        if actual < layout.total as i64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("shmsc: segment is {actual} bytes, need {}", layout.total),
            ));
        }
        let seg = Self::map(fd, layout)?;
        validate_stamp(seg.ptr, &layout)?;
        Ok(seg)
    }

    fn map(fd: OwnedFd, layout: Layout) -> io::Result<Self> {
        // SAFETY: mapping a validated length over a whole owned fd.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                layout.total,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Segment {
            ptr: ptr.cast(),
            len: layout.total,
            fd,
            layout,
        })
    }

    /// Raw base pointer of the mapping.
    pub(crate) fn base(&self) -> *mut u8 {
        self.ptr
    }

    /// The backing fd (passed to the peer during the handshake).
    pub(crate) fn fd(&self) -> &OwnedFd {
        &self.fd
    }
}

/// Creates an anonymous shared-memory fd of `len` bytes.
#[cfg(unix)]
fn create_anon_fd(len: usize) -> io::Result<OwnedFd> {
    #[cfg(target_os = "linux")]
    let fd = {
        // MFD_ALLOW_SEALING so the size can be frozen after ftruncate (below):
        // once shared with the peer, a hostile dialer must not be able to
        // shrink the segment and SIGBUS us on the next ring access.
        // SAFETY: plain syscall; the name is a debugging label only.
        let raw = unsafe {
            libc::syscall(
                libc::SYS_memfd_create,
                c"shmsc-segment".as_ptr(),
                (libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING) as libc::c_uint,
            )
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fresh fd returned by memfd_create, owned from here on.
        unsafe { OwnedFd::from_raw_fd(raw as std::os::fd::RawFd) }
    };
    #[cfg(not(target_os = "linux"))]
    let fd = {
        // shm_open + immediate shm_unlink yields an anonymous segment. macOS
        // limits names to 31 bytes (PSHMNAMLEN); this one is well under.
        use std::io::Write as _;
        let mut attempts = 0u32;
        loop {
            let mut name = [0u8; 32];
            let uniq = {
                // A cheap unique-enough name: pid + a monotonic counter.
                static CTR: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
                let c = CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                (std::process::id(), c)
            };
            let mut cur = std::io::Cursor::new(&mut name[..]);
            let _ = write!(cur, "/shmsc-{:x}-{:x}", uniq.0, uniq.1);
            // SAFETY: NUL-terminated buffer; O_EXCL prevents races on the name.
            let raw = unsafe {
                libc::shm_open(
                    name.as_ptr().cast::<libc::c_char>(),
                    libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                    0o600 as libc::c_uint,
                )
            };
            if raw < 0 {
                let err = io::Error::last_os_error();
                attempts += 1;
                if err.kind() == io::ErrorKind::AlreadyExists && attempts < 16 {
                    continue;
                }
                return Err(err);
            }
            // Unlink immediately: the fd keeps the segment alive, the name
            // does not outlive this call, and process death reclaims all.
            // SAFETY: same NUL-terminated name that was just created.
            unsafe {
                libc::shm_unlink(name.as_ptr().cast::<libc::c_char>());
            }
            // SAFETY: fresh fd returned by shm_open, owned from here on.
            break unsafe { OwnedFd::from_raw_fd(raw) };
        }
    };
    // SAFETY: sizing a fresh, exclusively-owned segment fd.
    let rc = unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    // Linux: freeze the size before the fd is mapped or passed to the peer.
    // F_SEAL_SHRINK | F_SEAL_GROW make the segment un-resizable by anyone
    // holding the fd (including the peer we hand it to over SCM_RIGHTS), so a
    // hostile peer cannot `ftruncate` it out from under our mapping and
    // SIGBUS this process on the next access past the new EOF; F_SEAL_SEAL
    // prevents the seals from being lifted. Writes are unaffected (no
    // F_SEAL_WRITE), so both rings still work. macOS/other Unix have no
    // memfd-style seal — see DESIGN.md §8; the anti-SIGBUS guarantee is
    // Linux-only there.
    #[cfg(target_os = "linux")]
    {
        // SAFETY: F_ADD_SEALS on our own freshly created memfd, before it is
        // mapped or shared.
        let rc = unsafe {
            libc::fcntl(
                fd.as_raw_fd(),
                libc::F_ADD_SEALS,
                libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(fd)
}

/// Sends `payload` plus one file descriptor over a Unix socket fd in a single
/// message (`SCM_RIGHTS`). Nonblocking-transparent: returns `WouldBlock`
/// untouched so it can run inside `tokio::net::UnixStream::async_io`.
#[cfg(unix)]
pub(crate) fn send_with_fd(
    sock: std::os::fd::RawFd,
    payload: &[u8],
    fd: &OwnedFd,
) -> io::Result<()> {
    let mut iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };
    let mut cmsg_buf = [0u8; 64];
    // SAFETY: standard sendmsg + SCM_RIGHTS construction; all pointers refer
    // to live stack buffers and the cmsg buffer is large enough for one fd.
    unsafe {
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_buf.as_mut_ptr().cast();
        msg.msg_controllen = libc::CMSG_SPACE(std::mem::size_of::<libc::c_int>() as u32) as _;
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<libc::c_int>() as u32) as _;
        std::ptr::write_unaligned(libc::CMSG_DATA(cmsg).cast::<libc::c_int>(), fd.as_raw_fd());
        let n = libc::sendmsg(sock, &msg, 0);
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        // A 32-byte datagram-with-ancillary on a stream socket never splits
        // in practice; treat a short write as a hard error to stay simple.
        if n as usize != payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "shmsc: short sendmsg",
            ));
        }
    }
    Ok(())
}

/// Receives exactly `len` payload bytes and one file descriptor from a Unix
/// socket fd. The fd must arrive with the first message. Nonblocking-
/// transparent, for use inside `async_io`.
#[cfg(unix)]
pub(crate) fn recv_with_fd(sock: std::os::fd::RawFd, len: usize) -> io::Result<(Vec<u8>, OwnedFd)> {
    let mut payload = vec![0u8; len];
    let mut cmsg_buf = [0u8; 64];
    let mut iov = libc::iovec {
        iov_base: payload.as_mut_ptr().cast(),
        iov_len: payload.len(),
    };
    // SAFETY: standard recvmsg; pointers refer to live buffers.
    let (n, fd) = unsafe {
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_buf.as_mut_ptr().cast();
        msg.msg_controllen = cmsg_buf.len() as _;
        let n = libc::recvmsg(sock, &mut msg, 0);
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        if (msg.msg_flags & libc::MSG_CTRUNC) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shmsc: truncated control data",
            ));
        }
        let mut fd = None;
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let raw = std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<libc::c_int>());
                fd = Some(OwnedFd::from_raw_fd(raw));
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
        (n as usize, fd)
    };
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "shmsc: peer closed in hello",
        ));
    }
    let Some(fd) = fd else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "shmsc: hello arrived without a segment fd",
        ));
    };
    if n != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "shmsc: short hello",
        ));
    }
    Ok((payload, fd))
}

/// A view of a pagefile-backed named file mapping. Windows kernel objects
/// are reference counted, so the mapping dies with the last open handle in
/// either process — the same no-leak property the Unix side gets from
/// anonymous unlinked segments.
#[cfg(windows)]
pub(crate) struct Segment {
    ptr: *mut u8,
    len: usize,
    /// Keeps the section object alive for the mapping's lifetime.
    _handle: std::os::windows::io::OwnedHandle,
    pub(crate) layout: Layout,
}

/// Longest section name accepted from a hello (defense against a corrupt
/// length; generated names are ~50 chars).
#[cfg(windows)]
pub(crate) const MAX_SECTION_NAME: usize = 256;

#[cfg(windows)]
impl Drop for Segment {
    fn drop(&mut self) {
        // SAFETY: ptr came from a successful MapViewOfFile, unmapped once.
        unsafe {
            windows_sys::Win32::System::Memory::UnmapViewOfFile(
                windows_sys::Win32::System::Memory::MEMORY_MAPPED_VIEW_ADDRESS {
                    Value: self.ptr.cast(),
                },
            );
        }
    }
}

#[cfg(windows)]
impl Segment {
    /// Creates a new randomly named section sized for `layout`, maps it, and
    /// returns the segment plus the name the peer opens it by.
    ///
    /// The name carries ~128 bits of OS-seeded randomness in the session-local
    /// `Local\` namespace, and the section inherits the creator's default
    /// security descriptor, so only the same user can open it — the Windows
    /// analog of the Unix side's `0600` boundary. The name is only ever
    /// disclosed over the (same-user) named pipe.
    pub(crate) fn create_named(layout: Layout) -> io::Result<(Self, String)> {
        use std::hash::{BuildHasher, Hasher};
        use std::os::windows::io::FromRawHandle;
        use windows_sys::Win32::Foundation::{ERROR_ALREADY_EXISTS, GetLastError};
        use windows_sys::Win32::System::Memory::{CreateFileMappingW, PAGE_READWRITE};

        let mut attempts = 0u32;
        loop {
            // OS-seeded entropy without a rand dependency: each RandomState
            // draws a fresh random key from the OS.
            let r1 = std::hash::RandomState::new().build_hasher().finish();
            let r2 = std::hash::RandomState::new().build_hasher().finish();
            let name = format!(r"Local\shmsc-{}-{r1:016x}{r2:016x}", std::process::id());
            let wide: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
            // SAFETY: plain CreateFileMappingW with a NUL-terminated wide
            // name; INVALID_HANDLE_VALUE selects a pagefile-backed section.
            let handle = unsafe {
                CreateFileMappingW(
                    windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE,
                    std::ptr::null(),
                    PAGE_READWRITE,
                    (layout.total as u64 >> 32) as u32,
                    (layout.total as u64 & 0xFFFF_FFFF) as u32,
                    wide.as_ptr(),
                )
            };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: handle is a fresh, owned section handle from here on.
            let owned =
                unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(handle.cast()) };
            // SAFETY: GetLastError immediately after the create call.
            if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
                attempts += 1;
                if attempts >= 16 {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "shmsc: could not create a unique section name",
                    ));
                }
                continue;
            }
            let seg = Self::map(owned, layout)?;
            // SAFETY: fresh, exclusively-owned mapping.
            unsafe {
                stamp_header(seg.ptr, &layout);
            }
            return Ok((seg, name));
        }
    }

    /// Opens the peer-created section by name and validates it against
    /// `layout` (independently validated from the hello message). Mapping
    /// exactly `layout.total` bytes fails if the section is smaller, so the
    /// peer's size claim is never trusted.
    pub(crate) fn open_named(name: &str, layout: Layout) -> io::Result<Self> {
        use std::os::windows::io::FromRawHandle;
        use windows_sys::Win32::System::Memory::{FILE_MAP_ALL_ACCESS, OpenFileMappingW};

        let wide: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        // SAFETY: plain OpenFileMappingW with a NUL-terminated wide name.
        let handle = unsafe { OpenFileMappingW(FILE_MAP_ALL_ACCESS, 0, wide.as_ptr()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fresh, owned section handle.
        let owned = unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(handle.cast()) };
        let seg = Self::map(owned, layout)?;
        validate_stamp(seg.ptr, &layout)?;
        Ok(seg)
    }

    fn map(handle: std::os::windows::io::OwnedHandle, layout: Layout) -> io::Result<Self> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::Memory::{FILE_MAP_ALL_ACCESS, MapViewOfFile};

        // SAFETY: mapping a validated length over a live section handle.
        let view = unsafe {
            MapViewOfFile(
                handle.as_raw_handle().cast(),
                FILE_MAP_ALL_ACCESS,
                0,
                0,
                layout.total,
            )
        };
        if view.Value.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Segment {
            ptr: view.Value.cast(),
            len: layout.total,
            _handle: handle,
            layout,
        })
    }

    /// Raw base pointer of the mapping.
    pub(crate) fn base(&self) -> *mut u8 {
        self.ptr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::net::UnixStream;

    #[test]
    fn layout_offsets() {
        let l = Layout::new(4096, 8192).unwrap();
        assert_eq!(l.c2s_hdr, 64);
        assert_eq!(l.c2s_data, 192);
        assert_eq!(l.s2c_hdr, 192 + 4096);
        assert_eq!(l.s2c_data, 320 + 4096);
        assert_eq!(l.total, 320 + 4096 + 8192);
    }

    #[test]
    fn layout_rejects_bad_caps() {
        assert!(Layout::new(3000, 4096).is_err()); // not a power of two
        assert!(Layout::new(2048, 4096).is_err()); // below the minimum
        assert!(Layout::new(4096, MAX_RING_CAP * 2).is_err()); // above the maximum
    }

    #[test]
    #[cfg(unix)]
    fn create_map_and_pass_fd() {
        let layout = Layout::new(4096, 4096).unwrap();
        let seg = Segment::create(layout).unwrap();
        // Write through one mapping, observe through a second mapping obtained
        // by passing the fd over a socketpair, as the real handshake does.
        let (a, b) = UnixStream::pair().unwrap();
        send_with_fd(a.as_raw_fd(), b"hello", seg.fd()).unwrap();
        let (payload, rfd) = recv_with_fd(b.as_raw_fd(), 5).unwrap();
        assert_eq!(&payload, b"hello");
        let seg2 = Segment::from_fd(rfd, layout).unwrap();
        // SAFETY: test-only in-bounds writes/reads at the first data byte.
        unsafe {
            seg.base().add(layout.c2s_data).write(0xEE);
            assert_eq!(seg2.base().add(layout.c2s_data).read(), 0xEE);
        }
    }

    /// The created segment fd must be size-sealed so a peer holding it (passed
    /// over SCM_RIGHTS) cannot `ftruncate` it and SIGBUS the other side on the
    /// next ring access — the truncation attack from the adversarial review.
    #[test]
    #[cfg(target_os = "linux")]
    fn linux_segment_size_is_sealed() {
        let layout = Layout::new(4096, 4096).unwrap();
        let seg = Segment::create(layout).unwrap();
        let fd = seg.fd().as_raw_fd();
        // Shrink and grow must both be refused (EPERM) by the size seals.
        // SAFETY: ftruncate on a valid owned fd; we assert it is refused.
        let shrink = unsafe { libc::ftruncate(fd, 0) };
        assert_ne!(shrink, 0, "shrink must be refused by F_SEAL_SHRINK");
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EPERM));
        // SAFETY: as above.
        let grow = unsafe { libc::ftruncate(fd, (layout.total + 4096) as libc::off_t) };
        assert_ne!(grow, 0, "grow must be refused by F_SEAL_GROW");
        // And the seal set itself is locked (F_SEAL_SEAL), so it cannot be lifted.
        // SAFETY: F_GET_SEALS on a valid fd.
        let seals = unsafe { libc::fcntl(fd, libc::F_GET_SEALS) };
        assert!(
            seals >= 0 && seals & libc::F_SEAL_SEAL != 0,
            "seals must be locked"
        );
    }
}
