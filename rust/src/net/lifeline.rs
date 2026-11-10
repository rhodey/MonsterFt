//! An inherited pipe tells children when their supervising parent disappears.
use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
};
use tokio::io::unix::AsyncFd;

pub struct LifetimePipe {
    reader: OwnedFd,
    _writer: OwnedFd,
}

fn set_flags(fd: RawFd, command: libc::c_int, flags: libc::c_int) -> io::Result<()> {
    // SAFETY: The caller keeps this owned descriptor alive for the operation.
    let old = unsafe { libc::fcntl(fd, command) };
    if old < 0 {
        return Err(io::Error::last_os_error());
    }
    let set = if command == libc::F_GETFD {
        libc::F_SETFD
    } else {
        libc::F_SETFL
    };
    if unsafe { libc::fcntl(fd, set, old | flags) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

impl LifetimePipe {
    pub fn new() -> io::Result<Self> {
        let mut descriptors = [-1; 2];
        // SAFETY: pipe initializes both descriptors on success; neither is shared yet.
        if unsafe { libc::pipe(descriptors.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let reader = unsafe { OwnedFd::from_raw_fd(descriptors[0]) };
        let writer = unsafe { OwnedFd::from_raw_fd(descriptors[1]) };
        set_flags(reader.as_raw_fd(), libc::F_GETFD, libc::FD_CLOEXEC)?;
        set_flags(writer.as_raw_fd(), libc::F_GETFD, libc::FD_CLOEXEC)?;
        set_flags(reader.as_raw_fd(), libc::F_GETFL, libc::O_NONBLOCK)?;
        Ok(Self {
            reader,
            _writer: writer,
        })
    }

    /// Pass this descriptor with command-fds; never pass the write end.
    pub fn child_reader(&self) -> io::Result<OwnedFd> {
        self.reader.try_clone()
    }
}

pub struct ParentLifetime(AsyncFd<OwnedFd>);

impl ParentLifetime {
    /// Adopt exactly the descriptor supplied by the parent, before other code can use it.
    ///
    /// # Safety
    /// `fd` must be a live read end inherited for this purpose, with no Rust owner or
    /// other concurrent user. It becomes owned here, including on an error path.
    pub unsafe fn from_inherited(fd: RawFd) -> io::Result<Self> {
        if fd < 3 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "lifetime descriptor must not be standard I/O",
            ));
        }
        // SAFETY: The caller transfers unique ownership under the documented contract.
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        Self::new(owned)
    }

    pub fn new(owned: OwnedFd) -> io::Result<Self> {
        set_flags(owned.as_raw_fd(), libc::F_GETFD, libc::FD_CLOEXEC)?;
        set_flags(owned.as_raw_fd(), libc::F_GETFL, libc::O_NONBLOCK)?;
        // SAFETY: AsyncFd owns the stable descriptor; no other code closes or replaces it.
        let async_fd =
            unsafe { AsyncFd::register_with_interest(owned, tokio::io::Interest::READABLE) }
                .map_err(io::Error::other)?;
        Ok(Self(async_fd))
    }

    pub async fn closed(&self) -> io::Result<()> {
        loop {
            let mut ready = self.0.readable().await?;
            let result = ready.try_io(|fd| {
                let mut byte = 0_u8;
                // SAFETY: The buffer is writable and the descriptor is live and nonblocking.
                let read = unsafe {
                    libc::read(fd.get_ref().as_raw_fd(), (&mut byte as *mut u8).cast(), 1)
                };
                if read == 0 {
                    return Ok(());
                }
                if read > 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "lifetime pipe received unexpected data",
                    ));
                }
                Err(io::Error::last_os_error())
            });
            match result {
                Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
                Ok(result) => return result,
                Err(_) => {}
            }
        }
    }
}
