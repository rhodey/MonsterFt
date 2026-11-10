//! Owner-thread IPC: shmem-ipc on Linux, iceoryx2 on macOS.
//!
//! Setup creates one duplex endpoint per child. Only setup differs by platform;
//! senders synchronously enqueue whole frames and `recv` drives both directions.
use bytes::Bytes;
use std::{
    io,
    os::fd::{OwnedFd, RawFd},
    path::PathBuf,
};

#[cfg(target_os = "macos")]
use super::iceoryx::IpcEndpoint as Backend;
#[cfg(target_os = "macos")]
pub use super::iceoryx::IpcSender;
#[cfg(target_os = "linux")]
pub use super::shmem::ShmemSender as IpcSender;
#[cfg(target_os = "linux")]
use super::shmem::{ShmemEndpoint as Backend, ShmemHandles, ShmemOptions};

/// Named resources are used on macOS; Linux endpoints use inherited descriptors.
/// A namespace belongs to one supervision attempt and is removed after child exit.
#[derive(Clone, Debug)]
pub struct IpcNamespace {
    pub directory: PathBuf,
    pub prefix: String,
}
#[derive(Clone, Debug)]
pub struct IpcOptions {
    pub namespace: Option<IpcNamespace>,
    pub max_frame_bytes: usize,
    pub max_queued_bytes: usize,
    pub max_queued_messages: usize,
}
impl Default for IpcOptions {
    fn default() -> Self {
        Self {
            namespace: None,
            max_frame_bytes: 128 * 1024 * 1024,
            max_queued_bytes: 256 * 1024 * 1024,
            max_queued_messages: 64,
        }
    }
}
#[cfg(target_os = "linux")]
impl From<IpcOptions> for ShmemOptions {
    fn from(options: IpcOptions) -> Self {
        Self {
            max_frame_bytes: options.max_frame_bytes,
            max_queued_bytes: options.max_queued_bytes,
            max_queued_messages: options.max_queued_messages,
            ..Self::default()
        }
    }
}

pub struct IpcEndpoint(Backend);
impl IpcEndpoint {
    /// Returns the parent's endpoint and descriptors to inherit once in the child.
    /// Names identify directions from the parent's perspective on both sides.
    pub fn parent(
        outgoing: &str,
        incoming: &str,
        options: IpcOptions,
    ) -> io::Result<(Self, Vec<OwnedFd>)> {
        if outgoing == incoming {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPC directions must differ",
            ));
        }
        #[cfg(target_os = "linux")]
        {
            let (endpoint, handles) = Backend::pair(options.into())?;
            Ok((Self(endpoint), handles.0.into_iter().collect()))
        }
        #[cfg(target_os = "macos")]
        {
            Ok((
                Self(Backend::open(outgoing, incoming, options)?),
                Vec::new(),
            ))
        }
    }

    /// Opens the opposite endpoint using the descriptors returned by `parent`.
    /// The options and names must match the parent; descriptors cannot be reused.
    pub fn child(
        outgoing: &str,
        incoming: &str,
        options: IpcOptions,
        descriptors: Vec<OwnedFd>,
    ) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let _ = (outgoing, incoming);
            let handles = descriptors.try_into().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "expected seven IPC descriptors",
                )
            })?;
            Ok(Self(ShmemHandles(handles).open(options.into())?))
        }
        #[cfg(target_os = "macos")]
        {
            if !descriptors.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "named IPC takes no descriptors",
                ));
            }
            Ok(Self(Backend::open(incoming, outgoing, options)?))
        }
    }

    /// Adopts the parent's consecutive descriptor mappings before opening resources.
    /// macOS opens named services and does not adopt any descriptors.
    ///
    /// # Safety
    /// On Linux, `first_fd..first_fd + 7` must be the live, otherwise unowned
    /// descriptors returned by `parent`, mapped once in the same order. No other
    /// child may attach to those rings. Options must match the parent.
    pub unsafe fn from_inherited(
        outgoing: &str,
        incoming: &str,
        options: IpcOptions,
        first_fd: RawFd,
    ) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let _ = (outgoing, incoming);
            if first_fd < 3 || first_fd.checked_add(6).is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid IPC descriptor range",
                ));
            }
            // SAFETY: Ownership of these seven descriptors is transferred by the caller.
            let handles = unsafe {
                ShmemHandles::from_inherited(std::array::from_fn(|i| first_fd + i as RawFd))
            }?;
            Ok(Self(handles.open(options.into())?))
        }
        #[cfg(target_os = "macos")]
        {
            let _ = first_fd;
            Self::child(outgoing, incoming, options, Vec::new())
        }
    }
    pub fn sender(&self) -> IpcSender {
        self.0.sender()
    }
    pub async fn ready(&mut self) -> io::Result<()> {
        self.0.ready().await
    }
    pub async fn recv(&mut self) -> io::Result<Bytes> {
        self.0.recv().await
    }
}

/// Called only after all endpoints are dropped and children have exited.
/// Linux's anonymous resources are reclaimed by descriptor ownership.
pub fn cleanup_namespace(namespace: &IpcNamespace) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        super::iceoryx::cleanup_namespace(namespace)
    }
    #[cfg(target_os = "linux")]
    {
        let _ = namespace;
        Ok(())
    }
}
