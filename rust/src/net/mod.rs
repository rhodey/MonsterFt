pub mod ipc;
pub mod lifeline;
pub mod tcp;
pub mod tcpcluster;

#[cfg(target_os = "macos")]
pub mod iceoryx;
#[cfg(target_os = "linux")]
pub mod shmem;
