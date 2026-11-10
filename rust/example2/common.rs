#![allow(dead_code)] // Shared by separate binaries, each using its own role helpers.

use bytes::Bytes;
use monsterft::{
    error::{Code, Error, Result},
    net::{
        ipc::{IpcEndpoint, IpcNamespace, IpcOptions},
        lifeline::ParentLifetime,
    },
};
use std::{os::fd::RawFd, path::PathBuf};

pub const NODES: [&str; 3] = ["1", "2", "3"];

// These arguments identify one node and one Supervisor startup attempt.
pub struct ChildArgs {
    pub id: String,
    pub database_path: PathBuf,
    pub namespace: IpcNamespace,
    pub parent_fd: RawFd,
}

impl ChildArgs {
    pub fn parse() -> Result<Self> {
        let args: Vec<_> = std::env::args().skip(1).collect();

        if args.len() != 5 || !NODES.contains(&args[0].as_str()) {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "expected nodeId databasePath ipcDirectory ipcPrefix parentFd",
            ));
        }

        Ok(Self {
            id: args[0].clone(),
            database_path: args[1].clone().into(),
            namespace: IpcNamespace {
                directory: args[2].clone().into(),
                prefix: args[3].clone(),
            },
            parent_fd: args[4]
                .parse()
                .map_err(|_| Error::new(Code::ArgumentIllegal, "invalid parent descriptor"))?,
        })
    }
}

pub fn endpoints(role: &str) -> (String, String) {
    (format!("gate-{role}"), format!("{role}-gate"))
}

pub fn child_endpoint(args: &ChildArgs, role: &str) -> Result<IpcEndpoint> {
    let (to_child, to_gate) = endpoints(role);

    // SAFETY: Entry maps this child's IPC descriptors consecutively from 4.
    // The lifetime pipe occupies 3; macOS needs only the named services.
    Ok(unsafe {
        IpcEndpoint::from_inherited(
            &to_child,
            &to_gate,
            IpcOptions {
                namespace: Some(args.namespace.clone()),
                ..IpcOptions::default()
            },
            4,
        )
    }?)
}

pub fn parent_lifetime(args: &ChildArgs) -> Result<ParentLifetime> {
    // The parent maps only the pipe read end to this explicit descriptor. Adopt
    // it before other child resources are opened; never scan /proc on macOS.
    Ok(unsafe { ParentLifetime::from_inherited(args.parent_fd) }?)
}

pub fn send_error(error: std::io::Error) -> Error {
    Error::new(Code::SendError, error.to_string())
}

pub fn command() -> Bytes {
    Bytes::from_static(b"increment")
}
