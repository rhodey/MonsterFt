//! Bounded, one-publisher/one-subscriber shared-memory byte channels.
//!
//! A channel carries whole frames. Notifications only wake the owner's event loop;
//! no frame bytes travel through the notification socket.

use bytes::Bytes;
use iceoryx2::{
    port::{listener::Listener, notifier::Notifier, publisher::Publisher, subscriber::Subscriber},
    prelude::*,
    service::port_factory::{event, publish_subscribe},
};
use std::{
    cell::RefCell,
    collections::VecDeque,
    io,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
    time::Duration,
};
use tokio::{io::unix::AsyncFd, sync::Notify};

type DataService = publish_subscribe::PortFactory<ipc::Service, [u8], ()>;
type EventService = event::PortFactory<ipc::Service>;

pub use super::ipc::{IpcNamespace, IpcOptions};

struct Queue {
    frames: VecDeque<Bytes>,
    bytes: usize,
    stopped: bool,
}

struct Outbound {
    options: IpcOptions,
    queue: RefCell<Queue>,
    wake: Notify,
}

/// Cloneable owner-thread handle. Enqueueing never waits for publication or ACK.
#[derive(Clone)]
pub struct IpcSender(Rc<Outbound>);

impl IpcSender {
    pub fn send(&self, frame: Bytes) -> io::Result<()> {
        let mut q = self.0.queue.borrow_mut();
        if q.stopped {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "IPC is stopped"));
        }
        if frame.len() > self.0.options.max_frame_bytes {
            q.stopped = true;
            self.0.wake.notify_one();
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPC frame is too large",
            ));
        }
        if q.frames.len() >= self.0.options.max_queued_messages
            || frame.len() > self.0.options.max_queued_bytes.saturating_sub(q.bytes)
        {
            q.stopped = true;
            self.0.wake.notify_one();
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "IPC outbound queue is full",
            ));
        }
        q.bytes += frame.len();
        q.frames.push_back(frame);
        drop(q);
        self.0.wake.notify_one();
        Ok(())
    }

    pub fn stop(&self) {
        let mut q = self.0.queue.borrow_mut();
        q.stopped = true;
        q.frames.clear();
        q.bytes = 0;
        drop(q);
        self.0.wake.notify_one();
    }

    pub fn is_stopped(&self) -> bool {
        self.0.queue.borrow().stopped
    }
    pub fn queued_bytes(&self) -> usize {
        self.0.queue.borrow().bytes
    }
}

// Own the listener inside AsyncFd: its descriptor cannot close before deregistration.
struct Notification(Listener<ipc::Service>);
impl AsRawFd for Notification {
    fn as_raw_fd(&self) -> RawFd {
        // SAFETY: AsyncFd borrows this descriptor only while its owning listener lives.
        unsafe { self.0.file_descriptor().native_handle() }
    }
}

/// Drive `recv()` continuously on the component's current-thread runtime.
/// Dropping it closes only this endpoint; resource names must be unique per attempt.
pub struct IpcEndpoint {
    sender: IpcSender,
    publisher: Publisher<ipc::Service, [u8], ()>,
    subscriber: Subscriber<ipc::Service, [u8], ()>,
    notifier: Notifier<ipc::Service>,
    notification: AsyncFd<Notification>,
    outgoing: DataService,
    incoming: DataService,
    outgoing_event: EventService,
    incoming_event: EventService,
    _node: Node<ipc::Service>,
    ready: bool,
}

fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

fn configuration(namespace: Option<&IpcNamespace>) -> io::Result<Config> {
    let mut config = Config::default();
    // Never discover external configuration or clean another application's resources.
    config.global.node.cleanup_dead_nodes_on_creation = false;
    config.global.node.cleanup_dead_nodes_on_destruction = false;
    config.global.service.cleanup_dead_nodes_on_open = false;
    if let Some(namespace) = namespace {
        if !namespace.directory.is_absolute() || namespace.prefix.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPC namespace is illegal",
            ));
        }
        let path = namespace.directory.to_str().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "IPC directory must be UTF-8")
        })?;
        config
            .global
            .set_root_path(&path.try_into().map_err(other)?);
        config.global.prefix = namespace.prefix.as_str().try_into().map_err(other)?;
    }
    Ok(config)
}

/// Remove crashed owners' resources only after all owners of this namespace exit.
/// The caller still owns removal of its temporary directory.
pub fn cleanup_namespace(namespace: &IpcNamespace) -> io::Result<()> {
    let config = configuration(Some(namespace))?;
    let mut error = None;
    Node::<ipc::Service>::list(&config, |state| {
        match state {
            NodeState::Dead(view) => {
                if let Err(e) = view.try_remove_stale_resources() {
                    error = Some(other(e));
                }
            }
            NodeState::Alive(_) => {
                error = Some(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "IPC namespace still has a live owner",
                ))
            }
            _ => error = Some(io::Error::other("IPC owner state is unavailable")),
        }
        CallbackProgression::Continue
    })
    .map_err(other)?;
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn data_service(node: &Node<ipc::Service>, name: &str) -> io::Result<DataService> {
    node.service_builder(&name.try_into().map_err(other)?)
        .publish_subscribe::<[u8]>()
        .max_publishers(1)
        .max_subscribers(1)
        .max_nodes(2)
        .history_size(0)
        .enable_safe_overflow(false)
        .subscriber_max_buffer_size(2)
        .subscriber_max_borrowed_samples(1)
        .open_or_create()
        .map_err(other)
}

fn event_service(node: &Node<ipc::Service>, name: &str) -> io::Result<EventService> {
    node.service_builder(&format!("{name}/wake").as_str().try_into().map_err(other)?)
        .event()
        .max_notifiers(1)
        .max_listeners(1)
        .max_nodes(2)
        .open_or_create()
        .map_err(other)
}

impl IpcEndpoint {
    pub fn open(outgoing_name: &str, incoming_name: &str, options: IpcOptions) -> io::Result<Self> {
        if outgoing_name == incoming_name
            || options.max_frame_bytes == 0
            || options.max_queued_bytes < options.max_frame_bytes
            || options.max_queued_messages == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPC options are illegal",
            ));
        }
        let config = configuration(options.namespace.as_ref())?;
        let node = NodeBuilder::new()
            .config(&config)
            .signal_handling_mode(SignalHandlingMode::Disabled)
            .create::<ipc::Service>()
            .map_err(other)?;
        let outgoing = data_service(&node, outgoing_name)?;
        let incoming = data_service(&node, incoming_name)?;
        let outgoing_event = event_service(&node, outgoing_name)?;
        let incoming_event = event_service(&node, incoming_name)?;
        let publisher = outgoing
            .publisher_builder()
            .initial_max_slice_len(4096.min(options.max_frame_bytes))
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .max_loaned_samples(1)
            .backpressure_strategy(BackpressureStrategy::DiscardData)
            .create()
            .map_err(other)?;
        let subscriber = incoming.subscriber_builder().create().map_err(other)?;
        let notifier = outgoing_event.notifier_builder().create().map_err(other)?;
        let listener = incoming_event.listener_builder().create().map_err(other)?;
        // SAFETY: Notification owns its stable descriptor until AsyncFd deregisters it.
        let notification = unsafe {
            AsyncFd::register_with_interest(Notification(listener), tokio::io::Interest::READABLE)
        }
        .map_err(other)?;
        let sender = IpcSender(Rc::new(Outbound {
            options,
            queue: RefCell::new(Queue {
                frames: VecDeque::new(),
                bytes: 0,
                stopped: false,
            }),
            wake: Notify::new(),
        }));
        Ok(Self {
            sender,
            publisher,
            subscriber,
            notifier,
            notification,
            outgoing,
            incoming,
            outgoing_event,
            incoming_event,
            _node: node,
            ready: false,
        })
    }

    pub fn sender(&self) -> IpcSender {
        self.sender.clone()
    }

    /// Gate startup must wait for both directions and their notifications.
    pub async fn ready(&mut self) -> io::Result<()> {
        while !self.ready {
            if self.sender.is_stopped() {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "IPC is stopped"));
            }
            self.ready = self.outgoing.dynamic_config().number_of_subscribers() == 1
                && self.incoming.dynamic_config().number_of_publishers() == 1
                && self.outgoing_event.dynamic_config().number_of_listeners() == 1
                && self.incoming_event.dynamic_config().number_of_notifiers() == 1;
            if !self.ready {
                tokio::select! { _ = tokio::time::sleep(Duration::from_millis(1)) => {}, _ = self.sender.0.wake.notified() => {} }
            }
        }
        Ok(())
    }

    fn publish(&self) -> io::Result<()> {
        loop {
            let frame = {
                let q = self.sender.0.queue.borrow();
                if q.stopped {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "IPC is stopped"));
                }
                q.frames.front().cloned()
            };
            let Some(frame) = frame else {
                return Ok(());
            };
            let sample = self
                .publisher
                .loan_slice_uninit(frame.len())
                .map_err(other)?
                .write_from_slice(&frame);
            if sample.send().map_err(other)? != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "IPC frame was not delivered",
                ));
            }
            if self.notifier.notify().map_err(other)? != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "IPC notification was not delivered",
                ));
            }
            let mut q = self.sender.0.queue.borrow_mut();
            q.frames.pop_front();
            q.bytes -= frame.len();
        }
    }

    async fn receive(&mut self) -> io::Result<Bytes> {
        if !self.ready {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "IPC is not ready",
            ));
        }
        loop {
            self.publish()?;
            if let Some(sample) = self.subscriber.receive().map_err(other)? {
                if sample.payload().len() > self.sender.0.options.max_frame_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "IPC received oversized frame",
                    ));
                }
                return Ok(Bytes::copy_from_slice(sample.payload()));
            }
            tokio::select! {
                _ = self.sender.0.wake.notified() => {},
                ready = self.notification.readable() => {
                    let mut ready = ready?;
                    let result = ready.try_io(|fd| {
                        let count = fd.get_ref().0.try_wait(|_| {}).map_err(other)?;
                        if count == 0 { Err(io::ErrorKind::WouldBlock.into()) } else { Ok(()) }
                    });
                    if let Ok(result) = result { result?; }
                },
            }
        }
    }

    pub async fn recv(&mut self) -> io::Result<Bytes> {
        let result = self.receive().await;
        if result.is_err() {
            self.sender.stop();
        }
        result
    }
}

impl Drop for IpcEndpoint {
    fn drop(&mut self) {
        self.sender.stop();
    }
}
