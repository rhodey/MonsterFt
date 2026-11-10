use crate::error::{Code, Error};
use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
    rc::Rc,
};

/// Local callbacks run on the owner thread. Error listeners cannot hide each other.
#[derive(Default)]
pub struct Diagnostics {
    warnings: Vec<WarningListener>,
    errors: Vec<WarningListener>,
    closes: Vec<Box<dyn FnMut()>>,
}
impl Diagnostics {
    pub fn on_warn(&mut self, listener: impl FnMut(&Error) + 'static) {
        self.warnings.push(Box::new(listener));
    }
    pub fn on_error(&mut self, listener: impl FnMut(&Error) + 'static) {
        self.errors.push(Box::new(listener));
    }
    pub fn on_close(&mut self, listener: impl FnMut() + 'static) {
        self.closes.push(Box::new(listener));
    }
    pub(crate) fn take_warnings(&mut self) -> Vec<WarningListener> {
        std::mem::take(&mut self.warnings)
    }
    pub(crate) fn restore_warnings(&mut self, mut warnings: Vec<WarningListener>) {
        warnings.append(&mut self.warnings);
        self.warnings = warnings;
    }
    pub(crate) fn error(&mut self, error: &Error) {
        for listener in &mut self.errors {
            let _ = catch_unwind(AssertUnwindSafe(|| listener(error)));
        }
    }
    pub(crate) fn close(&mut self) {
        let mut errors = Vec::new();
        for listener in &mut self.closes {
            if let Err(panic) = catch_unwind(AssertUnwindSafe(listener)) {
                errors.push(panic_error(panic));
            }
        }
        for error in errors {
            self.error(&error);
        }
    }
}

pub(crate) type WarningListener = Box<dyn FnMut(&Error)>;

/// Let the owner stop and report each failed listener before invoking the next.
pub(crate) fn warn(
    listeners: &mut [WarningListener],
    error: &Error,
    mut failed: impl FnMut(Error),
) -> Option<Error> {
    let mut failure = None;
    for listener in listeners {
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| listener(error))) {
            let error = panic_error(panic);
            failure.get_or_insert_with(|| error.clone());
            failed(error);
        }
    }
    failure
}

/// Owner-thread cancellation observed at execution boundaries. This handle is !Send.
#[derive(Default)]
struct StopState {
    stopped: Cell<bool>,
    changed: tokio::sync::Notify,
}
#[derive(Clone, Default)]
pub struct StopHandle(Rc<StopState>);
impl StopHandle {
    pub fn stop(&self) {
        if !self.0.stopped.replace(true) {
            self.0.changed.notify_waiters();
        }
    }
    pub fn is_stopped(&self) -> bool {
        self.0.stopped.get()
    }
    pub async fn stopped(&self) {
        let notified = self.0.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_stopped() {
            notified.await;
        }
    }
}

pub(crate) fn panic_error(panic: Box<dyn std::any::Any + Send>) -> Error {
    let message = if let Some(s) = panic.downcast_ref::<String>() {
        s.as_str()
    } else if let Some(s) = panic.downcast_ref::<&str>() {
        s
    } else {
        "non-string panic"
    };
    Error::new(
        Code::MonsterIllegal,
        format!("component panicked: {message}"),
    )
}
