//! Task handle — a cancellable future for a single Lua operation.
//!
//! A [`Task`] is returned by [`Isle::spawn_eval`], [`Isle::spawn_call`],
//! and [`Isle::spawn_exec`].
//! It provides a [`CancelToken`] for interruption and a blocking
//! [`wait`](Task::wait) method to collect the result.

use crate::error::IsleError;
use crate::hook::CancelToken;
use std::sync::mpsc;

/// Handle to a pending Lua operation.
///
/// The operation runs on the Lua thread.  The caller can:
/// - [`wait`](Task::wait) for the result (blocking).
/// - [`cancel`](Task::cancel) the operation.
/// - [`try_recv`](Task::try_recv) to poll without blocking.
///
/// Dropping a `Task` before its result was received **cancels** the
/// operation.  Call [`detach`](Task::detach) to let it run to completion
/// without keeping the handle.
#[must_use = "dropping a Task cancels the operation; use `.detach()` to let it run"]
pub struct Task<T = String> {
    rx: mpsc::Receiver<Result<T, IsleError>>,
    cancel: CancelToken,
    /// Result received or detached: dropping must not cancel.
    released: std::cell::Cell<bool>,
}

impl<T> Task<T> {
    pub(crate) fn new(rx: mpsc::Receiver<Result<T, IsleError>>, cancel: CancelToken) -> Self {
        Self {
            rx,
            cancel,
            released: std::cell::Cell::new(false),
        }
    }

    /// Block until the result is available.
    pub fn wait(self) -> Result<T, IsleError> {
        let result = self.rx.recv();
        self.released.set(true);
        result.map_err(|_| IsleError::RecvFailed)?
    }

    /// Let the operation run to completion without this handle.
    ///
    /// The result is discarded.  The operation can still be cancelled
    /// through a clone of its [`cancel_token`](Self::cancel_token).
    pub fn detach(self) {
        self.released.set(true);
    }

    /// Cancel the operation.
    ///
    /// This signals the Lua debug hook to interrupt execution.
    /// The task will eventually return [`IsleError::Cancelled`].
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Non-blocking poll for the result.
    pub fn try_recv(&self) -> Option<Result<T, IsleError>> {
        let result = self.rx.try_recv().ok();
        if result.is_some() {
            self.released.set(true);
        }
        result
    }

    /// Access the cancel token (e.g. to share with other code).
    pub fn cancel_token(&self) -> &CancelToken {
        &self.cancel
    }
}

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        if !self.released.get() {
            self.cancel.cancel();
        }
    }
}
