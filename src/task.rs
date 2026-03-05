//! Task handle — a cancellable future for a single Lua operation.
//!
//! A [`Task`] is returned by [`Isle::spawn_eval`] and [`Isle::spawn_call`].
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
pub struct Task<T = String> {
    rx: mpsc::Receiver<Result<T, IsleError>>,
    cancel: CancelToken,
}

impl<T> Task<T> {
    pub(crate) fn new(rx: mpsc::Receiver<Result<T, IsleError>>, cancel: CancelToken) -> Self {
        Self { rx, cancel }
    }

    /// Block until the result is available.
    pub fn wait(self) -> Result<T, IsleError> {
        self.rx
            .recv()
            .map_err(|e| IsleError::RecvFailed(e.to_string()))?
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
        self.rx.try_recv().ok()
    }

    /// Access the cancel token (e.g. to share with other code).
    pub fn cancel_token(&self) -> &CancelToken {
        &self.cancel
    }
}
