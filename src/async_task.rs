//! Async task handle — a cancellable [`Future`] for a single Lua operation.
//!
//! An [`AsyncTask`] is returned by [`AsyncIsle::spawn_eval`](crate::AsyncIsle::spawn_eval),
//! [`AsyncIsle::spawn_call`](crate::AsyncIsle::spawn_call), and
//! [`AsyncIsle::spawn_exec`](crate::AsyncIsle::spawn_exec).
//!
//! It implements [`Future`] so it can be `.await`ed directly.

use crate::error::IsleError;
use crate::hook::CancelToken;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Async handle to a pending Lua operation.
///
/// Implements [`Future`] — `.await` it to get the result.
///
/// # Type parameter `T`
///
/// `AsyncTask` is generic over its output type `T`, following the
/// established Rust async ecosystem convention
/// ([`tokio::task::JoinHandle<T>`][tokio-jh],
/// [`async_task::Task<T>`][async-task],
/// [`async_std::task::JoinHandle<T>`][async-std-jh]).
///
/// `T` is the type the request converts its result to on the Lua
/// thread: the `T` of [`AsyncIsle::eval`](crate::AsyncIsle::eval) and
/// the other request methods.  There is no default.
///
/// [tokio-jh]: https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html
/// [async-task]: https://docs.rs/async-task/latest/async_task/struct.Task.html
/// [async-std-jh]: https://docs.rs/async-std/latest/async_std/task/struct.JoinHandle.html
///
/// # Cancellation
///
/// Call [`cancel()`](AsyncTask::cancel) or clone the
/// [`cancel_token()`](AsyncTask::cancel_token) before awaiting:
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use mlua_isle::AsyncIsle;
/// use std::time::Duration;
///
/// let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await?;
/// let task = isle.spawn_eval::<()>("while true do end");
/// let token = task.cancel_token().clone();
/// tokio::spawn(async move {
///     tokio::time::sleep(Duration::from_millis(100)).await;
///     token.cancel();
/// });
/// let result = task.await; // Err(Cancelled)
/// assert!(result.is_err());
/// driver.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Dropping
///
/// Dropping an `AsyncTask` before it resolves **cancels** the operation,
/// like [`async_task::Task`][async-task] and tokio-util's
/// `AbortOnDropHandle`.  Call [`detach`](AsyncTask::detach) to let it run
/// to completion without keeping the handle.
#[must_use = "dropping an AsyncTask cancels the operation; use `.detach()` to let it run"]
pub struct AsyncTask<T> {
    rx: tokio::sync::oneshot::Receiver<Result<T, IsleError>>,
    cancel: CancelToken,
    /// Resolved or detached: dropping must not cancel.
    released: bool,
}

impl<T> AsyncTask<T> {
    pub(crate) fn new(
        rx: tokio::sync::oneshot::Receiver<Result<T, IsleError>>,
        cancel: CancelToken,
    ) -> Self {
        Self {
            rx,
            cancel,
            released: false,
        }
    }

    /// Let the operation run to completion without this handle.
    ///
    /// The result is discarded.  The operation can still be cancelled
    /// through a clone of its [`cancel_token`](Self::cancel_token).
    pub fn detach(mut self) {
        self.released = true;
    }

    /// Cancel the operation.
    ///
    /// Signals the Lua debug hook to interrupt execution.
    /// The task will resolve to [`IsleError::Cancelled`].
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Access the cancel token (e.g. to clone and share with another task).
    pub fn cancel_token(&self) -> &CancelToken {
        &self.cancel
    }
}

impl<T> Future for AsyncTask<T> {
    type Output = Result<T, IsleError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let polled = Pin::new(&mut self.rx).poll(cx);
        if polled.is_ready() {
            self.released = true;
        }
        match polled {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            // The oneshot sender was dropped without sending a result.
            // This happens when the Lua thread panics or shuts down while
            // a request is in flight.
            Poll::Ready(Err(_)) => Poll::Ready(Err(IsleError::RecvFailed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for AsyncTask<T> {
    fn drop(&mut self) {
        if !self.released {
            self.cancel.cancel();
        }
    }
}
