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
/// The default `T = String` matches the built-in `eval`/`call`/`exec`
/// methods which return `String`.  The generic parameter allows
/// downstream code to construct `AsyncTask<T>` with custom result
/// types when wrapping or extending the API.
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
/// let task = isle.spawn_eval("while true do end");
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
pub struct AsyncTask<T = String> {
    rx: tokio::sync::oneshot::Receiver<Result<T, IsleError>>,
    cancel: CancelToken,
}

impl<T> AsyncTask<T> {
    pub(crate) fn new(
        rx: tokio::sync::oneshot::Receiver<Result<T, IsleError>>,
        cancel: CancelToken,
    ) -> Self {
        Self { rx, cancel }
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
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            // The oneshot sender was dropped without sending a result.
            // This happens when the Lua thread panics or shuts down while
            // a request is in flight.  The string "oneshot closed" is used
            // to distinguish this from std::sync::mpsc recv errors in the
            // synchronous `Task`.
            Poll::Ready(Err(_)) => Poll::Ready(Err(IsleError::RecvFailed("oneshot closed".into()))),
            Poll::Pending => Poll::Pending,
        }
    }
}
