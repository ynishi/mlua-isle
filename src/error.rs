//! Error types for mlua-isle.

/// Errors returned by Isle operations.
#[derive(Debug, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum IsleError {
    /// The Lua VM thread has already shut down.
    #[error("isle shut down")]
    Shutdown,

    /// The operation was cancelled via [`CancelToken`](crate::CancelToken).
    #[error("cancelled")]
    Cancelled,

    /// A Lua runtime error.
    #[error("lua error: {0}")]
    Lua(String),

    /// The Lua thread panicked.
    #[error("lua thread panicked")]
    ThreadPanic,

    /// The request channel is full (backpressure).
    ///
    /// Only returned by [`AsyncIsle`](crate::AsyncIsle) `spawn_*` methods
    /// when the bounded channel has no capacity.  Unlike [`Shutdown`](Self::Shutdown),
    /// this is a transient condition — the Lua thread is still alive and
    /// retrying may succeed.
    #[cfg(feature = "tokio")]
    #[error("channel full (backpressure)")]
    ChannelFull,

    /// Failed to receive response from the Lua thread.
    ///
    /// The oneshot / mpsc response channel was dropped before a result
    /// was sent.  This typically means the Lua thread panicked or was
    /// shut down while a request was in flight.
    #[error("recv failed: {0}")]
    RecvFailed(String),

    /// Error during Isle initialization.
    #[error("init error: {0}")]
    Init(String),

    /// All pool slots are in use and no Isle is available.
    #[cfg(feature = "pool")]
    #[error("pool exhausted (max_size={0})")]
    PoolExhausted(usize),

    /// Pool internal lock poisoned (another thread panicked while holding the lock).
    #[cfg(feature = "pool")]
    #[error("pool lock poisoned: {0}")]
    PoolPoisoned(String),
}

impl From<mlua::Error> for IsleError {
    fn from(e: mlua::Error) -> Self {
        let msg = e.to_string();
        if msg.contains("__isle_cancelled__") {
            Self::Cancelled
        } else {
            Self::Lua(msg)
        }
    }
}
