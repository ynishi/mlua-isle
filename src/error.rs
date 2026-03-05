//! Error types for mlua-isle.

/// Errors returned by Isle operations.
#[derive(Debug, thiserror::Error)]
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

    /// Failed to send request to the Lua thread.
    #[error("send failed (isle shut down)")]
    SendFailed,

    /// Failed to receive response from the Lua thread.
    #[error("recv failed: {0}")]
    RecvFailed(String),

    /// Error during Isle initialization.
    #[error("init error: {0}")]
    Init(String),
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
