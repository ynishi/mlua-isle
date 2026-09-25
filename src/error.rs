//! Error types for mlua-isle.
//!
//! One error type, [`IsleError`], is returned by every public function,
//! on the actors and on the in-thread layer ([`runtime`](crate::runtime)).
//! A Lua error crosses to Rust as a [`LuaFailure`]: its kind, the message
//! as Lua prints it, the traceback, and (with the `serde` feature) the
//! raised value.  It is built on the VM thread, so it is `Send`.
//!
//! Cancellation is a value, not a message: the cancel hook and
//! [`cancellable`](crate::cancellable) raise
//! `mlua::Error::external(Cancelled)`, and Rust recognises it by
//! downcasting (see [`Cancelled`]).

use std::fmt;

/// Errors returned by Isle operations.
///
/// Match on the variant; the Lua error of a request or root is
/// [`IsleError::Lua`]:
///
/// ```rust
/// use mlua_isle::{Isle, IsleError, LuaErrorKind};
///
/// let isle = Isle::spawn(|_| Ok(())).unwrap();
/// match isle.eval::<()>("error('boom')") {
///     Err(IsleError::Lua(f)) => {
///         assert_eq!(f.kind, LuaErrorKind::Runtime);
///         assert!(f.message.ends_with("boom"));
///     }
///     other => panic!("unexpected: {other:?}"),
/// }
/// isle.shutdown().unwrap();
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IsleError {
    /// The Lua VM thread has already shut down.
    #[error("isle shut down")]
    Shutdown,

    /// The operation was cancelled via [`CancelToken`](crate::CancelToken).
    #[error("cancelled")]
    Cancelled,

    /// A request or root raised a Lua error.
    #[error("lua error: {0}")]
    Lua(LuaFailure),

    /// The init closure failed (or, for the kinds noted on
    /// [`LuaErrorKind::External`], the VM thread could not be set up).
    #[error("init error: {0}")]
    Init(LuaFailure),

    /// `call` / `coroutine_call` named a global that is not a function.
    /// Carries the name.
    #[error("function '{0}' not found")]
    NotFound(String),

    /// The Lua thread panicked.  Carries the panic message when the
    /// payload is a `&str` or a `String`.
    #[error("lua thread panicked{}", .0.as_deref().map(|m| format!(": {m}")).unwrap_or_default())]
    ThreadPanic(Option<String>),

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
    /// The response channel was dropped before a result was sent.  This
    /// typically means the Lua thread panicked or was shut down while a
    /// request was in flight.
    #[error("recv failed")]
    RecvFailed,

    /// All pool slots are in use and no Isle is available.
    #[cfg(feature = "pool")]
    #[error("pool exhausted (max_size={0})")]
    PoolExhausted(usize),

    /// Pool internal lock poisoned (another thread panicked while holding the lock).
    #[cfg(feature = "pool")]
    #[error("pool lock poisoned: {0}")]
    PoolPoisoned(String),
}

/// What kind of Lua error a [`LuaFailure`] is.
///
/// Taken from the [`mlua::Error`] variant when the error was raised by
/// mlua or by Rust code; a value raised from Lua with `error(v)` is
/// [`Runtime`](Self::Runtime).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LuaErrorKind {
    /// A Lua runtime error: `error(v)` in Lua, or an operation on the
    /// wrong type (`mlua::Error::RuntimeError`).
    Runtime,
    /// The chunk did not compile (`mlua::Error::SyntaxError`).
    Syntax,
    /// The allocator refused memory (`mlua::Error::MemoryError`).
    Memory,
    /// A Rust function called from Lua returned `Err`
    /// (`mlua::Error::CallbackError`).  This is the kind of a host
    /// function that returns `Err(mlua::Error::external(e))` and is
    /// called from Lua: mlua wraps the error in a `CallbackError` on
    /// the way through Lua.
    Callback,
    /// A Rust error that did not pass through a Lua call
    /// (`mlua::Error::ExternalError`), for example one returned by an
    /// `exec` closure.  Also the kind of an [`IsleError::Init`] that is
    /// not a Lua error: the OS refused to start the VM thread, the
    /// thread's tokio runtime could not be built, or a pool was
    /// configured with `max_size == 0`.
    External,
    /// A value did not convert between Lua and Rust
    /// (`FromLuaConversionError`, `BadArgument`, serde errors).
    Conversion,
    /// Any other `mlua::Error` variant.
    Other,
}

/// A Lua error, as it crosses from the VM thread to Rust.
///
/// Built on the VM thread from the raised value (or from the
/// [`mlua::Error`] when the error came from Rust or from mlua), so it is
/// `Send + Sync`.  The raw value itself stays on the Lua side: a host
/// that needs it (an error object with a metatable, say) catches it
/// with `pcall` in Lua.
///
/// `Display` prints [`message`](Self::message).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LuaFailure {
    /// The kind of error.
    pub kind: LuaErrorKind,
    /// The error as Lua prints it: `tostring(err)` on the VM thread
    /// (so a `__tostring` metamethod is honoured).  For an error that
    /// came from Rust, its `Display` (for a `CallbackError`, the
    /// innermost cause; for a `RuntimeError`, the message without the
    /// traceback).
    pub message: String,
    /// The Lua stack traceback at the point of the error, when one was
    /// taken (errors raised in Lua and errors from Rust functions called
    /// from Lua have one; an error before any Lua ran, such as a syntax
    /// error, does not).
    pub traceback: Option<String>,
    /// The raised value converted to JSON, when it converts (tables of
    /// strings / numbers / booleans, and those values themselves).
    /// `None` for values that do not (functions, userdata, cyclic
    /// tables) and for errors that came from Rust.
    #[cfg(feature = "serde")]
    pub value: Option<serde_json::Value>,
}

impl LuaFailure {
    /// A failure of `kind` with `message` and no traceback (or value).
    ///
    /// For an `exec` closure that reports its own error:
    /// `Err(IsleError::Lua(LuaFailure::new(LuaErrorKind::Runtime, "bad input")))`.
    pub fn new(kind: LuaErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            traceback: None,
            #[cfg(feature = "serde")]
            value: None,
        }
    }

    /// Build a failure from an [`mlua::Error`]: the kind from the
    /// variant, the message from its `Display` (see
    /// [`message`](Self::message)), the traceback from a
    /// `CallbackError` or from the traceback mlua appends to a
    /// `RuntimeError`.
    pub fn from_mlua(e: &mlua::Error) -> Self {
        match e {
            mlua::Error::CallbackError { traceback, cause } => {
                let (mut cause, mut traceback) = (cause, traceback);
                while let mlua::Error::CallbackError {
                    cause: inner,
                    traceback: inner_tb,
                } = &**cause
                {
                    cause = inner;
                    traceback = inner_tb;
                }
                // The innermost cause, built the same way (so a
                // `RuntimeError` cause has its traceback split off).
                let inner = Self::from_mlua(cause);
                let mut f = Self::new(LuaErrorKind::Callback, inner.message);
                f.traceback = Some(traceback.clone())
                    .filter(|t| !t.is_empty())
                    .or(inner.traceback);
                f
            }
            mlua::Error::WithContext { context, cause } => {
                let mut f = Self::from_mlua(cause);
                f.message = format!("{context}\n{}", f.message);
                f
            }
            mlua::Error::RuntimeError(msg) => {
                let (message, traceback) = split_traceback(msg);
                let mut f = Self::new(LuaErrorKind::Runtime, message);
                f.traceback = traceback;
                f
            }
            mlua::Error::SyntaxError { message, .. } => {
                Self::new(LuaErrorKind::Syntax, message.clone())
            }
            mlua::Error::MemoryError(msg) => Self::new(LuaErrorKind::Memory, msg.clone()),
            mlua::Error::ExternalError(_) => Self::new(LuaErrorKind::External, e.to_string()),
            mlua::Error::FromLuaConversionError { .. } | mlua::Error::BadArgument { .. } => {
                Self::new(LuaErrorKind::Conversion, e.to_string())
            }
            #[cfg(feature = "serde")]
            mlua::Error::SerializeError(_) | mlua::Error::DeserializeError(_) => {
                Self::new(LuaErrorKind::Conversion, e.to_string())
            }
            _ => Self::new(LuaErrorKind::Other, e.to_string()),
        }
    }
}

impl fmt::Display for LuaFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<mlua::Error> for LuaFailure {
    fn from(e: mlua::Error) -> Self {
        Self::from_mlua(&e)
    }
}

/// Split the traceback mlua appends to a `RuntimeError` message.
fn split_traceback(msg: &str) -> (String, Option<String>) {
    match msg.find("\nstack traceback:") {
        Some(pos) => (msg[..pos].to_string(), Some(msg[pos + 1..].to_string())),
        None => (msg.to_string(), None),
    }
}

/// The error a cancellation raises in Lua.
///
/// The cancel hook and [`cancellable`](crate::cancellable) raise
/// `mlua::Error::external(Cancelled)`.  Rust recognises it by value, with
/// [`mlua::Error::downcast_ref`], which walks the `CallbackError` /
/// `WithContext` / `BadArgument` chain down to the `ExternalError`:
///
/// ```rust
/// use mlua_isle::Cancelled;
///
/// fn is_cancel(e: &mlua::Error) -> bool {
///     e.downcast_ref::<Cancelled>().is_some()
/// }
/// assert!(is_cancel(&mlua::Error::external(Cancelled)));
/// assert!(!is_cancel(&mlua::Error::runtime("cancelled")));
/// ```
///
/// Lua code sees it as an error value (a userdata whose `tostring` is
/// `cancelled` plus a traceback); the `task` library's
/// `task.is_cancelled(err)` is true for it (see [`tasks`](crate::tasks)).
/// A host function that wants to report a cancel it observed itself can
/// return `Err(mlua::Error::external(Cancelled))`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Cancelled;

impl fmt::Display for Cancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("cancelled")
    }
}

impl std::error::Error for Cancelled {}

/// Whether `e` is (or wraps) the cancellation error.
pub(crate) fn is_cancel(e: &mlua::Error) -> bool {
    e.downcast_ref::<Cancelled>().is_some()
}

/// The cancellation error, as raised into Lua.
pub(crate) fn cancel_error() -> mlua::Error {
    mlua::Error::external(Cancelled)
}

impl From<mlua::Error> for IsleError {
    /// [`IsleError::Cancelled`] when `e` wraps [`Cancelled`] (found by
    /// downcast, never by message), else [`IsleError::Lua`].
    fn from(e: mlua::Error) -> Self {
        if is_cancel(&e) {
            Self::Cancelled
        } else {
            Self::Lua(LuaFailure::from_mlua(&e))
        }
    }
}

/// The message of a panic payload, when it is a `&str` or a `String`.
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> Option<String> {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync + 'static>() {}

    #[test]
    fn error_types_are_send_sync() {
        assert_send_sync::<IsleError>();
        assert_send_sync::<LuaFailure>();
        assert_send_sync::<Cancelled>();
    }

    #[test]
    // `CallbackError::cause` is an `Arc<mlua::Error>`, which is not
    // `Send` without mlua's `error-send` feature.
    #[allow(clippy::arc_with_non_send_sync)]
    fn cancel_is_found_through_callback_error() {
        let wrapped = mlua::Error::CallbackError {
            traceback: String::new(),
            cause: std::sync::Arc::new(cancel_error()),
        };
        assert!(matches!(IsleError::from(wrapped), IsleError::Cancelled));
        assert!(matches!(
            IsleError::from(mlua::Error::runtime("__isle_cancelled__")),
            IsleError::Lua(_)
        ));
    }

    #[test]
    #[allow(clippy::arc_with_non_send_sync)]
    fn nested_messages_carry_no_traceback() {
        let inner = mlua::Error::runtime("x:1: boom\nstack traceback:\n\t[C]: in ?");
        let cb = mlua::Error::CallbackError {
            traceback: "stack traceback:\n\t[C]: in f".into(),
            cause: std::sync::Arc::new(inner.clone()),
        };
        let f = LuaFailure::from_mlua(&cb);
        assert_eq!(f.kind, LuaErrorKind::Callback);
        assert_eq!(f.message, "x:1: boom");
        assert_eq!(
            f.traceback.as_deref(),
            Some("stack traceback:\n\t[C]: in f")
        );

        let ctx = mlua::Error::WithContext {
            context: "while loading".into(),
            cause: std::sync::Arc::new(inner),
        };
        let f = LuaFailure::from_mlua(&ctx);
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        assert_eq!(f.message, "while loading\nx:1: boom");
        assert!(f.traceback.is_some());
    }

    #[test]
    fn runtime_error_traceback_is_split() {
        let f = LuaFailure::from_mlua(&mlua::Error::runtime(
            "x:1: boom\nstack traceback:\n\t[C]: in ?",
        ));
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        assert_eq!(f.message, "x:1: boom");
        assert_eq!(
            f.traceback.as_deref(),
            Some("stack traceback:\n\t[C]: in ?")
        );
    }
}
