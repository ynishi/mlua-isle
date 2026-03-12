//! Thread-isolated Lua VM with cancellation for mlua.
//!
//! `mlua-isle` runs a Lua VM on a dedicated thread and communicates via
//! channels.  This solves two fundamental problems with mlua:
//!
//! 1. **`Lua` is `!Send`** — it cannot cross thread boundaries.  By
//!    confining the VM to one thread and sending requests over a channel,
//!    callers on any thread (UI, async runtime, etc.) can interact with
//!    Lua without `Send` issues.
//!
//! 2. **Cancellation** — long-running Lua code (including blocking Rust
//!    callbacks like HTTP calls) can be interrupted via a cancel token
//!    that triggers both a Lua debug hook and a caller-side signal.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐   mpsc    ┌──────────────────┐
//! │  caller thread   │─────────►│  Lua thread       │
//! │  (UI / async)    │          │  (mlua confined)   │
//! │                  │◄─────────│                    │
//! │  Isle handle     │  oneshot  │  Lua VM + hook    │
//! └─────────────────┘           └──────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust
//! use mlua_isle::Isle;
//!
//! let isle = Isle::spawn(|lua| {
//!     lua.globals().set("greeting", "hello")?;
//!     Ok(())
//! }).unwrap();
//!
//! let result: String = isle.eval("return greeting").unwrap();
//! assert_eq!(result, "hello");
//!
//! isle.shutdown().unwrap();
//! ```

mod error;
mod handle;
mod hook;
#[cfg(feature = "pool")]
mod pool;
mod task;
mod thread;

#[cfg(feature = "tokio")]
mod async_isle;
#[cfg(feature = "tokio")]
mod async_task;

pub use error::IsleError;
pub use handle::Isle;
pub use hook::CancelToken;
pub use task::Task;

#[cfg(feature = "pool")]
pub use pool::{IslePool, PoolConfig, PoolStrategy, PooledIsle};

#[cfg(feature = "tokio")]
pub use async_isle::{AsyncIsle, AsyncIsleBuilder, AsyncIsleDriver};
#[cfg(feature = "tokio")]
pub use async_task::AsyncTask;

/// Type alias for exec closures to keep the `Request` enum readable.
pub(crate) type ExecFn = Box<dyn FnOnce(&mlua::Lua) -> Result<String, IsleError> + Send>;

/// Channel sender for results.
pub(crate) type ResultTx = std::sync::mpsc::Sender<Result<String, IsleError>>;

/// Request sent from caller to the Lua thread.
pub(crate) enum Request {
    /// Evaluate a Lua chunk and return the result as a string.
    Eval {
        code: String,
        cancel: CancelToken,
        tx: ResultTx,
    },
    /// Call a named global function with string arguments.
    Call {
        func: String,
        args: Vec<String>,
        cancel: CancelToken,
        tx: ResultTx,
    },
    /// Execute an arbitrary closure on the Lua thread.
    Exec {
        f: ExecFn,
        cancel: CancelToken,
        tx: ResultTx,
    },
    /// Graceful shutdown.
    Shutdown,
}
