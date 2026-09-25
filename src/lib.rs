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
//! # Layers
//!
//! The crate has two layers:
//!
//! - **The actor layer** (crate root): [`Isle`], `AsyncIsle` and the
//!   pools put a VM on a thread of their own; `Send` handles send
//!   requests over channels.
//! - **The in-thread layer** ([`runtime`]): for a host that owns the
//!   [`mlua::Lua`] and drives the executor itself.
//!   [`runtime::Vm`] owns the VM's debug hook, its
//!   [`runtime::Config`] (cancel grace, preemption) and the `task`
//!   library, and runs a root coroutine under a [`CancelToken`]
//!   (`Vm::run`, `tokio` feature).
//!
//! The actors are built on [`runtime`]: each attaches a [`runtime::Vm`]
//! to its VM, and a coroutine request is a `Vm::run`.  The contracts of
//! the layer are stated in the [`runtime`] module docs.
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
pub mod hooks;
#[cfg(feature = "pool")]
mod pool;
pub mod runtime;
mod task;
mod thread;

#[cfg(feature = "tokio")]
mod async_isle;
#[cfg(all(feature = "pool", feature = "tokio"))]
mod async_pool;
#[cfg(feature = "tokio")]
mod async_task;
#[cfg(feature = "tokio")]
mod scope;
#[cfg(feature = "tokio")]
pub mod tasks;

pub use error::IsleError;
pub use handle::Isle;
pub use hook::{current_token, CancelToken};
#[cfg(feature = "tokio")]
pub use scope::{cancellable, run_root};
pub use task::Task;

#[cfg(feature = "pool")]
pub use pool::{IslePool, PoolConfig, PoolStrategy, PooledIsle};

#[cfg(feature = "tokio")]
pub use async_isle::{AsyncIsle, AsyncIsleBuilder, AsyncIsleDriver};
#[cfg(all(feature = "pool", feature = "tokio"))]
pub use async_pool::{AsyncIslePool, AsyncPooledIsle};
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
