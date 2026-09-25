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
mod protect;
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

pub use error::{Cancelled, IsleError, LuaErrorKind, LuaFailure};
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

/// The work of one request, run on the VM thread.
///
/// A job owns everything its request needs: the code or the arguments,
/// and the typed sender its result goes back through.  It converts the
/// result on the VM thread (`FromLuaMulti`, or the `exec` closure's own
/// `T`) and sends a `Send` value, so the request type needs no type
/// parameter.  A coroutine job `spawn_local`s its future and returns.
pub(crate) type Job = Box<dyn FnOnce(&mlua::Lua, &CancelToken) + Send>;

/// Request sent from a handle ([`Isle`], `AsyncIsle`) to the VM thread.
pub(crate) enum Request {
    /// Run a job under the request's cancel token.
    Run { job: Job, cancel: CancelToken },
    /// Graceful shutdown.
    Shutdown,
}
