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
//! to its VM, and a coroutine request is a `Vm::run`.  The [`runtime`]
//! module docs are the canonical description of the in-thread layer:
//! its contracts, the hook, and the Lua `task` library.
//!
//! # Moved to `runtime` in 0.8.0
//!
//! The in-thread API of 0.7 is deprecated; each old name forwards to
//! its `runtime` replacement and will be removed in the release after
//! 0.8.0.
//!
//! | 0.7 | 0.8 |
//! |---|---|
//! | `run_root(lua, token, f, args)` | [`Vm::attach`](runtime::Vm::attach) once, then `vm.run(&token, f, args)` |
//! | `cancellable(fut)` (root) | `runtime::cancellable(fut)` |
//! | `current_token()` (root) | [`runtime::current_token()`] |
//! | `hooks::install(lua)` | [`Vm::attach(lua, config)`](runtime::Vm::attach) |
//! | `hooks::configure` / `hooks::config` | [`Vm::set_config`](runtime::Vm::set_config) / [`Vm::config`](runtime::Vm::config) |
//! | `hooks::add_hook` / `hooks::remove_hook` | [`Vm::add_hook`](runtime::Vm::add_hook) / [`Vm::remove_hook`](runtime::Vm::remove_hook) |
//! | `hooks::CancelConfig` | [`runtime::Config`] (the old name is an alias) |
//! | `hooks::HookId` | [`runtime::HookId`] |
//! | `tasks::install(lua)` (`tokio`) | `Vm::attach(lua, config)?.task_lib()` (`tokio`) |
//! | the `hooks` / `tasks` modules | [`runtime`] |
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
#[deprecated(
    since = "0.8.0",
    note = "use `mlua_isle::runtime` (`Vm`, `Config`, `HookId`)"
)]
pub mod hooks;
mod hub;
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
mod task_lib;
#[cfg(feature = "tokio")]
#[deprecated(since = "0.8.0", note = "use `mlua_isle::runtime::Vm::task_lib`")]
pub mod tasks;

pub use error::{Cancelled, IsleError, LuaErrorKind, LuaFailure};
pub use handle::Isle;
pub use hook::CancelToken;
pub use task::Task;

#[cfg(feature = "pool")]
pub use pool::{IslePool, PoolConfig, PoolStrategy, PooledIsle};

#[cfg(feature = "tokio")]
pub use async_isle::{AsyncIsle, AsyncIsleBuilder, AsyncIsleDriver};
#[cfg(all(feature = "pool", feature = "tokio"))]
pub use async_pool::{AsyncIslePool, AsyncPooledIsle};
#[cfg(feature = "tokio")]
pub use async_task::AsyncTask;

/// Token of the request or task currently executing on this thread.
/// Forwards to [`runtime::current_token`].
#[deprecated(since = "0.8.0", note = "use `mlua_isle::runtime::current_token`")]
pub fn current_token() -> Option<CancelToken> {
    runtime::current_token()
}

/// Make an async host function's future stop when the calling request
/// or task is cancelled.  Forwards to `runtime::cancellable`.
#[cfg(feature = "tokio")]
#[deprecated(since = "0.8.0", note = "use `mlua_isle::runtime::cancellable`")]
pub async fn cancellable<F, T>(fut: F) -> mlua::Result<T>
where
    F: std::future::Future<Output = mlua::Result<T>>,
{
    runtime::cancellable(fut).await
}

/// Run `func(args)` as a root coroutine under `token`.  Forwards to
/// [`Vm::run`](runtime::Vm::run) on the VM's [`Vm`](runtime::Vm),
/// attaching it with its stored config first if it is not attached.
///
/// Migration: `Vm::attach(&lua, config)?` once, then
/// `vm.run(&token, func, args).await`.
///
/// # Errors
///
/// As [`Vm::run`](runtime::Vm::run), plus the error of
/// [`Vm::attach`](runtime::Vm::attach) when the VM was not attached.
#[cfg(feature = "tokio")]
#[deprecated(
    since = "0.8.0",
    note = "use `mlua_isle::runtime::Vm::attach` once, then `vm.run(&token, f, args)`"
)]
pub async fn run_root(
    lua: &mlua::Lua,
    token: CancelToken,
    func: mlua::Function,
    args: mlua::MultiValue,
) -> Result<mlua::MultiValue, IsleError> {
    runtime::of_or_attach(lua)?.run(&token, func, args).await
}

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
