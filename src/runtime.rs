//! Running Lua on a VM you own: the in-thread layer of the crate.
//!
//! The crate has two layers.  The actors ([`Isle`](crate::Isle),
//! `AsyncIsle`, the pools) put a VM on a thread of their own and take
//! requests over channels.  This module is the layer underneath: a host
//! that owns the [`Lua`] and drives the executor itself (its own thread
//! and [`LocalSet`](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html))
//! uses it directly, and the actors are built on it.
//!
//! [`Vm`] is the entry point: [`Vm::attach`] takes over the VM's debug
//! hook, stores its [`Config`] and creates the `task` library.  The
//! hook part (`attach`, `of`, `config`, `set_config`, `add_hook`,
//! `remove_hook`) is always available; `Vm::run`, `Vm::task_lib`,
//! `cancellable` and `current_scope` need the `tokio` feature.  With
//! it, setup is three calls (see `Vm::run` for the full example):
//!
//! ```text
//! let vm = Vm::attach(&lua, Config { grace: Duration::from_secs(1), ..Default::default() })?;
//! lua.globals().set("task", vm.task_lib()?)?;
//! let out = local.run_until(vm.run(&token, main, ())).await?;
//! ```
//!
//! # Contracts
//!
//! 1. **When `run` resolves, nothing the root started is alive.**  This
//!    holds for Lua tasks (`task.spawn`) and for host tasks spawned
//!    through the scope (`current_scope()` then
//!    `ScopeHandle::spawn_local`), transitively and in any mix,
//!    whether the root finished or was cancelled.  A host task that
//!    ignores its token is dropped when the grace ends.  Tasks spawned
//!    into a scope that is already ending share its remaining time; a
//!    spawn after the remaining time is gone starts nothing.  The layer
//!    does not drain the host's `LocalSet`: a `spawn_local` that bypasses the
//!    scope (for example `current_token().child_token()` plus a bare
//!    `tokio::task::spawn_local`) is cancelled with the request but is
//!    neither waited for nor dropped, and is the host's
//!    responsibility.  This holds when `run` is awaited to the end:
//!    dropping the `run` future instead only schedules the tasks for
//!    abort, and a task in a CPU loop blocks `run` until it yields,
//!    which without [`Config::preempt_every`] it never does.
//! 2. **One error type**, [`IsleError`], on this layer and on the
//!    actors, with one payload for a Lua error: [`IsleError::Lua`]
//!    carries a [`LuaFailure`] (kind, message as Lua prints it,
//!    traceback, and with the `serde` feature the raised value as
//!    JSON), built on the VM thread from the raised value, the same
//!    for `run` and for every actor request.  A cancel is
//!    [`IsleError::Cancelled`], recognised by value: the cancel error
//!    is `mlua::Error::external(`[`Cancelled`]`)`, found by downcast,
//!    never by message.
//! 3. **The layer owns the VM's debug hook.**  Register callbacks with
//!    [`Vm::add_hook`], never with `Lua::set_hook` /
//!    `Lua::set_global_hook`, which replace the hook and stop
//!    cancellation (see [Hooks](#hooks)).
//! 4. **One [`Config`] per VM**, read and written through [`Vm`]
//!    ([`Vm::config`], [`Vm::set_config`]; a second [`Vm::attach`]
//!    replaces it).
//! 5. **Cancellation is a token the host creates** and passes to
//!    `run`; `run` spawns nothing and returns no handle.  Ctrl-C, a
//!    timeout or a hook callback cancel that [`CancelToken`].
//!
//! Two requirements on the VM itself: attach it before sandboxing its
//! globals (the first [`Vm::attach`] captures `xpcall`), and keep mlua's
//! default `LuaOptions::catch_rust_panics = true`, whose `xpcall` is
//! yieldable (with `false` every yield in a `run` root fails; see
//! [`Vm::attach`]).
//!
//! Host functions called from Lua reach the running request or task
//! through the context functions, which read a thread-local and so take
//! no receiver: [`current_token`], `cancellable` and `current_scope`.
//!
//! # Hooks
//!
//! Lua has one hook slot per thread, and mlua's hook setters replace
//! whatever was there, so the layer owns the VM's hook: [`Vm::attach`]
//! installs a single **global** hook (one callback shared by every
//! thread of the VM, including coroutines the Lua code creates) that,
//! in order,
//!
//! 1. raises the cancellation error ([`Cancelled`]) when the token of
//!    the request or task currently executing is cancelled (checked
//!    every 1000 instructions),
//! 2. calls the callbacks registered with [`Vm::add_hook`], each at its
//!    own [`HookTriggers`],
//! 3. yields the running root coroutine or task every N checks when
//!    preemption is enabled ([`Config::preempt_every`]).
//!
//! Calling [`Lua::set_hook`], [`Lua::set_global_hook`] or
//! [`mlua::Thread::set_hook`] on the VM replaces this hook, and
//! cancellation stops working.  The hook is re-installed when a
//! `set_hook` replacement is noticed (at the start of every actor
//! request and of every `Vm::run`), but a replacement through
//! `set_global_hook` cannot be detected.
//!
//! # The `task` library
//!
//! `Vm::task_lib` (`tokio` feature) returns the VM's `task` table,
//! conventionally set as the global `task`:
//!
//! | Lua | meaning |
//! |---|---|
//! | `task.spawn(f, ...)` | Start `f(...)` as a concurrent task of the current coroutine request or task.  Returns a handle. |
//! | `h:join()` | Wait for the task.  Returns `true, ...` (what `f` returned) or `false, err`, where `err` is the raw Lua error value, or `task.CANCELLED` if the task was cancelled.  A handle can be joined once. |
//! | `h:cancel()` | Request cancellation.  Does not wait. |
//! | `h:done()` | Whether the task has finished. |
//! | `local h <close> = task.spawn(...)` | On scope exit, a task that was not joined is cancelled and waited for. |
//! | `task.is_cancelled(err)` | Whether `err` is a cancellation: the error a cancel raises (caught with `pcall`, or received by a `__close` handler) or `task.CANCELLED`.  `false` for any other value. |
//! | `task.CANCELLED` | What `join` returns as `err` for a cancelled task. |
//!
//! Tasks are **structured**: when a coroutine request or task finishes,
//! the tasks it spawned and did not join are cancelled, and it waits
//! for them before its own result is delivered.  Cancelling a request
//! or task cancels all of its tasks (their tokens are children of its
//! token, see [`CancelToken::child_token`]), and the cancelled request
//! or task still resolves only after they, and their own tasks, have
//! finished or been dropped.  The [grace](Config::grace) is one deadline
//! for the whole tree: a task spawned during cleanup gets the time that
//! remains, not a fresh grace period.
//!
//! `task.spawn` works inside `Vm::run`, inside coroutine requests
//! (`AsyncIsle::coroutine_eval` / `coroutine_call`) and inside tasks
//! (including host tasks).  Sync requests (`eval` / `call` / `exec`)
//! cannot await, so `task.spawn` raises an error there.
//!
//! A cancel reaches Lua code as an error: the cancel hook raises it
//! while Lua code runs, and an async host function wrapped with
//! `cancellable` returns it while the coroutine awaits.  Its value is
//! `mlua::Error::external(`[`Cancelled`]`)` (a userdata to Lua);
//! `task.is_cancelled(err)` is the test, and it is also true for
//! `task.CANCELLED`, so one predicate covers both:
//!
//! ```lua
//! local ok, err = pcall(sleep, 1000)
//! if not ok and task.is_cancelled(err) then
//!   -- cancelled: clean up and let the cancel continue
//!   error(err, 0)
//! end
//! ```
//!
//! The predicate lives in the library: a VM that runs without the `task`
//! table has no `task.is_cancelled`; Rust code tests an `mlua::Error`
//! with `e.downcast_ref::<Cancelled>()`.
//!
//! A task that runs a CPU loop never yields on its own, so a sibling on
//! the same thread cannot run to cancel it; enable
//! [`Config::preempt_every`] for that.  Cancelling from another thread
//! (an `AsyncTask` handle) works without it.
//!
//! # Host tasks
//!
//! A host function that starts work of its own takes the scope of the
//! running request or task with `current_scope()` and spawns into it
//! with `ScopeHandle::spawn_local`.  Such a task is structured like a
//! Lua task: it is cancelled when the request or task ends or is
//! cancelled, gets the grace (one deadline for the whole tree), is
//! dropped when the grace ends, and is waited for.  Take the handle in
//! the synchronous part of the host function (the `create_function`
//! body, or a `create_async_function` body before its first `.await`)
//! and move it into the future; `current_scope()` is `Some` only while
//! a coroutine request or task is being polled, so it is `None` in a
//! sync request.  `spawn_local` returns a `ScopedTask`; there are three
//! ways to let go of it: await it (wait for the value), keep it for as
//! long as the task should run (dropping it cancels the task now), or
//! `detach()` it (fire and forget: the task runs on without a handle and
//! is still cancelled, given the grace, dropped and waited for when the
//! scope ends).
//!
//! ```text
//! let bg = lua.create_function(|_, ()| {
//!     let scope = current_scope().expect("inside a coroutine request");
//!     scope.spawn_local(async move { /* host work */ }).detach();
//!     Ok(())
//! })?;
//! ```

use crate::hub;
use mlua::debug::Debug;
use mlua::{HookTriggers, Lua, VmState};
use std::cell::RefCell;
use std::fmt;
use std::time::Duration;

pub use crate::error::{Cancelled, IsleError, LuaErrorKind, LuaFailure};
pub use crate::hook::{current_token, CancelToken};
#[cfg(feature = "tokio")]
pub use crate::scope::{cancellable, current_scope, ScopeHandle, ScopedTask};

/// Settings of a VM.  One per VM, read and written through [`Vm`]
/// ([`Vm::attach`], [`Vm::config`], [`Vm::set_config`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Config {
    /// How long a cancelled coroutine request or task may keep running
    /// to finish its cleanup before it is dropped.
    ///
    /// On cancel, the coroutine first receives the cancellation as a Lua
    /// error (from the cancel hook while it runs, or from an async
    /// function wrapped with `cancellable` while it awaits).  That error
    /// unwinds normally, so `__close` handlers run and may await.  If
    /// the coroutine has not finished when the grace period ends, it is
    /// dropped: the awaited Rust future is released, and pending
    /// `__close` handlers run without being able to yield (a Lua 5.4
    /// restriction).
    ///
    /// The grace period is one deadline for the cancelled request or
    /// task and every task it spawned, transitively: a task started
    /// during cleanup (from a `__close` handler, say) gets the time that
    /// remains, not a fresh grace period.
    ///
    /// Default: zero (drop at once).
    pub grace: Duration,
    /// Yield the running coroutine request or task every this many
    /// cancel checks (a check runs every 1000 instructions), so that
    /// other tasks on the same thread, including one that cancels it,
    /// get to run while it is in a CPU loop.
    ///
    /// Only the coroutine that the crate created for the request or task
    /// is yielded, never a coroutine the Lua code created itself (that
    /// yield would reach the Lua code's own `coroutine.resume` as a
    /// spurious yield).  Code in a non-yieldable context (a metamethod,
    /// a C function boundary) is not yielded.
    ///
    /// Yielding lets other tasks interleave at points the Lua code did
    /// not mark, so Lua code that updates shared state across such a
    /// point can observe changes made by another task.
    ///
    /// Default: `None` (never preempt).
    pub preempt_every: Option<u32>,
}

/// Handle of a callback registered with [`Vm::add_hook`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HookId(pub(crate) u64);

/// Marks a VM as attached; holds the per-VM state that is not in the
/// hook hub.
#[derive(Default)]
struct Attached {
    /// The `task` table, created on the first [`Vm::task_lib`] call, so
    /// that a VM that never asks for it (an actor whose init closure
    /// does not) runs no extra Lua.  Kept in the registry, not as a
    /// global.
    #[cfg(feature = "tokio")]
    task: std::cell::OnceCell<mlua::RegistryKey>,
}

/// The in-thread handle of a Lua VM run by this crate.
///
/// A `Vm` is a clone of the [`Lua`] handle; its state lives in the VM,
/// so every `Vm` of the same VM (from [`Vm::attach`] or [`Vm::of`]) sees
/// the same state.  It holds the VM strongly: do not store it in
/// something the VM owns (a Lua function's captured state, app data),
/// or the VM is never freed.
#[derive(Clone)]
pub struct Vm {
    lua: Lua,
}

impl fmt::Debug for Vm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Vm")
            .field("config", &self.config())
            .finish_non_exhaustive()
    }
}

impl Vm {
    /// Install the hook and store `config`.
    ///
    /// Calling it again on the same VM re-installs the hook, replaces
    /// the config (last wins) and returns a `Vm` for the same state; the
    /// task table, if `Vm::task_lib` created it, is kept.  Runs no Lua
    /// code.
    ///
    /// Hook callbacks registered before (with [`Vm::add_hook`]) are
    /// kept.
    ///
    /// The first `attach` also captures the `xpcall` global, which
    /// [`Vm::run`] uses to bring a raised Lua value back as a value (see
    /// [`LuaFailure`]).  **Attach before sandboxing the globals**
    /// (removing `xpcall`, or [`Lua::set_globals`] with a whitelist):
    /// after that the capture is kept, and later changes to the globals
    /// or a re-attach do not affect it.
    ///
    /// **The coroutine path needs mlua's default
    /// `LuaOptions::catch_rust_panics = true`.**  With `false`, mlua
    /// replaces the global `xpcall` with a version that is not
    /// yieldable, so every yield in a root run by [`Vm::run`] (an async
    /// host function, `task.join`, preemption) fails with "attempt to
    /// yield across a C-call boundary".  [`Lua::new`] uses the default.
    ///
    /// # Errors
    ///
    /// [`IsleError::Init`] with [`LuaErrorKind::External`] when the VM's
    /// `xpcall` global is not a function (it was sandboxed away before
    /// the first attach).
    pub fn attach(lua: &Lua, config: Config) -> Result<Vm, IsleError> {
        crate::protect::install(lua)?;
        hub::install(lua)?;
        hub::set_config(lua, config);
        let attached = lua.app_data_ref::<Attached>().is_some();
        if !attached {
            lua.set_app_data(Attached::default());
        }
        Ok(Vm { lua: lua.clone() })
    }

    /// The `Vm` of `lua`, if [`Vm::attach`] was called on it.
    pub fn of(lua: &Lua) -> Option<Vm> {
        let attached = lua.app_data_ref::<Attached>().is_some();
        attached.then(|| Vm { lua: lua.clone() })
    }

    /// The VM's settings.
    pub fn config(&self) -> Config {
        hub::config(&self.lua)
    }

    /// Replace the VM's settings.  Takes effect for requests and tasks
    /// that start afterwards (and, for `preempt_every`, at once).
    pub fn set_config(&self, config: Config) {
        hub::set_config(&self.lua, config);
    }

    /// Register a hook callback, run from the VM's hook after the cancel
    /// check at `triggers` (see [Hooks](self#hooks)).
    ///
    /// Instruction counts are approximated to the hook's step (the
    /// smallest count among all registrations and the cancel check).
    /// Returning [`VmState::Yield`] yields the running coroutine,
    /// including a coroutine the Lua code created itself, where the
    /// yield reaches the Lua code's `coroutine.resume`.
    ///
    /// The callback applies to the main thread and to coroutines created
    /// afterwards; coroutines that already exist keep the instruction
    /// count and events they were created with.  It is kept across a
    /// second [`Vm::attach`].
    ///
    /// The callback is not re-entered.  A callback that runs Lua code
    /// can be hooked again from inside itself: resuming a coroutine is
    /// the usual case, because the new thread has hooks enabled while
    /// the hooked thread does not.  That inner call fails with
    /// [`mlua::Error::RecursiveMutCallback`].
    ///
    /// Known limit: a request's Lua error goes through the crate's
    /// message handler (a C function under `xpcall`).  Count and line
    /// callbacks do not fire inside it, but a callback with `on_calls` /
    /// `on_returns` fires for the handler's own call and return.  If
    /// that callback returns `Err`, its error replaces the original Lua
    /// error (the request fails with "error in error handling" or the
    /// callback's error).  Do not fail from call / return callbacks if
    /// the original error matters.
    pub fn add_hook<F>(&self, triggers: HookTriggers, f: F) -> Result<HookId, IsleError>
    where
        F: FnMut(&Lua, &Debug) -> mlua::Result<VmState> + 'static,
    {
        let f = RefCell::new(f);
        hub::add_hook(&self.lua, triggers, move |lua, debug| {
            let mut f = f
                .try_borrow_mut()
                .map_err(|_| mlua::Error::RecursiveMutCallback)?;
            f(lua, debug)
        })
    }

    /// Remove a callback registered with [`Vm::add_hook`].  Returns
    /// whether it was registered.
    pub fn remove_hook(&self, id: HookId) -> Result<bool, IsleError> {
        hub::remove_hook(&self.lua, id)
    }

    /// The `task` library table (see [The `task` library](self#the-task-library)).
    ///
    /// It is not set as a global: the host decides where it lives,
    /// e.g. `lua.globals().set("task", vm.task_lib()?)`.  The table is
    /// created on the first call (this runs the library's Lua chunk);
    /// every later call, through any `Vm` of the same VM, returns the
    /// same table.
    ///
    /// In an actor's init closure the VM is not attached yet: attach it
    /// there (the actor re-attaches after the closure and keeps the
    /// config and the table).  `?` converts the [`IsleError`] into the
    /// closure's `mlua::Error`.
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use mlua_isle::runtime::{Config, Vm};
    /// use mlua_isle::AsyncIsle;
    ///
    /// let (isle, driver) = AsyncIsle::spawn(|lua| {
    ///     let vm = Vm::attach(lua, Config::default())?;
    ///     lua.globals().set("task", vm.task_lib()?)
    /// })
    /// .await?;
    /// let r: i64 = isle
    ///     .coroutine_eval(
    ///         "local h = task.spawn(function(a, b) return a + b end, 1, 2)
    ///          local ok, sum = h:join()
    ///          return sum",
    ///     )
    ///     .await?;
    /// assert_eq!(r, 3);
    /// driver.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Fails only when creating the table fails (e.g. the VM's memory
    /// limit is reached).
    #[cfg(feature = "tokio")]
    pub fn task_lib(&self) -> Result<mlua::Table, IsleError> {
        if let Some(t) = self.cached_task_lib()? {
            return Ok(t);
        }
        // Created without holding the app data borrow: the chunk runs
        // under the hook, whose callbacks may touch app data.
        let key = self
            .lua
            .create_registry_value(crate::task_lib::create(&self.lua)?)?;
        {
            let a = self.attached();
            // A table stored meanwhile wins; `key` is then dropped.
            let _ = a.task.set(key);
        }
        Ok(self
            .cached_task_lib()?
            .expect("the task table was just stored"))
    }

    #[cfg(feature = "tokio")]
    fn cached_task_lib(&self) -> Result<Option<mlua::Table>, IsleError> {
        let a = self.attached();
        match a.task.get() {
            Some(key) => Ok(Some(self.lua.registry_value(key)?)),
            None => Ok(None),
        }
    }

    #[cfg(feature = "tokio")]
    fn attached(&self) -> mlua::AppDataRef<'_, Attached> {
        self.lua
            .app_data_ref::<Attached>()
            .expect("a Vm exists only for an attached VM")
    }

    /// Run `f(args)` as a root coroutine under `token`.
    ///
    /// Resolves only after everything the root started has ended: the
    /// Lua tasks it spawned and the host tasks spawned through
    /// [`current_scope`], transitively (contract 1 of the
    /// [module docs](self)).  Resolves to `Err(IsleError::Cancelled)` if
    /// `token` was cancelled, whatever the coroutine returned; the
    /// coroutine and its tasks then get the VM's [`Config::grace`], as
    /// one deadline, before they are dropped.  The cancellation error
    /// raised by some other token (a host function that returned
    /// `Err(mlua::Error::external(Cancelled))`) also resolves to
    /// `Err(IsleError::Cancelled)`.
    ///
    /// A Lua error resolves to `Err(IsleError::Lua(f))`, where the
    /// [`LuaFailure`] is built from the raised value itself on this
    /// thread: `f.message` is `tostring(err)` and, with the `serde`
    /// feature, `f.value` is the value as JSON (so `error({ code = 42 })`
    /// gives `f.value["code"] == 42`).
    ///
    /// A task in a CPU loop cannot be dropped until it yields; without
    /// [`Config::preempt_every`] the wait blocks on it.
    ///
    /// Await it inside a [`tokio::task::LocalSet`].
    ///
    /// # Dropping the future
    ///
    /// Dropping the returned future before it resolves (wrapping it in
    /// [`tokio::time::timeout`], or a losing [`tokio::select!`] arm)
    /// drops the coroutine at once but only schedules the tasks it
    /// spawned for abort: tokio drops them on a later poll of the
    /// `LocalSet`, as with
    /// [`AbortHandle::abort`](tokio::task::AbortHandle::abort).  To have
    /// the tasks gone before you continue, [cancel](CancelToken::cancel)
    /// the token and await the future instead of dropping it.
    ///
    /// ```rust
    /// use mlua_isle::runtime::{CancelToken, Config, Vm};
    /// use std::time::Duration;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// let local = tokio::task::LocalSet::new();
    /// let lua = mlua::Lua::new();
    ///
    /// let vm = Vm::attach(&lua, Config { grace: Duration::from_secs(1), ..Default::default() })?;
    /// lua.globals().set("task", vm.task_lib()?)?;
    /// let main: mlua::Function = lua
    ///     .load("return function(x) local _, v = task.spawn(function() return x * 2 end):join() return v end")
    ///     .eval()?;
    ///
    /// let token = CancelToken::new();
    /// let out = local.block_on(&rt, vm.run(&token, main, 21))?;
    /// assert_eq!(out[0].as_i64(), Some(42));
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "tokio")]
    pub async fn run(
        &self,
        token: &CancelToken,
        f: mlua::Function,
        args: impl mlua::IntoLuaMulti,
    ) -> Result<mlua::MultiValue, IsleError> {
        let args = args.into_lua_multi(&self.lua)?;
        crate::scope::run_root(&self.lua, token.clone(), f, args).await
    }
}

/// Attach an actor's VM after its init closure ran.
///
/// With `config`, it replaces whatever the init closure configured;
/// without, the VM keeps the init closure's settings (the default if it
/// set none).  The hook is (re-)installed either way, so a hook the init
/// closure set with `Lua::set_hook` is replaced, as before.
pub(crate) fn attach_after_init(lua: &Lua, config: Option<Config>) -> Result<Vm, IsleError> {
    let config = config.unwrap_or_else(|| hub::config(lua));
    Vm::attach(lua, config)
}

/// Make sure the VM is attached and its hook is in place, before a
/// request runs.  Re-installs the hook if `Lua::set_hook` replaced it;
/// keeps the stored config.
pub(crate) fn ensure_attached(lua: &Lua) -> Result<(), IsleError> {
    let attached = lua.app_data_ref::<Attached>().is_some();
    if attached {
        hub::ensure_installed(lua)
    } else {
        // Unreachable for the actors (they attach before reporting a
        // successful spawn); kept as a defensive path.
        attach_after_init(lua, None).map(drop)
    }
}

/// The [`Vm`] of `lua`, attaching it with its stored config (the
/// default if none was set) when it is not attached yet.
pub(crate) fn of_or_attach(lua: &Lua) -> Result<Vm, IsleError> {
    match Vm::of(lua) {
        Some(vm) => Ok(vm),
        None => attach_after_init(lua, None),
    }
}
