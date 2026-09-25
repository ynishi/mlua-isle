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
//! `remove_hook`) is always available; `Vm::run`, `Vm::task_lib` and
//! `cancellable` need the `tokio` feature.  With it, setup is three
//! calls (see `Vm::run` for the full example):
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
//!    holds for Lua tasks (`task.spawn`), transitively, whether the root
//!    finished or was cancelled.  Host tasks are not covered yet: a
//!    host function that starts work with its own `spawn_local` is not
//!    waited for (scoped host tasks are a follow-up, issue #8).  The
//!    layer does not drain the host's `LocalSet`.  This holds when
//!    `run` is awaited to the end: dropping the `run` future instead
//!    only schedules the tasks for abort, and a task in a CPU loop
//!    blocks `run` until it yields, which without
//!    [`Config::preempt_every`] it never does.
//! 2. **One error type**, [`IsleError`], on this layer and on the
//!    actors.  Lua errors are carried as their message today; a typed
//!    payload shared by both layers is issue #11.
//! 3. **The layer owns the VM's debug hook.**  Register callbacks with
//!    [`Vm::add_hook`], never with `Lua::set_hook` /
//!    `Lua::set_global_hook`, which replace the hook and stop
//!    cancellation (see [`crate::hooks`]).
//! 4. **One [`Config`] per VM**, read and written through [`Vm`]
//!    ([`Vm::config`], [`Vm::set_config`]; a second [`Vm::attach`]
//!    replaces it).
//! 5. **Cancellation is a token the host creates** and passes to
//!    `run`; `run` spawns nothing and returns no handle.  Ctrl-C, a
//!    timeout or a hook callback cancel that [`CancelToken`].
//!
//! Host functions called from Lua reach the running request or task
//! through the context functions, which read a thread-local and so take
//! no receiver: [`current_token`] and `cancellable`.

use crate::error::IsleError;
use crate::hooks::{self, CancelConfig};
use mlua::debug::Debug;
use mlua::{HookTriggers, Lua, VmState};
use std::cell::RefCell;
use std::fmt;
use std::time::Duration;

pub use crate::hook::{current_token, CancelToken};
pub use crate::hooks::HookId;
#[cfg(feature = "tokio")]
pub use crate::scope::cancellable;

/// Settings of a VM.  One per VM, read and written through [`Vm`].
///
/// The same settings as [`CancelConfig`], which converts to and from it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Config {
    /// How long a cancelled coroutine request or task may keep running
    /// to finish its cleanup before it is dropped.  One deadline for the
    /// cancelled coroutine and every task it spawned, transitively.
    /// See [`CancelConfig::grace`].
    ///
    /// Default: zero (drop at once).
    pub grace: Duration,
    /// Yield the running coroutine request or task every this many
    /// cancel checks (one check every 1000 instructions), so that other
    /// tasks on the thread can run while it is in a CPU loop.
    /// See [`CancelConfig::preempt_every`].
    ///
    /// Default: `None` (never preempt).
    pub preempt_every: Option<u32>,
}

impl From<CancelConfig> for Config {
    fn from(c: CancelConfig) -> Self {
        Self {
            grace: c.grace,
            preempt_every: c.preempt_every,
        }
    }
}

impl From<Config> for CancelConfig {
    fn from(c: Config) -> Self {
        Self {
            grace: c.grace,
            preempt_every: c.preempt_every,
        }
    }
}

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
    /// Hook callbacks registered before (with [`Vm::add_hook`] or
    /// [`hooks::add_hook`]) are kept.
    pub fn attach(lua: &Lua, config: Config) -> Result<Vm, IsleError> {
        hooks::install(lua)?;
        hooks::configure(lua, config.into());
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
        hooks::config(&self.lua).into()
    }

    /// Replace the VM's settings.  Takes effect for requests and tasks
    /// that start afterwards (and, for `preempt_every`, at once).
    pub fn set_config(&self, config: Config) {
        hooks::configure(&self.lua, config.into());
    }

    /// Register a hook callback, run from the VM's hook after the cancel
    /// check at `triggers`.  See [`hooks::add_hook`] for how triggers
    /// combine and where the callback applies.
    ///
    /// The callback is not re-entered.  A callback that runs Lua code
    /// can be hooked again from inside itself: resuming a coroutine is
    /// the usual case, because the new thread has hooks enabled while
    /// the hooked thread does not.  That inner call fails with
    /// [`mlua::Error::RecursiveMutCallback`].
    pub fn add_hook<F>(&self, triggers: HookTriggers, f: F) -> Result<HookId, IsleError>
    where
        F: FnMut(&Lua, &Debug) -> mlua::Result<VmState> + 'static,
    {
        let f = RefCell::new(f);
        hooks::add_hook(&self.lua, triggers, move |lua, debug| {
            let mut f = f
                .try_borrow_mut()
                .map_err(|_| mlua::Error::RecursiveMutCallback)?;
            f(lua, debug)
        })
    }

    /// Remove a callback registered with [`Vm::add_hook`].  Returns
    /// whether it was registered.
    pub fn remove_hook(&self, id: HookId) -> Result<bool, IsleError> {
        hooks::remove_hook(&self.lua, id)
    }

    /// The `task` library table (see [`tasks`](crate::tasks)).
    ///
    /// It is not set as a global: the host decides where it lives,
    /// e.g. `lua.globals().set("task", vm.task_lib()?)`.  The table is
    /// created on the first call (this runs the library's Lua chunk);
    /// every later call, through any `Vm` of the same VM, returns the
    /// same table.
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
            .create_registry_value(crate::tasks::install(&self.lua)?)?;
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
    /// Lua tasks it spawned, transitively (contract 1 of the
    /// [module docs](self)).  Resolves to `Err(IsleError::Cancelled)` if
    /// `token` was cancelled; the coroutine and its tasks then get the
    /// VM's [`Config::grace`], as one deadline, before they are dropped.
    ///
    /// Await it inside a [`tokio::task::LocalSet`].  Dropping the future
    /// instead of cancelling `token` only schedules the tasks for abort
    /// (see [`run_root`](crate::run_root)).
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
    let config = config.unwrap_or_else(|| hooks::config(lua).into());
    Vm::attach(lua, config)
}

/// Make sure the VM is attached and its hook is in place, before a
/// request runs.  Re-installs the hook if `Lua::set_hook` replaced it;
/// keeps the stored config.
pub(crate) fn ensure_attached(lua: &Lua) -> Result<(), IsleError> {
    let attached = lua.app_data_ref::<Attached>().is_some();
    if attached {
        hooks::ensure_installed(lua)
    } else {
        // Unreachable for the actors (they attach before reporting a
        // successful spawn); kept as a defensive path.
        attach_after_init(lua, None).map(drop)
    }
}
