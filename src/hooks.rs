//! Deprecated: the hook API moved to [`runtime::Vm`].
//!
//! | 0.7 | 0.8 |
//! |---|---|
//! | `hooks::install(lua)` | [`Vm::attach(lua, config)`](crate::runtime::Vm::attach) |
//! | `hooks::configure(lua, c)` | [`vm.set_config(c)`](crate::runtime::Vm::set_config) |
//! | `hooks::config(lua)` | [`vm.config()`](crate::runtime::Vm::config) |
//! | `hooks::add_hook(lua, t, f)` | [`vm.add_hook(t, f)`](crate::runtime::Vm::add_hook) |
//! | `hooks::remove_hook(lua, id)` | [`vm.remove_hook(id)`](crate::runtime::Vm::remove_hook) |
//! | `hooks::CancelConfig` | [`runtime::Config`] |
//! | `hooks::HookId` | [`runtime::HookId`] |
//!
//! Every item here forwards to the `runtime` one and will be removed in
//! the release after 0.8.0.  How the hook works is described in the
//! [Hooks](crate::runtime#hooks) section of the `runtime` docs.

use crate::error::IsleError;
use crate::runtime::{self, Vm};
use mlua::debug::Debug;
use mlua::{HookTriggers, Lua, VmState};

/// Handle of a hook callback.  Now [`runtime::HookId`].
#[deprecated(since = "0.8.0", note = "use `mlua_isle::runtime::HookId`")]
pub type HookId = runtime::HookId;

/// Settings of a VM.  Now [`runtime::Config`], the same struct (this is
/// an alias of it).
#[deprecated(since = "0.8.0", note = "use `mlua_isle::runtime::Config`")]
pub type CancelConfig = runtime::Config;

/// Install (or re-install) the hook.  Forwards to [`Vm::attach`] with
/// the VM's stored config (the default if none was set), so the VM is
/// attached afterwards ([`Vm::of`] returns it).
///
/// # Errors
///
/// As [`Vm::attach`]: fails when the VM's `xpcall` global is not a
/// function.
#[deprecated(
    since = "0.8.0",
    note = "use `mlua_isle::runtime::Vm::attach(lua, config)`"
)]
pub fn install(lua: &Lua) -> Result<(), IsleError> {
    Vm::attach(lua, crate::hub::config(lua)).map(drop)
}

/// Register a hook callback.  Attaches the VM with its stored config
/// if it is not attached, then registers `callback` in the same hook as
/// [`Vm::add_hook`], with the 0.7 semantics: an `Fn` callback that may
/// be re-entered (`Vm::add_hook` takes an `FnMut` and is not
/// re-entered).
#[deprecated(since = "0.8.0", note = "use `mlua_isle::runtime::Vm::add_hook`")]
pub fn add_hook<F>(
    lua: &Lua,
    triggers: HookTriggers,
    callback: F,
) -> Result<runtime::HookId, IsleError>
where
    F: Fn(&Lua, &Debug) -> mlua::Result<VmState> + 'static,
{
    runtime::of_or_attach(lua)?;
    crate::hub::add_hook(lua, triggers, callback)
}

/// Remove a hook callback.  The same removal as [`Vm::remove_hook`];
/// does not attach the VM.
#[deprecated(since = "0.8.0", note = "use `mlua_isle::runtime::Vm::remove_hook`")]
pub fn remove_hook(lua: &Lua, id: runtime::HookId) -> Result<bool, IsleError> {
    crate::hub::remove_hook(lua, id)
}

/// Set the VM's settings.  Forwards to [`Vm::set_config`]; on a VM that
/// is not attached yet it stores the config that the next
/// [`Vm::attach`] of an actor keeps (it does not attach).
#[deprecated(
    since = "0.8.0",
    note = "use `mlua_isle::runtime::Vm::set_config` (or pass the config to `Vm::attach`)"
)]
pub fn configure(lua: &Lua, config: runtime::Config) {
    match Vm::of(lua) {
        Some(vm) => vm.set_config(config),
        None => crate::hub::set_config(lua, config),
    }
}

/// The VM's settings.  Forwards to [`Vm::config`]; on a VM that is not
/// attached yet it reads the stored config (the default if none was
/// set).
#[deprecated(since = "0.8.0", note = "use `mlua_isle::runtime::Vm::config`")]
pub fn config(lua: &Lua) -> runtime::Config {
    match Vm::of(lua) {
        Some(vm) => vm.config(),
        None => crate::hub::config(lua),
    }
}
