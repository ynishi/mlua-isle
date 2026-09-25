//! Deprecated: the `task` library is [`Vm::task_lib`](crate::runtime::Vm::task_lib).
//!
//! `tasks::install(lua)` → `Vm::attach(lua, config)?.task_lib()?` (or
//! `vm.task_lib()?` on a `Vm` you already have).  The library itself
//! (`task.spawn`, `join`, `cancel`, `done`, `<close>`, `task.CANCELLED`,
//! `task.is_cancelled`) is unchanged and documented in
//! [The `task` library](crate::runtime#the-task-library) section of the
//! `runtime` docs.  This module will be removed in the release after
//! 0.8.0.

use crate::runtime;
use mlua::{Lua, Table};

/// The `task` library table.  Forwards to
/// [`Vm::task_lib`](crate::runtime::Vm::task_lib), attaching the VM with
/// its stored config first if it is not attached.
///
/// Returns the VM's one `task` table: every call (and every
/// `Vm::task_lib`) returns the same table, where 0.7 created a new one
/// per call.
///
/// # Errors
///
/// The error of `Vm::attach` or `Vm::task_lib`, converted with
/// `From<IsleError> for mlua::Error`.
#[deprecated(
    since = "0.8.0",
    note = "use `mlua_isle::runtime::Vm::task_lib` (after `Vm::attach`)"
)]
pub fn install(lua: &Lua) -> mlua::Result<Table> {
    runtime::of_or_attach(lua)
        .and_then(|vm| vm.task_lib())
        .map_err(mlua::Error::from)
}
