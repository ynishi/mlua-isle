//! Lua thread — the dedicated thread that owns the Lua VM.
//!
//! This module is internal.  The public API is [`Isle`](crate::Isle).

use crate::error::IsleError;
use crate::hook;
use crate::Request;
use std::sync::mpsc;

/// Instruction check interval for the cancel hook.
pub(crate) const HOOK_INTERVAL: u32 = 1000;

/// RAII guard that removes the Lua debug hook on drop.
///
/// Ensures `remove_hook` is called even if a panic occurs during
/// Lua execution, preventing a stale hook from affecting subsequent
/// requests on the same Lua VM.
struct HookGuard<'a> {
    lua: &'a mlua::Lua,
}

impl<'a> HookGuard<'a> {
    fn new(lua: &'a mlua::Lua, cancel: &hook::CancelToken) -> Result<Self, IsleError> {
        hook::install_cancel_hook(lua, cancel.clone(), HOOK_INTERVAL)?;
        Ok(Self { lua })
    }
}

impl Drop for HookGuard<'_> {
    fn drop(&mut self) {
        hook::remove_hook(self.lua);
    }
}

/// Run the Lua event loop on the current thread.
///
/// This function blocks until a `Shutdown` request is received or the
/// channel is disconnected.
pub(crate) fn run_loop(lua: mlua::Lua, rx: mpsc::Receiver<Request>) {
    while let Ok(req) = rx.recv() {
        match req {
            Request::Eval { code, cancel, tx } => {
                let result = execute_eval(&lua, &code, &cancel);
                let _ = tx.send(result);
            }
            Request::Call {
                func,
                args,
                cancel,
                tx,
            } => {
                let result = execute_call(&lua, &func, &args, &cancel);
                let _ = tx.send(result);
            }
            Request::Exec { f, cancel, tx } => {
                let result = execute_exec(&lua, f, &cancel);
                let _ = tx.send(result);
            }
            Request::Shutdown => break,
        }
    }
}

pub(crate) fn execute_eval(
    lua: &mlua::Lua,
    code: &str,
    cancel: &hook::CancelToken,
) -> Result<String, IsleError> {
    let _guard = HookGuard::new(lua, cancel)?;
    let result: mlua::Result<mlua::Value> = lua.load(code).eval();

    match result {
        Ok(val) => lua_value_to_string(lua, val),
        Err(e) => Err(IsleError::from(e)),
    }
}

pub(crate) fn execute_exec(
    lua: &mlua::Lua,
    f: impl FnOnce(&mlua::Lua) -> Result<String, IsleError>,
    cancel: &hook::CancelToken,
) -> Result<String, IsleError> {
    let _guard = HookGuard::new(lua, cancel)?;
    f(lua)
}

pub(crate) fn execute_call(
    lua: &mlua::Lua,
    func_name: &str,
    args: &[String],
    cancel: &hook::CancelToken,
) -> Result<String, IsleError> {
    let _guard = HookGuard::new(lua, cancel)?;

    let func: mlua::Function = lua
        .globals()
        .get(func_name)
        .map_err(|e| IsleError::Lua(format!("function '{func_name}' not found: {e}")))?;

    let lua_args: Vec<mlua::Value> = args
        .iter()
        .map(|s| lua.create_string(s).map(mlua::Value::String))
        .collect::<mlua::Result<Vec<_>>>()
        .map_err(IsleError::from)?;

    let multi = mlua::MultiValue::from_vec(lua_args);
    let val: mlua::Value = func.call(multi).map_err(IsleError::from)?;
    lua_value_to_string(lua, val)
}

/// Convert a Lua value to a String representation.
///
/// - `Nil` → empty string
/// - `String` → the string
/// - `Integer/Number/Boolean` → tostring
/// - `Table` → serialized via tostring (or a simple repr)
pub(crate) fn lua_value_to_string(lua: &mlua::Lua, val: mlua::Value) -> Result<String, IsleError> {
    match val {
        mlua::Value::Nil => Ok(String::new()),
        mlua::Value::String(s) => s
            .to_str()
            .map(|s| s.to_string())
            .map_err(|e| IsleError::Lua(format!("UTF-8 error: {e}"))),
        mlua::Value::Integer(n) => Ok(n.to_string()),
        mlua::Value::Number(n) => Ok(n.to_string()),
        mlua::Value::Boolean(b) => Ok(b.to_string()),
        other => {
            // Use Lua's tostring() for tables and other types
            let tostring: mlua::Function = lua
                .globals()
                .get("tostring")
                .map_err(|e| IsleError::Lua(format!("tostring not found: {e}")))?;
            let s: String = tostring
                .call(other)
                .map_err(|e| IsleError::Lua(format!("tostring failed: {e}")))?;
            Ok(s)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lua_value_to_string_types() {
        let lua = mlua::Lua::new();

        assert_eq!(lua_value_to_string(&lua, mlua::Value::Nil).unwrap(), "");
        assert_eq!(
            lua_value_to_string(&lua, mlua::Value::Boolean(true)).unwrap(),
            "true"
        );
        assert_eq!(
            lua_value_to_string(&lua, mlua::Value::Integer(42)).unwrap(),
            "42"
        );
    }
}
