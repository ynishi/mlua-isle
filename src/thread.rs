//! Lua thread — the dedicated thread that owns the Lua VM.
//!
//! This module is internal.  The public API is [`Isle`](crate::Isle).

use crate::error::IsleError;
use crate::hook;
use crate::Request;
use std::sync::mpsc;

/// Instruction check interval for the cancel hook.
const HOOK_INTERVAL: u32 = 1000;

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
                hook::install_cancel_hook(&lua, cancel, HOOK_INTERVAL);
                let result = f(&lua);
                hook::remove_hook(&lua);
                let _ = tx.send(result);
            }
            Request::Shutdown => break,
        }
    }
}

fn execute_eval(
    lua: &mlua::Lua,
    code: &str,
    cancel: &hook::CancelToken,
) -> Result<String, IsleError> {
    hook::install_cancel_hook(lua, cancel.clone(), HOOK_INTERVAL);
    let result: mlua::Result<mlua::Value> = lua.load(code).eval();
    hook::remove_hook(lua);

    match result {
        Ok(val) => lua_value_to_string(lua, val),
        Err(e) => Err(IsleError::from(e)),
    }
}

fn execute_call(
    lua: &mlua::Lua,
    func_name: &str,
    args: &[String],
    cancel: &hook::CancelToken,
) -> Result<String, IsleError> {
    hook::install_cancel_hook(lua, cancel.clone(), HOOK_INTERVAL);

    let func: mlua::Function = lua
        .globals()
        .get(func_name)
        .map_err(|e| IsleError::Lua(format!("function '{func_name}' not found: {e}")))?;

    let lua_args: Vec<mlua::Value> = args
        .iter()
        .map(|s| mlua::Value::String(lua.create_string(s).unwrap()))
        .collect();

    let multi = mlua::MultiValue::from_vec(lua_args);
    let result: mlua::Result<mlua::Value> = func.call(multi);
    hook::remove_hook(lua);

    match result {
        Ok(val) => lua_value_to_string(lua, val),
        Err(e) => Err(IsleError::from(e)),
    }
}

/// Convert a Lua value to a String representation.
///
/// - `Nil` → empty string
/// - `String` → the string
/// - `Integer/Number/Boolean` → tostring
/// - `Table` → serialized via tostring (or a simple repr)
fn lua_value_to_string(lua: &mlua::Lua, val: mlua::Value) -> Result<String, IsleError> {
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
