//! Lua thread — the dedicated thread that owns the Lua VM.
//!
//! This module is internal.  The public API is [`Isle`](crate::Isle).

use crate::error::IsleError;
use crate::hook;
use crate::protect;
use crate::Request;
use std::sync::mpsc;

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
    crate::runtime::ensure_attached(lua)?;
    let _enter = hook::EnterGuard::new(cancel);
    let result = load_eval(lua, code)
        .map_err(IsleError::from)
        .and_then(|func| protect::call(lua, func, mlua::MultiValue::new()));
    let values = cancelled_wins(result, cancel)?;
    lua_value_to_string(lua, values.into_iter().next().unwrap_or(mlua::Value::Nil))
}

/// Chunk name of a sync `eval` (error positions read `eval:<line>:`).
pub(crate) const EVAL_CHUNK: &str = "=eval";

/// Compile `code` the way [`mlua::Chunk::eval`] does: as an expression
/// (`return <code>`) if that compiles, else as a block.  Both forms get
/// the chunk name [`EVAL_CHUNK`].
fn load_eval(lua: &mlua::Lua, code: &str) -> mlua::Result<mlua::Function> {
    match lua
        .load(format!("return {code}"))
        .set_name(EVAL_CHUNK)
        .into_function()
    {
        Ok(f) => Ok(f),
        Err(_) => lua.load(code).set_name(EVAL_CHUNK).into_function(),
    }
}

/// An error of a request whose token was cancelled is
/// [`IsleError::Cancelled`], whatever the Lua code made of the cancel
/// (it may have caught it and raised something else).
pub(crate) fn cancelled_wins<T>(
    result: Result<T, IsleError>,
    cancel: &hook::CancelToken,
) -> Result<T, IsleError> {
    match result {
        Err(_) if cancel.is_cancelled() => Err(IsleError::Cancelled),
        other => other,
    }
}

pub(crate) fn execute_exec(
    lua: &mlua::Lua,
    f: impl FnOnce(&mlua::Lua) -> Result<String, IsleError>,
    cancel: &hook::CancelToken,
) -> Result<String, IsleError> {
    crate::runtime::ensure_attached(lua)?;
    let _enter = hook::EnterGuard::new(cancel);
    f(lua)
}

/// The global function `name`, or [`IsleError::NotFound`] when the
/// global is not a function.
pub(crate) fn global_function(lua: &mlua::Lua, name: &str) -> Result<mlua::Function, IsleError> {
    match lua.globals().get::<mlua::Value>(name)? {
        mlua::Value::Function(f) => Ok(f),
        _ => Err(IsleError::NotFound(name.to_string())),
    }
}

/// The string arguments of a `call` as Lua values.
pub(crate) fn string_args(lua: &mlua::Lua, args: &[String]) -> Result<mlua::MultiValue, IsleError> {
    let lua_args = args
        .iter()
        .map(|s| lua.create_string(s).map(mlua::Value::String))
        .collect::<mlua::Result<Vec<_>>>()?;
    Ok(mlua::MultiValue::from_vec(lua_args))
}

pub(crate) fn execute_call(
    lua: &mlua::Lua,
    func_name: &str,
    args: &[String],
    cancel: &hook::CancelToken,
) -> Result<String, IsleError> {
    crate::runtime::ensure_attached(lua)?;
    let _enter = hook::EnterGuard::new(cancel);

    let func = global_function(lua, func_name)?;
    let multi = string_args(lua, args)?;
    let values = cancelled_wins(protect::call(lua, func, multi), cancel)?;
    lua_value_to_string(lua, values.into_iter().next().unwrap_or(mlua::Value::Nil))
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
        mlua::Value::String(s) => s.to_str().map(|s| s.to_string()).map_err(IsleError::from),
        mlua::Value::Integer(n) => Ok(n.to_string()),
        mlua::Value::Number(n) => Ok(n.to_string()),
        mlua::Value::Boolean(b) => Ok(b.to_string()),
        other => {
            // Use Lua's tostring() for tables and other types
            let tostring: mlua::Function = lua.globals().get("tostring")?;
            let s: String = tostring.call(other)?;
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
