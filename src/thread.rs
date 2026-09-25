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
            Request::Run { job, cancel } => job(&lua, &cancel),
            Request::Shutdown => break,
        }
    }
}

/// Run `code` as a sync `eval` and convert its return values to `T` on
/// the VM thread.
pub(crate) fn execute_eval<T: mlua::FromLuaMulti>(
    lua: &mlua::Lua,
    code: &str,
    cancel: &hook::CancelToken,
) -> Result<T, IsleError> {
    crate::runtime::ensure_attached(lua)?;
    let _enter = hook::EnterGuard::new(cancel);
    let result = load_eval(lua, code)
        .map_err(IsleError::from)
        .and_then(|func| protect::call(lua, func, mlua::MultiValue::new()));
    let values = cancelled_wins(result, cancel)?;
    Ok(T::from_lua_multi(values, lua)?)
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

pub(crate) fn execute_exec<T>(
    lua: &mlua::Lua,
    f: impl FnOnce(&mlua::Lua) -> Result<T, IsleError>,
    cancel: &hook::CancelToken,
) -> Result<T, IsleError> {
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

/// Call the global function `func_name` with `args` as a sync `call`
/// and convert its return values to `T`.  Both conversions run on the
/// VM thread.
pub(crate) fn execute_call<A: mlua::IntoLuaMulti, T: mlua::FromLuaMulti>(
    lua: &mlua::Lua,
    func_name: &str,
    args: A,
    cancel: &hook::CancelToken,
) -> Result<T, IsleError> {
    crate::runtime::ensure_attached(lua)?;
    let _enter = hook::EnterGuard::new(cancel);

    let func = global_function(lua, func_name)?;
    let multi = args.into_lua_multi(lua)?;
    let values = cancelled_wins(protect::call(lua, func, multi), cancel)?;
    Ok(T::from_lua_multi(values, lua)?)
}
