//! Calling Lua under `xpcall`, so that a raised value comes back to Rust
//! as a value.
//!
//! A raw value that reaches Rust through mlua's own error path is
//! flattened to `tostring(v)` (mlua's `pop_error`).  Called through
//! `xpcall`, the value is returned instead, still in hand on the VM
//! thread when the [`LuaFailure`] is built.
//!
//! The pieces are created once per VM by [`install`], before the init
//! closure runs (and by [`Vm::attach`](crate::runtime::Vm::attach)):
//!
//! - `xpcall` is captured then, so an init closure that removes it or
//!   replaces the globals table does not break requests.
//! - The message handler is a C function: it runs no Lua instructions,
//!   so the cancel hook and user count / line hooks do not fire inside
//!   it (a hook error there would replace the original error with
//!   "error in error handling"), and it never converts the error value
//!   (converting a `WrappedFailure` panic would resume the panic inside
//!   the handler).  It stores the traceback in a registry table keyed
//!   (weakly) by the erroring thread, together with the error value it
//!   saw, and returns the error unchanged.
//!   Keyed by thread, because coroutine requests interleave on one VM
//!   and a `__close` handler may yield between the message handler and
//!   the return of `xpcall`.
//! - `take(err)` (also a C function) reads and clears the running
//!   thread's entry.  The handler ran for `err` only when the stored
//!   value is `err` itself (`rawequal`): an entry left by an earlier
//!   error, for example one whose unwind then ran a `__close` handler
//!   that hit a memory error, does not count.
//!
//! Lua does not call the message handler for a memory error (or for
//! "error in error handling"); such an error is classified by its
//! message, without a traceback.  On a stack overflow the handler cannot
//! reserve stack and stores nothing, so that failure is
//! [`LuaErrorKind::Runtime`] ("stack overflow") without a traceback.
//!
//! # Safety of the C functions
//!
//! A memory error inside `lua_createtable`, `luaL_traceback` or
//! `lua_rawset` longjmps (unwinds) over the C function's frame.  That is
//! sound only because the frame holds no Rust values with destructors
//! (only the raw `lua_State` pointer and integers); keep it that way.

use crate::error::{IsleError, LuaErrorKind, LuaFailure};
use mlua::{Function, Lua, MultiValue, RegistryKey, Value};
use std::ffi::{c_int, CStr};

/// Registry field of the per-thread traceback table.  The C handler
/// looks it up by this name.
const TRACES: &CStr = c"__mlua_isle_traces";

/// The root coroutine wrapper: marks the isle-created coroutine, then
/// calls `f` under `xpcall` and returns `true, ...` or
/// `false, err, handled, traceback`.  Uses no globals.
///
/// `xpcall` closes to-be-closed variables during the unwind, where their
/// `__close` can still yield; an error that escaped the coroutine would
/// leave them open until the coroutine is closed from C.
#[cfg(feature = "tokio")]
pub(crate) const WRAP_CALL: &str = "\
local mark, f, xpcall, handler, take = ...
local function finish(ok, ...)
  if ok then return true, ... end
  local e = ...
  return false, e, take(e)
end
return function(...)
  mark()
  take()
  return finish(xpcall(f, handler, ...))
end";

/// The protect pieces of a VM, kept in its app data.
struct Protect {
    xpcall: RegistryKey,
    handler: RegistryKey,
    take: RegistryKey,
}

/// Create the protect pieces of `lua` if it has none.  Captures the
/// `xpcall` global, so call it before the globals are sandboxed.
///
/// A failure is a setup failure, [`IsleError::Init`]: kind
/// [`LuaErrorKind::External`] when `xpcall` is not a function, else
/// built from the `mlua::Error`.
pub(crate) fn install(lua: &Lua) -> Result<(), IsleError> {
    install_inner(lua).map_err(|e| IsleError::Init(LuaFailure::from_mlua(&e)))?
}

fn install_inner(lua: &Lua) -> mlua::Result<Result<(), IsleError>> {
    if lua.app_data_ref::<Protect>().is_some() {
        return Ok(Ok(()));
    }
    let xpcall = match lua.globals().get::<Value>("xpcall")? {
        Value::Function(f) => f,
        _ => {
            return Ok(Err(IsleError::Init(LuaFailure::new(
                LuaErrorKind::External,
                "the `xpcall` global is not a function; \
                 attach the VM before sandboxing its globals",
            ))))
        }
    };
    let traces = lua.create_table()?;
    traces.set_metatable(Some(lua.create_table_from([("__mode", "k")])?))?;
    lua.set_named_registry_value(TRACES.to_str().expect("ASCII key"), &traces)?;
    // SAFETY: `trace_handler` follows the Lua C function protocol: it
    // checks the stack before pushing and returns one value.  A Lua
    // error inside it (a memory error in `lua_createtable`,
    // `luaL_traceback` or `lua_rawset`) longjmps over its frame, which
    // is sound because the frame holds no values with destructors; keep
    // it that way (see the module docs).
    let handler = unsafe { lua.create_c_function(trace_handler)? };
    // SAFETY: as above; `take_handler` returns one or two values and its
    // frame likewise holds no values with destructors.
    let take = unsafe { lua.create_c_function(take_handler)? };
    let protect = Protect {
        xpcall: lua.create_registry_value(xpcall)?,
        handler: lua.create_registry_value(handler)?,
        take: lua.create_registry_value(take)?,
    };
    lua.set_app_data(protect);
    Ok(Ok(()))
}

/// The protect pieces of a VM: `xpcall`, the message handler, `take`.
pub(crate) type Parts = (Function, Function, Function);

/// `(xpcall, handler, take)` of `lua`, installing them first if needed
/// (defensive: every caller has attached the VM, which installs them).
pub(crate) fn parts(lua: &Lua) -> Result<Parts, IsleError> {
    install(lua)?;
    let p = lua.app_data_ref::<Protect>().expect("installed just above");
    Ok((
        lua.registry_value(&p.xpcall)?,
        lua.registry_value(&p.handler)?,
        lua.registry_value(&p.take)?,
    ))
}

/// Message handler: store `{err, traceback}` for the erroring thread,
/// return the error value unchanged.
unsafe extern "C-unwind" fn trace_handler(l: *mut mlua::ffi::lua_State) -> c_int {
    use mlua::ffi;
    ffi::lua_settop(l, 1);
    // Fails on a stack overflow: nothing is stored (see the module docs).
    if ffi::lua_checkstack(l, 4 + ffi::LUA_TRACEBACK_STACK) == 0 {
        return 1;
    }
    if ffi::lua_getfield(l, ffi::LUA_REGISTRYINDEX, TRACES.as_ptr()) != ffi::LUA_TTABLE {
        ffi::lua_settop(l, 1);
        return 1;
    }
    // [err, traces]
    ffi::lua_pushthread(l);
    ffi::lua_createtable(l, 2, 0);
    ffi::lua_pushvalue(l, 1);
    ffi::lua_rawseti(l, -2, 1);
    // Level 1: the function that raised the error (0 is this handler).
    ffi::luaL_traceback(l, l, std::ptr::null(), 1);
    ffi::lua_rawseti(l, -2, 2);
    // [err, traces, thread, entry]
    ffi::lua_rawset(l, 2);
    ffi::lua_settop(l, 1);
    1
}

/// Lua-callable `take(err)`: read and clear the running thread's entry.
/// Returns `false` (the handler did not run for `err`) or
/// `true, traceback` (`traceback` nil when none was taken).  The handler
/// ran for `err` when the entry's stored error is `rawequal` to `err`.
///
/// A C function because the key must be the thread that is actually
/// running: `Lua::current_thread` returns the owning thread instead of a
/// coroutine that `call_async` created, which is what a root runs in.
unsafe extern "C-unwind" fn take_handler(l: *mut mlua::ffi::lua_State) -> c_int {
    use mlua::ffi;
    ffi::lua_settop(l, 1);
    if ffi::lua_checkstack(l, 4) == 0 {
        ffi::lua_settop(l, 0);
        ffi::lua_pushboolean(l, 0);
        return 1;
    }
    if ffi::lua_getfield(l, ffi::LUA_REGISTRYINDEX, TRACES.as_ptr()) != ffi::LUA_TTABLE {
        ffi::lua_settop(l, 0);
        ffi::lua_pushboolean(l, 0);
        return 1;
    }
    // [err, traces]
    ffi::lua_pushthread(l);
    ffi::lua_rawget(l, 2);
    // [err, traces, entry]
    if ffi::lua_type(l, 3) != ffi::LUA_TTABLE {
        ffi::lua_settop(l, 0);
        ffi::lua_pushboolean(l, 0);
        return 1;
    }
    ffi::lua_pushthread(l);
    ffi::lua_pushnil(l);
    ffi::lua_rawset(l, 2);
    ffi::lua_rawgeti(l, 3, 1);
    // [err, traces, entry, stored]
    if ffi::lua_rawequal(l, 1, 4) == 0 {
        ffi::lua_settop(l, 0);
        ffi::lua_pushboolean(l, 0);
        return 1;
    }
    ffi::lua_pushboolean(l, 1);
    ffi::lua_rawgeti(l, 3, 2);
    // [err, traces, entry, stored, true, traceback]
    2
}

/// Call `take(err)` on this thread from Rust: whether the message
/// handler ran for `err`, and its traceback.  `err` nil only clears.
fn take_trace(take: &Function, err: Value) -> mlua::Result<(bool, Option<String>)> {
    let (handled, trace): (bool, Option<mlua::LuaString>) = take.call(err)?;
    Ok((handled, trace.map(|s| s.to_string_lossy())))
}

/// Call `func(args)` under `xpcall` on this thread (a sync request).
pub(crate) fn call(lua: &Lua, func: Function, args: MultiValue) -> Result<MultiValue, IsleError> {
    let (xpcall, handler, take) = parts(lua)?;
    take_trace(&take, Value::Nil)?;
    let mut call_args = MultiValue::with_capacity(args.len() + 2);
    call_args.push_back(Value::Function(func));
    call_args.push_back(Value::Function(handler));
    call_args.extend(args);
    let mut out: MultiValue = xpcall.call(call_args)?;
    match out.pop_front() {
        Some(Value::Boolean(true)) => Ok(out),
        _ => {
            let err = out.pop_front().unwrap_or(Value::Nil);
            let (handled, trace) = take_trace(&take, err.clone())?;
            Err(error_from_value(lua, err, handled, trace))
        }
    }
}

/// Turn what [`WRAP_CALL`] returned into a `Result`.
#[cfg(feature = "tokio")]
pub(crate) fn unwrap_root(lua: &Lua, mut values: MultiValue) -> Result<MultiValue, IsleError> {
    match values.pop_front() {
        Some(Value::Boolean(true)) => Ok(values),
        Some(Value::Boolean(false)) => {
            let err = values.pop_front().unwrap_or(Value::Nil);
            let handled = matches!(values.pop_front(), Some(Value::Boolean(true)));
            let trace = match values.pop_front() {
                Some(Value::String(s)) => Some(s.to_string_lossy()),
                _ => None,
            };
            Err(error_from_value(lua, err, handled, trace))
        }
        // Not produced by `WRAP_CALL`; kept total.
        other => Err(IsleError::Lua(LuaFailure::new(
            LuaErrorKind::Other,
            format!("unexpected protected-call result: {other:?}"),
        ))),
    }
}

/// Build the error for a raised Lua value, on the VM thread.
///
/// - A Rust error (`Value::Error`, mlua's `WrappedFailure`) goes through
///   `From<mlua::Error>`, so the cancellation error becomes
///   [`IsleError::Cancelled`]; `trace` fills a missing traceback.
/// - An error for which the message handler did not run (`handled`
///   false) is a memory error or an error in error handling: classified
///   by its message, no traceback, no value.
/// - Any other value is a [`LuaErrorKind::Runtime`] failure whose
///   message is `tostring(value)` and, with `serde`, whose value is the
///   value as JSON when it converts.
fn error_from_value(lua: &Lua, value: Value, handled: bool, trace: Option<String>) -> IsleError {
    if let Value::Error(e) = &value {
        return match IsleError::from((**e).clone()) {
            IsleError::Lua(mut f) => {
                if f.traceback.is_none() {
                    f.traceback = trace;
                }
                IsleError::Lua(f)
            }
            other => other,
        };
    }
    let message = match value.to_string() {
        Ok(s) => s,
        Err(_) => match &value {
            Value::String(s) => s.to_string_lossy(),
            other => format!("{other:?}"),
        },
    };
    if !handled {
        let kind = if message.contains("not enough memory") {
            LuaErrorKind::Memory
        } else if message.contains("error in error handling") {
            LuaErrorKind::Other
        } else {
            LuaErrorKind::Runtime
        };
        return IsleError::Lua(LuaFailure::new(kind, message));
    }
    let mut f = LuaFailure::new(LuaErrorKind::Runtime, message);
    f.traceback = trace;
    #[cfg(feature = "serde")]
    {
        use mlua::LuaSerdeExt;
        f.value = lua.from_value::<serde_json::Value>(value).ok();
    }
    #[cfg(not(feature = "serde"))]
    let _ = lua;
    IsleError::Lua(f)
}
