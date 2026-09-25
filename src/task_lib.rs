//! The Lua `task` library: its Lua part and the host functions under
//! it.  Crate-internal; documented in the [`runtime`](crate::runtime)
//! module docs ("The `task` library").

use crate::scope::{self, Spawned, Wrap};
use mlua::{Function, Lua, MultiValue, Table, Value, Variadic};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

const LIB: &str = r#"
local spawn_raw, join_raw, cancel_raw, done_raw, release_raw, is_cancel_error, CANCELLED = ...
local rawequal = rawequal

local Task = {}
Task.__index = Task

function Task:join()
  if self._joined then error("task already joined", 2) end
  self._joined = true
  return join_raw(self._id)
end

function Task:cancel()
  if not self._joined then cancel_raw(self._id) end
end

function Task:done()
  return self._joined or done_raw(self._id)
end

Task.__close = function(self)
  if not self._joined then
    cancel_raw(self._id)
    self:join()
  end
end

Task.__gc = function(self)
  if not self._joined then release_raw(self._id) end
end

local task = { CANCELLED = CANCELLED }

function task.is_cancelled(err)
  return rawequal(err, CANCELLED) or is_cancel_error(err)
end

function task.spawn(f, ...)
  return setmetatable({ _id = spawn_raw(f, ...) }, Task)
end

return task
"#;

#[derive(Default)]
struct Registry {
    tasks: RefCell<HashMap<u64, Spawned<MultiValue>>>,
    next_id: Cell<u64>,
}

impl Registry {
    fn get(&self, id: u64) -> mlua::Result<Spawned<MultiValue>> {
        self.tasks
            .borrow()
            .get(&id)
            .cloned()
            .ok_or_else(|| mlua::Error::runtime("unknown task"))
    }
}

/// Create the `task` library table for `lua`.  Called once per VM by
/// [`Vm::task_lib`](crate::runtime::Vm::task_lib), which documents the
/// library.
pub(crate) fn create(lua: &Lua) -> mlua::Result<Table> {
    let reg = Rc::new(Registry::default());
    let cancelled_marker = lua.create_table()?;
    cancelled_marker.set_metatable(Some(lua.create_table_from([(
        "__tostring",
        lua.create_function(|_, _: Value| Ok("task.CANCELLED"))?,
    )])?))?;

    let r = reg.clone();
    let spawn_raw = lua.create_function(move |lua, (f, args): (Function, Variadic<Value>)| {
        let scope = scope::current().ok_or_else(|| {
            mlua::Error::runtime(
                "task.spawn: not inside a coroutine request or task (sync requests cannot spawn)",
            )
        })?;
        let grace = crate::hub::config(lua).grace;
        let (root, body) = scope::lua_body(lua, Wrap::PCall, f, MultiValue::from_iter(args))?;
        // `None` in the result slot means cancelled.
        let task = scope.spawn(grace, Some(root), body, |out, token| match out {
            None => None,
            Some(Ok(values)) => {
                let failed = matches!(values.front(), Some(Value::Boolean(false)));
                if failed && token.is_cancelled() {
                    None
                } else {
                    Some(values)
                }
            }
            Some(Err(e)) if token.is_cancelled() => {
                drop(e);
                None
            }
            Some(Err(e)) => Some(MultiValue::from_vec(vec![
                Value::Boolean(false),
                scope::error_value(e),
            ])),
        });

        let id = r.next_id.get();
        r.next_id.set(id + 1);
        r.tasks.borrow_mut().insert(id, task);
        Ok(id)
    })?;

    let r = reg.clone();
    let marker = cancelled_marker.clone();
    let join_raw = lua.create_async_function(move |_, id: u64| {
        let r = r.clone();
        let marker = marker.clone();
        async move {
            let task = r.get(id)?;
            task.state.wait_done().await;
            r.tasks.borrow_mut().remove(&id);
            let out = task.result.borrow_mut().take();
            Ok(match out {
                Some(values) => values,
                None => MultiValue::from_vec(vec![Value::Boolean(false), Value::Table(marker)]),
            })
        }
    })?;

    let r = reg.clone();
    let cancel_raw = lua.create_function(move |_, id: u64| {
        r.get(id)?.state.token.cancel();
        Ok(())
    })?;

    let r = reg.clone();
    let done_raw = lua.create_function(move |_, id: u64| Ok(r.get(id)?.state.is_done()))?;

    let r = reg;
    let release_raw = lua.create_function(move |_, id: u64| {
        if let Some(task) = r.tasks.borrow_mut().remove(&id) {
            task.state.token.cancel();
        }
        Ok(())
    })?;

    // True for the error a cancel raises: mlua hands a Rust error
    // (`WrappedFailure` userdata) to Rust as `Value::Error`.
    let is_cancel_error = lua.create_function(|_, v: Value| {
        Ok(matches!(&v, Value::Error(e) if crate::error::is_cancel(e)))
    })?;

    lua.load(LIB).set_name("=mlua_isle.tasks").call((
        spawn_raw,
        join_raw,
        cancel_raw,
        done_raw,
        release_raw,
        is_cancel_error,
        cancelled_marker,
    ))
}
