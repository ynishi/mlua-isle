//! A structured task library for Lua code running in an isle.
//!
//! [`install`] returns a Lua table (conventionally set as the global
//! `task`) with:
//!
//! | Lua | meaning |
//! |---|---|
//! | `task.spawn(f, ...)` | Start `f(...)` as a concurrent task of the current coroutine request or task.  Returns a handle. |
//! | `h:join()` | Wait for the task.  Returns `true, ...` (what `f` returned) or `false, err`, where `err` is the raw Lua error value, or `task.CANCELLED` if the task was cancelled.  A handle can be joined once. |
//! | `h:cancel()` | Request cancellation.  Does not wait. |
//! | `h:done()` | Whether the task has finished. |
//! | `local h <close> = task.spawn(...)` | On scope exit, a task that was not joined is cancelled and waited for. |
//!
//! Tasks are **structured**: when a coroutine request or task finishes,
//! the tasks it spawned and did not join are cancelled, and it waits
//! for them before its own result is delivered.  Cancelling a request
//! or task cancels all of its tasks (their tokens are
//! [children](crate::CancelToken::child_token) of its token).
//!
//! `task.spawn` works inside coroutine requests
//! ([`AsyncIsle::coroutine_eval`](crate::AsyncIsle::coroutine_eval) /
//! [`coroutine_call`](crate::AsyncIsle::coroutine_call)), inside tasks,
//! and inside [`run_root`](crate::run_root).  Sync requests (`eval` /
//! `call` / `exec`) cannot await, so `task.spawn` raises an error there.
//!
//! A task that runs a CPU loop never yields on its own, so a sibling on
//! the same thread cannot run to cancel it; enable
//! [`CancelConfig::preempt_every`](crate::hooks::CancelConfig::preempt_every)
//! for that.  Cancelling from another thread (an [`AsyncTask`](crate::AsyncTask)
//! handle) works without it.

use crate::scope::{self, FinishOnDrop, Outcome, TaskState, WRAP_PCALL};
use mlua::{Function, Lua, MultiValue, Table, Value, Variadic};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

const LIB: &str = r#"
local spawn_raw, join_raw, cancel_raw, done_raw, release_raw, CANCELLED = ...

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

function task.spawn(f, ...)
  return setmetatable({ _id = spawn_raw(f, ...) }, Task)
end

return task
"#;

#[derive(Default)]
struct Registry {
    tasks: RefCell<HashMap<u64, Rc<TaskState>>>,
    next_id: Cell<u64>,
}

impl Registry {
    fn get(&self, id: u64) -> mlua::Result<Rc<TaskState>> {
        self.tasks
            .borrow()
            .get(&id)
            .cloned()
            .ok_or_else(|| mlua::Error::runtime("unknown task"))
    }
}

/// Create the task library for `lua` and return its table.
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use mlua_isle::{tasks, AsyncIsle};
///
/// let (isle, driver) = AsyncIsle::spawn(|lua| {
///     let task = tasks::install(lua)?;
///     lua.globals().set("task", task)
/// })
/// .await?;
/// let r = isle
///     .coroutine_eval(
///         "local h = task.spawn(function(a, b) return a + b end, 1, 2)
///          local ok, sum = h:join()
///          return sum",
///     )
///     .await?;
/// assert_eq!(r, "3");
/// driver.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub fn install(lua: &Lua) -> mlua::Result<Table> {
    let reg = Rc::new(Registry::default());
    let cancelled_marker = lua.create_table()?;
    cancelled_marker.set_metatable(Some(lua.create_table_from([(
        "__tostring",
        lua.create_function(|_, _: Value| Ok("task.CANCELLED"))?,
    )])?))?;

    let r = reg.clone();
    let spawn_raw = lua.create_function(move |lua, (f, args): (Function, Variadic<Value>)| {
        let scope = scope::current_scope().ok_or_else(|| {
            mlua::Error::runtime(
                "task.spawn: not inside a coroutine request or task (sync requests cannot spawn)",
            )
        })?;
        let parent = crate::hook::current_token()
            .ok_or_else(|| mlua::Error::runtime("task.spawn: no current cancel token"))?;
        let token = parent.child_token();
        let state = Rc::new(TaskState::new(token.clone()));
        let run = scope::scoped_call(
            lua,
            token.clone(),
            WRAP_PCALL,
            f,
            MultiValue::from_iter(args),
        )?;
        let grace = crate::hooks::config(lua).grace;
        let st = state.clone();
        let handle = tokio::task::spawn_local(async move {
            let finish = FinishOnDrop(st.clone());
            let out = scope::with_grace(&token, grace, run).await;
            let outcome = match out {
                None => Outcome::Cancelled,
                Some(Ok(values)) => {
                    let failed = matches!(values.front(), Some(Value::Boolean(false)));
                    if failed && token.is_cancelled() {
                        Outcome::Cancelled
                    } else {
                        Outcome::Values(values)
                    }
                }
                Some(Err(e)) if token.is_cancelled() => {
                    drop(e);
                    Outcome::Cancelled
                }
                Some(Err(e)) => Outcome::Values(MultiValue::from_vec(vec![
                    Value::Boolean(false),
                    scope::error_value(e),
                ])),
            };
            st.finish(outcome);
            drop(finish);
        });
        *state.abort.borrow_mut() = Some(handle.abort_handle());
        scope.add(state.clone());

        let id = r.next_id.get();
        r.next_id.set(id + 1);
        r.tasks.borrow_mut().insert(id, state);
        Ok(id)
    })?;

    let r = reg.clone();
    let marker = cancelled_marker.clone();
    let join_raw = lua.create_async_function(move |_, id: u64| {
        let r = r.clone();
        let marker = marker.clone();
        async move {
            let state = r.get(id)?;
            state.wait_done().await;
            r.tasks.borrow_mut().remove(&id);
            Ok(match state.take_outcome() {
                Some(Outcome::Values(values)) => values,
                Some(Outcome::Cancelled) | None => {
                    MultiValue::from_vec(vec![Value::Boolean(false), Value::Table(marker)])
                }
            })
        }
    })?;

    let r = reg.clone();
    let cancel_raw = lua.create_function(move |_, id: u64| {
        r.get(id)?.token.cancel();
        Ok(())
    })?;

    let r = reg.clone();
    let done_raw = lua.create_function(move |_, id: u64| Ok(r.get(id)?.is_done()))?;

    let r = reg;
    let release_raw = lua.create_function(move |_, id: u64| {
        if let Some(state) = r.tasks.borrow_mut().remove(&id) {
            state.token.cancel();
        }
        Ok(())
    })?;

    lua.load(LIB).set_name("=mlua_isle.tasks").call((
        spawn_raw,
        join_raw,
        cancel_raw,
        done_raw,
        release_raw,
        cancelled_marker,
    ))
}
