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
//! | `task.is_cancelled(err)` | Whether `err` is a cancellation: the error a cancel raises (caught with `pcall`, or received by a `__close` handler) or `task.CANCELLED`.  `false` for any other value. |
//! | `task.CANCELLED` | What `join` returns as `err` for a cancelled task. |
//!
//! Tasks are **structured**: when a coroutine request or task finishes,
//! the tasks it spawned and did not join are cancelled, and it waits
//! for them before its own result is delivered.  Cancelling a request
//! or task cancels all of its tasks (their tokens are
//! [children](crate::CancelToken::child_token) of its token), and the
//! cancelled request or task still resolves only after they, and their
//! own tasks, have finished or been dropped.  The cancel
//! [grace period](crate::hooks::CancelConfig::grace) is one deadline for
//! the whole tree: a task spawned during cleanup gets the time that
//! remains, not a fresh grace period.
//!
//! `task.spawn` works inside coroutine requests
//! ([`AsyncIsle::coroutine_eval`](crate::AsyncIsle::coroutine_eval) /
//! [`coroutine_call`](crate::AsyncIsle::coroutine_call)), inside tasks
//! (including host tasks), and inside [`run_root`](crate::run_root) /
//! [`Vm::run`](crate::runtime::Vm::run).  Sync requests (`eval` /
//! `call` / `exec`) cannot await, so `task.spawn` raises an error there.
//!
//! # Telling a cancel from an error
//!
//! A cancel reaches Lua code as an error: the cancel hook raises it
//! while Lua code runs, and an async host function wrapped with
//! [`cancellable`](crate::cancellable) returns it while the coroutine
//! awaits.  Its value is `mlua::Error::external(`[`Cancelled`](crate::Cancelled)`)`
//! (a userdata to Lua); `task.is_cancelled(err)` is the test, and it is
//! also true for `task.CANCELLED`, so one predicate covers both:
//!
//! ```lua
//! local ok, err = pcall(sleep, 1000)
//! if not ok and task.is_cancelled(err) then
//!   -- cancelled: clean up and let the cancel continue
//!   error(err, 0)
//! end
//! ```
//!
//! The predicate lives in this library: a VM that runs without the
//! `task` table (a host that uses [`Vm::attach`](crate::runtime::Vm::attach)
//! but never sets [`Vm::task_lib`](crate::runtime::Vm::task_lib)) has no
//! `task.is_cancelled`; Rust code tests an `mlua::Error` with
//! `e.downcast_ref::<Cancelled>()`.
//!
//! A task that runs a CPU loop never yields on its own, so a sibling on
//! the same thread cannot run to cancel it; enable
//! [`CancelConfig::preempt_every`](crate::hooks::CancelConfig::preempt_every)
//! for that.  Cancelling from another thread (an [`AsyncTask`](crate::AsyncTask)
//! handle) works without it.
//!
//! # Host tasks
//!
//! Host code adds a task to the same scope with
//! [`runtime::current_scope`](crate::runtime::current_scope) and
//! [`ScopeHandle::spawn_local`](crate::runtime::ScopeHandle::spawn_local):
//! the host future is structured like a `task.spawn` task (cancelled
//! with its request or task, given the same grace deadline, dropped
//! when the grace ends, waited for), and inside it
//! [`current_token`](crate::current_token), `cancellable` and
//! `current_scope` refer to the host task itself.  Take the handle in
//! the synchronous part of the host function and move it into the
//! future.  The returned [`ScopedTask`](crate::runtime::ScopedTask) can
//! be awaited (wait for the value), kept (dropping it cancels the task
//! now), or [detached](crate::runtime::ScopedTask::detach) (the task
//! runs on without a handle and is still cancelled, dropped and waited
//! for when its scope ends).
//!
//! [`current_token()`](crate::current_token)`.child_token()` with a bare
//! `tokio::task::spawn_local` gives cancellation only: the request
//! neither waits for that task nor drops it, so a host future that does
//! not watch the token keeps running after the request resolved (next
//! to the next request on the VM, and keeping an
//! [`AsyncIsleDriver::shutdown`](crate::AsyncIsleDriver::shutdown) from
//! returning).

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
/// let r: i64 = isle
///     .coroutine_eval(
///         "local h = task.spawn(function(a, b) return a + b end, 1, 2)
///          local ok, sum = h:join()
///          return sum",
///     )
///     .await?;
/// assert_eq!(r, 3);
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
        let scope = scope::current().ok_or_else(|| {
            mlua::Error::runtime(
                "task.spawn: not inside a coroutine request or task (sync requests cannot spawn)",
            )
        })?;
        let grace = crate::hooks::config(lua).grace;
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
