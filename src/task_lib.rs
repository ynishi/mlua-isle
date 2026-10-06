//! The Lua `task` library: its Lua part and the host functions under
//! it.  Crate-internal; documented in the [`runtime`](crate::runtime)
//! module docs ("The `task` library").

use crate::chan::{self, ChanUd};
use crate::scope::{self, cancellable, Spawned, Wrap};
use crate::select::{self, Cases, DefaultCase, SelectFuture, Timer, TimerUd};
use mlua::{AnyUserData, Function, Lua, MultiValue, Table, Value, Variadic};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

const LIB: &str = r#"
local spawn_raw, join_raw, cancel_raw, done_raw, release_raw, is_cancel_error, CANCELLED, raw = ...
local rawequal, setmetatable, type, error = rawequal, setmetatable, type, error

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

-- Channels, timers and select.  The objects hold the host userdata
-- (`_c`, `_t`); `on` / `arm_recv` / `arm` build plain case tables
-- `{ kind, target, handler }` that the select host functions read.
local chan_raw, send_raw, try_send_raw = raw.chan, raw.send, raw.try_send
local recv_raw, try_recv_raw, close_raw = raw.recv, raw.try_recv, raw.close
local closed_raw, len_raw, cap_raw = raw.closed, raw.len, raw.cap
local after_raw, wait_raw = raw.after, raw.wait
local select_handlers, select_raw = raw.select, raw.select_raw

local function check_handler(f)
  if type(f) ~= "function" then error("handler must be a function", 3) end
end

local Channel = {}
Channel.__index = Channel

function Channel:send(v) return send_raw(self._c, v) end
function Channel:try_send(v) return try_send_raw(self._c, v) end
function Channel:recv() return recv_raw(self._c) end
function Channel:try_recv() return try_recv_raw(self._c) end
function Channel:close() close_raw(self._c) end
function Channel:closed() return closed_raw(self._c) end
function Channel:len() return len_raw(self._c) end
function Channel:cap() return cap_raw(self._c) end

function Channel:on(f)
  check_handler(f)
  return { kind = "recv", target = self._c, handler = f }
end

function Channel:arm_recv()
  return { kind = "recv", target = self._c }
end

local Timer = {}
Timer.__index = Timer

function Timer:wait() return wait_raw(self._t) end

function Timer:on(f)
  check_handler(f)
  return { kind = "timer", target = self._t, handler = f }
end

function Timer:arm()
  return { kind = "timer", target = self._t }
end

function task.channel(cap)
  return setmetatable({ _c = chan_raw(cap) }, Channel)
end

function task.after(ms)
  return setmetatable({ _t = after_raw(ms) }, Timer)
end

-- The host function calls the handler under `pcall` and returns
-- `ok, ...`; re-raise here so that the raw error value survives.
local function finish(ok, ...)
  if ok then return ... end
  error((...), 0)
end

function task.select(cases, opts)
  return finish(select_handlers(cases, opts))
end

task.select_raw = select_raw

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
        channel_parts(lua)?,
    ))
}

/// Calls a select handler as `pcall(f, ...)`.  A tail call: once
/// `pcall` returns, no instruction of this function runs, so an error
/// that escapes it was raised before the handler was entered.
const CALL_HANDLER: &str = "local pcall = pcall return function(f, ...) return pcall(f, ...) end";

fn closed_error(method: &str) -> mlua::Error {
    mlua::Error::runtime(format!("{method}: channel is closed"))
}

/// The host functions under channels, timers and select, as the table
/// `raw` that the library's Lua part reads.
fn channel_parts(lua: &Lua) -> mlua::Result<Table> {
    let raw = lua.create_table()?;

    raw.set(
        "chan",
        lua.create_function(|_, cap: Value| {
            let cap = match cap {
                Value::Integer(n) => Some(n),
                Value::Number(n) if n.fract() == 0.0 => Some(n as i64),
                _ => None,
            };
            match cap {
                Some(0) => Err(mlua::Error::runtime(
                    "task.channel: cap = 0 (rendezvous) is not supported yet",
                )),
                Some(n) if n >= 1 => Ok(ChanUd(chan::new(n as usize))),
                _ => Err(mlua::Error::runtime(
                    "task.channel: cap must be an integer >= 1",
                )),
            }
        })?,
    )?;

    raw.set(
        "send",
        lua.create_async_function(|_, (ud, v): (AnyUserData, Value)| {
            let chan = select::chan_of(&ud);
            async move {
                let chan = chan?;
                let mut slot = Some(v);
                cancellable(async {
                    std::future::poll_fn(|cx| chan::poll_send(&chan, cx, &mut slot))
                        .await
                        .map_err(|_| closed_error("ch:send"))
                })
                .await
            }
        })?,
    )?;

    raw.set(
        "try_send",
        lua.create_function(|_, (ud, v): (AnyUserData, Value)| {
            chan::try_send(&select::chan_of(&ud)?, v).map_err(|_| closed_error("ch:try_send"))
        })?,
    )?;

    raw.set(
        "recv",
        lua.create_async_function(|_, ud: AnyUserData| {
            let chan = select::chan_of(&ud);
            async move {
                let chan = chan?;
                let got = cancellable(async {
                    Ok(std::future::poll_fn(|cx| chan::poll_recv(&chan, cx)).await)
                })
                .await?;
                Ok(select::recv_values(got))
            }
        })?,
    )?;

    raw.set(
        "try_recv",
        lua.create_function(|_, ud: AnyUserData| {
            Ok(match chan::try_recv(&select::chan_of(&ud)?) {
                chan::TryRecv::Value(v) => (v, true, true),
                chan::TryRecv::Closed => (Value::Nil, false, true),
                chan::TryRecv::Empty => (Value::Nil, false, false),
            })
        })?,
    )?;

    raw.set(
        "close",
        lua.create_function(|_, ud: AnyUserData| {
            chan::close(&select::chan_of(&ud)?);
            Ok(())
        })?,
    )?;
    raw.set(
        "closed",
        lua.create_function(|_, ud: AnyUserData| Ok(chan::is_closed(&select::chan_of(&ud)?)))?,
    )?;
    raw.set(
        "len",
        lua.create_function(|_, ud: AnyUserData| Ok(chan::len(&select::chan_of(&ud)?)))?,
    )?;
    raw.set(
        "cap",
        lua.create_function(|_, ud: AnyUserData| Ok(chan::cap(&select::chan_of(&ud)?)))?,
    )?;

    raw.set(
        "after",
        lua.create_function(|_, ms: f64| Ok(TimerUd(Timer::after(ms)?)))?,
    )?;

    raw.set(
        "wait",
        lua.create_async_function(|_, ud: AnyUserData| {
            let timer = ud.borrow::<TimerUd>().map(|t| t.0);
            async move {
                let timer = timer?;
                cancellable(async move {
                    tokio::time::sleep_until(timer.deadline).await;
                    Ok(())
                })
                .await
            }
        })?,
    )?;

    raw.set(
        "select_raw",
        lua.create_async_function(|lua, (cases, opts): (Table, Option<Table>)| {
            let cases = select::read_cases(&lua, "task.select_raw", cases, opts, false);
            async move {
                let Cases {
                    arms,
                    default,
                    start,
                    ..
                } = cases?;
                let mut sel = SelectFuture { arms, start };
                let (i, mut values) = match default {
                    DefaultCase::None => cancellable(&mut sel).await?,
                    _ => match sel.poll_now() {
                        Some(out) => out?,
                        None => return Ok(MultiValue::from_vec(vec![Value::Integer(0)])),
                    },
                };
                values.push_front(Value::Integer(i as i64 + 1));
                Ok(values)
            }
        })?,
    )?;

    let call_handler: Function = lua
        .load(CALL_HANDLER)
        .set_name("=mlua_isle.select")
        .eval()?;
    raw.set(
        "select",
        lua.create_async_function(move |lua, (cases, opts): (Table, Option<Table>)| {
            let cases = select::read_cases(&lua, "task.select", cases, opts, true);
            let call = call_handler.clone();
            async move {
                let Cases {
                    arms,
                    handlers,
                    default,
                    start,
                } = cases?;
                let mut sel = SelectFuture { arms, start };
                let (i, values) = match default {
                    DefaultCase::Handler(f) => match sel.poll_now() {
                        Some(out) => out?,
                        None => return call.call_async::<MultiValue>(f).await,
                    },
                    _ => cancellable(&mut sel).await?,
                };
                // Call the handler in this same poll: no Lua code runs
                // between the take and the handler, so a cancel arrives
                // either before the take (the wait above returned it)
                // or inside the handler.
                let mut args = values.clone();
                args.push_front(Value::Function(handlers[i].clone()));
                match call.call_async::<MultiValue>(args).await {
                    Ok(out) => Ok(out),
                    Err(e) => {
                        // Raised before `pcall` entered the handler (the
                        // cancel hook, say): the handler never had the
                        // value, so give it back.
                        sel.arms[i].untake(values);
                        Err(e)
                    }
                }
            }
        })?,
    )?;

    Ok(raw)
}
