//! The Lua `task` library: its Lua part and the host functions under
//! it.  Crate-internal; documented in the [`runtime`](crate::runtime)
//! module docs ("The `task` library").

use crate::chan::{self, Chan, ChanUd, Receiving, Sending};
use crate::scope::{self, cancellable, ScopedTask, Spawned, Wrap};
use crate::select::{self, Cases, DefaultCase, SelectFuture, Timer, TimerUd};
use mlua::{AnyUserData, Function, Lua, MultiValue, Table, Value, Variadic};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

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
local ticker_raw, stop_raw, task_case_raw = raw.ticker, raw.stop, raw.task_case
local select_handlers, select_raw = raw.select, raw.select_raw

local function check_handler(f)
  if type(f) ~= "function" then error("handler must be a function", 3) end
end

-- The case of a task handle: what `join` would return, once the task
-- has finished.  Choosing it marks the handle joined.
local function task_case(self, f)
  if self._joined then error("task already joined", 3) end
  return { kind = "task", target = task_case_raw(self._id), handle = self, handler = f }
end

function Task:on(f)
  check_handler(f)
  return task_case(self, f)
end

function Task:arm()
  return task_case(self)
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

function Channel:on_send(v, f)
  check_handler(f)
  return { kind = "send", target = self._c, value = v, handler = f }
end

function Channel:arm_send(v)
  return { kind = "send", target = self._c, value = v }
end

-- A ticker is a `Channel` (receive-only) with `stop`.
local Ticker = setmetatable({}, { __index = Channel })
Ticker.__index = Ticker

function Ticker:stop() stop_raw(self._tk) end

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

function task.ticker(ms)
  local c, t = ticker_raw(ms)
  return setmetatable({ _c = c, _tk = t }, Ticker)
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

-- A channel to the host (`runtime::channel_to_host`) is a `Channel`
-- (send-only).
local SendChannel = setmetatable({}, { __index = Channel })
SendChannel.__index = SendChannel

local function refuse(method)
  return function()
    error(method .. ": channel is send-only (a channel to the host; the host receives)", 2)
  end
end
SendChannel.recv = refuse("ch:recv")
SendChannel.try_recv = refuse("ch:try_recv")
SendChannel.on = refuse("ch:on")
SendChannel.arm_recv = refuse("ch:arm_recv")

-- The constructor `runtime::channel` wraps a host channel with (the
-- same `Channel` object as `task.channel`), and `runtime::channel_to_host`
-- a channel to the host (`send_only = true`).
local function wrap_channel(c, send_only)
  return setmetatable({ _c = c }, send_only and SendChannel or Channel)
end

return task, wrap_channel
"#;

/// The tasks started with `task.spawn` that were not joined yet, by
/// the id their Lua handle holds.
pub(crate) struct Registry {
    tasks: RefCell<HashMap<u64, Spawned<MultiValue>>>,
    next_id: Cell<u64>,
    /// `task.CANCELLED`.
    cancelled: Table,
}

impl Registry {
    pub(crate) fn get(&self, id: u64) -> mlua::Result<Spawned<MultiValue>> {
        self.tasks
            .borrow()
            .get(&id)
            .cloned()
            .ok_or_else(|| mlua::Error::runtime("unknown task"))
    }

    /// Forget a task that was joined.
    pub(crate) fn forget(&self, id: u64) {
        self.tasks.borrow_mut().remove(&id);
    }

    /// Take back a task whose join was undone.
    pub(crate) fn restore(&self, id: u64, task: Spawned<MultiValue>) {
        self.tasks.borrow_mut().insert(id, task);
    }

    /// What `join` returns for a cancelled task: `false, task.CANCELLED`.
    pub(crate) fn cancelled_values(&self) -> MultiValue {
        MultiValue::from_vec(vec![
            Value::Boolean(false),
            Value::Table(self.cancelled.clone()),
        ])
    }
}

/// The target of a task case (`h:on(f)` / `h:arm()`): the task behind a
/// handle.
pub(crate) struct TaskUd {
    pub(crate) id: u64,
    pub(crate) reg: Rc<Registry>,
}

impl mlua::UserData for TaskUd {}

/// The host side of a ticker (`tk._tk`): the host task that feeds its
/// channel.  Dropping it (`stop`, or when the ticker is collected)
/// cancels the task.
struct TickerUd {
    task: RefCell<Option<ScopedTask<()>>>,
    chan: Chan,
}

impl mlua::UserData for TickerUd {}

/// Closes a ticker's channel when its host task ends, however it ends
/// (stopped, cancelled with its scope, or dropped at the end of the
/// grace).
struct CloseOnDrop(Chan);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        chan::close(&self.0);
    }
}

/// Create the `task` library table for `lua`, and the function that
/// wraps a channel userdata (`ChanUd`) in the library's `Channel`
/// object (for `runtime::channel`).  Called once per VM by
/// [`Vm::task_lib`](crate::runtime::Vm::task_lib), which documents the
/// library.
pub(crate) fn create(lua: &Lua) -> mlua::Result<(Table, Function)> {
    let cancelled_marker = lua.create_table()?;
    cancelled_marker.set_metatable(Some(lua.create_table_from([(
        "__tostring",
        lua.create_function(|_, _: Value| Ok("task.CANCELLED"))?,
    )])?))?;
    let reg = Rc::new(Registry {
        tasks: RefCell::new(HashMap::new()),
        next_id: Cell::new(0),
        cancelled: cancelled_marker.clone(),
    });

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
    let join_raw = lua.create_async_function(move |_, id: u64| {
        let r = r.clone();
        async move {
            let task = r.get(id)?;
            task.state.wait_done().await;
            r.forget(id);
            let out = task.result.borrow_mut().take();
            Ok(match out {
                Some(values) => values,
                None => r.cancelled_values(),
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

    let r = reg.clone();
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
        channel_parts(lua, reg)?,
    ))
}

/// Calls a select handler as `pcall(f, ...)`.  A tail call: once
/// `pcall` returns, no instruction of this function runs, so an error
/// that escapes it was raised before the handler was entered.
const CALL_HANDLER: &str = "local pcall = pcall return function(f, ...) return pcall(f, ...) end";

fn closed_error(method: &str) -> mlua::Error {
    mlua::Error::runtime(format!("{method}: channel is closed"))
}

/// The error of a send into a receive-only channel (a host channel or
/// a ticker), `Ok` for a local channel.
fn check_sendable(method: &str, chan: &Chan) -> mlua::Result<()> {
    match chan::receive_only(chan) {
        None => Ok(()),
        Some(why) => Err(mlua::Error::runtime(format!(
            "{method}: channel is receive-only ({why})"
        ))),
    }
}

/// The error of a receive from a send-only channel (a channel to the
/// host), `Ok` otherwise.
fn check_receivable(method: &str, chan: &Chan) -> mlua::Result<()> {
    match chan::send_only(chan) {
        None => Ok(()),
        Some(why) => Err(mlua::Error::runtime(format!(
            "{method}: channel is send-only ({why})"
        ))),
    }
}

/// The period of `task.ticker(ms)`.
fn ticker_period(ms: f64) -> mlua::Result<Duration> {
    if ms.is_nan() || ms <= 0.0 {
        return Err(mlua::Error::runtime("task.ticker: ms must be > 0"));
    }
    Duration::try_from_secs_f64(ms / 1000.0)
        .ok()
        .filter(|d| !d.is_zero())
        .ok_or_else(|| mlua::Error::runtime("task.ticker: ms is out of range"))
}

/// The host task of a ticker: push the time of each tick (milliseconds
/// since `start`, the tick's scheduled time) into `chan`, keeping only
/// the newest unread one, until the task's token is cancelled or the
/// channel is closed.  Closes the channel when it ends.
async fn run_ticker(chan: Chan, start: tokio::time::Instant, period: Duration) {
    let _close = CloseOnDrop(chan.clone());
    let token = crate::hook::current_token();
    let mut ticks = tokio::time::interval_at(start + period, period);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let at = match &token {
            Some(token) => tokio::select! {
                biased;
                _ = token.cancelled() => return,
                at = ticks.tick() => at,
            },
            None => ticks.tick().await,
        };
        let ms = (at - start).as_nanos() as f64 / 1e6;
        if !chan::push_newest(&chan, Value::Number(ms)) {
            return;
        }
    }
}

/// The host functions under channels, timers and select, as the table
/// `raw` that the library's Lua part reads.
fn channel_parts(lua: &Lua, reg: Rc<Registry>) -> mlua::Result<Table> {
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
                Some(n) if n >= 0 => Ok(ChanUd(chan::new(n as usize))),
                _ => Err(mlua::Error::runtime(
                    "task.channel: cap must be an integer >= 0",
                )),
            }
        })?,
    )?;

    raw.set(
        "send",
        lua.create_async_function(|lua, (ud, v): (AnyUserData, Value)| {
            let chan = select::chan_of(&ud);
            async move {
                let chan = chan?;
                check_sendable("ch:send", &chan)?;
                let mut send = Sending::new(chan, v, None);
                select::wait_send(&mut send, &lua)
                    .await?
                    .map_err(|_| closed_error("ch:send"))
            }
        })?,
    )?;

    raw.set(
        "try_send",
        lua.create_function(|lua, (ud, v): (AnyUserData, Value)| {
            let chan = select::chan_of(&ud)?;
            check_sendable("ch:try_send", &chan)?;
            chan::try_send(&chan, lua, v)?.map_err(|_| closed_error("ch:try_send"))
        })?,
    )?;

    raw.set(
        "recv",
        lua.create_async_function(|lua, ud: AnyUserData| {
            let chan = select::chan_of(&ud);
            async move {
                let chan = chan?;
                check_receivable("ch:recv", &chan)?;
                let mut recv = Receiving::new(chan, None);
                let got = cancellable(std::future::poll_fn(|cx| recv.poll(cx, &lua))).await?;
                Ok(select::recv_values(got))
            }
        })?,
    )?;

    raw.set(
        "try_recv",
        lua.create_function(|lua, ud: AnyUserData| {
            let chan = select::chan_of(&ud)?;
            check_receivable("ch:try_recv", &chan)?;
            Ok(match chan::try_recv(&chan, lua)? {
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
        "ticker",
        lua.create_function(|_, ms: f64| {
            let period = ticker_period(ms)?;
            let scope = scope::current_scope().ok_or_else(|| {
                mlua::Error::runtime(
                    "task.ticker: not inside a coroutine request or task (sync requests cannot tick)",
                )
            })?;
            let chan = chan::new_ticker();
            let start = tokio::time::Instant::now();
            let task = scope.spawn_local(run_ticker(chan.clone(), start, period));
            let ticker = TickerUd {
                task: RefCell::new(Some(task)),
                chan: chan.clone(),
            };
            Ok((ChanUd(chan), ticker))
        })?,
    )?;

    raw.set(
        "stop",
        lua.create_function(|_, ud: AnyUserData| {
            let t = ud.borrow::<TickerUd>()?;
            // Dropping the `ScopedTask` cancels the host task.
            let task = t.task.borrow_mut().take();
            drop(task);
            chan::close(&t.chan);
            Ok(())
        })?,
    )?;

    raw.set(
        "task_case",
        lua.create_function(move |_, id: u64| {
            reg.get(id)?;
            Ok(TaskUd {
                id,
                reg: reg.clone(),
            })
        })?,
    )?;

    raw.set(
        "select_raw",
        lua.create_async_function(|lua, (cases, opts): (Table, Option<Table>)| {
            let cases = select::read_cases(&lua, "task.select_raw", cases, opts, false);
            async move {
                let Cases {
                    arms,
                    mark,
                    default,
                    start,
                    ..
                } = cases?;
                let mut sel = SelectFuture {
                    arms,
                    start,
                    mark,
                    lua: lua.clone(),
                };
                let (i, mut values) = match default {
                    DefaultCase::None => select::wait_select(&mut sel).await?,
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
                    mark,
                    handlers,
                    default,
                    start,
                } = cases?;
                let mut sel = SelectFuture {
                    arms,
                    start,
                    mark,
                    lua: lua.clone(),
                };
                let (i, values) = match default {
                    DefaultCase::Handler(f) => match sel.poll_now() {
                        Some(out) => out?,
                        // `poll_now` withdrew every case: no offer of
                        // this select stays open while `default` runs.
                        None => return call.call_async::<MultiValue>(f).await,
                    },
                    _ => select::wait_select(&mut sel).await?,
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
                        // value, so give it back (a sent value stays
                        // sent).
                        sel.arms[i].untake(values);
                        Err(e)
                    }
                }
            }
        })?,
    )?;

    Ok(raw)
}
