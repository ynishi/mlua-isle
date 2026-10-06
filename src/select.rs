//! `task.after` timers and the core of `task.select` /
//! `task.select_raw`: arms that consume only when they are ready, and
//! the future that polls them.
//!
//! Crate-internal; documented in the [`runtime`](crate::runtime)
//! module docs ("Channels, timers and select").

use crate::chan::{self, Chan, ChanUd};
use mlua::{AnyUserData, Function, Lua, MultiValue, Table, Value};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, Sleep};

/// A one-shot timer (`task.after(ms)`): ready from `deadline` on, and
/// ready for good once it is.
#[derive(Clone, Copy)]
pub(crate) struct Timer {
    pub(crate) deadline: Instant,
}

impl Timer {
    /// A timer ready `ms` milliseconds from now (`ms <= 0`: at once).
    pub(crate) fn after(ms: f64) -> mlua::Result<Timer> {
        let now = Instant::now();
        if ms.is_nan() {
            return Err(mlua::Error::runtime("task.after: ms is NaN"));
        }
        if ms <= 0.0 {
            return Ok(Timer { deadline: now });
        }
        let deadline = Duration::try_from_secs_f64(ms / 1000.0)
            .ok()
            .and_then(|d| now.checked_add(d))
            .ok_or_else(|| mlua::Error::runtime("task.after: ms is out of range"))?;
        Ok(Timer { deadline })
    }
}

/// The Lua side of a timer: the userdata that the `task` library's
/// `Timer` objects hold (`t._t`) and that timer cases name as their
/// target.  It has no methods of its own.
pub(crate) struct TimerUd(pub(crate) Timer);

impl mlua::UserData for TimerUd {}

/// One case of a select.
pub(crate) enum Arm {
    /// Receive from a local channel.
    Recv(Chan),
    /// Wait for a timer.  The `Sleep` is created on the first poll that
    /// finds the timer not ready yet.
    Timer(Timer, Option<Pin<Box<Sleep>>>),
}

impl Arm {
    /// If the case is ready, consume it and return its values; else
    /// register the task to be woken and return `Pending`.  Consumes
    /// only when it returns `Ready`.
    ///
    /// Values: `v, true` (a value) or `nil, false` (closed and empty)
    /// for a receive, none for a timer.
    pub(crate) fn poll_take(&mut self, cx: &mut Context<'_>) -> Poll<mlua::Result<MultiValue>> {
        match self {
            Arm::Recv(chan) => chan::poll_recv(chan, cx).map(|got| Ok(recv_values(got))),
            Arm::Timer(timer, sleep) => {
                if Instant::now() >= timer.deadline {
                    return Poll::Ready(Ok(MultiValue::new()));
                }
                let sleep =
                    sleep.get_or_insert_with(|| Box::pin(tokio::time::sleep_until(timer.deadline)));
                sleep.as_mut().poll(cx).map(|()| Ok(MultiValue::new()))
            }
        }
    }

    /// Give back what [`poll_take`](Self::poll_take) took, when it could
    /// not be handed over: a received value goes back to the front of
    /// its channel.  A timer consumed nothing.
    pub(crate) fn untake(&self, values: MultiValue) {
        if let Arm::Recv(chan) = self {
            let mut it = values.into_iter();
            if let (Some(v), Some(Value::Boolean(true))) = (it.next(), it.next()) {
                chan::unrecv(chan, v);
            }
        }
    }
}

/// The values of a receive: `v, true`, or `nil, false` when the channel
/// is closed and empty.
pub(crate) fn recv_values(got: Option<Value>) -> MultiValue {
    match got {
        Some(v) => MultiValue::from_vec(vec![v, Value::Boolean(true)]),
        None => MultiValue::from_vec(vec![Value::Nil, Value::Boolean(false)]),
    }
}

/// Polls its arms in order from `start` (wrapping around) and resolves
/// with the first one that is ready: its index and values.  The arms
/// after it are not polled, so they consume nothing.
pub(crate) struct SelectFuture {
    pub(crate) arms: Vec<Arm>,
    pub(crate) start: usize,
}

impl Future for SelectFuture {
    type Output = mlua::Result<(usize, MultiValue)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let n = self.arms.len();
        let start = self.start;
        for k in 0..n {
            let i = (start + k) % n;
            if let Poll::Ready(out) = self.arms[i].poll_take(cx) {
                return Poll::Ready(out.map(|values| (i, values)));
            }
        }
        Poll::Pending
    }
}

impl SelectFuture {
    /// Poll once without waiting (for `default`): `None` when no arm is
    /// ready.  A no-op waker is registered with the arms that are not
    /// ready; it is dropped at their next wake-up.
    pub(crate) fn poll_now(&mut self) -> Option<mlua::Result<(usize, MultiValue)>> {
        let mut cx = Context::from_waker(std::task::Waker::noop());
        match Pin::new(self).poll(&mut cx) {
            Poll::Ready(out) => Some(out),
            Poll::Pending => None,
        }
    }
}

/// The `default` of a select's options.
pub(crate) enum DefaultCase {
    /// No default: wait for a case.
    None,
    /// `select_raw` with `default = true`: return index 0.
    Raw,
    /// `select` with `default = f`: call `f`.
    Handler(Function),
}

/// A select's cases and options, read from the plain tables that the
/// `task` library's builders produce (`{ kind, target, handler }`).
pub(crate) struct Cases {
    pub(crate) arms: Vec<Arm>,
    /// One per arm (handler form only; empty for the raw form).
    pub(crate) handlers: Vec<Function>,
    pub(crate) default: DefaultCase,
    pub(crate) start: usize,
}

/// Read `cases` and `opts` for `task.select` (`handlers = true`, every
/// case needs a handler and `default` is a function) or
/// `task.select_raw` (`default` is a boolean).  Takes the round-robin
/// start position from the VM unless `opts.biased` is true.
pub(crate) fn read_cases(
    lua: &Lua,
    name: &str,
    cases: Table,
    opts: Option<Table>,
    handlers: bool,
) -> mlua::Result<Cases> {
    let mut arms = Vec::new();
    let mut fns = Vec::new();
    for (k, case) in cases.sequence_values::<Value>().enumerate() {
        let n = k + 1;
        let Value::Table(case) = case? else {
            return Err(mlua::Error::runtime(format!(
                "{name}: case {n} is not a case (build cases with ch:on(f) / t:on(f), or ch:arm_recv() / t:arm() for select_raw)"
            )));
        };
        let kind: String = case.get("kind")?;
        let target: Value = case.get("target")?;
        let arm = match (kind.as_str(), target) {
            ("recv", Value::UserData(ud)) if ud.is::<ChanUd>() => Arm::Recv(chan_of(&ud)?),
            ("timer", Value::UserData(ud)) if ud.is::<TimerUd>() => {
                Arm::Timer(ud.borrow::<TimerUd>()?.0, None)
            }
            ("recv" | "timer", _) => {
                return Err(mlua::Error::runtime(format!(
                    "{name}: case {n}: target is not a {}",
                    if kind == "recv" { "channel" } else { "timer" }
                )))
            }
            _ => {
                return Err(mlua::Error::runtime(format!(
                    "{name}: case {n}: unknown kind '{kind}'"
                )))
            }
        };
        arms.push(arm);
        if handlers {
            match case.get::<Value>("handler")? {
                Value::Function(f) => fns.push(f),
                _ => {
                    return Err(mlua::Error::runtime(format!(
                        "{name}: case {n} has no handler (build it with ch:on(f) / t:on(f))"
                    )))
                }
            }
        }
    }

    let (biased, default) = match opts {
        None => (false, DefaultCase::None),
        Some(opts) => {
            let biased = match opts.get::<Value>("biased")? {
                Value::Nil => false,
                Value::Boolean(b) => b,
                _ => {
                    return Err(mlua::Error::runtime(format!(
                        "{name}: opts.biased must be a boolean"
                    )))
                }
            };
            let default = match (opts.get::<Value>("default")?, handlers) {
                (Value::Nil, _) | (Value::Boolean(false), false) => DefaultCase::None,
                (Value::Boolean(true), false) => DefaultCase::Raw,
                (Value::Function(f), true) => DefaultCase::Handler(f),
                (_, false) => {
                    return Err(mlua::Error::runtime(format!(
                        "{name}: opts.default must be a boolean"
                    )))
                }
                (_, true) => {
                    return Err(mlua::Error::runtime(format!(
                        "{name}: opts.default must be a function"
                    )))
                }
            };
            (biased, default)
        }
    };

    if arms.is_empty() && matches!(default, DefaultCase::None) {
        return Err(mlua::Error::runtime(format!(
            "{name}: no cases and no default"
        )));
    }
    let start = if biased || arms.is_empty() {
        0
    } else {
        crate::hub::next_select_turn(lua) % arms.len()
    };
    Ok(Cases {
        arms,
        handlers: fns,
        default,
        start,
    })
}

/// The channel behind a `ChanUd` userdata.
pub(crate) fn chan_of(ud: &AnyUserData) -> mlua::Result<Chan> {
    Ok(ud.borrow::<ChanUd>()?.0.clone())
}
