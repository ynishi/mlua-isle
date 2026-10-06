//! `task.after` timers and the core of `task.select` /
//! `task.select_raw`: arms that consume only when they are ready, the
//! future that polls them, and the cancellable waits of `send` and
//! `select`.
//!
//! Crate-internal; documented in the [`runtime`](crate::runtime)
//! module docs ("Channels, timers and select").

use crate::chan::{self, Chan, ChanUd, Closed, Mark, Receiving, SelectMark, Sending};
use crate::scope::Spawned;
use crate::task_lib::{Registry, TaskUd};
use mlua::{AnyUserData, Function, Lua, MultiValue, Table, Value};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
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
    /// Receive from a channel (local or host).
    Recv(Receiving),
    /// Send a value into a local channel.
    Send(Sending),
    /// Wait for a timer.  The `Sleep` is created on the first poll that
    /// finds the timer not ready yet.
    Timer(Timer, Option<Pin<Box<Sleep>>>),
    /// Wait for a `task.spawn` task to finish.
    Task(TaskArm),
}

/// The case of a `task.spawn` handle (`h:on(f)` / `h:arm()`).
pub(crate) struct TaskArm {
    id: u64,
    reg: Rc<Registry>,
    task: Spawned<MultiValue>,
    /// The Lua handle, whose `_joined` the case sets.
    handle: Table,
    /// The wait for the finish, kept across polls: a `Notified` that is
    /// dropped is no longer registered, so it would miss the
    /// `notify_waiters` of the finish.
    wait: Option<Pin<Box<dyn Future<Output = ()>>>>,
    /// What the case took from the task's result slot, for `untake`.
    taken: Option<Option<MultiValue>>,
}

fn joined(handle: &Table) -> mlua::Result<bool> {
    Ok(matches!(
        handle.raw_get::<Value>("_joined")?,
        Value::Boolean(true)
    ))
}

impl TaskArm {
    fn poll_take(&mut self, cx: &mut Context<'_>) -> Poll<mlua::Result<MultiValue>> {
        if !self.task.state.is_done() {
            let state = self.task.state.clone();
            let wait = self
                .wait
                .get_or_insert_with(|| Box::pin(async move { state.wait_done().await }));
            if wait.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
        }
        self.wait = None;
        Poll::Ready(self.join())
    }

    /// Join the finished task, as `h:join()` does: mark the handle
    /// joined, forget the task and return its values.
    fn join(&mut self) -> mlua::Result<MultiValue> {
        if joined(&self.handle)? {
            return Err(mlua::Error::runtime("task already joined"));
        }
        self.handle.raw_set("_joined", true)?;
        self.reg.forget(self.id);
        let out = self.task.result.borrow_mut().take();
        self.taken = Some(out.clone());
        Ok(match out {
            Some(values) => values,
            None => self.reg.cancelled_values(),
        })
    }

    /// Undo [`join`](Self::join): the handle can be joined again.
    fn untake(&mut self) {
        if let Some(out) = self.taken.take() {
            *self.task.result.borrow_mut() = out;
            self.reg.restore(self.id, self.task.clone());
            // Best effort: the table is a plain Lua table.
            let _ = self.handle.raw_set("_joined", false);
        }
    }
}

impl Arm {
    /// If the case is ready, consume it and return its values; else
    /// register the task to be woken and return `Pending`.  Consumes
    /// only when it returns `Ready`.
    ///
    /// Values: `v, true` (a value) or `nil, false` (closed and empty)
    /// for a receive; `true` (sent) or `false` (closed) for a send; none
    /// for a timer; what `join` returns for a task.  A host value that
    /// fails to convert is `Ready(Err)` (the value is dropped).
    pub(crate) fn poll_take(
        &mut self,
        cx: &mut Context<'_>,
        lua: &Lua,
    ) -> Poll<mlua::Result<MultiValue>> {
        match self {
            Arm::Recv(recv) => recv.poll(cx, lua).map(|got| got.map(recv_values)),
            Arm::Send(send) => send.poll(cx).map(|r| Ok(send_values(r.is_ok()))),
            Arm::Timer(timer, sleep) => {
                if Instant::now() >= timer.deadline {
                    return Poll::Ready(Ok(MultiValue::new()));
                }
                let sleep =
                    sleep.get_or_insert_with(|| Box::pin(tokio::time::sleep_until(timer.deadline)));
                sleep.as_mut().poll(cx).map(|()| Ok(MultiValue::new()))
            }
            Arm::Task(task) => task.poll_take(cx),
        }
    }

    /// Whether this is a send case whose value a receiver already took
    /// (rendezvous): it is delivered and must be the case chosen.
    fn delivered(&self) -> bool {
        matches!(self, Arm::Send(send) if send.delivered())
    }

    /// Stop waiting on a case that was not chosen: withdraw its
    /// rendezvous offer or waiter (a value `try_send` left in the
    /// waiter's slot goes back to the front of the channel).
    fn withdraw(&mut self) {
        match self {
            Arm::Recv(recv) => recv.withdraw(),
            Arm::Send(send) => send.withdraw(),
            Arm::Timer(..) => {}
            Arm::Task(task) => task.wait = None,
        }
    }

    /// Give back what [`poll_take`](Self::poll_take) took, when it could
    /// not be handed over: a received value goes back to the front of
    /// its channel, a joined task becomes joinable again.  A sent value
    /// stays sent (it cannot be taken back); a timer consumed nothing.
    pub(crate) fn untake(&mut self, values: MultiValue) {
        match self {
            Arm::Recv(recv) => {
                let mut it = values.into_iter();
                if let (Some(v), Some(Value::Boolean(true))) = (it.next(), it.next()) {
                    chan::unrecv(recv.chan(), v);
                }
            }
            Arm::Task(task) => task.untake(),
            Arm::Send(_) | Arm::Timer(..) => {}
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

/// The values of a send case: `true` (sent) or `false` (closed).
fn send_values(sent: bool) -> MultiValue {
    MultiValue::from_vec(vec![Value::Boolean(sent)])
}

/// Polls its arms in order from `start` (wrapping around) and resolves
/// with the first one that is ready: its index and values.  The arms
/// after it are not polled, so they consume nothing.  A send case whose
/// rendezvous offer was taken comes first: its value is delivered.
///
/// When it resolves, the arms that were not chosen withdraw their
/// rendezvous offers and waiters at once (the select may run a handler
/// before it is dropped); dropping it withdraws them too.
pub(crate) struct SelectFuture {
    pub(crate) arms: Vec<Arm>,
    pub(crate) start: usize,
    /// The select's mark, carried by its rendezvous offers and waiters.
    pub(crate) mark: Mark,
    /// The VM, for converting the values of host channels.
    pub(crate) lua: Lua,
}

impl Future for SelectFuture {
    type Output = mlua::Result<(usize, MultiValue)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if let Some(out) = this.take_delivered() {
            return Poll::Ready(out);
        }
        let n = this.arms.len();
        for k in 0..n {
            let i = (this.start + k) % n;
            if let Poll::Ready(out) = this.arms[i].poll_take(cx, &this.lua) {
                this.resolve(i);
                return Poll::Ready(out.map(|values| (i, values)));
            }
        }
        Poll::Pending
    }
}

impl SelectFuture {
    /// Poll once without waiting (for `default`): `None` when no arm is
    /// ready.  A no-op waker is registered with the arms that are not
    /// ready; it is dropped at their next wake-up.  When it returns
    /// `None`, every arm has been withdrawn.
    pub(crate) fn poll_now(&mut self) -> Option<mlua::Result<(usize, MultiValue)>> {
        let mut cx = Context::from_waker(std::task::Waker::noop());
        match Pin::new(&mut *self).poll(&mut cx) {
            Poll::Ready(out) => Some(out),
            Poll::Pending => {
                self.withdraw_all();
                None
            }
        }
    }

    /// If a send case's rendezvous offer was taken, choose it.
    fn take_delivered(&mut self) -> Option<mlua::Result<(usize, MultiValue)>> {
        let i = self.arms.iter().position(Arm::delivered)?;
        self.resolve(i);
        Some(Ok((i, send_values(true))))
    }

    /// `chosen` was chosen: claim the select and withdraw the others.
    fn resolve(&mut self, chosen: usize) {
        self.mark.claim();
        for (j, arm) in self.arms.iter_mut().enumerate() {
            if j != chosen {
                arm.withdraw();
            }
        }
    }

    fn withdraw_all(&mut self) {
        self.mark.claim();
        for arm in &mut self.arms {
            arm.withdraw();
        }
    }
}

/// Wait for `sel` under the current token, as
/// [`cancellable`](crate::runtime::cancellable) does, except that a
/// select whose send case was delivered (its rendezvous offer taken)
/// before the cancel was seen resolves with that case: the value cannot
/// be taken back.
pub(crate) async fn wait_select(sel: &mut SelectFuture) -> mlua::Result<(usize, MultiValue)> {
    match crate::hook::current_token() {
        None => sel.await,
        Some(token) => tokio::select! {
            biased;
            _ = token.cancelled() => match sel.take_delivered() {
                Some(out) => out,
                None => Err(crate::error::cancel_error()),
            },
            out = &mut *sel => out,
        },
    }
}

/// Wait for `send` under the current token, as
/// [`cancellable`](crate::runtime::cancellable) does, except that a
/// send whose rendezvous offer a receiver took before the cancel was
/// seen returns as sent (#19, Open 5): a completed send returns
/// normally.
pub(crate) async fn wait_send(send: &mut Sending) -> mlua::Result<Result<(), Closed>> {
    match crate::hook::current_token() {
        None => Ok(std::future::poll_fn(|cx| send.poll(cx)).await),
        Some(token) => tokio::select! {
            biased;
            _ = token.cancelled() => {
                if send.delivered() {
                    Ok(Ok(()))
                } else {
                    Err(crate::error::cancel_error())
                }
            }
            out = std::future::poll_fn(|cx| send.poll(cx)) => Ok(out),
        },
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
    /// The select's mark, carried by the arms' rendezvous offers and
    /// waiters.
    pub(crate) mark: Mark,
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
    let mark = SelectMark::new();
    let mut arms = Vec::new();
    let mut fns = Vec::new();
    for (k, case) in cases.sequence_values::<Value>().enumerate() {
        let n = k + 1;
        let Value::Table(case) = case? else {
            return Err(mlua::Error::runtime(format!(
                "{name}: case {n} is not a case (build cases with ch:on(f) / ch:on_send(v, f) / t:on(f) / h:on(f), or ch:arm_recv() / ch:arm_send(v) / t:arm() / h:arm() for select_raw)"
            )));
        };
        let kind: String = case.get("kind")?;
        let target: Value = case.get("target")?;
        let arm = match (kind.as_str(), target) {
            ("recv", Value::UserData(ud)) if ud.is::<ChanUd>() => {
                Arm::Recv(Receiving::new(chan_of(&ud)?, Some(mark.clone())))
            }
            ("send", Value::UserData(ud)) if ud.is::<ChanUd>() => {
                let chan = chan_of(&ud)?;
                if let Some(why) = chan::receive_only(&chan) {
                    return Err(mlua::Error::runtime(format!(
                        "{name}: case {n}: channel is receive-only ({why})"
                    )));
                }
                let v: Value = case.get("value")?;
                Arm::Send(Sending::new(chan, v, Some(mark.clone())))
            }
            ("timer", Value::UserData(ud)) if ud.is::<TimerUd>() => {
                Arm::Timer(ud.borrow::<TimerUd>()?.0, None)
            }
            ("task", Value::UserData(ud)) if ud.is::<TaskUd>() => {
                let (id, reg) = {
                    let t = ud.borrow::<TaskUd>()?;
                    (t.id, t.reg.clone())
                };
                let handle: Table = case.get("handle")?;
                if joined(&handle)? {
                    return Err(mlua::Error::runtime(format!(
                        "{name}: case {n}: task already joined"
                    )));
                }
                let task = reg.get(id)?;
                Arm::Task(TaskArm {
                    id,
                    reg,
                    task,
                    handle,
                    wait: None,
                    taken: None,
                })
            }
            ("recv" | "send" | "timer" | "task", _) => {
                return Err(mlua::Error::runtime(format!(
                    "{name}: case {n}: target is not a {}",
                    match kind.as_str() {
                        "timer" => "timer",
                        "task" => "task",
                        _ => "channel",
                    }
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
                        "{name}: case {n} has no handler (build it with ch:on(f) / ch:on_send(v, f) / t:on(f) / h:on(f))"
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
        mark,
        handlers: fns,
        default,
        start,
    })
}

/// The channel behind a `ChanUd` userdata.
pub(crate) fn chan_of(ud: &AnyUserData) -> mlua::Result<Chan> {
    Ok(ud.borrow::<ChanUd>()?.0.clone())
}
