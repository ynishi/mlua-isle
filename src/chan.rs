//! Local channels of the `task` library (`task.channel`): a bounded
//! FIFO of Lua values shared by the tasks of one VM.
//!
//! Crate-internal; documented in the [`runtime`](crate::runtime)
//! module docs ("Channels, timers and select").
//!
//! Everything runs on the VM's thread, so the state is an
//! `Rc<RefCell<ChanCore>>`.  Waiting receivers and senders are kept as
//! [`Waker`]s, not behind a `tokio::sync::Notify`: a poll checks the
//! buffer and registers its waker under one borrow, so a value pushed
//! between the check and the registration cannot be missed, and a
//! value is taken in the same poll that sees it (no other receiver can
//! take it between a wake-up and the take).
//!
//! A **host channel** (`runtime::channel`) is the same `ChanCore` with
//! a [`HostSource`]: the receiving end of a `tokio::sync::mpsc`
//! channel that `Send` host code feeds.  A receive takes from `buf`
//! first (where [`unrecv`] puts values back) and then from the source,
//! so the receive arms of `select`, `Arm::untake` and the Lua `Channel`
//! object work on it unchanged.

use mlua::{IntoLua, Lua, Value};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use tokio::sync::mpsc;

/// A local channel: the state behind a `task.channel` and the arms
/// that receive from it.
pub(crate) type Chan = Rc<RefCell<ChanCore>>;

/// The state of a local channel.
pub(crate) struct ChanCore {
    buf: VecDeque<Value>,
    cap: usize,
    closed: bool,
    /// Tasks waiting for a value (or the close), in arrival order.
    recv_wakers: Vec<Waker>,
    /// Tasks waiting for room (or the close), in arrival order.
    send_wakers: Vec<Waker>,
    /// The host end that feeds a host channel; `None` for a local
    /// channel.  An `Rc` so that a receive can poll it (and convert the
    /// value, which may run Lua code: an allocation can run a `__gc`)
    /// without holding the borrow of the core.
    source: Option<Rc<dyn HostSource>>,
}

/// The values of a host channel: the receiving end of the host's
/// `tokio::sync::mpsc` channel, converting each value to a Lua value
/// when it is taken (on the VM thread).
pub(crate) trait HostSource {
    /// Take the next value: `Ready(Ok(Some(v)))`, `Ready(Ok(None))` when
    /// the channel is closed and drained, `Ready(Err(e))` when the value
    /// failed to convert (the value is dropped).  Otherwise registers
    /// the task and returns `Pending`.
    fn poll_take(&self, cx: &mut Context<'_>, lua: &Lua) -> Poll<mlua::Result<Option<Value>>>;
    /// Take the next value without waiting.
    fn try_take(&self, lua: &Lua) -> mlua::Result<TryRecv>;
    /// Close the channel: the host's sends fail from now on; queued
    /// values can still be taken.
    fn close(&self);
    /// Whether the channel is closed (by `close` or because every host
    /// sender was dropped).
    fn is_closed(&self) -> bool;
    /// How many values are queued.
    fn len(&self) -> usize;
    /// The capacity the channel was created with.
    fn cap(&self) -> usize;
}

/// Wakes every task that waits on a host channel.
///
/// A tokio `Receiver` keeps a single receiver waker (an `AtomicWaker`,
/// tokio `src/sync/mpsc/chan.rs`): each `poll_recv` replaces the
/// waker of the previous one.  With several Lua receivers on one host
/// channel, only the one that polled last would be woken, and if that
/// one did not take the value (its `select` chose another case, or it
/// was cancelled), the others would sleep with a value queued.  The
/// receiver is therefore always polled with this waker, which wakes
/// every task registered since the last wake-up; each re-polls, one
/// takes the value and the rest register again.  `Send + Sync`: the
/// host's senders wake it from other threads.
#[derive(Default)]
struct FanOut {
    wakers: Mutex<Vec<Waker>>,
}

impl FanOut {
    fn register(&self, waker: &Waker) {
        let mut list = self.wakers.lock().unwrap_or_else(PoisonError::into_inner);
        register(&mut list, waker);
    }
}

impl Wake for FanOut {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let wakers =
            std::mem::take(&mut *self.wakers.lock().unwrap_or_else(PoisonError::into_inner));
        wake_all(wakers);
    }
}

/// The [`HostSource`] over a `tokio::sync::mpsc::Receiver<T>`.
struct HostRx<T> {
    rx: RefCell<mpsc::Receiver<T>>,
    fan: Arc<FanOut>,
    /// `fan` as a [`Waker`], the one waker the receiver is polled with.
    waker: Waker,
}

impl<T: IntoLua + 'static> HostSource for HostRx<T> {
    fn poll_take(&self, cx: &mut Context<'_>, lua: &Lua) -> Poll<mlua::Result<Option<Value>>> {
        // Register before polling, so that a value sent between the
        // poll and the registration still wakes this task.
        self.fan.register(cx.waker());
        let got = self
            .rx
            .borrow_mut()
            .poll_recv(&mut Context::from_waker(&self.waker));
        match got {
            Poll::Ready(Some(v)) => Poll::Ready(v.into_lua(lua).map(Some)),
            Poll::Ready(None) => Poll::Ready(Ok(None)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn try_take(&self, lua: &Lua) -> mlua::Result<TryRecv> {
        let got = self.rx.borrow_mut().try_recv();
        match got {
            Ok(v) => Ok(TryRecv::Value(v.into_lua(lua)?)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(TryRecv::Empty),
            Err(mpsc::error::TryRecvError::Disconnected) => Ok(TryRecv::Closed),
        }
    }

    fn close(&self) {
        self.rx.borrow_mut().close();
    }

    fn is_closed(&self) -> bool {
        self.rx.borrow().is_closed()
    }

    fn len(&self) -> usize {
        self.rx.borrow().len()
    }

    fn cap(&self) -> usize {
        self.rx.borrow().max_capacity()
    }
}

/// The channel was closed.
#[derive(Debug)]
pub(crate) struct Closed;

/// What a receive that does not wait found.
pub(crate) enum TryRecv {
    /// The value at the front, now taken.
    Value(Value),
    /// The channel is closed and empty.
    Closed,
    /// The channel is empty and open.
    Empty,
}

/// Register `waker` unless an equivalent one is already in `list`.
///
/// A waker stays in the list until the next wake-up, even if the task
/// that registered it stopped waiting; that only causes a spurious
/// wake.  Deduplication keeps a task that waits repeatedly on the same
/// channel from growing the list.
fn register(list: &mut Vec<Waker>, waker: &Waker) {
    if !list.iter().any(|w| w.will_wake(waker)) {
        list.push(waker.clone());
    }
}

fn wake_all(wakers: Vec<Waker>) {
    for w in wakers {
        w.wake();
    }
}

/// Create a channel of capacity `cap` (at least 1).
pub(crate) fn new(cap: usize) -> Chan {
    debug_assert!(cap >= 1);
    Rc::new(RefCell::new(ChanCore {
        buf: VecDeque::new(),
        cap,
        closed: false,
        recv_wakers: Vec::new(),
        send_wakers: Vec::new(),
        source: None,
    }))
}

/// Create a host channel over `rx` (capacity `rx.max_capacity()`).
pub(crate) fn new_host<T: IntoLua + 'static>(rx: mpsc::Receiver<T>) -> Chan {
    let fan = Arc::new(FanOut::default());
    let cap = rx.max_capacity();
    let source = HostRx {
        rx: RefCell::new(rx),
        waker: Waker::from(fan.clone()),
        fan,
    };
    Rc::new(RefCell::new(ChanCore {
        buf: VecDeque::new(),
        cap,
        closed: false,
        recv_wakers: Vec::new(),
        send_wakers: Vec::new(),
        source: Some(Rc::new(source)),
    }))
}

/// Whether `chan` is a host channel (receive-only on the Lua side).
pub(crate) fn is_host(chan: &Chan) -> bool {
    chan.borrow().source.is_some()
}

/// Take the value at the front: `Ready(Ok(Some(v)))`, or
/// `Ready(Ok(None))` when the channel is closed and empty.  Otherwise
/// registers the task and returns `Pending`.  Consumes only when it
/// returns `Ready(Ok(Some))`, or `Ready(Err)`: a host value that failed
/// to convert is dropped and the error returned.
pub(crate) fn poll_recv(
    chan: &Chan,
    cx: &mut Context<'_>,
    lua: &Lua,
) -> Poll<mlua::Result<Option<Value>>> {
    let mut c = chan.borrow_mut();
    if let Some(v) = c.buf.pop_front() {
        let wakers = std::mem::take(&mut c.send_wakers);
        drop(c);
        wake_all(wakers);
        return Poll::Ready(Ok(Some(v)));
    }
    let Some(source) = c.source.clone() else {
        if c.closed {
            return Poll::Ready(Ok(None));
        }
        register(&mut c.recv_wakers, cx.waker());
        return Poll::Pending;
    };
    // Host channel: also registered with the core, which `unrecv` and
    // `close` wake.
    register(&mut c.recv_wakers, cx.waker());
    drop(c);
    source.poll_take(cx, lua)
}

/// Take the value at the front without waiting.  Errs when a host
/// value failed to convert (the value is dropped).
pub(crate) fn try_recv(chan: &Chan, lua: &Lua) -> mlua::Result<TryRecv> {
    let mut c = chan.borrow_mut();
    if let Some(v) = c.buf.pop_front() {
        let wakers = std::mem::take(&mut c.send_wakers);
        drop(c);
        wake_all(wakers);
        return Ok(TryRecv::Value(v));
    }
    if let Some(source) = c.source.clone() {
        drop(c);
        return source.try_take(lua);
    }
    Ok(if c.closed {
        TryRecv::Closed
    } else {
        TryRecv::Empty
    })
}

/// Put back a value that a receive took but could not hand over (see
/// [`Arm::untake`](crate::select::Arm::untake)): it goes to the front,
/// ahead of everything sent since, even past the capacity or after a
/// close.
pub(crate) fn unrecv(chan: &Chan, v: Value) {
    let mut c = chan.borrow_mut();
    c.buf.push_front(v);
    let wakers = std::mem::take(&mut c.recv_wakers);
    drop(c);
    wake_all(wakers);
}

/// Push `slot`'s value once there is room: `Ready(Ok)` when it was
/// pushed (`slot` is then `None`), `Ready(Err(Closed))` when the channel
/// is closed (the value stays in `slot`).  Otherwise registers the task
/// and returns `Pending`.
pub(crate) fn poll_send(
    chan: &Chan,
    cx: &mut Context<'_>,
    slot: &mut Option<Value>,
) -> Poll<Result<(), Closed>> {
    let mut c = chan.borrow_mut();
    if c.closed {
        return Poll::Ready(Err(Closed));
    }
    if c.buf.len() < c.cap {
        if let Some(v) = slot.take() {
            c.buf.push_back(v);
        }
        let wakers = std::mem::take(&mut c.recv_wakers);
        drop(c);
        wake_all(wakers);
        return Poll::Ready(Ok(()));
    }
    register(&mut c.send_wakers, cx.waker());
    Poll::Pending
}

/// Push `v` if there is room, without waiting.  `Ok(false)` when full.
pub(crate) fn try_send(chan: &Chan, v: Value) -> Result<bool, Closed> {
    let mut c = chan.borrow_mut();
    if c.closed {
        return Err(Closed);
    }
    if c.buf.len() >= c.cap {
        return Ok(false);
    }
    c.buf.push_back(v);
    let wakers = std::mem::take(&mut c.recv_wakers);
    drop(c);
    wake_all(wakers);
    Ok(true)
}

/// Close the channel and wake everyone waiting on it.  Idempotent.
/// For a host channel, the host's sends fail from now on.
pub(crate) fn close(chan: &Chan) {
    let mut c = chan.borrow_mut();
    if c.closed {
        return;
    }
    c.closed = true;
    if let Some(source) = &c.source {
        source.close();
    }
    let mut wakers = std::mem::take(&mut c.recv_wakers);
    wakers.append(&mut c.send_wakers);
    drop(c);
    wake_all(wakers);
}

pub(crate) fn is_closed(chan: &Chan) -> bool {
    let c = chan.borrow();
    c.closed || c.source.as_ref().is_some_and(|s| s.is_closed())
}

/// Values held: the front buffer plus, for a host channel, the values
/// queued by the host.
pub(crate) fn len(chan: &Chan) -> usize {
    let c = chan.borrow();
    c.buf.len() + c.source.as_ref().map_or(0, |s| s.len())
}

pub(crate) fn cap(chan: &Chan) -> usize {
    let c = chan.borrow();
    c.source.as_ref().map_or(c.cap, |s| s.cap())
}

/// The Lua side of a channel: the userdata that the `task` library's
/// `Channel` objects hold (`ch._c`) and that receive cases name as
/// their target.  It has no methods of its own.
pub(crate) struct ChanUd(pub(crate) Chan);

impl mlua::UserData for ChanUd {}
