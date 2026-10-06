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

use mlua::Value;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

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
    }))
}

/// Take the value at the front: `Ready(Some(v))`, or `Ready(None)` when
/// the channel is closed and empty.  Otherwise registers the task and
/// returns `Pending`.  Consumes only when it returns `Ready(Some)`.
pub(crate) fn poll_recv(chan: &Chan, cx: &mut Context<'_>) -> Poll<Option<Value>> {
    let mut c = chan.borrow_mut();
    if let Some(v) = c.buf.pop_front() {
        let wakers = std::mem::take(&mut c.send_wakers);
        drop(c);
        wake_all(wakers);
        return Poll::Ready(Some(v));
    }
    if c.closed {
        return Poll::Ready(None);
    }
    register(&mut c.recv_wakers, cx.waker());
    Poll::Pending
}

/// Take the value at the front without waiting.
pub(crate) fn try_recv(chan: &Chan) -> TryRecv {
    let mut c = chan.borrow_mut();
    if let Some(v) = c.buf.pop_front() {
        let wakers = std::mem::take(&mut c.send_wakers);
        drop(c);
        wake_all(wakers);
        return TryRecv::Value(v);
    }
    if c.closed {
        TryRecv::Closed
    } else {
        TryRecv::Empty
    }
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
pub(crate) fn close(chan: &Chan) {
    let mut c = chan.borrow_mut();
    if c.closed {
        return;
    }
    c.closed = true;
    let mut wakers = std::mem::take(&mut c.recv_wakers);
    wakers.append(&mut c.send_wakers);
    drop(c);
    wake_all(wakers);
}

pub(crate) fn is_closed(chan: &Chan) -> bool {
    chan.borrow().closed
}

pub(crate) fn len(chan: &Chan) -> usize {
    chan.borrow().buf.len()
}

pub(crate) fn cap(chan: &Chan) -> usize {
    chan.borrow().cap
}

/// The Lua side of a channel: the userdata that the `task` library's
/// `Channel` objects hold (`ch._c`) and that receive cases name as
/// their target.  It has no methods of its own.
pub(crate) struct ChanUd(pub(crate) Chan);

impl mlua::UserData for ChanUd {}
