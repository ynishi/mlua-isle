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
//!
//! A **rendezvous channel** (`cap == 0`) holds no values of its own.  A
//! sender posts an [`Offer`] (the value, its waker, its state and the
//! select it belongs to) on the offer queue and waits until a receiver
//! takes it; a receiver that finds no offer waits as a [`Waiter`] with an
//! empty slot, which `try_send` fills.  Offers and waiters are held by
//! the waiting future ([`Sending`], [`Receiving`]) and removed when it is
//! dropped, so a dropped `send` / `recv` / `select` leaves nothing
//! behind; a filled slot nobody took goes back to the front of `buf`
//! (where `unrecv` puts values too), so no value is lost.  A select's
//! offers and waiters carry its [`SelectMark`]: a receive never takes an
//! offer of its own select, and once one offer of a select was taken the
//! select's other offers and waiters are passed over.
//!
//! A **ticker** (`task.ticker`) is a local channel of capacity 1 that is
//! receive-only on the Lua side and fed by a host task with
//! [`push_newest`].

use mlua::{IntoLua, Lua, Value};
use std::cell::{Cell, RefCell};
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
    /// Rendezvous (`cap == 0`): the senders' offers, in arrival order.
    /// Only `Open` offers are queued; taking or withdrawing one removes
    /// it.
    offers: VecDeque<Rc<Offer>>,
    /// Rendezvous (`cap == 0`): the receivers waiting for an offer, in
    /// arrival order.
    waiters: VecDeque<Rc<Waiter>>,
    /// A ticker: receive-only on the Lua side, fed by the host.
    ticker: bool,
}

/// The select that a rendezvous offer or waiter belongs to: one per
/// select, compared by pointer.
pub(crate) struct SelectMark {
    /// Set when one of the select's offers was taken, or when the
    /// select chose a case: its other offers and waiters are then passed
    /// over.
    claimed: Cell<bool>,
}

/// A select's mark, shared by its arms.
pub(crate) type Mark = Rc<SelectMark>;

impl SelectMark {
    pub(crate) fn new() -> Mark {
        Rc::new(SelectMark {
            claimed: Cell::new(false),
        })
    }

    pub(crate) fn claim(&self) {
        self.claimed.set(true);
    }
}

fn claimed(mark: &Option<Mark>) -> bool {
    mark.as_ref().is_some_and(|m| m.claimed.get())
}

fn same_select(a: &Option<Mark>, b: &Option<Mark>) -> bool {
    matches!((a, b), (Some(a), Some(b)) if Rc::ptr_eq(a, b))
}

/// Where a rendezvous offer is in its life.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OfferState {
    /// Queued, waiting for a receiver.
    Open,
    /// A receiver took the value: the send is complete.
    Taken,
    /// Removed without being taken (its select chose another case, its
    /// sender stopped waiting, or the channel was closed).
    Withdrawn,
}

/// A sender's value posted on a rendezvous channel.
struct Offer {
    value: RefCell<Option<Value>>,
    state: Cell<OfferState>,
    waker: RefCell<Waker>,
    select: Option<Mark>,
}

/// A receiver waiting on a rendezvous channel; `try_send` fills `slot`.
struct Waiter {
    slot: RefCell<Option<Value>>,
    waker: RefCell<Waker>,
    select: Option<Mark>,
}

impl ChanCore {
    fn with_cap(cap: usize, source: Option<Rc<dyn HostSource>>) -> ChanCore {
        ChanCore {
            buf: VecDeque::new(),
            cap,
            closed: false,
            recv_wakers: Vec::new(),
            send_wakers: Vec::new(),
            source,
            offers: VecDeque::new(),
            waiters: VecDeque::new(),
            ticker: false,
        }
    }

    /// The wakers of every waiting receiver (plain wakers and
    /// rendezvous waiters), to wake once the borrow is released.
    fn receiver_wakers(&mut self) -> Vec<Waker> {
        let mut wakers = std::mem::take(&mut self.recv_wakers);
        wakers.extend(self.waiters.iter().map(|w| w.waker.borrow().clone()));
        wakers
    }

    /// Take the first open offer that `me` may pair with: not of the
    /// same select, and not of a select that already has its case.
    /// Marks it taken and claims its select; returns the value and the
    /// sender's waker.
    fn take_offer(&mut self, me: &Option<Mark>) -> Option<(Value, Waker)> {
        let pos = self.offers.iter().position(|o| {
            o.state.get() == OfferState::Open && !claimed(&o.select) && !same_select(&o.select, me)
        })?;
        let offer = self.offers.remove(pos)?;
        offer.state.set(OfferState::Taken);
        if let Some(m) = &offer.select {
            m.claim();
        }
        let v = offer.value.borrow_mut().take().unwrap_or(Value::Nil);
        let waker = offer.waker.borrow().clone();
        Some((v, waker))
    }
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

/// Create a channel of capacity `cap` (`0`: a rendezvous channel).
pub(crate) fn new(cap: usize) -> Chan {
    Rc::new(RefCell::new(ChanCore::with_cap(cap, None)))
}

/// Create the channel of a ticker: capacity 1, receive-only on the Lua
/// side, fed with [`push_newest`].
pub(crate) fn new_ticker() -> Chan {
    let mut core = ChanCore::with_cap(1, None);
    core.ticker = true;
    Rc::new(RefCell::new(core))
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
    Rc::new(RefCell::new(ChanCore::with_cap(cap, Some(Rc::new(source)))))
}

/// Why `chan` is receive-only on the Lua side (a host channel or a
/// ticker), or `None` for a local channel.
pub(crate) fn receive_only(chan: &Chan) -> Option<&'static str> {
    let c = chan.borrow();
    if c.source.is_some() {
        Some("a host channel; the host sends")
    } else if c.ticker {
        Some("a ticker")
    } else {
        None
    }
}

/// Take the value at the front of a buffered or host channel:
/// `Ready(Ok(Some(v)))`, or `Ready(Ok(None))` when the channel is closed
/// and empty.  Otherwise registers the task and returns `Pending`.
/// Consumes only when it returns `Ready(Ok(Some))`, or `Ready(Err)`: a
/// host value that failed to convert is dropped and the error returned.
fn poll_recv(chan: &Chan, cx: &mut Context<'_>, lua: &Lua) -> Poll<mlua::Result<Option<Value>>> {
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
    if c.cap == 0 {
        if let Some((v, waker)) = c.take_offer(&None) {
            drop(c);
            waker.wake();
            return Ok(TryRecv::Value(v));
        }
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
    let wakers = c.receiver_wakers();
    drop(c);
    wake_all(wakers);
}

/// Push `slot`'s value into a buffered channel once there is room:
/// `Ready(Ok)` when it was pushed (`slot` is then `None`),
/// `Ready(Err(Closed))` when the channel is closed (the value stays in
/// `slot`).  Otherwise registers the task and returns `Pending`.
fn poll_send(
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
///
/// Rendezvous: fill the slot of the first waiting receiver (a plain
/// `recv` or a select's receive case whose select has no case yet) and
/// wake it; `Ok(false)` when no receiver is waiting.  Nothing is posted.
pub(crate) fn try_send(chan: &Chan, v: Value) -> Result<bool, Closed> {
    let mut c = chan.borrow_mut();
    if c.closed {
        return Err(Closed);
    }
    if c.cap == 0 {
        let waiter = c
            .waiters
            .iter()
            .find(|w| w.slot.borrow().is_none() && !claimed(&w.select))
            .cloned();
        drop(c);
        return Ok(match waiter {
            Some(w) => {
                *w.slot.borrow_mut() = Some(v);
                let waker = w.waker.borrow().clone();
                waker.wake();
                true
            }
            None => false,
        });
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
/// For a host channel, the host's sends fail from now on.  Rendezvous:
/// the open offers are withdrawn (their senders see the close).
pub(crate) fn close(chan: &Chan) {
    let mut c = chan.borrow_mut();
    if c.closed {
        return;
    }
    c.closed = true;
    if let Some(source) = &c.source {
        source.close();
    }
    let mut wakers = c.receiver_wakers();
    wakers.append(&mut c.send_wakers);
    for offer in std::mem::take(&mut c.offers) {
        offer.state.set(OfferState::Withdrawn);
        wakers.push(offer.waker.borrow().clone());
    }
    drop(c);
    wake_all(wakers);
}

pub(crate) fn is_closed(chan: &Chan) -> bool {
    let c = chan.borrow();
    c.closed || c.source.as_ref().is_some_and(|s| s.is_closed())
}

/// Values held: the front buffer plus, for a host channel, the values
/// queued by the host.  A rendezvous channel holds nothing (its offers
/// are not counted) except a value given back to its front.
pub(crate) fn len(chan: &Chan) -> usize {
    let c = chan.borrow();
    c.buf.len() + c.source.as_ref().map_or(0, |s| s.len())
}

pub(crate) fn cap(chan: &Chan) -> usize {
    let c = chan.borrow();
    c.source.as_ref().map_or(c.cap, |s| s.cap())
}

/// Push a tick into a ticker's channel, replacing the unread one if the
/// channel is full.  `false` when the channel is closed.
pub(crate) fn push_newest(chan: &Chan, v: Value) -> bool {
    let mut c = chan.borrow_mut();
    if c.closed {
        return false;
    }
    if c.buf.len() >= c.cap {
        c.buf.pop_back();
    }
    c.buf.push_back(v);
    let wakers = c.receiver_wakers();
    drop(c);
    wake_all(wakers);
    true
}

/// A receive from a channel in progress: a `recv`, or a select's
/// receive case.  On a rendezvous channel it holds the receiver's
/// [`Waiter`] while it waits; dropping it (or [`withdraw`]) removes the
/// waiter and gives a value `try_send` left in its slot back to the
/// front of the channel.
///
/// [`withdraw`]: Receiving::withdraw
pub(crate) struct Receiving {
    chan: Chan,
    select: Option<Mark>,
    waiter: Option<Rc<Waiter>>,
}

impl Receiving {
    /// A receive from `chan`, as a case of the select `select` (`None`
    /// for a plain `recv`).
    pub(crate) fn new(chan: Chan, select: Option<Mark>) -> Receiving {
        Receiving {
            chan,
            select,
            waiter: None,
        }
    }

    pub(crate) fn chan(&self) -> &Chan {
        &self.chan
    }

    /// Take a value: as [`poll_recv`] for a buffered or host channel.
    /// Rendezvous: the value `try_send` put in this receiver's slot, else
    /// a value given back to the front, else the first open offer it may
    /// pair with (marked taken, its sender woken), else `None` when the
    /// channel is closed; otherwise waits as a waiter.
    pub(crate) fn poll(
        &mut self,
        cx: &mut Context<'_>,
        lua: &Lua,
    ) -> Poll<mlua::Result<Option<Value>>> {
        let mut c = self.chan.borrow_mut();
        if c.cap != 0 || c.source.is_some() {
            drop(c);
            return poll_recv(&self.chan, cx, lua);
        }
        if let Some(w) = &self.waiter {
            let filled = w.slot.borrow_mut().take();
            if let Some(v) = filled {
                Self::forget(&mut c, &mut self.waiter);
                return Poll::Ready(Ok(Some(v)));
            }
        }
        if let Some(v) = c.buf.pop_front() {
            Self::forget(&mut c, &mut self.waiter);
            return Poll::Ready(Ok(Some(v)));
        }
        if let Some((v, waker)) = c.take_offer(&self.select) {
            Self::forget(&mut c, &mut self.waiter);
            drop(c);
            waker.wake();
            return Poll::Ready(Ok(Some(v)));
        }
        if c.closed {
            Self::forget(&mut c, &mut self.waiter);
            return Poll::Ready(Ok(None));
        }
        match &self.waiter {
            Some(w) => {
                let mut waker = w.waker.borrow_mut();
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
            }
            None => {
                let w = Rc::new(Waiter {
                    slot: RefCell::new(None),
                    waker: RefCell::new(cx.waker().clone()),
                    select: self.select.clone(),
                });
                c.waiters.push_back(w.clone());
                self.waiter = Some(w);
            }
        }
        Poll::Pending
    }

    /// Remove `waiter` (an empty one) from the channel.
    fn forget(c: &mut ChanCore, waiter: &mut Option<Rc<Waiter>>) {
        if let Some(w) = waiter.take() {
            c.waiters.retain(|x| !Rc::ptr_eq(x, &w));
        }
    }

    /// Stop waiting: remove the waiter, and put a value `try_send` left
    /// in its slot back at the front of the channel.  Idempotent.
    pub(crate) fn withdraw(&mut self) {
        let Some(w) = self.waiter.take() else {
            return;
        };
        let mut c = self.chan.borrow_mut();
        c.waiters.retain(|x| !Rc::ptr_eq(x, &w));
        let filled = w.slot.borrow_mut().take();
        if let Some(v) = filled {
            c.buf.push_front(v);
            let wakers = c.receiver_wakers();
            drop(c);
            wake_all(wakers);
        }
    }
}

impl Drop for Receiving {
    fn drop(&mut self) {
        self.withdraw();
    }
}

/// A send into a channel in progress: a `send`, or a select's send
/// case.  On a rendezvous channel it holds the sender's [`Offer`] while
/// it waits; dropping it (or [`withdraw`]) withdraws an offer that was
/// not taken.
///
/// [`withdraw`]: Sending::withdraw
pub(crate) struct Sending {
    chan: Chan,
    /// The value, until it is pushed (buffered) or posted (rendezvous).
    value: Option<Value>,
    select: Option<Mark>,
    offer: Option<Rc<Offer>>,
}

impl Sending {
    /// A send of `v` into `chan`, as a case of the select `select`
    /// (`None` for a plain `send`).
    pub(crate) fn new(chan: Chan, v: Value, select: Option<Mark>) -> Sending {
        Sending {
            chan,
            value: Some(v),
            select,
            offer: None,
        }
    }

    /// `Ready(Ok)` once the value is sent, `Ready(Err(Closed))` when the
    /// channel is closed (nothing sent).  Buffered: as [`poll_send`].
    /// Rendezvous: the first poll posts the offer and wakes the waiting
    /// receivers; the send is complete once a receiver took the offer.
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Closed>> {
        if self.chan.borrow().cap != 0 {
            return poll_send(&self.chan, cx, &mut self.value);
        }
        if let Some(offer) = &self.offer {
            return match offer.state.get() {
                OfferState::Taken => Poll::Ready(Ok(())),
                OfferState::Withdrawn => Poll::Ready(Err(Closed)),
                OfferState::Open => {
                    let mut waker = offer.waker.borrow_mut();
                    if !waker.will_wake(cx.waker()) {
                        *waker = cx.waker().clone();
                    }
                    Poll::Pending
                }
            };
        }
        let mut c = self.chan.borrow_mut();
        if c.closed {
            return Poll::Ready(Err(Closed));
        }
        let offer = Rc::new(Offer {
            value: RefCell::new(self.value.take()),
            state: Cell::new(OfferState::Open),
            waker: RefCell::new(cx.waker().clone()),
            select: self.select.clone(),
        });
        c.offers.push_back(offer.clone());
        self.offer = Some(offer);
        let wakers = c.receiver_wakers();
        drop(c);
        wake_all(wakers);
        Poll::Pending
    }

    /// Whether a receiver took this send's rendezvous offer: the value
    /// is delivered, whatever happens to the waiting future.
    pub(crate) fn delivered(&self) -> bool {
        self.offer
            .as_ref()
            .is_some_and(|o| o.state.get() == OfferState::Taken)
    }

    /// Withdraw an offer that is still open (a taken one stays taken).
    /// Idempotent.
    pub(crate) fn withdraw(&mut self) {
        let Some(offer) = &self.offer else {
            return;
        };
        if offer.state.get() != OfferState::Open {
            return;
        }
        offer.state.set(OfferState::Withdrawn);
        self.chan
            .borrow_mut()
            .offers
            .retain(|o| !Rc::ptr_eq(o, offer));
        self.offer = None;
    }
}

impl Drop for Sending {
    fn drop(&mut self) {
        self.withdraw();
    }
}

/// The Lua side of a channel: the userdata that the `task` library's
/// `Channel` objects hold (`ch._c`) and that receive cases name as
/// their target.  It has no methods of its own.
pub(crate) struct ChanUd(pub(crate) Chan);

impl mlua::UserData for ChanUd {}
