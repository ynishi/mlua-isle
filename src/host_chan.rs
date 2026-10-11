//! Host channels: a `Send` [`Sender`] that host code (any thread, any
//! task) uses to feed values into a running Lua loop, [`Request`]s
//! that Lua answers, and channels to the host: a `Send` [`Receiver`]
//! that takes the values Lua sends.
//!
//! Re-exported from [`runtime`](crate::runtime) and documented in its
//! module docs ("Host channels and requests", "Channels to the host").
//! The Lua side is the `task` library's `Channel` object over a host
//! channel ([`chan::new_host`]) or a channel to the host
//! ([`chan::new_to_host`]); this module is the host side.

use crate::chan::{self, ChanUd};
use crate::error::{IsleError, LuaErrorKind, LuaFailure};
use crate::runtime::Vm;
use mlua::{AnyUserData, FromLua, IntoLua, Lua, MetaMethod, MultiValue, Table, UserData, Value};
use std::cell::RefCell;
use std::fmt;
use tokio::sync::{mpsc, oneshot};

/// The largest capacity tokio's bounded channel accepts (its
/// semaphore's `MAX_PERMITS`); a larger one would panic.
const MAX_CAP: usize = usize::MAX >> 3;

fn setup_error(message: &str) -> IsleError {
    IsleError::Init(LuaFailure::new(LuaErrorKind::External, message))
}

/// Create a host channel: a [`Sender`] for the host and its Lua side,
/// a `task` library `Channel` object that is receive-only.
///
/// Call it on the VM thread, after [`Vm::attach`] and
/// [`Vm::task_lib`] (the Lua object is built by the library, with the
/// same methods as a `task.channel`).  The [`Sender`] is `Send + Clone`;
/// move it to any thread or task.  Each value is converted to a Lua
/// value (`T: IntoLua`) when Lua takes it, on the VM thread.
///
/// The contracts (capacity, closing, conversion errors, several Lua
/// receivers, requests) are in the
/// [runtime module docs](crate::runtime#host-channels-and-requests).
///
/// With an [`AsyncIsle`](crate::AsyncIsle), create the channel in a
/// request and return the `Sender` from it:
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use mlua_isle::runtime::{channel, Config, Vm};
/// use mlua_isle::AsyncIsle;
///
/// let (isle, driver) = AsyncIsle::spawn(|lua| {
///     let vm = Vm::attach(lua, Config::default())?;
///     lua.globals().set("task", vm.task_lib()?)
/// })
/// .await?;
/// let tx = isle
///     .exec(|lua| {
///         let (tx, events) = channel::<i64>(lua, 16)?;
///         lua.globals().set("events", events)?;
///         Ok(tx)
///     })
///     .await?;
///
/// let producer = tokio::spawn(async move {
///     for i in 1..=3 {
///         tx.send(i).await.unwrap();
///     }
///     // Dropping the last Sender closes the channel.
/// });
/// let sum: i64 = isle
///     .coroutine_eval(
///         "local sum = 0
///          while true do
///            local v, ok = events:recv()
///            if not ok then return sum end
///            sum = sum + v
///          end",
///     )
///     .await?;
/// producer.await?;
/// assert_eq!(sum, 6);
/// driver.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// [`IsleError::Init`] with [`LuaErrorKind::External`] when the VM is
/// not attached, when its `task` library was not created, or when
/// `cap` is 0 (rendezvous is not supported yet) or larger than tokio's
/// bounded channel allows.  A Lua error building the object (the VM's
/// memory limit) is [`IsleError::Lua`].
pub fn channel<T>(lua: &Lua, cap: usize) -> Result<(Sender<T>, LuaChannel), IsleError>
where
    T: IntoLua + Send + 'static,
{
    let vm = Vm::of(lua).ok_or_else(|| {
        setup_error("runtime::channel: the VM is not attached (call Vm::attach first)")
    })?;
    let ctor = vm.channel_ctor()?.ok_or_else(|| {
        setup_error(
            "runtime::channel: the VM's task library was not created (call Vm::task_lib first)",
        )
    })?;
    if cap == 0 {
        return Err(setup_error(
            "runtime::channel: cap = 0 (rendezvous) is not supported yet",
        ));
    }
    if cap > MAX_CAP {
        return Err(setup_error("runtime::channel: cap is too large"));
    }
    let (tx, rx) = mpsc::channel(cap);
    let table: Table = ctor.call(ChanUd(chan::new_host(rx)))?;
    Ok((Sender { inner: tx }, LuaChannel(table)))
}

/// Create a channel to the host: its Lua side, a `task` library
/// `Channel` object that is send-only, and a [`Receiver`] for the host.
///
/// Call it on the VM thread, after [`Vm::attach`] and [`Vm::task_lib`].
/// The [`Receiver`] is `Send` (not `Clone`); move it to any thread or
/// task.  Each value Lua sends is converted to `T` (`T: FromLua`) on the
/// VM thread, once room for it has been reserved: a value that fails to
/// convert raises in the Lua sender and is not sent.
///
/// The contracts (capacity, waiting, closing, conversion errors,
/// cancellation) are in the
/// [runtime module docs](crate::runtime#channels-to-the-host).
///
/// With an [`AsyncIsle`](crate::AsyncIsle), create the channel in the
/// init closure and return the `Receiver` with
/// [`spawn_with`](crate::AsyncIsleBuilder::spawn_with):
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use mlua_isle::runtime::{channel_to_host, Config, Vm};
/// use mlua_isle::AsyncIsle;
///
/// let (isle, driver, mut reports) = AsyncIsle::builder()
///     .spawn_with(|lua| {
///         let vm = Vm::attach(lua, Config::default())?;
///         lua.globals().set("task", vm.task_lib()?)?;
///         let (ch, rx) = channel_to_host::<String>(lua, 16)?;
///         lua.globals().set("reports", ch)?;
///         Ok(rx)
///     })
///     .await?;
///
/// let consumer = tokio::spawn(async move {
///     let mut got = Vec::new();
///     while let Some(r) = reports.recv().await {
///         got.push(r);
///     }
///     got
/// });
/// isle.coroutine_eval::<()>(
///     "for i = 1, 3 do reports:send('report ' .. i) end
///      reports:close()",
/// )
/// .await?;
/// assert_eq!(consumer.await?, ["report 1", "report 2", "report 3"]);
/// driver.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// [`IsleError::Init`] with [`LuaErrorKind::External`] when the VM is
/// not attached, when its `task` library was not created, or when
/// `cap` is 0 or larger than tokio's bounded channel allows.  A Lua
/// error building the object (the VM's memory limit) is
/// [`IsleError::Lua`].
pub fn channel_to_host<T>(lua: &Lua, cap: usize) -> Result<(LuaChannel, Receiver<T>), IsleError>
where
    T: FromLua + Send + 'static,
{
    let vm = Vm::of(lua).ok_or_else(|| {
        setup_error("runtime::channel_to_host: the VM is not attached (call Vm::attach first)")
    })?;
    let ctor = vm.channel_ctor()?.ok_or_else(|| {
        setup_error(
            "runtime::channel_to_host: the VM's task library was not created (call Vm::task_lib first)",
        )
    })?;
    if cap == 0 {
        return Err(setup_error("runtime::channel_to_host: cap must be >= 1"));
    }
    if cap > MAX_CAP {
        return Err(setup_error("runtime::channel_to_host: cap is too large"));
    }
    let (tx, rx) = mpsc::channel(cap);
    let table: Table = ctor.call((ChanUd(chan::new_to_host(tx)), true))?;
    Ok((LuaChannel(table), Receiver { inner: rx }))
}

/// The Lua side of a host channel or of a channel to the host: the
/// `task` library's `Channel` object, receive-only or send-only.  Set it
/// where Lua code can reach it (it implements [`IntoLua`]), e.g.
/// `lua.globals().set("events", ch)`.
#[derive(Clone, Debug)]
pub struct LuaChannel(Table);

impl LuaChannel {
    /// The Lua object.
    pub fn table(&self) -> &Table {
        &self.0
    }

    /// The Lua object, by value.
    pub fn into_table(self) -> Table {
        self.0
    }
}

impl IntoLua for LuaChannel {
    fn into_lua(self, _: &Lua) -> mlua::Result<Value> {
        Ok(Value::Table(self.0))
    }
}

/// The host's end of a host channel (see [`channel`]).  `Send + Clone`;
/// the channel closes, after Lua received the queued values, when every
/// clone is dropped.
///
/// There is no `send_timeout`: wrap [`send`](Self::send) (or
/// [`request`](Self::request)) in [`tokio::time::timeout`].  A send
/// that times out sent nothing.
pub struct Sender<T> {
    inner: mpsc::Sender<T>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            inner: self.inner.clone(),
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

impl<T> Sender<T> {
    /// Send `value`, waiting while the channel is full.
    ///
    /// Cancel safe in the sense that dropping the future before it
    /// resolves sent nothing (the value is dropped with the future).
    ///
    /// # Errors
    ///
    /// [`SendError`] with the value when the channel is closed: Lua
    /// called `close` on it, or the Lua side was collected (or the VM
    /// dropped).
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.inner.send(value).await.map_err(|e| SendError(e.0))
    }

    /// Send `value` if there is room, without waiting.
    ///
    /// # Errors
    ///
    /// [`TrySendError::Full`] when the channel is full,
    /// [`TrySendError::Closed`] when it is closed; both give the value
    /// back.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.inner.try_send(value).map_err(|e| match e {
            mpsc::error::TrySendError::Full(v) => TrySendError::Full(v),
            mpsc::error::TrySendError::Closed(v) => TrySendError::Closed(v),
        })
    }

    /// Whether the channel is closed: Lua called `close` on it, or the
    /// Lua side was collected (or the VM dropped).
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl<Req, Resp, E> Sender<Request<Req, Resp, E>> {
    /// Send `req` as a [`Request`] and wait for Lua's answer.
    ///
    /// Waits while the channel is full, then until Lua answers.  There
    /// is no timeout: an unanswered request that Lua neither answers nor
    /// closes is reported only when Lua collects it, so wrap the call in
    /// [`tokio::time::timeout`].  Dropping the future (a timeout) makes a
    /// later `req:reply(v)` or `req:reject(reason)` in Lua return `false`.
    ///
    /// # Errors
    ///
    /// [`RequestError::Closed`] with `req` when the channel is closed;
    /// [`RequestError::Rejected`] with the reason when Lua called
    /// `req:reject(reason)`; [`RequestError::NoReply`] when Lua closed or
    /// collected the request without answering, or `req` failed to
    /// convert to a Lua value (Lua never saw it).
    pub async fn request(&self, req: Req) -> Result<Resp, RequestError<Req, E>> {
        let (reply, answer) = oneshot::channel();
        self.inner
            .send(Request { req, reply })
            .await
            .map_err(|e| RequestError::Closed(e.0.req))?;
        answer
            .await
            .map_err(|_| RequestError::NoReply)?
            .map_err(RequestError::Rejected)
    }
}

/// A request sent with [`Sender::request`]: a `Req` and a one-shot
/// answer, either a reply of type `Resp` or a rejection of type `E`
/// (`String` by default).
///
/// It is built by [`Sender::request`] only.  On the Lua side it is a
/// userdata (converted when Lua takes it from the channel):
///
/// | Lua | meaning |
/// |---|---|
/// | `req.value` | The `Req`, converted with `IntoLua`. |
/// | `req:reply(v)` | Answer with `v`, converted with `Resp: FromLua`.  Returns `true`, or `false` when the requester stopped waiting (not an error).  Raises when the request was already answered or closed, and when the conversion fails (the request then stays unanswered and can be answered again). |
/// | `req:reject(reason)` | Answer with a rejection: the requester gets [`RequestError::Rejected`] with `reason`, converted with `E: FromLua`.  Returns and raises as `reply` does. |
/// | `req:replied()` | Whether `reply` or `reject` succeeded (delivered or not). |
/// | `local req <close> = ...` | Closing an unanswered request answers [`RequestError::NoReply`] at once.  Closing an answered (replied or rejected) one does nothing. |
///
/// A request that is neither answered nor closed reports `NoReply` when
/// Lua collects it.
pub struct Request<Req, Resp, E = String> {
    req: Req,
    reply: oneshot::Sender<Result<Resp, E>>,
}

impl<Req: fmt::Debug, Resp, E> fmt::Debug for Request<Req, Resp, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
            .field("req", &self.req)
            .finish_non_exhaustive()
    }
}

impl<Req, Resp, E> IntoLua for Request<Req, Resp, E>
where
    Req: IntoLua,
    Resp: FromLua + 'static,
    E: FromLua + 'static,
{
    /// A userdata holding the answer channel; the converted `Req` is its
    /// user value (in the collector's sight), read as `req.value`.  If
    /// `Req` fails to convert, the answer channel is dropped (the
    /// requester gets `NoReply`) and the error returned.
    fn into_lua(self, lua: &Lua) -> mlua::Result<Value> {
        let Request { req, reply } = self;
        let value = req.into_lua(lua)?;
        let ud = lua.create_userdata(RequestUd {
            state: RefCell::new(ReplyState::Open(reply)),
        })?;
        ud.set_user_value(value)?;
        Ok(Value::UserData(ud))
    }
}

enum ReplyState<Resp, E> {
    Open(oneshot::Sender<Result<Resp, E>>),
    /// Replied or rejected.
    Answered,
    Closed,
}

/// The Lua side of a [`Request`].
struct RequestUd<Resp, E> {
    state: RefCell<ReplyState<Resp, E>>,
}

impl<Resp, E> RequestUd<Resp, E> {
    /// Answer with `answer` (already converted).  `method` names the Lua
    /// method in the error messages.
    fn answer(&self, method: &str, answer: Result<Resp, E>) -> mlua::Result<bool> {
        let prev = std::mem::replace(&mut *self.state.borrow_mut(), ReplyState::Answered);
        match prev {
            ReplyState::Open(tx) => Ok(tx.send(answer).is_ok()),
            other => {
                // Answered or closed during the conversion.
                let e = answered_error(method, &other);
                *self.state.borrow_mut() = other;
                Err(e.expect("not open"))
            }
        }
    }
}

fn answered_error<Resp, E>(method: &str, state: &ReplyState<Resp, E>) -> Option<mlua::Error> {
    match state {
        ReplyState::Open(_) => None,
        ReplyState::Answered => Some(mlua::Error::runtime(format!(
            "req:{method}: request already answered"
        ))),
        ReplyState::Closed => Some(mlua::Error::runtime(format!(
            "req:{method}: request is closed"
        ))),
    }
}

impl<Resp: FromLua + 'static, E: FromLua + 'static> UserData for RequestUd<Resp, E> {
    fn add_fields<F: mlua::UserDataFields<Self>>(fields: &mut F) {
        fields.add_field_function_get("value", |_, ud: AnyUserData| ud.user_value::<Value>());
    }

    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        // `reply` and `reject` take a `Value` and convert it by hand, so
        // a conversion failure raises the plain conversion error (not
        // one wrapped as a bad argument) and leaves the request open.
        methods.add_method("reply", |lua, this, v: Value| {
            if let Some(e) = answered_error("reply", &this.state.borrow()) {
                return Err(e);
            }
            // Converted without holding the borrow: the conversion may
            // run Lua code (an allocation can run a `__gc`).
            let resp = Resp::from_lua(v, lua)?;
            this.answer("reply", Ok(resp))
        });
        methods.add_method("reject", |lua, this, v: Value| {
            if let Some(e) = answered_error("reject", &this.state.borrow()) {
                return Err(e);
            }
            // As in `reply`: converted without holding the borrow.
            let reason = E::from_lua(v, lua)?;
            this.answer("reject", Err(reason))
        });
        methods.add_method("replied", |_, this, ()| {
            Ok(matches!(*this.state.borrow(), ReplyState::Answered))
        });
        methods.add_meta_method(MetaMethod::Close, |_, this, _: MultiValue| {
            let mut state = this.state.borrow_mut();
            if matches!(*state, ReplyState::Open(_)) {
                // Drops the answer sender: the requester sees `NoReply`.
                *state = ReplyState::Closed;
            }
            Ok(())
        });
    }
}

/// [`Sender::send`] failed: the channel is closed.  Carries the value.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("host channel is closed")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// [`Sender::try_send`] failed.  Carries the value.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel is full.
    Full(T),
    /// The channel is closed.
    Closed(T),
}

impl<T> TrySendError<T> {
    /// The value that was not sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(v) | TrySendError::Closed(v) => v,
        }
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("Full(..)"),
            TrySendError::Closed(_) => f.write_str("Closed(..)"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("host channel is full"),
            TrySendError::Closed(_) => f.write_str("host channel is closed"),
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// The host's end of a channel to the host (see [`channel_to_host`]).
/// `Send`, not `Clone`.
///
/// Dropping it closes the channel: Lua's sends raise from then on.
pub struct Receiver<T> {
    inner: mpsc::Receiver<T>,
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

impl<T> Receiver<T> {
    /// Receive the next value, waiting while the channel is empty.
    /// `None` once the channel is closed and every queued value has
    /// been received.
    ///
    /// Cancel safe: dropping the future before it resolves received
    /// nothing.
    pub async fn recv(&mut self) -> Option<T> {
        self.inner.recv().await
    }

    /// Receive the next value without waiting.
    ///
    /// # Errors
    ///
    /// [`TryRecvError::Empty`] when no value is queued and the channel
    /// is open, [`TryRecvError::Closed`] when it is closed and every
    /// queued value has been received.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.inner.try_recv().map_err(|e| match e {
            mpsc::error::TryRecvError::Empty => TryRecvError::Empty,
            mpsc::error::TryRecvError::Disconnected => TryRecvError::Closed,
        })
    }

    /// Close the channel: Lua's sends raise from now on (and waiting
    /// ones wake and raise); the values already queued can still be
    /// received.  Idempotent.
    pub fn close(&mut self) {
        self.inner.close();
    }

    /// Whether the channel is closed: [`close`](Self::close) was called,
    /// Lua called `close` on it, or the Lua side was collected (or the
    /// VM dropped).  Values may still be queued.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

/// [`Receiver::try_recv`] found no value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TryRecvError {
    /// No value is queued; the channel is open.
    Empty,
    /// The channel is closed and every queued value has been received.
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => f.write_str("channel to the host is empty"),
            TryRecvError::Closed => f.write_str("channel to the host is closed"),
        }
    }
}

impl std::error::Error for TryRecvError {}

/// [`Sender::request`] failed.
///
/// `E` is the rejection reason's type, `String` by default (the `E` of
/// the [`Request`]).
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RequestError<Req, E = String> {
    /// The channel is closed.  Carries the request's value.
    Closed(Req),
    /// Lua closed or collected the request without answering (or the
    /// value failed to convert to a Lua value).
    NoReply,
    /// Lua rejected the request with `req:reject(reason)`.  Carries the
    /// reason, converted with `E: FromLua`.
    Rejected(E),
}

impl<Req, E: fmt::Debug> fmt::Debug for RequestError<Req, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::Closed(_) => f.write_str("Closed(..)"),
            RequestError::NoReply => f.write_str("NoReply"),
            RequestError::Rejected(e) => f.debug_tuple("Rejected").field(e).finish(),
        }
    }
}

impl<Req, E: fmt::Display> fmt::Display for RequestError<Req, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::Closed(_) => f.write_str("host channel is closed"),
            RequestError::NoReply => f.write_str("request was not answered"),
            RequestError::Rejected(e) => write!(f, "request was rejected: {e}"),
        }
    }
}

impl<Req, E: fmt::Debug + fmt::Display> std::error::Error for RequestError<Req, E> {}
