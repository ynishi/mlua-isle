//! Host channels: a `Send` [`Sender`] that host code (any thread, any
//! task) uses to feed values into a running Lua loop, and [`Request`]s
//! that Lua answers.
//!
//! Re-exported from [`runtime`](crate::runtime) and documented in its
//! module docs ("Host channels and requests").  The Lua side is the
//! `task` library's `Channel` object over a host channel
//! ([`chan::new_host`]); this module is the host side.

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

/// The Lua side of a host channel: the `task` library's `Channel`
/// object, receive-only.  Set it where Lua code can reach it (it
/// implements [`IntoLua`]), e.g. `lua.globals().set("events", ch)`.
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

impl<Req, Resp> Sender<Request<Req, Resp>> {
    /// Send `req` as a [`Request`] and wait for Lua's reply.
    ///
    /// Waits while the channel is full, then until Lua replies.  There
    /// is no timeout: an unanswered request that Lua neither replies to
    /// nor closes is reported only when Lua collects it, so wrap the call
    /// in [`tokio::time::timeout`].  Dropping the future (a timeout)
    /// makes a later `req:reply(v)` in Lua return `false`.
    ///
    /// # Errors
    ///
    /// [`RequestError::Closed`] with `req` when the channel is closed;
    /// [`RequestError::NoReply`] when Lua closed or collected the request
    /// without replying, or `req` failed to convert to a Lua value (Lua
    /// never saw it).
    pub async fn request(&self, req: Req) -> Result<Resp, RequestError<Req>> {
        let (reply, answer) = oneshot::channel();
        self.inner
            .send(Request { req, reply })
            .await
            .map_err(|e| RequestError::Closed(e.0.req))?;
        answer.await.map_err(|_| RequestError::NoReply)
    }
}

/// A request sent with [`Sender::request`]: a `Req` and a one-shot
/// reply of type `Resp`.
///
/// It is built by [`Sender::request`] only.  On the Lua side it is a
/// userdata (converted when Lua takes it from the channel):
///
/// | Lua | meaning |
/// |---|---|
/// | `req.value` | The `Req`, converted with `IntoLua`. |
/// | `req:reply(v)` | Answer with `v`, converted with `Resp: FromLua`.  Returns `true`, or `false` when the requester stopped waiting (not an error).  Raises when the request was already answered or closed, and when the conversion fails (the request then stays unanswered and can be answered again). |
/// | `req:replied()` | Whether `reply` succeeded (delivered or not). |
/// | `local req <close> = ...` | Closing an unanswered request answers [`RequestError::NoReply`] at once.  Closing an answered one does nothing. |
///
/// A request that is neither answered nor closed reports `NoReply` when
/// Lua collects it.
pub struct Request<Req, Resp> {
    req: Req,
    reply: oneshot::Sender<Resp>,
}

impl<Req: fmt::Debug, Resp> fmt::Debug for Request<Req, Resp> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
            .field("req", &self.req)
            .finish_non_exhaustive()
    }
}

impl<Req, Resp> IntoLua for Request<Req, Resp>
where
    Req: IntoLua,
    Resp: FromLua + 'static,
{
    /// A userdata holding the reply; the converted `Req` is its user
    /// value (in the collector's sight), read as `req.value`.  If `Req`
    /// fails to convert, the reply is dropped (the requester gets
    /// `NoReply`) and the error returned.
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

enum ReplyState<Resp> {
    Open(oneshot::Sender<Resp>),
    Replied,
    Closed,
}

/// The Lua side of a [`Request`].
struct RequestUd<Resp> {
    state: RefCell<ReplyState<Resp>>,
}

fn answered_error(state: &ReplyState<impl Sized>) -> Option<mlua::Error> {
    match state {
        ReplyState::Open(_) => None,
        ReplyState::Replied => Some(mlua::Error::runtime("req:reply: request already answered")),
        ReplyState::Closed => Some(mlua::Error::runtime("req:reply: request is closed")),
    }
}

impl<Resp: FromLua + 'static> UserData for RequestUd<Resp> {
    fn add_fields<F: mlua::UserDataFields<Self>>(fields: &mut F) {
        fields.add_field_function_get("value", |_, ud: AnyUserData| ud.user_value::<Value>());
    }

    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("reply", |lua, this, v: Value| {
            if let Some(e) = answered_error(&this.state.borrow()) {
                return Err(e);
            }
            // Converted without holding the borrow: the conversion may
            // run Lua code (an allocation can run a `__gc`).
            let resp = Resp::from_lua(v, lua)?;
            let prev = std::mem::replace(&mut *this.state.borrow_mut(), ReplyState::Replied);
            match prev {
                ReplyState::Open(tx) => Ok(tx.send(resp).is_ok()),
                other => {
                    // Answered or closed during the conversion.
                    let e = answered_error(&other);
                    *this.state.borrow_mut() = other;
                    Err(e.expect("not open"))
                }
            }
        });
        methods.add_method("replied", |_, this, ()| {
            Ok(matches!(*this.state.borrow(), ReplyState::Replied))
        });
        methods.add_meta_method(MetaMethod::Close, |_, this, _: MultiValue| {
            let mut state = this.state.borrow_mut();
            if matches!(*state, ReplyState::Open(_)) {
                // Drops the reply sender: the requester sees `NoReply`.
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

/// [`Sender::request`] failed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RequestError<Req> {
    /// The channel is closed.  Carries the request's value.
    Closed(Req),
    /// Lua closed or collected the request without replying (or the
    /// value failed to convert to a Lua value).
    NoReply,
}

impl<Req> fmt::Debug for RequestError<Req> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::Closed(_) => f.write_str("Closed(..)"),
            RequestError::NoReply => f.write_str("NoReply"),
        }
    }
}

impl<Req> fmt::Display for RequestError<Req> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::Closed(_) => f.write_str("host channel is closed"),
            RequestError::NoReply => f.write_str("request was not answered"),
        }
    }
}

impl<Req> std::error::Error for RequestError<Req> {}
