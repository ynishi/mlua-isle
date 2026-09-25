//! Structured execution of Lua coroutines on an isle thread.
//!
//! Every coroutine request and every task spawned from Lua runs in a
//! **scope**: a [`CancelToken`] plus the set of tasks spawned from it.
//! While the coroutine is polled, the scope is the current one on the
//! thread (the cancel hook and `task.spawn` read it).  When the
//! coroutine finishes, the scope cancels the tasks that were not joined
//! and waits for them, so no task outlives the request or task that
//! spawned it.

use crate::hook::{CancelToken, EnterGuard, CANCELLED_SENTINEL};
use crate::hooks;
use mlua::{Function, Lua, MultiValue, Value};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;

/// Wraps a request body: marks the isle-created coroutine, then calls.
///
/// The call goes through `pcall` and the error is re-raised: an error
/// that escapes a coroutine leaves its to-be-closed variables open until
/// the coroutine is closed from C, where `__close` cannot yield, while
/// `pcall` closes them during the unwind, where it can.
pub(crate) const WRAP_CALL: &str = "local mark, f = ... \
     local pack, unpack = table.pack, table.unpack \
     return function(...) \
       mark() \
       local r = pack(pcall(f, ...)) \
       if r[1] then return unpack(r, 2, r.n) end \
       error(r[2], 0) \
     end";
/// Wraps a task body: marks the coroutine, then calls under `pcall` so
/// that the raw Lua error value survives (a table stays a table).
pub(crate) const WRAP_PCALL: &str =
    "local mark, f = ... return function(...) mark() return pcall(f, ...) end";

thread_local! {
    static SCOPE: RefCell<Option<Rc<Scope>>> = const { RefCell::new(None) };
}

/// The scope of the coroutine currently being polled, if any.
pub(crate) fn current_scope() -> Option<Rc<Scope>> {
    SCOPE.with(|s| s.borrow().clone())
}

/// RAII guard that makes `scope` the current scope of this thread.
///
/// The previous scope is restored on drop (also when the inner poll
/// panics), so guards nest.
struct ScopeEnterGuard {
    prev: Option<Rc<Scope>>,
}

impl ScopeEnterGuard {
    fn new(scope: &Rc<Scope>) -> Self {
        let prev = SCOPE.with(|s| s.replace(Some(scope.clone())));
        Self { prev }
    }
}

impl Drop for ScopeEnterGuard {
    fn drop(&mut self) {
        let prev = self.prev.take();
        SCOPE.with(|s| *s.borrow_mut() = prev);
    }
}

/// Tasks spawned from one coroutine request or task.
#[derive(Default)]
pub(crate) struct Scope {
    children: RefCell<Vec<Rc<TaskState>>>,
    /// When the coroutine is dropped, once it has been cancelled.  A task
    /// spawned from it after that inherits this deadline.
    deadline: Cell<Option<Instant>>,
}

impl Scope {
    /// The grace deadline of this scope's coroutine, if it was cancelled.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline.get()
    }

    pub(crate) fn add(&self, child: Rc<TaskState>) {
        let mut children = self.children.borrow_mut();
        children.retain(|c| !c.is_done());
        children.push(child);
    }

    fn cancel_all(&self) {
        for c in self.children.borrow().iter() {
            c.token.cancel();
        }
    }

    async fn wait_all(&self) {
        let children = self.children.borrow().clone();
        for c in children {
            c.wait_done().await;
        }
    }

    fn abort_all(&self) {
        for c in self.children.borrow().iter() {
            c.token.cancel();
            if let Some(h) = c.abort.borrow_mut().take() {
                h.abort();
            }
        }
    }
}

/// How a task ended.
pub(crate) enum Outcome {
    /// What the task body's `pcall` returned: `true, ...` or `false, err`.
    Values(MultiValue),
    /// The task was cancelled.
    Cancelled,
}

/// Shared state of one spawned task.
pub(crate) struct TaskState {
    pub(crate) token: CancelToken,
    done: Cell<bool>,
    notify: tokio::sync::Notify,
    outcome: RefCell<Option<Outcome>>,
    pub(crate) abort: RefCell<Option<tokio::task::AbortHandle>>,
}

impl TaskState {
    pub(crate) fn new(token: CancelToken) -> Self {
        Self {
            token,
            done: Cell::new(false),
            notify: tokio::sync::Notify::new(),
            outcome: RefCell::new(None),
            abort: RefCell::new(None),
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done.get()
    }

    pub(crate) fn finish(&self, outcome: Outcome) {
        if self.done.replace(true) {
            return;
        }
        *self.outcome.borrow_mut() = Some(outcome);
        self.abort.borrow_mut().take();
        self.notify.notify_waiters();
    }

    pub(crate) async fn wait_done(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.done.get() {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn take_outcome(&self) -> Option<Outcome> {
        self.outcome.borrow_mut().take()
    }
}

/// Marks a task finished as cancelled if its future is dropped first.
pub(crate) struct FinishOnDrop(pub(crate) Rc<TaskState>);

impl Drop for FinishOnDrop {
    fn drop(&mut self) {
        self.0.finish(Outcome::Cancelled);
    }
}

/// Future adapter that makes the token and scope current while the
/// inner future is polled, and forgets the coroutine's "isle-created"
/// mark when dropped.
struct Scoped<F> {
    token: CancelToken,
    scope: Rc<Scope>,
    root: Rc<Cell<Option<usize>>>,
    fut: Pin<Box<F>>,
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let _enter = EnterGuard::new(&self.token);
        let _scope = ScopeEnterGuard::new(&self.scope);
        self.fut.as_mut().poll(cx)
    }
}

impl<F> Drop for Scoped<F> {
    fn drop(&mut self) {
        if let Some(ptr) = self.root.take() {
            hooks::unmark_root(ptr);
        }
    }
}

/// Aborts a scope's tasks when the scope is dropped before it drained.
struct AbortOnDrop(Option<Rc<Scope>>);

impl AbortOnDrop {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(scope) = self.0.take() {
            scope.abort_all();
        }
    }
}

/// Call `func` with `args` in a new coroutine that runs in its own scope
/// under `token`.  `wrap` is [`WRAP_CALL`] or [`WRAP_PCALL`].
///
/// Once `token` is cancelled, the coroutine gets `grace` to finish and
/// is then dropped; the future yields `None` in that case.  `inherited`
/// is the deadline of the spawning coroutine's scope, if that coroutine
/// was already cancelled: the grace never ends later than it, so a task
/// started during its parent's cleanup gets only the remaining time.
///
/// Either way, the returned future resolves only after the tasks the
/// coroutine spawned (and did not join) have been cancelled and have
/// finished or been dropped.  Each of those tasks waits for its own
/// tasks the same way, so the wait covers the whole tree.  Dropping the
/// returned future early drops the coroutine and aborts those tasks
/// without waiting.
pub(crate) fn scoped_call(
    lua: &Lua,
    token: CancelToken,
    grace: Duration,
    inherited: Option<Instant>,
    wrap: &str,
    func: Function,
    args: MultiValue,
) -> mlua::Result<impl Future<Output = Option<mlua::Result<MultiValue>>>> {
    let root = Rc::new(Cell::new(None));
    let r = root.clone();
    let mark = lua.create_function(move |lua, ()| {
        let ptr = lua.current_thread().to_pointer() as usize;
        hooks::mark_root(ptr);
        r.set(Some(ptr));
        Ok(())
    })?;
    let body: Function = lua.load(wrap).call((mark, func))?;
    let scope = Rc::new(Scope::default());
    let run = Scoped {
        token: token.clone(),
        scope: scope.clone(),
        root,
        fut: Box::pin(body.call_async::<MultiValue>(args)),
    };
    Ok(async move {
        // No task can be spawned before the first poll, so the guard is
        // created here rather than outside the future.
        let guard = AbortOnDrop(Some(scope.clone()));
        // On timeout this drops `run` (the coroutine) but not the scope.
        // The tasks are not aborted here: aborting a task drops its
        // future before it waits for its own tasks.  Their tokens are
        // children of `token`, so they were cancelled with it, and each
        // one ends by the same deadline (see `with_grace`) and then
        // waits for its own tasks.
        let out = with_grace(&token, grace, inherited, &scope, run).await;
        scope.cancel_all();
        scope.wait_all().await;
        guard.disarm();
        out
    })
}

/// Run `fut`; once `token` is cancelled, let it run until
/// `now + grace` or `inherited`, whichever is earlier, and then drop it.
/// The deadline is stored in `scope`, so the tasks `fut` spawns during
/// its cleanup inherit it.  Returns `None` when `fut` was dropped.
///
/// A deadline already in the past still gives `fut` one poll.
async fn with_grace<F: Future>(
    token: &CancelToken,
    grace: Duration,
    inherited: Option<Instant>,
    scope: &Scope,
    fut: F,
) -> Option<F::Output> {
    tokio::pin!(fut);
    tokio::select! {
        biased;
        out = &mut fut => return Some(out),
        _ = token.cancelled() => {}
    }
    let own = Instant::now() + grace;
    let deadline = inherited.map_or(own, |d| d.min(own));
    scope.deadline.set(Some(deadline));
    tokio::time::timeout_at(deadline, fut).await.ok()
}

/// Make an async host function's future stop when the calling request
/// or task is cancelled.
///
/// Wrap the body of a
/// [`create_async_function`](mlua::Lua::create_async_function) with it:
/// when the token of the request or task that called the function is
/// cancelled while the future is pending, the future is dropped and the
/// call returns the cancellation error to the Lua code.  That error
/// unwinds the coroutine normally, so its `__close` handlers run and can
/// await (see [`CancelConfig::grace`](crate::hooks::CancelConfig::grace)).
///
/// Outside a request or task the future runs unchanged.
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use mlua_isle::{cancellable, AsyncIsle};
/// use std::time::Duration;
///
/// let (isle, driver) = AsyncIsle::spawn(|lua| {
///     let sleep = lua.create_async_function(|_, ms: u64| {
///         cancellable(async move {
///             tokio::time::sleep(Duration::from_millis(ms)).await;
///             Ok(())
///         })
///     })?;
///     lua.globals().set("sleep", sleep)
/// })
/// .await?;
/// isle.coroutine_eval("sleep(1)").await?;
/// driver.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub async fn cancellable<F, T>(fut: F) -> mlua::Result<T>
where
    F: Future<Output = mlua::Result<T>>,
{
    match crate::hook::current_token() {
        None => fut.await,
        Some(token) => tokio::select! {
            biased;
            _ = token.cancelled() => Err(mlua::Error::runtime(CANCELLED_SENTINEL)),
            out = fut => out,
        },
    }
}

/// Run `func(args)` as a root coroutine: in its own scope under `token`,
/// with the VM's cancel grace (see [`hooks::configure`]).
///
/// This is what a coroutine request of an [`AsyncIsle`](crate::AsyncIsle)
/// does.  Use it to run Lua with task support on a VM you drive
/// yourself: call [`hooks::install`] and [`tasks::install`](crate::tasks::install)
/// once, then await this inside a [`tokio::task::LocalSet`].
///
/// Resolves to `Err(IsleError::Cancelled)` if `token` was cancelled,
/// whatever the coroutine returned.
///
/// On cancel, the coroutine gets the grace period and is then dropped,
/// and this resolves only after the tasks it spawned, transitively,
/// have finished or been dropped.  The grace period is one deadline for
/// the coroutine and all of those tasks: a task started during cleanup
/// gets the time that remains, not a grace period of its own.
/// So nothing the cancelled call started is still alive when the next
/// call on the VM begins.  A task in a CPU loop cannot be dropped until
/// it yields; without
/// [`CancelConfig::preempt_every`](crate::hooks::CancelConfig::preempt_every)
/// the wait blocks on it.
pub async fn run_root(
    lua: &Lua,
    token: CancelToken,
    func: Function,
    args: MultiValue,
) -> Result<MultiValue, crate::IsleError> {
    hooks::ensure_installed(lua)?;
    let grace = hooks::config(lua).grace;
    let out = scoped_call(lua, token.clone(), grace, None, WRAP_CALL, func, args)?.await;
    if token.is_cancelled() {
        return Err(crate::IsleError::Cancelled);
    }
    match out {
        Some(r) => r.map_err(crate::IsleError::from),
        None => Err(crate::IsleError::Cancelled),
    }
}

/// Convert an `mlua::Error` into a Lua value (for `false, err` results).
pub(crate) fn error_value(e: mlua::Error) -> Value {
    Value::Error(Box::new(e))
}
