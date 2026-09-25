//! Structured execution of Lua coroutines on an isle thread.
//!
//! Every coroutine request and every task spawned from Lua runs in a
//! **scope**: a [`CancelToken`] plus the set of tasks spawned from it.
//! While the coroutine is polled, the scope is the current one on the
//! thread (the cancel hook and `task.spawn` read it).  When the
//! coroutine finishes, the scope cancels the tasks that were not joined
//! and waits for them, so no task outlives the request or task that
//! spawned it.
//!
//! A task is either a Lua function (`task.spawn`) or a host future
//! ([`ScopeHandle::spawn_local`]); both go through [`Scope::spawn`] and
//! differ only in the body.

use crate::error::IsleError;
use crate::hook::{CancelToken, EnterGuard};
use crate::hub;
use mlua::{Function, Lua, MultiValue, Value};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;

/// How a Lua body is wrapped by [`lua_body`].
pub(crate) enum Wrap {
    /// A root: [`WRAP_CALL`](crate::protect::WRAP_CALL), `xpcall` with
    /// the traceback handler (the VM's protect parts); the error comes
    /// back as values.
    Call(crate::protect::Parts),
    /// A task: [`WRAP_PCALL`]; `false, err` is the task's result.
    PCall,
}

/// Wraps a task body: marks the coroutine, then calls under `pcall` so
/// that the raw Lua error value survives (a table stays a table).
pub(crate) const WRAP_PCALL: &str =
    "local mark, f = ... return function(...) mark() return pcall(f, ...) end";

/// The "isle-created coroutine" mark of a Lua body: the coroutine's
/// pointer once the body started, to unmark when the body is dropped.
type RootMark = Rc<Cell<Option<usize>>>;

thread_local! {
    static SCOPE: RefCell<Option<Rc<Scope>>> = const { RefCell::new(None) };
}

/// The scope of the coroutine or host task currently being polled, if
/// any.
pub(crate) fn current() -> Option<Rc<Scope>> {
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

/// Where a scope is in its life.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    /// Its body (coroutine or host future) is running.
    Running,
    /// The body ended; the scope cancels its tasks and waits for them.
    /// A task added now is cancelled at once and waited for too.
    Draining,
    /// The scope waited for its tasks (or was dropped before it
    /// could).  Nothing can be added any more.
    Closed,
}

/// Tasks spawned from one coroutine request or task.
pub(crate) struct Scope {
    /// The token of the request or task that owns the scope.
    token: CancelToken,
    /// The grace period of the scope's body once `token` is cancelled.
    /// Host tasks spawned into the scope get the same grace.
    grace: Duration,
    phase: Cell<Phase>,
    children: RefCell<Vec<Rc<TaskState>>>,
    /// The scope of the coroutine that spawned this one (`None` for a
    /// root).  Weak: the parent's future holds it until its tasks ended.
    parent: Option<Weak<Scope>>,
    /// When the coroutine is dropped, set once it has observed its
    /// cancel.
    deadline: Cell<Option<Instant>>,
}

impl Scope {
    fn new(token: CancelToken, grace: Duration, parent: Option<&Rc<Scope>>) -> Self {
        Self {
            token,
            grace,
            phase: Cell::new(Phase::Running),
            children: RefCell::new(Vec::new()),
            parent: parent.map(Rc::downgrade),
            deadline: Cell::new(None),
        }
    }

    /// The grace deadline of this scope's coroutine, if it was cancelled.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline.get()
    }

    /// The earliest deadline among the ancestors that already have one.
    ///
    /// An ancestor that takes its deadline later takes `now + grace` or
    /// earlier from its own ancestors, never earlier than a deadline
    /// derived from this one, so a scope that clamps to this ends no
    /// later than any of its ancestors.
    fn ancestors_deadline(&self) -> Option<Instant> {
        let mut earliest: Option<Instant> = None;
        let mut next = self.parent.as_ref().and_then(Weak::upgrade);
        while let Some(scope) = next {
            if let Some(d) = scope.deadline() {
                earliest = Some(earliest.map_or(d, |e| e.min(d)));
            }
            next = scope.parent.as_ref().and_then(Weak::upgrade);
        }
        earliest
    }

    fn add(&self, child: Rc<TaskState>) {
        if self.phase.get() == Phase::Draining {
            // The body already ended and `cancel_all` already ran; this
            // task (spawned through a `ScopeHandle` held by another
            // task) is cancelled like its siblings and `wait_all` picks
            // it up.
            child.token.cancel();
        }
        let mut children = self.children.borrow_mut();
        children.retain(|c| !c.is_done());
        children.push(child);
    }

    /// Start `fut` as a task of this scope, on the current `LocalSet`.
    ///
    /// The task runs in a new child scope under a
    /// [child](CancelToken::child_token) of this scope's token, which is
    /// the current token (and the child scope the current scope) while
    /// `fut` is polled.  Once that token is cancelled, `fut` gets
    /// `grace`, clamped to the deadline of any ancestor scope (see
    /// [`with_grace`]), and is then dropped; the task then cancels and
    /// waits for its own tasks.  `root` is the "isle-created coroutine"
    /// mark of a Lua body (forgotten when `fut` is dropped), `None` for
    /// a host future.
    ///
    /// `conv` turns what `fut` returned (`None` if it was dropped) into
    /// the task's result, given the task's token; `None` means
    /// cancelled.
    ///
    /// On a closed scope, and on a draining scope whose deadline has
    /// passed, nothing is spawned: `fut` is dropped here and the task is
    /// returned already finished as cancelled.  (A task started after
    /// the deadline would still get one poll, see [`with_grace`], so a
    /// future that spawns its replacement in that poll could otherwise
    /// keep the drain going forever.)
    pub(crate) fn spawn<F, T, C>(
        self: &Rc<Self>,
        grace: Duration,
        root: Option<RootMark>,
        fut: F,
        conv: C,
    ) -> Spawned<T>
    where
        F: Future + 'static,
        T: 'static,
        C: FnOnce(Option<F::Output>, &CancelToken) -> Option<T> + 'static,
    {
        let token = self.token.child_token();
        let state = Rc::new(TaskState::new(token.clone()));
        let result = Rc::new(RefCell::new(None));
        let out_of_time = match self.phase.get() {
            Phase::Running => false,
            Phase::Draining => self.deadline().is_some_and(|d| d <= Instant::now()),
            Phase::Closed => true,
        };
        if out_of_time {
            token.cancel();
            state.finish();
            return Spawned { state, result };
        }
        let child = Rc::new(Scope::new(token.clone(), grace, Some(self)));
        let run = run_in(child, root, fut);
        let st = state.clone();
        let slot = result.clone();
        let handle = tokio::task::spawn_local(async move {
            let finish = FinishOnDrop(st.clone());
            let out = run.await;
            *slot.borrow_mut() = conv(out, &token);
            st.finish();
            drop(finish);
        });
        *state.abort.borrow_mut() = Some(handle.abort_handle());
        self.add(state.clone());
        Spawned { state, result }
    }

    fn cancel_all(&self) {
        for c in self.children.borrow().iter() {
            c.token.cancel();
        }
    }

    /// Wait until every task of the scope has finished, including tasks
    /// added while waiting.
    async fn wait_all(&self) {
        loop {
            let next = self
                .children
                .borrow()
                .iter()
                .find(|c| !c.is_done())
                .cloned();
            match next {
                Some(c) => c.wait_done().await,
                None => return,
            }
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

/// A spawned task: its shared state and its result slot.  The slot is
/// filled before the task is marked done; `None` after that means the
/// task was cancelled (or dropped).
pub(crate) struct Spawned<T> {
    pub(crate) state: Rc<TaskState>,
    pub(crate) result: Rc<RefCell<Option<T>>>,
}

impl<T> Clone for Spawned<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            result: self.result.clone(),
        }
    }
}

/// Shared state of one spawned task.
pub(crate) struct TaskState {
    pub(crate) token: CancelToken,
    done: Cell<bool>,
    notify: tokio::sync::Notify,
    abort: RefCell<Option<tokio::task::AbortHandle>>,
}

impl TaskState {
    fn new(token: CancelToken) -> Self {
        Self {
            token,
            done: Cell::new(false),
            notify: tokio::sync::Notify::new(),
            abort: RefCell::new(None),
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done.get()
    }

    fn finish(&self) {
        if self.done.replace(true) {
            return;
        }
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
}

/// Marks a task finished (with an empty result slot, i.e. cancelled) if
/// its future is dropped first.
struct FinishOnDrop(Rc<TaskState>);

impl Drop for FinishOnDrop {
    fn drop(&mut self) {
        self.0.finish();
    }
}

/// Future adapter that makes the token and scope current while the
/// inner future is polled, and, for a Lua body, forgets the coroutine's
/// "isle-created" mark when dropped (`root` is `None` for a host
/// future).
struct Scoped<F> {
    token: CancelToken,
    scope: Rc<Scope>,
    root: Option<RootMark>,
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
        if let Some(ptr) = self.root.as_ref().and_then(|r| r.take()) {
            hub::unmark_root(ptr);
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
            scope.phase.set(Phase::Closed);
            scope.abort_all();
        }
    }
}

/// Build the coroutine body of a Lua call: `func` wrapped per `wrap`
/// (see [`Wrap`]), which marks the coroutine as isle-created.  Returns
/// the mark (to forget when the body is dropped) and the not yet polled
/// call.
pub(crate) fn lua_body(
    lua: &Lua,
    wrap: Wrap,
    func: Function,
    args: MultiValue,
) -> mlua::Result<(RootMark, impl Future<Output = mlua::Result<MultiValue>>)> {
    let root = Rc::new(Cell::new(None));
    let r = root.clone();
    let mark = lua.create_function(move |lua, ()| {
        let ptr = lua.current_thread().to_pointer() as usize;
        hub::mark_root(ptr);
        r.set(Some(ptr));
        Ok(())
    })?;
    let body: Function = match wrap {
        Wrap::Call((xpcall, handler, take)) => lua
            .load(crate::protect::WRAP_CALL)
            .set_name("=mlua_isle.root")
            .call((mark, func, xpcall, handler, take))?,
        Wrap::PCall => lua.load(WRAP_PCALL).call((mark, func))?,
    };
    Ok((root, body.call_async::<MultiValue>(args)))
}

/// Run `fut` as the body of `scope`, under the scope's token.
///
/// Once the token is cancelled, `fut` gets the scope's grace (clamped
/// to the deadline of any ancestor scope, so the whole tree shares one
/// deadline, see [`with_grace`]) and is then dropped; the future yields
/// `None` in that case.
///
/// Either way, the returned future resolves only after the tasks
/// spawned into the scope (and not joined) have been cancelled and have
/// finished or been dropped, including tasks added while it waits.
/// Each of those tasks waits for its own tasks the same way, so the wait
/// covers the whole tree.  Dropping the returned future early drops
/// `fut` and aborts those tasks without waiting.
fn run_in<F: Future>(
    scope: Rc<Scope>,
    root: Option<RootMark>,
    fut: F,
) -> impl Future<Output = Option<F::Output>> {
    let run = Scoped {
        token: scope.token.clone(),
        scope: scope.clone(),
        root,
        fut: Box::pin(fut),
    };
    async move {
        // No task can be spawned before the first poll, so the guard is
        // created here rather than outside the future.
        let guard = AbortOnDrop(Some(scope.clone()));
        // On timeout this drops `run` (the body) but not the scope.
        // The tasks are not aborted here: aborting a task drops its
        // future before it waits for its own tasks.  Their tokens are
        // children of the scope's token, so they were cancelled with it,
        // and each one ends by the same deadline (see `with_grace`) and
        // then waits for its own tasks.
        let out = with_grace(&scope, run).await;
        // A body that ended without a cancel has no deadline yet.  Take
        // one now, so that tasks spawned into the draining scope (through
        // a stored `ScopeHandle`) share the remaining time instead of
        // each getting a fresh grace, which would let a chain of them
        // keep the drain going.
        if scope.deadline.get().is_none() {
            let own = Instant::now() + scope.grace;
            let deadline = scope.ancestors_deadline().map_or(own, |d| d.min(own));
            scope.deadline.set(Some(deadline));
        }
        scope.phase.set(Phase::Draining);
        scope.cancel_all();
        scope.wait_all().await;
        scope.phase.set(Phase::Closed);
        guard.disarm();
        out
    }
}

/// Run `fut`; once the scope's token is cancelled, let it run until
/// `now + grace` (the scope's grace) or the earliest deadline of an
/// ancestor scope, whichever is earlier, and then drop it.  Returns
/// `None` when `fut` was dropped.
///
/// The deadline is taken in the first poll that observes the cancel,
/// before `fut` is polled, and stored in `scope`: tasks that observe the
/// cancel later (spawned before it but polled after, or spawned during
/// cleanup) read it, even when `fut` finishes in that same poll.  The
/// ancestors are read at observation time, not at spawn time, because a
/// task spawned before the cancel may observe it after its parent did.
///
/// A deadline already in the past still gives `fut` one poll.
async fn with_grace<F: Future>(scope: &Scope, fut: F) -> Option<F::Output> {
    tokio::pin!(fut);
    tokio::select! {
        biased;
        _ = scope.token.cancelled() => {}
        out = &mut fut => return Some(out),
    }
    let own = Instant::now() + scope.grace;
    let deadline = scope.ancestors_deadline().map_or(own, |d| d.min(own));
    scope.deadline.set(Some(deadline));
    // `Timeout` polls `fut` before its timer, so this poll still reaches
    // `fut` even with a deadline in the past.
    tokio::time::timeout_at(deadline, fut).await.ok()
}

/// Make an async host function's future stop when the calling request
/// or task is cancelled.
///
/// Wrap the body of a
/// [`create_async_function`](mlua::Lua::create_async_function) with it:
/// when the token of the request or task that called the function is
/// cancelled while the future is pending, the future is dropped and the
/// call returns the cancellation error
/// (`mlua::Error::external(`[`Cancelled`](crate::Cancelled)`)`) to the
/// Lua code, where `task.is_cancelled(err)` recognises it.  That error
/// unwinds the coroutine normally, so its `__close` handlers run and can
/// await (see [`Config::grace`](crate::runtime::Config::grace)).
///
/// Outside a request or task the future runs unchanged.
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use mlua_isle::runtime::cancellable;
/// use mlua_isle::AsyncIsle;
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
/// isle.coroutine_eval::<()>("sleep(1)").await?;
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
            _ = token.cancelled() => Err(crate::error::cancel_error()),
            out = fut => out,
        },
    }
}

/// Run `func(args)` as a root coroutine: in its own scope under `token`,
/// with the VM's grace.  The body of
/// [`Vm::run`](crate::runtime::Vm::run), which documents it.
pub(crate) async fn run_root(
    lua: &Lua,
    token: CancelToken,
    func: Function,
    args: MultiValue,
) -> Result<MultiValue, crate::IsleError> {
    hub::ensure_installed(lua)?;
    let grace = hub::config(lua).grace;
    let parts = crate::protect::parts(lua)?;
    let (root, body) = lua_body(lua, Wrap::Call(parts), func, args)?;
    let scope = Rc::new(Scope::new(token.clone(), grace, None));
    let out = run_in(scope, Some(root), body).await;
    if token.is_cancelled() {
        return Err(crate::IsleError::Cancelled);
    }
    match out {
        Some(Ok(values)) => crate::protect::unwrap_root(lua, values),
        Some(Err(e)) => Err(crate::IsleError::from(e)),
        None => Err(crate::IsleError::Cancelled),
    }
}

/// Convert an `mlua::Error` into a Lua value (for `false, err` results).
pub(crate) fn error_value(e: mlua::Error) -> Value {
    Value::Error(Box::new(e))
}

/// The scope of the request or task currently running, as a handle that
/// host code can spawn tasks into.
///
/// `Some` only while a coroutine request or task is being polled: inside
/// a host function called from Lua (a
/// [`create_function`](mlua::Lua::create_function) body, or the future
/// of a [`create_async_function`](mlua::Lua::create_async_function)),
/// and inside a host task started with [`ScopeHandle::spawn_local`]
/// (there it is that task's own scope).  This holds on both the
/// [`AsyncIsle`](crate::AsyncIsle) path and the
/// [`Vm::run`](crate::runtime::Vm::run) path.  `None`
/// in a sync request (`eval` / `call` / `exec`), which cannot await,
/// and outside any request.
///
/// Take the handle in the synchronous part of the host function (the
/// `create_function` body, or the part of a `create_async_function`
/// that runs before the first `.await`) and move it into the future
/// that uses it.
pub fn current_scope() -> Option<ScopeHandle> {
    current().map(|scope| ScopeHandle { scope })
}

/// The scope of a coroutine request or task, for spawning host tasks
/// into it.  Obtained from [`current_scope`].
///
/// A task spawned through the handle is structured like a Lua task
/// started with `task.spawn` (see [the `task` library](crate::runtime#the-task-library)): when the
/// request or task that owns the scope ends or is cancelled, the host
/// task is cancelled, given the grace, dropped when the grace ends, and
/// waited for before the request resolves.  A host future that never
/// looks at its token is still dropped at the end of the grace.
///
/// Compare `current_token().child_token()` + `tokio::task::spawn_local`,
/// which gives cancellation only: the request neither waits for such a
/// task nor drops it.
///
/// Tasks spawned into a scope that is already ending (its body finished
/// or was cancelled, and it is waiting for its tasks) are cancelled at
/// once and share its remaining time: one grace deadline from the
/// moment the scope started ending, not a fresh grace each.  A spawn
/// after that remaining time is gone starts nothing: the future is
/// dropped at once and the [`ScopedTask`] resolves to
/// `Err(IsleError::Cancelled)`.
///
/// The handle keeps the scope's bookkeeping alive, not the request.
/// Once the scope has waited for its tasks, spawning through a stored
/// handle starts nothing: the future is dropped at once and the
/// returned [`ScopedTask`] resolves to `Err(IsleError::Cancelled)`.
#[derive(Clone)]
pub struct ScopeHandle {
    scope: Rc<Scope>,
}

impl ScopeHandle {
    /// Start `fut` as a task of this scope with
    /// [`tokio::task::spawn_local`] (call it on the `LocalSet` that runs
    /// the request, e.g. from a host function).
    ///
    /// - The task's token is a [child](CancelToken::child_token) of
    ///   [`token`](Self::token).  While `fut` is polled it is the
    ///   [`current_token`](crate::runtime::current_token), so
    ///   [`cancellable`] works inside, and [`current_scope`] returns the
    ///   task's own scope, so tasks spawned from inside are waited for by
    ///   this one.
    /// - When the request or task that owns this scope ends (and the
    ///   task was not awaited to completion) or is cancelled, the task is
    ///   cancelled.  It then gets the VM's cancel grace, as part of the
    ///   same deadline as the rest of the tree (a task spawned during
    ///   cleanup gets the time that remains), and is dropped when the
    ///   grace ends.  The request resolves only after it has finished or
    ///   been dropped.
    ///
    /// The grace is the one the owning request or task was started
    /// with.
    ///
    /// Dropping the returned [`ScopedTask`] cancels the task.  Await it
    /// for the value, keep it for as long as the task should run, or
    /// [`detach`](ScopedTask::detach) it to let the task run on in the
    /// scope without a handle.
    ///
    /// A panic in `fut` is caught by tokio: the task counts as finished,
    /// its own tasks are aborted, and the [`ScopedTask`] resolves to
    /// `Err(IsleError::Cancelled)`.  A host that needs the panic payload
    /// catches it inside the future.
    ///
    /// Spawn on the `LocalSet` that runs the request.  Spawning on a
    /// different `LocalSet` on the same thread leaves the scope waiting
    /// for a task that nobody polls.
    ///
    /// # Panics
    ///
    /// While the scope is still running, this calls
    /// [`tokio::task::spawn_local`], which panics outside a `LocalSet`.
    pub fn spawn_local<F, T>(&self, fut: F) -> ScopedTask<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let Spawned { state, result } = self.scope.spawn(self.scope.grace, None, fut, |out, _| out);
        ScopedTask {
            state,
            result,
            wait: None,
            detached: false,
        }
    }

    /// The token of the request or task that owns this scope.  The
    /// tokens of tasks spawned through the handle are its children.
    pub fn token(&self) -> &CancelToken {
        &self.scope.token
    }
}

impl std::fmt::Debug for ScopeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeHandle")
            .field("token", &self.scope.token)
            .field("phase", &self.scope.phase.get())
            .finish_non_exhaustive()
    }
}

/// A host task started with [`ScopeHandle::spawn_local`].
///
/// Await it for the task's output: `Ok(value)` when the future finished,
/// `Err(IsleError::Cancelled)` when the task was cancelled (or dropped)
/// before it finished.
///
/// Three ways to let go of it:
///
/// - **Await it** to wait for the value.
/// - **Drop it** to cancel the task now.  The drop does not wait; the
///   scope's wait is the guarantee, so the request still resolves only
///   after the task has finished or been dropped.
/// - **[`detach`](Self::detach) it** to let the task run on without a
///   handle, as a fire-and-forget task of its scope.
///
/// A panic in the task's future is caught by tokio: the task counts as
/// finished, its own tasks are aborted, and the handle resolves to
/// `Err(IsleError::Cancelled)`.  Catch the panic inside the future if
/// the payload is needed.
#[must_use = "dropping a ScopedTask cancels the task; use .detach() to let it run"]
pub struct ScopedTask<T> {
    state: Rc<TaskState>,
    result: Rc<RefCell<Option<T>>>,
    wait: Option<Pin<Box<dyn Future<Output = ()>>>>,
    /// Set by `detach`: the drop then leaves the task alone.
    detached: bool,
}

impl<T> ScopedTask<T> {
    /// Let the task run on without this handle.
    ///
    /// Consumes the handle without cancelling the task; its result is
    /// discarded.  The task stays in its scope: when the request or task
    /// that owns the scope ends or is cancelled, the detached task is
    /// still cancelled, given the grace, dropped when the grace ends,
    /// and waited for before the request resolves.
    ///
    /// Compare dropping the handle, which cancels the task now, and
    /// awaiting it, which waits for the value.
    pub fn detach(mut self) {
        self.detached = true;
    }
}

impl<T> Future for ScopedTask<T> {
    type Output = Result<T, IsleError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if !this.state.is_done() {
            if this.wait.is_none() {
                let state = this.state.clone();
                this.wait = Some(Box::pin(async move { state.wait_done().await }));
            }
            if let Some(wait) = this.wait.as_mut() {
                if wait.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
            }
        }
        this.wait = None;
        Poll::Ready(this.result.borrow_mut().take().ok_or(IsleError::Cancelled))
    }
}

impl<T> Drop for ScopedTask<T> {
    fn drop(&mut self) {
        if !self.detached && !self.state.is_done() {
            self.state.token.cancel();
        }
    }
}

impl<T> std::fmt::Debug for ScopedTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedTask")
            .field("token", &self.state.token)
            .field("finished", &self.state.is_done())
            .field("detached", &self.detached)
            .finish_non_exhaustive()
    }
}
