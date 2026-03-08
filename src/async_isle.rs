//! Async handle and driver for the thread-isolated Lua VM.
//!
//! This module follows the **Handle / Driver** separation pattern
//! (see [tokio `Runtime` / `Handle`][tokio-handle], [Alice Ryhl's actor
//! pattern][actor]):
//!
//! - [`AsyncIsle`] is a **lightweight, cloneable handle** that sends
//!   requests to the Lua thread.  It can be shared across tasks without
//!   `Arc`.
//! - [`AsyncIsleDriver`] is the **lifecycle owner** that holds the OS
//!   thread's `JoinHandle` and provides [`shutdown`](AsyncIsleDriver::shutdown).
//!
//! [tokio-handle]: https://docs.rs/tokio/latest/tokio/runtime/struct.Handle.html
//! [actor]: https://ryhl.io/blog/actors-with-tokio/
//!
//! # Design rationale
//!
//! The Lua VM runs on a dedicated **OS thread** (`Lua` is `!Send`).
//! Communication uses a bounded `tokio::sync::mpsc` channel so callers
//! get backpressure ([`IsleError::ChannelFull`]) rather than unbounded
//! memory growth.
//!
//! Separating Handle from Driver achieves:
//!
//! 1. **SRP** — Handle handles communication; Driver handles lifecycle.
//! 2. **Clone without Arc** — Handle is cheap to clone (mpsc `Sender`
//!    clone).  No `Arc<AsyncIsle>` needed.
//! 3. **Deterministic shutdown** — Driver owns the `JoinHandle`, so
//!    exactly one owner calls `shutdown`.
//! 4. **Natural channel-close** — When all Handle clones *and* the
//!    Driver are dropped, every `Sender` clone is dropped, the channel
//!    disconnects, `blocking_recv` returns `None`, and the Lua thread
//!    exits.  The Driver does **not** send a `Shutdown` message on
//!    drop ([matklad: "in Rust, cancellation is drop"][matklad-stop]).
//!    This prevents premature thread termination while active Handle
//!    clones still exist.
//!
//! [matklad-stop]: https://matklad.github.io/2018/03/03/stopping-a-rust-worker.html
//!
//! ## Thread completion notification
//!
//! [`AsyncIsleDriver::shutdown`] must await the Lua thread's
//! termination without consuming a tokio blocking-pool thread.
//! `tokio::task::spawn_blocking` is
//! [documented][spawn-blocking-doc] as intended for **short-lived**
//! blocking operations; long-lived waits can exhaust the pool
//! (default cap ≈ 512) and starve other `spawn_blocking` callers.
//!
//! Instead, the Lua thread sends a **oneshot completion signal**
//! (`done_tx`) just before it exits.  `shutdown` awaits `done_rx`
//! (pure async, zero pool threads consumed), then calls
//! `JoinHandle::join()` which returns **immediately** because the
//! thread has already exited.  This follows the [tokio bridging
//! guide][bridging]'s recommendation of using channels for
//! sync→async completion notification.
//!
//! [spawn-blocking-doc]: https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html
//! [bridging]: https://tokio.rs/tokio/topics/bridging
//!
//! # Example
//!
//! ```rust
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use mlua_isle::AsyncIsle;
//!
//! let (isle, driver) = AsyncIsle::spawn(|lua| {
//!     lua.globals().set("greeting", "hello")?;
//!     Ok(())
//! }).await?;
//!
//! // Clone freely — no Arc needed.
//! let isle2 = isle.clone();
//!
//! let result: String = isle.eval("return greeting").await?;
//! assert_eq!(result, "hello");
//!
//! // Explicit clean shutdown (joins the OS thread).
//! driver.shutdown().await?;
//! # Ok(())
//! # }
//! ```

use crate::async_task::AsyncTask;
use crate::error::IsleError;
use crate::hook::CancelToken;
use crate::thread;
use std::thread::JoinHandle;

/// Closure type for async exec requests.
type AsyncExecFn = Box<dyn FnOnce(&mlua::Lua) -> Result<String, IsleError> + Send>;

/// Response sender (oneshot).
type AsyncResultTx = tokio::sync::oneshot::Sender<Result<String, IsleError>>;

/// Request sent from async callers to the Lua thread.
enum AsyncRequest {
    Eval {
        code: String,
        cancel: CancelToken,
        tx: AsyncResultTx,
    },
    Call {
        func: String,
        args: Vec<String>,
        cancel: CancelToken,
        tx: AsyncResultTx,
    },
    Exec {
        f: AsyncExecFn,
        cancel: CancelToken,
        tx: AsyncResultTx,
    },
    Shutdown,
}

/// Default capacity for the request channel.
///
/// The Lua thread processes requests sequentially, so this acts as a
/// backpressure limit.  If the channel is full, `spawn_*` methods
/// return [`IsleError::ChannelFull`] immediately rather than risk OOM.
const DEFAULT_CHANNEL_CAPACITY: usize = 256;

/// Cloneable async handle to a thread-isolated Lua VM.
///
/// `AsyncIsle` holds only a channel sender and can be freely cloned
/// to share across tokio tasks — no `Arc` wrapper needed.
///
/// All request methods are non-blocking: they enqueue a message via
/// [`try_send`](tokio::sync::mpsc::Sender::try_send) and return an
/// [`AsyncTask`] (which implements [`Future`](std::future::Future)).
///
/// # Lifecycle
///
/// 1. [`AsyncIsle::spawn`] creates the Lua VM and returns
///    `(AsyncIsle, AsyncIsleDriver)`.
/// 2. Clone and distribute the `AsyncIsle` handle.
/// 3. Use [`eval`](Self::eval), [`call`](Self::call), or
///    [`exec`](Self::exec) from any task.
/// 4. Call [`AsyncIsleDriver::shutdown`] for a clean thread join,
///    or simply drop everything to let the channel-close mechanism
///    terminate the Lua thread.
#[derive(Clone)]
pub struct AsyncIsle {
    tx: tokio::sync::mpsc::Sender<AsyncRequest>,
}

/// Lifecycle driver for the async Lua VM thread.
///
/// `AsyncIsleDriver` is the sole owner of the OS thread's
/// [`JoinHandle`](std::thread::JoinHandle).  It is **not** `Clone`.
///
/// # Shutdown
///
/// - **Explicit**: Call [`shutdown`](Self::shutdown) to send a
///   `Shutdown` message, await the thread's completion, and join it.
/// - **Implicit (drop)**: The Driver's `Sender` clone is dropped.
///   The Lua thread continues serving remaining [`AsyncIsle`] handles.
///   When **all** handles *and* the Driver have been dropped, the
///   channel disconnects, `blocking_recv` returns `None`, and the
///   thread exits naturally.  See ["in Rust, cancellation is
///   drop"][matklad-stop].
///
/// The Driver does **not** send a `Shutdown` message on drop.
/// Doing so would terminate the Lua thread while other Handle
/// clones may still be actively sending requests.
///
/// [matklad-stop]: https://matklad.github.io/2018/03/03/stopping-a-rust-worker.html
///
/// # Thread completion strategy
///
/// `shutdown` does **not** use [`tokio::task::spawn_blocking`] to
/// wait for `JoinHandle::join()`.  `spawn_blocking` is intended for
/// short-lived blocking work; waiting for an OS thread that may still
/// be draining a 256-deep request queue would occupy a blocking-pool
/// thread for an unbounded duration, risking pool exhaustion.
///
/// Instead, the Lua thread sends a oneshot signal (`done_tx`) just
/// before it exits.  `shutdown` awaits the signal (pure async), then
/// calls `join()` on the already-finished thread (returns instantly).
/// See the [tokio bridging guide] for this pattern.
///
/// [tokio bridging guide]: https://tokio.rs/tokio/topics/bridging
#[must_use = "call .shutdown().await for clean thread join; dropping without shutdown detaches the thread"]
pub struct AsyncIsleDriver {
    tx: tokio::sync::mpsc::Sender<AsyncRequest>,
    done_rx: Option<tokio::sync::oneshot::Receiver<()>>,
    join: Option<JoinHandle<()>>,
}

/// Builder for [`AsyncIsle`] with configurable parameters.
///
/// Use [`AsyncIsle::builder`] to create.  Call [`spawn`](Self::spawn)
/// to create the Lua VM with the configured settings.
///
/// # Example
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use mlua_isle::AsyncIsle;
///
/// let (isle, driver) = AsyncIsle::builder()
///     .channel_capacity(64)
///     .thread_name("my-lua-worker")
///     .spawn(|_lua| Ok(()))
///     .await?;
///
/// driver.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct AsyncIsleBuilder {
    channel_capacity: usize,
    thread_name: String,
}

impl Default for AsyncIsleBuilder {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            thread_name: "mlua-isle-async".into(),
        }
    }
}

impl AsyncIsleBuilder {
    /// Set the bounded channel capacity (backpressure limit).
    ///
    /// When the channel is full, `spawn_*` methods return
    /// [`IsleError::ChannelFull`] immediately.
    ///
    /// Default: 256.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Set the OS thread name (visible in debuggers and `top`/`htop`).
    ///
    /// Default: `"mlua-isle-async"`.
    pub fn thread_name(mut self, name: &str) -> Self {
        self.thread_name = name.to_string();
        self
    }

    /// Spawn the Lua VM with the configured settings.
    ///
    /// See [`AsyncIsle::spawn`] for details.
    pub async fn spawn<F>(self, init: F) -> Result<(AsyncIsle, AsyncIsleDriver), IsleError>
    where
        F: FnOnce(&mlua::Lua) -> Result<(), mlua::Error> + Send + 'static,
    {
        AsyncIsle::spawn_inner(init, self.channel_capacity, self.thread_name).await
    }
}

impl AsyncIsle {
    /// Create a builder for configuring the async isle.
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use mlua_isle::AsyncIsle;
    ///
    /// let (isle, driver) = AsyncIsle::builder()
    ///     .channel_capacity(128)
    ///     .spawn(|_lua| Ok(()))
    ///     .await?;
    /// # driver.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder() -> AsyncIsleBuilder {
        AsyncIsleBuilder::default()
    }

    /// Spawn a new Lua VM on a dedicated thread with default settings.
    ///
    /// Returns `(handle, driver)`:
    /// - **handle** ([`AsyncIsle`]) — clone and share freely.
    /// - **driver** ([`AsyncIsleDriver`]) — call
    ///   [`shutdown`](AsyncIsleDriver::shutdown) when done.
    ///
    /// The `init` closure runs on the Lua thread before any requests
    /// are processed.  Use it to register globals, load libraries, etc.
    ///
    /// For custom settings, use [`AsyncIsle::builder`].
    ///
    /// # Errors
    ///
    /// Returns [`IsleError::Init`] if the init closure fails.
    pub async fn spawn<F>(init: F) -> Result<(Self, AsyncIsleDriver), IsleError>
    where
        F: FnOnce(&mlua::Lua) -> Result<(), mlua::Error> + Send + 'static,
    {
        Self::spawn_inner(init, DEFAULT_CHANNEL_CAPACITY, "mlua-isle-async".into()).await
    }

    async fn spawn_inner<F>(
        init: F,
        channel_capacity: usize,
        thread_name: String,
    ) -> Result<(Self, AsyncIsleDriver), IsleError>
    where
        F: FnOnce(&mlua::Lua) -> Result<(), mlua::Error> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel::<AsyncRequest>(channel_capacity);
        let (init_tx, init_rx) = tokio::sync::oneshot::channel::<Result<(), IsleError>>();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

        let join = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let lua = mlua::Lua::new();
                match init(&lua) {
                    Ok(()) => {
                        let _ = init_tx.send(Ok(()));
                        run_async_loop(lua, rx);
                    }
                    Err(e) => {
                        let _ = init_tx.send(Err(IsleError::Init(e.to_string())));
                    }
                }
                // Signal completion to the driver.
                // Sent regardless of init success/failure so that
                // `shutdown().await` never hangs.
                let _ = done_tx.send(());
            })
            .map_err(|e| IsleError::Init(format!("thread spawn failed: {e}")))?;

        init_rx
            .await
            .map_err(|e| IsleError::Init(format!("init channel closed: {e}")))??;

        let handle = Self { tx: tx.clone() };
        let driver = AsyncIsleDriver {
            tx,
            done_rx: Some(done_rx),
            join: Some(join),
        };

        Ok((handle, driver))
    }

    /// Evaluate a Lua chunk (async).
    ///
    /// Equivalent to `spawn_eval(code).await`.
    pub async fn eval(&self, code: &str) -> Result<String, IsleError> {
        self.spawn_eval(code).await
    }

    /// Evaluate a Lua chunk, returning a cancellable [`AsyncTask`].
    ///
    /// The returned task implements [`Future`](std::future::Future) —
    /// `.await` it to get the result.
    pub fn spawn_eval(&self, code: &str) -> AsyncTask {
        let cancel = CancelToken::new();
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();

        let req = AsyncRequest::Eval {
            code: code.to_string(),
            cancel: cancel.clone(),
            tx: resp_tx,
        };

        match self.tx.try_send(req) {
            Ok(()) => AsyncTask::new(resp_rx, cancel),
            Err(e) => make_error_task(try_send_to_isle_error(e), cancel),
        }
    }

    /// Call a named global Lua function with string arguments (async).
    ///
    /// Equivalent to `spawn_call(func, args).await`.
    pub async fn call(&self, func: &str, args: &[&str]) -> Result<String, IsleError> {
        self.spawn_call(func, args).await
    }

    /// Call a named global Lua function, returning a cancellable [`AsyncTask`].
    pub fn spawn_call(&self, func: &str, args: &[&str]) -> AsyncTask {
        let cancel = CancelToken::new();
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();

        let req = AsyncRequest::Call {
            func: func.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
            cancel: cancel.clone(),
            tx: resp_tx,
        };

        match self.tx.try_send(req) {
            Ok(()) => AsyncTask::new(resp_rx, cancel),
            Err(e) => make_error_task(try_send_to_isle_error(e), cancel),
        }
    }

    /// Execute an arbitrary closure on the Lua thread (async).
    ///
    /// Equivalent to `spawn_exec(f).await`.
    pub async fn exec<F>(&self, f: F) -> Result<String, IsleError>
    where
        F: FnOnce(&mlua::Lua) -> Result<String, IsleError> + Send + 'static,
    {
        self.spawn_exec(f).await
    }

    /// Execute a closure on the Lua thread, returning a cancellable [`AsyncTask`].
    pub fn spawn_exec<F>(&self, f: F) -> AsyncTask
    where
        F: FnOnce(&mlua::Lua) -> Result<String, IsleError> + Send + 'static,
    {
        let cancel = CancelToken::new();
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();

        let req = AsyncRequest::Exec {
            f: Box::new(f),
            cancel: cancel.clone(),
            tx: resp_tx,
        };

        match self.tx.try_send(req) {
            Ok(()) => AsyncTask::new(resp_rx, cancel),
            Err(e) => make_error_task(try_send_to_isle_error(e), cancel),
        }
    }

    /// Check if the Lua thread is still alive.
    ///
    /// Returns `false` once the channel is closed (i.e. the Lua thread
    /// has exited or is in the process of exiting).
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }
}

impl AsyncIsleDriver {
    /// Graceful shutdown: send a `Shutdown` message and join the OS thread.
    ///
    /// This method:
    /// 1. Sends `Shutdown` via the mpsc channel (respecting backpressure).
    /// 2. Awaits the **oneshot completion signal** from the Lua thread
    ///    — pure async, **no blocking-pool thread consumed**.
    /// 3. Calls `JoinHandle::join()` on the already-exited thread
    ///    (returns immediately).
    ///
    /// After shutdown, all [`AsyncIsle`] handles' requests will return
    /// [`IsleError::Shutdown`].
    ///
    /// # Why not `spawn_blocking`?
    ///
    /// `tokio::task::spawn_blocking` is [intended for short-lived
    /// blocking work][sb].  Waiting for `JoinHandle::join()` on a Lua
    /// thread that is still draining up to 256 queued requests would
    /// occupy a blocking-pool thread for an unbounded duration,
    /// risking **pool exhaustion** and starving other
    /// `spawn_blocking` callers.
    ///
    /// [sb]: https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html
    ///
    /// # Errors
    ///
    /// Returns [`IsleError::ThreadPanic`] if the Lua thread panicked
    /// or the join operation fails.
    pub async fn shutdown(mut self) -> Result<(), IsleError> {
        // Use .send().await to respect backpressure instead of try_send,
        // which would silently drop the shutdown signal when the channel is full.
        let _ = self.tx.send(AsyncRequest::Shutdown).await;

        // Await the Lua thread's completion signal (pure async).
        if let Some(done_rx) = self.done_rx.take() {
            done_rx.await.map_err(|_| IsleError::ThreadPanic)?;
        }

        // The thread has already exited — join() returns immediately.
        if let Some(join) = self.join.take() {
            join.join().map_err(|_| IsleError::ThreadPanic)?;
        }

        Ok(())
    }

    /// Check if the Lua thread is still alive.
    pub fn is_alive(&self) -> bool {
        self.join.as_ref().is_some_and(|j| !j.is_finished())
    }
}

// No Drop impl: the Driver does NOT send Shutdown on drop.
//
// Rationale ("in Rust, cancellation is drop" — matklad):
//   Sending Shutdown would kill the Lua thread while other
//   AsyncIsle Handle clones may still be sending requests.
//   Instead, the Driver's tx is simply dropped, reducing the
//   sender reference count.  When ALL senders (Handles + Driver)
//   are dropped, the channel disconnects and blocking_recv()
//   returns None, exiting the Lua thread naturally.

// ── helpers ──────────────────────────────────────────────────────────

/// Create an [`AsyncTask`] that resolves to an error immediately.
fn make_error_task(err: IsleError, cancel: CancelToken) -> AsyncTask {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = tx.send(Err(err));
    AsyncTask::new(rx, cancel)
}

/// Map a [`try_send`](tokio::sync::mpsc::Sender::try_send) error to the
/// appropriate [`IsleError`] variant.
///
/// - [`TrySendError::Full`] → [`IsleError::ChannelFull`] (transient)
/// - [`TrySendError::Closed`] → [`IsleError::Shutdown`] (permanent)
fn try_send_to_isle_error<T>(err: tokio::sync::mpsc::error::TrySendError<T>) -> IsleError {
    match err {
        tokio::sync::mpsc::error::TrySendError::Full(_) => IsleError::ChannelFull,
        tokio::sync::mpsc::error::TrySendError::Closed(_) => IsleError::Shutdown,
    }
}

/// Lua event loop for async requests (runs on the dedicated Lua thread).
fn run_async_loop(lua: mlua::Lua, mut rx: tokio::sync::mpsc::Receiver<AsyncRequest>) {
    while let Some(req) = rx.blocking_recv() {
        match req {
            AsyncRequest::Eval { code, cancel, tx } => {
                let result = thread::execute_eval(&lua, &code, &cancel);
                let _ = tx.send(result);
            }
            AsyncRequest::Call {
                func,
                args,
                cancel,
                tx,
            } => {
                let result = thread::execute_call(&lua, &func, &args, &cancel);
                let _ = tx.send(result);
            }
            AsyncRequest::Exec { f, cancel, tx } => {
                let result = thread::execute_exec(&lua, f, &cancel);
                let _ = tx.send(result);
            }
            AsyncRequest::Shutdown => break,
        }
    }
}
