//! Isle — the public handle for interacting with the Lua thread.

use crate::error::IsleError;
use crate::hook::CancelToken;
use crate::task::Task;
use crate::thread;
use crate::Request;
use std::sync::mpsc;
use std::thread::JoinHandle;

/// Handle to a thread-isolated Lua VM.
///
/// `Isle` owns the communication channel and the join handle for the
/// Lua thread.  All operations are thread-safe (`Isle: Send + Sync`).
///
/// # Lifecycle
///
/// 1. [`Isle::spawn`] creates the Lua VM on a dedicated thread.
/// 2. Use [`eval`](Isle::eval), [`call`](Isle::call), or [`exec`](Isle::exec)
///    to run code.
/// 3. [`shutdown`](Isle::shutdown) sends a graceful stop signal and
///    joins the thread.
///
/// If the `Isle` is dropped without calling `shutdown`, the channel
/// disconnects and the Lua thread exits on its next receive attempt.
#[must_use = "use .shutdown() for clean thread join; dropping without shutdown leaks the thread"]
pub struct Isle {
    tx: mpsc::Sender<Request>,
    join: Option<JoinHandle<()>>,
}

// SAFETY: `Isle` contains `mpsc::Sender<Request>` and `Option<JoinHandle<()>>`.
// - `mpsc::Sender::send(&self)` uses internal synchronization (Mutex) and is
//   safe to call concurrently from multiple threads.
// - `JoinHandle` is only accessed mutably in `shutdown(mut self)` which takes
//   ownership, preventing concurrent access.
// - The `Lua` VM itself never leaves its dedicated thread; `Isle` only holds
//   the channel endpoint, not the VM.
unsafe impl Sync for Isle {}

impl Isle {
    /// Spawn a new Lua VM on a dedicated thread.
    ///
    /// The `init` closure runs on the Lua thread before any requests
    /// are processed.  Use it to register globals, install mlua-pkg
    /// resolvers, load mlua-batteries, etc.
    ///
    /// # Errors
    ///
    /// Returns [`IsleError::Init`] if the init closure fails.
    pub fn spawn<F>(init: F) -> Result<Self, IsleError>
    where
        F: FnOnce(&mlua::Lua) -> Result<(), mlua::Error> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<Request>();
        let (init_tx, init_rx) = mpsc::channel::<Result<(), IsleError>>();

        let join = std::thread::Builder::new()
            .name("mlua-isle".into())
            .spawn(move || {
                let lua = mlua::Lua::new();
                match init(&lua) {
                    Ok(()) => {
                        let _ = init_tx.send(Ok(()));
                        thread::run_loop(lua, rx);
                    }
                    Err(e) => {
                        let _ = init_tx.send(Err(IsleError::Init(e.to_string())));
                    }
                }
            })
            .map_err(|e| IsleError::Init(format!("thread spawn failed: {e}")))?;

        // Wait for init to complete
        init_rx
            .recv()
            .map_err(|e| IsleError::Init(format!("init channel closed: {e}")))??;

        Ok(Self {
            tx,
            join: Some(join),
        })
    }

    /// Evaluate a Lua chunk (blocking).
    ///
    /// Returns the result as a string.  Equivalent to
    /// `spawn_eval(code).wait()`.
    pub fn eval(&self, code: &str) -> Result<String, IsleError> {
        self.spawn_eval(code).wait()
    }

    /// Evaluate a Lua chunk, returning a cancellable [`Task`].
    pub fn spawn_eval(&self, code: &str) -> Task {
        let cancel = CancelToken::new();
        let (resp_tx, resp_rx) = mpsc::channel();

        let req = Request::Eval {
            code: code.to_string(),
            cancel: cancel.clone(),
            tx: resp_tx,
        };

        if self.tx.send(req).is_err() {
            // Channel closed — return a task that immediately errors
            let (err_tx, err_rx) = mpsc::channel();
            let _ = err_tx.send(Err(IsleError::Shutdown));
            return Task::new(err_rx, cancel);
        }

        Task::new(resp_rx, cancel)
    }

    /// Call a named global Lua function with string arguments (blocking).
    pub fn call(&self, func: &str, args: &[&str]) -> Result<String, IsleError> {
        self.spawn_call(func, args).wait()
    }

    /// Call a named global Lua function, returning a cancellable [`Task`].
    pub fn spawn_call(&self, func: &str, args: &[&str]) -> Task {
        let cancel = CancelToken::new();
        let (resp_tx, resp_rx) = mpsc::channel();

        let req = Request::Call {
            func: func.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
            cancel: cancel.clone(),
            tx: resp_tx,
        };

        if self.tx.send(req).is_err() {
            let (err_tx, err_rx) = mpsc::channel();
            let _ = err_tx.send(Err(IsleError::Shutdown));
            return Task::new(err_rx, cancel);
        }

        Task::new(resp_rx, cancel)
    }

    /// Execute an arbitrary closure on the Lua thread (blocking).
    ///
    /// The closure receives `&Lua` and can perform any operation.
    /// This is the escape hatch for complex interactions that don't
    /// fit into `eval` or `call`.
    ///
    /// **Note:** The cancel hook only fires during Lua instruction
    /// execution.  If the closure blocks in Rust code (e.g. HTTP
    /// calls, file I/O), cancellation will not take effect until
    /// control returns to the Lua VM.
    pub fn exec<F>(&self, f: F) -> Result<String, IsleError>
    where
        F: FnOnce(&mlua::Lua) -> Result<String, IsleError> + Send + 'static,
    {
        self.spawn_exec(f).wait()
    }

    /// Execute a closure on the Lua thread, returning a cancellable [`Task`].
    pub fn spawn_exec<F>(&self, f: F) -> Task
    where
        F: FnOnce(&mlua::Lua) -> Result<String, IsleError> + Send + 'static,
    {
        let cancel = CancelToken::new();
        let (resp_tx, resp_rx) = mpsc::channel();

        let req = Request::Exec {
            f: Box::new(f),
            cancel: cancel.clone(),
            tx: resp_tx,
        };

        if self.tx.send(req).is_err() {
            let (err_tx, err_rx) = mpsc::channel();
            let _ = err_tx.send(Err(IsleError::Shutdown));
            return Task::new(err_rx, cancel);
        }

        Task::new(resp_rx, cancel)
    }

    /// Graceful shutdown: signal the Lua thread to exit and join it.
    ///
    /// After shutdown, all subsequent requests will return
    /// [`IsleError::Shutdown`].
    pub fn shutdown(mut self) -> Result<(), IsleError> {
        let _ = self.tx.send(Request::Shutdown);
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

impl Drop for Isle {
    fn drop(&mut self) {
        // Send shutdown signal; ignore errors (channel may already be closed)
        let _ = self.tx.send(Request::Shutdown);
        // Don't join on drop — let the thread exit on its own.
        // Use explicit shutdown() for a clean join.
    }
}
