//! Isle — the public handle for interacting with the Lua thread.

use crate::error::{panic_message, IsleError, LuaFailure};
use crate::hook::CancelToken;
use crate::task::Task;
use crate::thread;
use crate::Request;
use std::sync::mpsc;
use std::sync::Mutex;
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
    join: Mutex<Option<JoinHandle<()>>>,
}

impl Isle {
    /// Spawn a new Lua VM on a dedicated thread.
    ///
    /// The `init` closure runs on the Lua thread before any requests
    /// are processed.  Use it to register globals, install mlua-pkg
    /// resolvers, load mlua-batteries, etc.
    ///
    /// # Errors
    ///
    /// Returns [`IsleError::Init`] if the init closure fails (or the OS
    /// refuses to start the thread), and [`IsleError::ThreadPanic`] with
    /// the panic message if the init closure panics.
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
                // Before `init`: captures `xpcall` while the globals are
                // intact (see `protect`).
                match crate::protect::install(&lua)
                    .and_then(|()| {
                        init(&lua).map_err(|e| IsleError::Init(LuaFailure::from_mlua(&e)))
                    })
                    .and_then(|()| crate::runtime::attach_after_init(&lua, None).map(drop))
                {
                    Ok(()) => {
                        let _ = init_tx.send(Ok(()));
                        thread::run_loop(lua, rx);
                    }
                    Err(e) => {
                        let _ = init_tx.send(Err(e));
                    }
                }
            })
            .map_err(|e| IsleError::Init(LuaFailure::from_mlua(&mlua::Error::external(e))))?;

        // Wait for init to complete.  A closed channel means the thread
        // ended without reporting: the init closure panicked.
        match init_rx.recv() {
            Ok(result) => result?,
            Err(_) => {
                let payload = join.join().err();
                return Err(IsleError::ThreadPanic(
                    payload.as_deref().and_then(panic_message),
                ));
            }
        }

        Ok(Self {
            tx,
            join: Mutex::new(Some(join)),
        })
    }

    /// Evaluate a Lua chunk (blocking) and convert its return values
    /// to `T`.  Equivalent to `spawn_eval(code).wait()`.
    ///
    /// The conversion ([`FromLuaMulti`](mlua::FromLuaMulti)) runs on the
    /// Lua thread, so any `T: FromLuaMulti + Send + 'static` works: a
    /// single value (`String`, `i64`, `f64`, `bool`,
    /// [`BString`](mlua::BString) for the raw bytes of a Lua string), `()`
    /// to ignore the result, `Option<T>` for a result that may be `nil`,
    /// or a tuple for several return values.  Values tied to the VM
    /// (`Table`, `Function`, `Value`, `MultiValue`) are not `Send`
    /// without mlua's `send` feature and then do not compile here; use
    /// [`exec`](Self::exec) to convert them on the Lua thread, or ask for
    /// a [`RegistryKey`](mlua::RegistryKey), which is `Send`, and read
    /// the value back in a later `exec`.
    ///
    /// ```rust
    /// # use mlua_isle::Isle;
    /// let isle = Isle::spawn(|_| Ok(())).unwrap();
    /// let n: i64 = isle.eval("return 1 + 1").unwrap();
    /// assert_eq!(n, 2);
    /// let (a, b) = isle.eval::<(i64, String)>("return 1, 'x'").unwrap();
    /// assert_eq!((a, b.as_str()), (1, "x"));
    /// assert_eq!(isle.eval::<Option<String>>("return nil").unwrap(), None);
    /// # isle.shutdown().unwrap();
    /// ```
    ///
    /// A Lua error is [`IsleError::Lua`], and so is a return value that
    /// does not convert to `T` (kind
    /// [`Conversion`](crate::LuaErrorKind::Conversion)).  A `FromLua` of
    /// your own that fails with an error raised by Lua code it runs (a
    /// `__index` metamethod, say) is `Lua` with that error's kind, not
    /// `Conversion`; the conversion runs under the request's token, so
    /// a cancel while it runs Lua code is `Cancelled`.  When the
    /// request's token was cancelled, the result is
    /// [`IsleError::Cancelled`] even if the Lua code caught the cancel
    /// and raised an error of its own (the same rule as coroutine
    /// requests and `Vm::run`); this holds for `call` too.
    pub fn eval<T>(&self, code: &str) -> Result<T, IsleError>
    where
        T: mlua::FromLuaMulti + Send + 'static,
    {
        self.spawn_eval(code).wait()
    }

    /// Evaluate a Lua chunk, returning a cancellable [`Task`].
    pub fn spawn_eval<T>(&self, code: &str) -> Task<T>
    where
        T: mlua::FromLuaMulti + Send + 'static,
    {
        let code = code.to_string();
        self.submit(move |lua, cancel| thread::execute_eval(lua, &code, cancel))
    }

    /// Call a named global Lua function (blocking) and convert its
    /// return values to `T`.
    ///
    /// `args` is converted on the Lua thread
    /// ([`IntoLuaMulti`](mlua::IntoLuaMulti)): `()` for no arguments, a
    /// value, a tuple such as `(1, true, "x")`, or a
    /// [`Variadic`](mlua::Variadic) for a number of arguments known only
    /// at run time (a `Vec` is one argument, a table).  The result
    /// follows the rules of [`eval`](Self::eval).  A global that is not
    /// a function is [`IsleError::NotFound`].
    pub fn call<A, T>(&self, func: &str, args: A) -> Result<T, IsleError>
    where
        A: mlua::IntoLuaMulti + Send + 'static,
        T: mlua::FromLuaMulti + Send + 'static,
    {
        self.spawn_call(func, args).wait()
    }

    /// Call a named global Lua function, returning a cancellable [`Task`].
    pub fn spawn_call<A, T>(&self, func: &str, args: A) -> Task<T>
    where
        A: mlua::IntoLuaMulti + Send + 'static,
        T: mlua::FromLuaMulti + Send + 'static,
    {
        let func = func.to_string();
        self.submit(move |lua, cancel| thread::execute_call(lua, &func, args, cancel))
    }

    /// Execute an arbitrary closure on the Lua thread (blocking).
    ///
    /// The closure receives `&Lua` and can perform any operation; its
    /// `T` crosses back as is (no conversion).  This is the escape
    /// hatch for complex interactions that don't fit into `eval` or
    /// `call`, and the place to turn a value tied to the VM (a `Table`)
    /// into a `Send` one.  `?` works on `mlua::Result` inside the
    /// closure (`IsleError: From<mlua::Error>`).
    ///
    /// **Note:** The cancel hook only fires during Lua instruction
    /// execution.  If the closure blocks in Rust code (e.g. HTTP
    /// calls, file I/O), cancellation will not take effect until
    /// control returns to the Lua VM.
    pub fn exec<F, T>(&self, f: F) -> Result<T, IsleError>
    where
        F: FnOnce(&mlua::Lua) -> Result<T, IsleError> + Send + 'static,
        T: Send + 'static,
    {
        self.spawn_exec(f).wait()
    }

    /// Execute a closure on the Lua thread, returning a cancellable [`Task`].
    pub fn spawn_exec<F, T>(&self, f: F) -> Task<T>
    where
        F: FnOnce(&mlua::Lua) -> Result<T, IsleError> + Send + 'static,
        T: Send + 'static,
    {
        self.submit(move |lua, cancel| thread::execute_exec(lua, f, cancel))
    }

    /// Send a job that runs `run` on the Lua thread and sends its
    /// result back through the returned task's channel.
    fn submit<T, R>(&self, run: R) -> Task<T>
    where
        T: Send + 'static,
        R: FnOnce(&mlua::Lua, &CancelToken) -> Result<T, IsleError> + Send + 'static,
    {
        let cancel = CancelToken::new();
        let (resp_tx, resp_rx) = mpsc::channel();

        let req = Request::Run {
            job: Box::new(move |lua, cancel| {
                let _ = resp_tx.send(run(lua, cancel));
            }),
            cancel: cancel.clone(),
        };

        if self.tx.send(req).is_err() {
            // Channel closed — return a task that immediately errors
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
    ///
    /// # Errors
    ///
    /// [`IsleError::ThreadPanic`] if the Lua thread panicked (a request
    /// whose Rust code panicked ends the thread; that request itself
    /// returns [`IsleError::RecvFailed`]), with the panic message when
    /// the payload is a `&str` or a `String`.
    pub fn shutdown(self) -> Result<(), IsleError> {
        let _ = self.tx.send(Request::Shutdown);
        let handle = self
            .join
            .lock()
            .map_err(|_| IsleError::ThreadPanic(None))?
            .take();
        if let Some(join) = handle {
            join.join()
                .map_err(|p| IsleError::ThreadPanic(panic_message(p.as_ref())))?;
        }
        Ok(())
    }

    /// Check if the Lua thread is still alive.
    pub fn is_alive(&self) -> bool {
        self.join
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|j| !j.is_finished()))
            .unwrap_or(false)
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
