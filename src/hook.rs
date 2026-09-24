//! Cancellation token and Lua debug hook.
//!
//! A [`CancelToken`] is a shared `AtomicBool` that can be checked from
//! both Rust code and a Lua debug hook.  When cancelled, the debug hook
//! raises a Lua error containing the sentinel `__isle_cancelled__`,
//! which is recognized by [`IsleError::from(mlua::Error)`].

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Cancellation signal shared between caller and Lua thread.
///
/// Clone is cheap (Arc).
///
/// Two cancellation pathways are wired:
///
/// 1. A Lua debug hook polls [`is_cancelled`](Self::is_cancelled) every
///    `N` Lua instructions.  This interrupts pure-Lua CPU-bound loops
///    (`while true do end` etc), including loops inside coroutines
///    that the Lua code itself creates.
///
/// 2. When the feature `tokio` is enabled, [`cancelled`](Self::cancelled)
///    provides an async signal that fires as soon as [`cancel`](Self::cancel)
///    is called.  Coroutine executors (`execute_coroutine_eval`,
///    `execute_coroutine_call`) use this in a `tokio::select!` to drop
///    the in-flight Lua coroutine (built with
///    [`Function::call_async`](mlua::Function::call_async), so the drop
///    terminates the coroutine at once) even when it is suspended
///    inside a Rust `.await` (e.g. a `create_async_function` awaiting a
///    tokio child process).  The debug hook alone cannot interrupt such
///    Rust-suspended coroutines because no Lua instructions execute
///    during the `.await`, so the hook never fires.
#[derive(Clone)]
pub struct CancelToken {
    flag: Arc<AtomicBool>,
    #[cfg(feature = "tokio")]
    notify: Arc<tokio::sync::Notify>,
}

impl CancelToken {
    /// Create a new token (not cancelled).
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "tokio")]
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Signal cancellation.
    ///
    /// Sets the atomic flag (observed by the Lua debug hook) and, when
    /// the `tokio` feature is enabled, notifies all waiters of the
    /// async [`cancelled`](Self::cancelled) signal.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::Release);
        #[cfg(feature = "tokio")]
        self.notify.notify_waiters();
    }

    /// Check whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// Await cancellation (async).
    ///
    /// Returns immediately if already cancelled; otherwise resolves
    /// when [`cancel`](Self::cancel) is called.  Intended for use in
    /// `tokio::select!` to race a Lua coroutine against its cancel
    /// signal — when this future wins, dropping the other branch
    /// releases any Rust async resources (e.g. a spawned child
    /// process) that the coroutine was awaiting.
    ///
    /// Race-free: the returned future is registered with the
    /// underlying [`tokio::sync::Notify`] via
    /// [`Notified::enable`](tokio::sync::futures::Notified::enable)
    /// before the flag is re-checked, so a `cancel()` call that
    /// happens between `cancelled()` being constructed and awaited
    /// is not lost.
    #[cfg(feature = "tokio")]
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        // Re-check after enabling: a cancel() that happened between
        // the initial is_cancelled() check and enable() would have
        // called notify_waiters() without us being registered, so
        // this second read catches it.
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    /// Token of the request currently executing on this Lua thread.
    ///
    /// Each isle owns a dedicated OS thread, so a thread-local is
    /// equivalent to per-VM state.  The global cancel hook reads it.
    static CURRENT: RefCell<Option<CancelToken>> = const { RefCell::new(None) };
}

/// RAII guard that makes `token` the current token of this thread.
///
/// The previous token is restored on drop, so guards nest.
pub(crate) struct EnterGuard {
    prev: Option<CancelToken>,
}

impl EnterGuard {
    pub(crate) fn new(token: &CancelToken) -> Self {
        let prev = CURRENT.with(|c| c.replace(Some(token.clone())));
        Self { prev }
    }
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        let prev = self.prev.take();
        CURRENT.with(|c| *c.borrow_mut() = prev);
    }
}

/// Install the cancel hook as a Lua **global** hook.
///
/// Called once per VM, after the user's init closure.  The hook
/// raises a Lua error with a sentinel message that
/// [`IsleError`](crate::IsleError) recognizes as a cancellation when
/// the token of the request currently executing on this thread (see
/// [`EnterGuard`]) is cancelled.
///
/// # Why a global hook
///
/// A per-thread hook ([`mlua::Lua::set_hook`] /
/// [`mlua::Thread::set_hook`]) does not reach coroutines created
/// from Lua (`coroutine.create` / `coroutine.wrap`): Lua copies the C
/// hook into the new thread, but mlua finds no callback registered
/// for that thread and removes the hook the first time it fires.  A
/// CPU loop inside such a coroutine could then never be cancelled.
/// The global hook's callback is shared by every thread of the VM.
///
/// # Instruction interval
///
/// The `interval` controls how often the check runs.  Lower values
/// give faster cancellation response at the cost of overhead.
/// A value of 1000 is a reasonable default.
pub(crate) fn install_cancel_hook(lua: &mlua::Lua, interval: u32) -> Result<(), crate::IsleError> {
    lua.set_global_hook(
        mlua::HookTriggers::new().every_nth_instruction(interval),
        |_lua, _debug| {
            let cancelled =
                CURRENT.with(|c| c.borrow().as_ref().is_some_and(CancelToken::is_cancelled));
            if cancelled {
                Err(mlua::Error::runtime("__isle_cancelled__"))
            } else {
                Ok(mlua::VmState::Continue)
            }
        },
    )
    .map_err(crate::IsleError::from)
}

/// Future adapter that makes `token` the current token while the
/// inner future is polled.
///
/// Coroutine requests interleave on one Lua thread, so the current
/// token must be switched on every poll rather than once per request.
#[cfg(feature = "tokio")]
pub(crate) struct Scoped<F> {
    token: CancelToken,
    fut: std::pin::Pin<Box<F>>,
}

#[cfg(feature = "tokio")]
impl<F> Scoped<F> {
    pub(crate) fn new(token: CancelToken, fut: F) -> Self {
        Self {
            token,
            fut: Box::pin(fut),
        }
    }
}

#[cfg(feature = "tokio")]
impl<F: std::future::Future> std::future::Future for Scoped<F> {
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        let _enter = EnterGuard::new(&self.token);
        self.fut.as_mut().poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_default_not_cancelled() {
        let token = CancelToken::new();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn token_cancel_sets_flag() {
        let token = CancelToken::new();
        let clone = token.clone();
        token.cancel();
        assert!(clone.is_cancelled());
    }

    #[test]
    fn hook_interrupts_lua_loop() {
        let lua = mlua::Lua::new();
        let token = CancelToken::new();
        install_cancel_hook(&lua, 100).unwrap();
        let _enter = EnterGuard::new(&token);

        // Schedule cancel after a short spin
        let t = token.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            t.cancel();
        });

        let result: mlua::Result<()> = lua.load("while true do end").exec();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("__isle_cancelled__"),
            "expected cancellation sentinel, got: {err_msg}"
        );
    }

    #[test]
    fn hook_interrupts_loop_in_lua_created_coroutine() {
        let lua = mlua::Lua::new();
        let token = CancelToken::new();
        install_cancel_hook(&lua, 100).unwrap();
        let _enter = EnterGuard::new(&token);

        let t = token.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            t.cancel();
        });

        let result: mlua::Result<()> = lua
            .load("coroutine.wrap(function() while true do end end)()")
            .exec();
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("__isle_cancelled__"),
            "expected cancellation sentinel, got: {err_msg}"
        );
    }

    #[test]
    fn hook_ignores_cancelled_token_that_is_not_current() {
        let lua = mlua::Lua::new();
        install_cancel_hook(&lua, 100).unwrap();
        let other = CancelToken::new();
        other.cancel();
        {
            let _outer = EnterGuard::new(&other);
            let _inner = EnterGuard::new(&CancelToken::new());
            let r: i64 = lua
                .load("local n = 0 for i = 1, 100000 do n = n + 1 end return n")
                .eval()
                .unwrap();
            assert_eq!(r, 100000);
        }
        assert!(CURRENT.with(|c| c.borrow().is_none()));
    }
}
