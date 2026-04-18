//! Cancellation token and Lua debug hook.
//!
//! A [`CancelToken`] is a shared `AtomicBool` that can be checked from
//! both Rust code and a Lua debug hook.  When cancelled, the debug hook
//! raises a Lua error containing the sentinel `__isle_cancelled__`,
//! which is recognized by [`IsleError::from(mlua::Error)`].

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
///    (`while true do end` etc).
///
/// 2. When the feature `tokio` is enabled, [`cancelled`](Self::cancelled)
///    provides an async signal that fires as soon as [`cancel`](Self::cancel)
///    is called.  Coroutine executors (`execute_coroutine_eval`,
///    `execute_coroutine_call`) use this in a `tokio::select!` to drop
///    the in-flight Lua coroutine even when the coroutine is suspended
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

/// Install a Lua debug hook that checks the cancel token every N instructions.
///
/// When the token is cancelled, the hook raises a Lua error with a
/// sentinel message that [`IsleError`](crate::IsleError) recognizes as
/// a cancellation.
///
/// # Instruction interval
///
/// The `interval` controls how often the check runs.  Lower values
/// give faster cancellation response at the cost of overhead.
/// A value of 1000 is a reasonable default.
pub(crate) fn install_cancel_hook(
    lua: &mlua::Lua,
    token: CancelToken,
    interval: u32,
) -> Result<(), crate::IsleError> {
    lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(interval),
        move |_lua, _debug| {
            if token.is_cancelled() {
                Err(mlua::Error::runtime("__isle_cancelled__"))
            } else {
                Ok(mlua::VmState::Continue)
            }
        },
    )
    .map_err(crate::IsleError::from)
}

/// Remove the debug hook (restores normal execution speed).
pub(crate) fn remove_hook(lua: &mlua::Lua) {
    lua.remove_hook();
}

/// Install a cancel hook on a Lua coroutine thread.
///
/// Same as [`install_cancel_hook`] but targets a specific [`Thread`](mlua::Thread)
/// instead of the main Lua state.  Used for cooperative coroutine execution
/// where each coroutine needs its own cancel check.
#[cfg(feature = "tokio")]
pub(crate) fn install_cancel_hook_on_thread(
    thread: &mlua::Thread,
    token: CancelToken,
    interval: u32,
) -> Result<(), crate::IsleError> {
    thread
        .set_hook(
            mlua::HookTriggers::new().every_nth_instruction(interval),
            move |_lua, _debug| {
                if token.is_cancelled() {
                    Err(mlua::Error::runtime("__isle_cancelled__"))
                } else {
                    Ok(mlua::VmState::Continue)
                }
            },
        )
        .map_err(crate::IsleError::from)
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
        install_cancel_hook(&lua, token.clone(), 100).unwrap();

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
}
