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
#[derive(Clone)]
pub struct CancelToken {
    flag: Arc<AtomicBool>,
}

impl CancelToken {
    /// Create a new token (not cancelled).
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal cancellation.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::Release);
    }

    /// Check whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
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
