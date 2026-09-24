//! The Lua debug hook of an isle, shared between cancellation and user
//! callbacks.
//!
//! Lua has one hook slot per thread, and mlua's hook setters replace
//! whatever was there.  An isle therefore owns the VM's hook: it
//! installs a single **global** hook ([`mlua::Lua::set_global_hook`])
//! whose callback, in order,
//!
//! 1. raises the cancellation error when the token of the request or
//!    task currently executing is cancelled,
//! 2. calls the callbacks registered with [`add_hook`], each at its own
//!    [`HookTriggers`],
//! 3. yields the running task every N instruction checks when
//!    preemption is enabled (see [`CancelConfig::preempt_every`]).
//!
//! A global hook is used because a per-thread hook does not reach
//! coroutines created from Lua (`coroutine.create` / `coroutine.wrap`):
//! Lua copies the C hook into the new thread, but mlua finds no
//! callback registered for that thread and removes the hook the first
//! time it fires.  The global hook's callback is shared by every thread
//! of the VM.
//!
//! # Do not set hooks directly
//!
//! Calling [`mlua::Lua::set_hook`], [`mlua::Lua::set_global_hook`] or
//! [`mlua::Thread::set_hook`] on an isle's VM replaces this hook, and
//! cancellation stops working.  Register callbacks with [`add_hook`]
//! instead.  An isle re-installs its hook when it notices that
//! `set_hook` replaced it (checked at the start of every request), but
//! a replacement through `set_global_hook` cannot be detected.

use crate::error::IsleError;
use crate::hook::{self, CANCELLED_SENTINEL};
use mlua::debug::{Debug, DebugEvent};
use mlua::{HookTriggers, Lua, VmState};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Duration;

/// Instruction interval of the cancel check.
pub(crate) const CANCEL_CHECK_INTERVAL: u32 = 1000;

/// Handle of a callback registered with [`add_hook`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HookId(u64);

type Callback = dyn Fn(&Lua, &Debug) -> mlua::Result<VmState>;

struct Entry {
    id: HookId,
    triggers: HookTriggers,
    callback: Box<Callback>,
    /// Instructions counted since this entry last ran (count triggers).
    counted: Cell<u32>,
}

/// Cancellation settings of a VM.  Set with [`configure`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CancelConfig {
    /// How long a cancelled coroutine request or task may keep running
    /// to finish its cleanup before it is dropped.
    ///
    /// On cancel, the coroutine first receives the cancellation as a Lua
    /// error (from the cancel hook while it runs, or from an async
    /// function wrapped with [`cancellable`](crate::cancellable) while
    /// it awaits).  That error unwinds normally, so `__close` handlers
    /// run and may await.  If the coroutine has not finished when the
    /// grace period ends, it is dropped: the awaited Rust future is
    /// released, and pending `__close` handlers run without being able
    /// to yield (a Lua 5.4 restriction).
    ///
    /// Default: zero (drop at once).
    pub grace: Duration,
    /// Yield the running coroutine request or task every this many
    /// cancel checks (a check runs every 1000 instructions), so that
    /// other tasks on the same thread —
    /// including one that cancels it — get to run while it is in a CPU
    /// loop.
    ///
    /// Only the coroutine that the isle created for the request or task
    /// is yielded, never a coroutine the Lua code created itself (that
    /// yield would reach the Lua code's own `coroutine.resume` as a
    /// spurious yield).  Code in a non-yieldable context (a metamethod,
    /// a C function boundary) is not yielded.
    ///
    /// Yielding lets other tasks interleave at points the Lua code did
    /// not mark, so Lua code that updates shared state across such a
    /// point can observe changes made by another task.
    ///
    /// Default: `None` (never preempt).
    pub preempt_every: Option<u32>,
}

#[derive(Default)]
struct Hub {
    entries: RefCell<Vec<Rc<Entry>>>,
    next_id: Cell<u64>,
    config: Cell<CancelConfig>,
    checks: Cell<u32>,
    /// Raw hook function of the main thread right after installation.
    installed: Cell<usize>,
}

thread_local! {
    /// Coroutines created by the isle for a coroutine request or task
    /// (the only ones preemption may yield), by thread pointer.
    static ROOTS: RefCell<HashSet<usize>> = RefCell::new(HashSet::new());
}

#[cfg(feature = "tokio")]
pub(crate) fn mark_root(ptr: usize) {
    ROOTS.with(|r| r.borrow_mut().insert(ptr));
}

#[cfg(feature = "tokio")]
pub(crate) fn unmark_root(ptr: usize) {
    ROOTS.with(|r| r.borrow_mut().remove(&ptr));
}

fn hub(lua: &Lua) -> Rc<Hub> {
    if let Some(h) = lua.app_data_ref::<Rc<Hub>>() {
        return h.clone();
    }
    let h = Rc::new(Hub::default());
    lua.set_app_data(h.clone());
    h
}

/// Install (or re-install) the isle hook on `lua`.
///
/// Called by the isles after the init closure; call it yourself when
/// you run Lua with this crate's cancellation or task support outside
/// an isle.
pub fn install(lua: &Lua) -> Result<(), IsleError> {
    let hub = hub(lua);
    let mut triggers = HookTriggers::new().every_nth_instruction(CANCEL_CHECK_INTERVAL);
    for e in hub.entries.borrow().iter() {
        triggers.on_calls |= e.triggers.on_calls;
        triggers.on_returns |= e.triggers.on_returns;
        triggers.every_line |= e.triggers.every_line;
        if let Some(n) = e.triggers.every_nth_instruction {
            let cur = triggers.every_nth_instruction.unwrap_or(n);
            triggers.every_nth_instruction = Some(cur.min(n).max(1));
        }
    }
    let step = triggers.every_nth_instruction.unwrap_or(1);
    let cancel_every = CANCEL_CHECK_INTERVAL.div_ceil(step).max(1);
    lua.set_global_hook(triggers, move |lua, debug| {
        dispatch(lua, debug, step, cancel_every)
    })
    .map_err(IsleError::from)?;
    hub.installed.set(raw_hook(lua));
    Ok(())
}

/// Re-install the isle hook if `Lua::set_hook` replaced it.
pub(crate) fn ensure_installed(lua: &Lua) -> Result<(), IsleError> {
    let hub = hub(lua);
    if hub.installed.get() == 0 || raw_hook(lua) != hub.installed.get() {
        install(lua)?;
    }
    Ok(())
}

fn raw_hook(lua: &Lua) -> usize {
    let mut out = 0usize;
    // SAFETY: `lua_gethook` only reads the hook field of the state.
    let _ = unsafe {
        lua.exec_raw::<()>((), |state| {
            out = mlua::ffi::lua_gethook(state).map_or(0, |f| f as usize);
        })
    };
    out
}

fn dispatch(lua: &Lua, debug: &Debug, step: u32, cancel_every: u32) -> mlua::Result<VmState> {
    let event = debug.event();
    let hub = hub(lua);
    let mut want_yield = false;

    if matches!(event, DebugEvent::Count) {
        let checks = hub.checks.get().wrapping_add(1);
        hub.checks.set(checks);
        if checks.is_multiple_of(cancel_every) {
            if hook::current_is_cancelled() {
                return Err(mlua::Error::runtime(CANCELLED_SENTINEL));
            }
            if let Some(n) = hub.config.get().preempt_every {
                if (checks / cancel_every).is_multiple_of(n.max(1)) && is_root(lua) {
                    want_yield = true;
                }
            }
        }
    }

    // Snapshot so that a callback may add or remove hooks.
    let entries: Vec<Rc<Entry>> = hub.entries.borrow().clone();
    for e in entries {
        let t = &e.triggers;
        let fire = match event {
            DebugEvent::Count => match t.every_nth_instruction {
                Some(n) => {
                    let c = e.counted.get() + step;
                    if c >= n {
                        e.counted.set(0);
                        true
                    } else {
                        e.counted.set(c);
                        false
                    }
                }
                None => false,
            },
            DebugEvent::Line => t.every_line,
            DebugEvent::Call | DebugEvent::TailCall => t.on_calls,
            DebugEvent::Ret => t.on_returns,
            DebugEvent::Unknown(_) => false,
        };
        if fire && matches!((e.callback)(lua, debug)?, VmState::Yield) {
            want_yield = true;
        }
    }

    Ok(if want_yield {
        VmState::Yield
    } else {
        VmState::Continue
    })
}

fn is_root(lua: &Lua) -> bool {
    let ptr = lua.current_thread().to_pointer() as usize;
    ROOTS.with(|r| r.borrow().contains(&ptr))
}

/// Register a hook callback on an isle's VM.
///
/// The callback runs from the isle's single global hook, after the
/// cancel check, at `triggers`.  Instruction counts are approximated to
/// the hook's step (the smallest count among all registrations and the
/// cancel check).  Returning [`VmState::Yield`] yields the running
/// coroutine, including a coroutine the Lua code created itself, where
/// the yield reaches the Lua code's `coroutine.resume`.
///
/// Call it in the init closure or from an `exec` closure.  The callback
/// applies to the main thread and to coroutines created afterwards;
/// coroutines that already exist keep the instruction count and events
/// they were created with.
pub fn add_hook<F>(lua: &Lua, triggers: HookTriggers, callback: F) -> Result<HookId, IsleError>
where
    F: Fn(&Lua, &Debug) -> mlua::Result<VmState> + 'static,
{
    let hub = hub(lua);
    let id = HookId(hub.next_id.get());
    hub.next_id.set(id.0 + 1);
    hub.entries.borrow_mut().push(Rc::new(Entry {
        id,
        triggers,
        callback: Box::new(callback),
        counted: Cell::new(0),
    }));
    install(lua)?;
    Ok(id)
}

/// Remove a callback registered with [`add_hook`].  Returns whether it
/// was registered.
pub fn remove_hook(lua: &Lua, id: HookId) -> Result<bool, IsleError> {
    let hub = hub(lua);
    let before = hub.entries.borrow().len();
    hub.entries.borrow_mut().retain(|e| e.id != id);
    let removed = hub.entries.borrow().len() != before;
    if removed {
        install(lua)?;
    }
    Ok(removed)
}

/// Set the cancellation settings of a VM.
pub fn configure(lua: &Lua, config: CancelConfig) {
    hub(lua).config.set(config);
}

/// The cancellation settings of a VM.
pub fn config(lua: &Lua) -> CancelConfig {
    hub(lua).config.get()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hook::{CancelToken, EnterGuard};

    fn cancel_soon(token: &CancelToken) {
        let t = token.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            t.cancel();
        });
    }

    fn assert_cancelled(r: mlua::Result<()>) {
        let msg = r.unwrap_err().to_string();
        assert!(msg.contains(CANCELLED_SENTINEL), "got: {msg}");
    }

    #[test]
    fn hook_interrupts_lua_loop() {
        let lua = Lua::new();
        install(&lua).unwrap();
        let token = CancelToken::new();
        let _enter = EnterGuard::new(&token);
        cancel_soon(&token);
        assert_cancelled(lua.load("while true do end").exec());
    }

    #[test]
    fn hook_interrupts_loop_in_lua_created_coroutine() {
        let lua = Lua::new();
        install(&lua).unwrap();
        let token = CancelToken::new();
        let _enter = EnterGuard::new(&token);
        cancel_soon(&token);
        assert_cancelled(
            lua.load("coroutine.wrap(function() while true do end end)()")
                .exec(),
        );
    }

    #[test]
    fn hook_ignores_cancelled_token_that_is_not_current() {
        let lua = Lua::new();
        install(&lua).unwrap();
        let other = CancelToken::new();
        other.cancel();
        let _outer = EnterGuard::new(&other);
        let _inner = EnterGuard::new(&CancelToken::new());
        let r: i64 = lua
            .load("local n = 0 for i = 1, 100000 do n = n + 1 end return n")
            .eval()
            .unwrap();
        assert_eq!(r, 100000);
    }

    #[test]
    fn user_callbacks_share_the_hook_with_cancellation() {
        let lua = Lua::new();
        install(&lua).unwrap();
        let lines = Rc::new(Cell::new(0u32));
        let counts = Rc::new(Cell::new(0u32));
        let (l, c) = (lines.clone(), counts.clone());
        add_hook(&lua, HookTriggers::EVERY_LINE, move |_, _| {
            l.set(l.get() + 1);
            Ok(VmState::Continue)
        })
        .unwrap();
        add_hook(
            &lua,
            HookTriggers::new().every_nth_instruction(10_000),
            move |_, _| {
                c.set(c.get() + 1);
                Ok(VmState::Continue)
            },
        )
        .unwrap();

        // Lines inside a Lua-created coroutine reach the user callback.
        lua.load("local co = coroutine.wrap(function()\n local x = 1\n x = x + 1\n end)\n co()")
            .exec()
            .unwrap();
        assert!(lines.get() >= 4, "line callback ran {} times", lines.get());

        lua.load("local n = 0 for i = 1, 1000000 do n = n + 1 end")
            .exec()
            .unwrap();
        assert!(counts.get() > 0, "count callback never ran");

        // Cancellation still works next to the user callbacks.
        let token = CancelToken::new();
        let _enter = EnterGuard::new(&token);
        cancel_soon(&token);
        assert_cancelled(lua.load("while true do end").exec());
    }

    #[test]
    fn user_callback_error_propagates_and_remove_hook_stops_it() {
        let lua = Lua::new();
        install(&lua).unwrap();
        let id = add_hook(&lua, HookTriggers::EVERY_LINE, |_, _| {
            Err(mlua::Error::runtime("limit reached"))
        })
        .unwrap();
        let err = lua.load("local x = 1").exec().unwrap_err().to_string();
        assert!(err.contains("limit reached"), "got: {err}");

        assert!(remove_hook(&lua, id).unwrap());
        assert!(!remove_hook(&lua, id).unwrap());
        lua.load("local x = 1").exec().unwrap();
    }

    #[test]
    fn ensure_installed_restores_hook_replaced_by_set_hook() {
        let lua = Lua::new();
        install(&lua).unwrap();
        lua.set_hook(HookTriggers::EVERY_LINE, |_, _| Ok(VmState::Continue))
            .unwrap();
        ensure_installed(&lua).unwrap();

        let token = CancelToken::new();
        let _enter = EnterGuard::new(&token);
        cancel_soon(&token);
        assert_cancelled(lua.load("while true do end").exec());
    }
}
