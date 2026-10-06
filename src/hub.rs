//! The hook hub of a VM: the one global Lua debug hook that
//! [`Vm`](crate::runtime::Vm) owns, shared between cancellation,
//! preemption and user callbacks, and the VM's [`Config`].
//!
//! Crate-internal; the public surface is [`Vm`](crate::runtime::Vm)
//! (see the "Hooks" section of the [`runtime`](crate::runtime) docs).
//!
//! A global hook ([`mlua::Lua::set_global_hook`]) is used because a
//! per-thread hook does not reach coroutines created from Lua
//! (`coroutine.create` / `coroutine.wrap`): Lua copies the C hook into
//! the new thread, but mlua finds no callback registered for that thread
//! and removes the hook the first time it fires.  The global hook's
//! callback is shared by every thread of the VM.

use crate::error::IsleError;
use crate::hook;
use crate::runtime::{Config, HookId};
use mlua::debug::{Debug, DebugEvent};
use mlua::{HookTriggers, Lua, VmState};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;

/// Instruction interval of the cancel check.
pub(crate) const CANCEL_CHECK_INTERVAL: u32 = 1000;

/// Chunk name that mlua gives the Lua code through which it runs every
/// async host function (`RawLua::create_async_callback`, mlua 0.12.1
/// `src/state/raw.rs:1582`: `.set_name("=__mlua_async_poll")`).  The
/// code polls the host future and, once the future is ready, returns
/// its results with a few more instructions of its own.
///
/// The name does not reach the debug info: the chunk is loaded with
/// `try_cache()` (`src/state/raw.rs:1581`), which first compiles the
/// text without a name (`Chunk::compile`, `src/chunk.rs:675`: chunk
/// name "?") and loads the dumped bytecode, which keeps that source.
/// [`in_async_poll`] therefore also recognises the chunk by its
/// environment.
const ASYNC_POLL_CHUNK: &str = "=__mlua_async_poll";

/// How many cancel checks may be deferred because they landed in mlua's
/// async poll chunk before the cancel is raised anyway (the count is
/// reset when a cancel is raised).  Bounds
/// the delay for a loop whose checks keep landing there (an async host
/// function that is always ready and not `cancellable`, called in a
/// loop whose length divides the check interval).
const MAX_DEFERRED: u32 = 16;

/// Whether the hook fired in mlua's async poll chunk.
///
/// A cancel raised there, after the host future returned its values and
/// before the chunk returned them, would lose those values (a value
/// `recv` took from a channel) or report a failure for work that was
/// done (a `send` that pushed its value).  The cancel is raised at a
/// later check instead: the next one outside the chunk, or at the next
/// `cancellable` await.  Before the future is ready, deferring changes
/// nothing: a `cancellable` future returns the cancel itself.
///
/// Recognised as a main chunk whose source is [`ASYNC_POLL_CHUNK`], or
/// `"?"` with the environment mlua gives the poll chunk (`get_future`
/// and `poll` functions; mlua 0.12.1 `src/state/raw.rs:1536-1540`).
/// Other chunks mlua compiles through `try_cache()` (the wrapper of
/// `Function::bind`, say) also have the source `"?"` but not that
/// environment.
fn in_async_poll(debug: &Debug) -> bool {
    {
        let src = debug.source();
        if src.what != "main" {
            return false;
        }
        match src.source.as_deref() {
            Some(ASYNC_POLL_CHUNK) => return true,
            Some("?") => {}
            _ => return false,
        }
    }
    let is_fn = |env: &mlua::Table, key: &str| {
        matches!(
            env.raw_get::<mlua::Value>(key),
            Ok(mlua::Value::Function(_))
        )
    };
    debug
        .function()
        .environment()
        .is_some_and(|env| is_fn(&env, "get_future") && is_fn(&env, "poll"))
}

type Callback = dyn Fn(&Lua, &Debug) -> mlua::Result<VmState>;

struct Entry {
    id: HookId,
    triggers: HookTriggers,
    callback: Box<Callback>,
    /// Instructions counted since this entry last ran (count triggers).
    counted: Cell<u32>,
}

#[derive(Default)]
struct Hub {
    entries: RefCell<Vec<Rc<Entry>>>,
    next_id: Cell<u64>,
    config: Cell<Config>,
    checks: Cell<u32>,
    /// Cancel checks deferred because they landed in mlua's async poll
    /// chunk (see [`in_async_poll`]).  Reset when a cancel is raised.
    deferred: Cell<u32>,
    /// Raw hook function of the main thread right after installation.
    installed: Cell<usize>,
    /// Round-robin counter of `task.select` / `task.select_raw`: each
    /// select that is not biased starts one position after the
    /// previous one.
    #[cfg(feature = "tokio")]
    select_turn: Cell<usize>,
}

thread_local! {
    /// Coroutines created by the crate for a coroutine request or task
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

/// Install (or re-install) the hook on `lua`.  Called by
/// [`Vm::attach`](crate::runtime::Vm::attach).
pub(crate) fn install(lua: &Lua) -> Result<(), IsleError> {
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

/// Re-install the hook if `Lua::set_hook` replaced it.
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
                // Only on the cancelled path: the source lookup costs a
                // `lua_getinfo`.
                if hub.deferred.get() < MAX_DEFERRED && in_async_poll(debug) {
                    hub.deferred.set(hub.deferred.get() + 1);
                } else {
                    hub.deferred.set(0);
                    return Err(crate::error::cancel_error());
                }
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

/// Register a hook callback (see
/// [`Vm::add_hook`](crate::runtime::Vm::add_hook) for the contract).
pub(crate) fn add_hook<F>(
    lua: &Lua,
    triggers: HookTriggers,
    callback: F,
) -> Result<HookId, IsleError>
where
    F: Fn(&Lua, &Debug) -> mlua::Result<VmState> + 'static,
{
    let hub = hub(lua);
    let n = hub.next_id.get();
    hub.next_id.set(n + 1);
    let id = HookId(n);
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
pub(crate) fn remove_hook(lua: &Lua, id: HookId) -> Result<bool, IsleError> {
    let hub = hub(lua);
    let before = hub.entries.borrow().len();
    hub.entries.borrow_mut().retain(|e| e.id != id);
    let removed = hub.entries.borrow().len() != before;
    if removed {
        install(lua)?;
    }
    Ok(removed)
}

/// Store the settings of a VM.
pub(crate) fn set_config(lua: &Lua, config: Config) {
    hub(lua).config.set(config);
}

/// The settings of a VM.
pub(crate) fn config(lua: &Lua) -> Config {
    hub(lua).config.get()
}

/// Take the VM's next round-robin turn for a select (the caller reduces
/// it modulo its number of cases).
#[cfg(feature = "tokio")]
pub(crate) fn next_select_turn(lua: &Lua) -> usize {
    let hub = hub(lua);
    let turn = hub.select_turn.get();
    hub.select_turn.set(turn.wrapping_add(1));
    turn
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hook::{CancelToken, EnterGuard};
    use std::time::Duration;

    fn cancel_soon(token: &CancelToken) {
        let t = token.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            t.cancel();
        });
    }

    fn assert_cancelled(r: mlua::Result<()>) {
        let e = r.unwrap_err();
        assert!(crate::error::is_cancel(&e), "got: {e}");
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

    /// A chunk that looks like mlua's async poll chunk (source "?",
    /// `get_future` and `poll` in its environment).
    fn poll_like(lua: &Lua, code: &str, with_env: bool) -> mlua::Function {
        let env = lua.create_table().unwrap();
        if with_env {
            let f = lua.create_function(|_, ()| Ok(())).unwrap();
            env.set("get_future", f.clone()).unwrap();
            env.set("poll", f).unwrap();
        }
        lua.load(code)
            .set_name("?")
            .set_environment(env)
            .into_function()
            .unwrap()
    }

    /// A cancel check that lands in mlua's async poll chunk is deferred,
    /// but only `MAX_DEFERRED` times in a row: a loop there is still
    /// cancelled.
    #[test]
    fn cancel_in_the_async_poll_chunk_is_deferred_but_not_forever() {
        let lua = Lua::new();
        install(&lua).unwrap();
        let token = CancelToken::new();
        token.cancel();
        let _enter = EnterGuard::new(&token);
        let counting = "local n = 0 for i = 1, 3000 do n = n + 1 end return n";
        let counted: i64 = poll_like(&lua, counting, true).call(()).unwrap();
        assert_eq!(counted, 3000);
        assert!(hub(&lua).deferred.get() > 0);
        assert_cancelled(poll_like(&lua, "while true do end", true).call(()));
        assert_eq!(hub(&lua).deferred.get(), 0);
        // A "?" chunk without that environment, and any other chunk, is
        // cancelled at the first check.
        assert_cancelled(poll_like(&lua, counting, false).call::<i64>(()).map(drop));
        assert_cancelled(lua.load(counting).exec());
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
