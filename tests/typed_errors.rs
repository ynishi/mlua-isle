//! Typed errors (#11): `LuaFailure` on every path, cancellation as a
//! value, `task.is_cancelled`, and the protected call surviving a
//! sandboxed VM.
//!
//! `sync_isle` needs no feature; `runtime_paths` needs `tokio`.

use mlua_isle::{Isle, IsleError, LuaErrorKind, LuaFailure};

#[derive(Debug)]
struct MyErr(u32);

impl std::fmt::Display for MyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "my error {}", self.0)
    }
}

impl std::error::Error for MyErr {}

fn failure<T: std::fmt::Debug>(r: Result<T, IsleError>) -> LuaFailure {
    match r {
        Err(IsleError::Lua(f)) => f,
        other => panic!("expected IsleError::Lua, got: {other:?}"),
    }
}

/// Code that allocates until a 1 MiB memory limit is hit.
const ALLOC_LOOP: &str = "local t = {} for i = 1, 1e8 do t[i] = tostring(i) end";

/// Raises `E0`, then its `<close>` handler runs out of memory during the
/// unwind (a memory error does not go through the message handler).
const CLOSE_OOM: &str = "local c <close> = setmetatable({}, { __close = function()
  local t = {} for i = 1, 1e8 do t[i] = tostring(i) end
end })
error('E0')";

/// Replace the globals with a whitelist that has no `xpcall` and no
/// `table`.
fn sandbox(lua: &mlua::Lua) -> mlua::Result<()> {
    let g = lua.globals();
    let white = lua.create_table()?;
    for name in ["error", "setmetatable", "tostring"] {
        white.set(name, g.get::<mlua::Value>(name)?)?;
    }
    lua.set_globals(white)
}

mod sync_isle {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn sync_eval_survives_init_removing_xpcall() {
        let isle = Isle::spawn(|lua| lua.globals().set("xpcall", mlua::Value::Nil)).unwrap();
        assert_eq!(isle.eval("return 1 + 1").unwrap(), "2");
        let f = failure(isle.eval("error({ code = 1 })"));
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        assert!(f.traceback.is_some());
        isle.shutdown().unwrap();
    }

    #[test]
    fn sync_eval_survives_whitelisted_globals() {
        let isle = Isle::spawn(sandbox).unwrap();
        assert_eq!(isle.eval("return 40 + 2").unwrap(), "42");
        let f = failure(isle.eval("error('x')"));
        assert_eq!(f.message, "eval:1: x");
        assert!(f.traceback.is_some(), "the message handler did not run");
        isle.shutdown().unwrap();
    }

    #[test]
    fn sync_memory_error_is_memory() {
        let isle = Isle::spawn(|lua| lua.set_memory_limit(1 << 20).map(drop)).unwrap();
        let f = failure(isle.eval(ALLOC_LOOP));
        assert_eq!(f.kind, LuaErrorKind::Memory, "got: {f:?}");
        assert_eq!(f.traceback, None);
        // The VM still serves.
        assert_eq!(isle.eval("return 1").unwrap(), "1");
        isle.shutdown().unwrap();
    }

    #[test]
    fn a_failing_line_hook_does_not_mask_the_lua_error() {
        // The hook fails at the first line event after `arm()`.  The
        // message handler runs no Lua, so that event does not come
        // before the error reaches Rust.
        let armed = Arc::new(AtomicBool::new(false));
        let a = armed.clone();
        let isle = Isle::spawn(move |lua| {
            let h = armed.clone();
            mlua_isle::hooks::add_hook(lua, mlua::HookTriggers::new().every_line(), move |_, _| {
                if h.swap(false, Ordering::SeqCst) {
                    return Err(mlua::Error::runtime("hook failed"));
                }
                Ok(mlua::VmState::Continue)
            })
            .map_err(|e| mlua::Error::runtime(e.to_string()))?;
            let arm = lua.create_function(move |_, ()| {
                a.store(true, Ordering::SeqCst);
                Ok(())
            })?;
            lua.globals().set("arm", arm)
        })
        .unwrap();
        let f = failure(isle.eval("arm() error('original')"));
        assert_eq!(f.message, "eval:1: original");
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        isle.shutdown().unwrap();
    }

    #[test]
    fn sync_memory_error_in_close_after_an_error_is_memory() {
        let isle = Isle::spawn(|lua| lua.set_memory_limit(1 << 20).map(drop)).unwrap();
        let f = failure(isle.eval(CLOSE_OOM));
        assert_eq!(f.kind, LuaErrorKind::Memory, "got: {f:?}");
        assert_eq!(f.traceback, None);
        assert_eq!(isle.eval("return 1").unwrap(), "1");
        isle.shutdown().unwrap();
    }

    #[test]
    fn attach_without_xpcall_is_an_init_error() {
        let lua = mlua::Lua::new();
        lua.globals().set("xpcall", mlua::Value::Nil).unwrap();
        let err = mlua_isle::runtime::Vm::attach(&lua, mlua_isle::runtime::Config::default())
            .unwrap_err();
        assert!(
            matches!(&err, IsleError::Init(f) if f.kind == LuaErrorKind::External),
            "got: {err:?}"
        );
    }

    #[test]
    fn a_cyclic_table_has_no_value_and_keeps_its_message() {
        let isle = Isle::spawn(|_| Ok(())).unwrap();
        let f = failure(isle.eval(
            "local t = setmetatable({}, { __tostring = function() return 'cyclic' end })
             t.self = t
             error(t)",
        ));
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        assert_eq!(f.message, "cyclic");
        assert!(f.traceback.is_some());
        #[cfg(feature = "serde")]
        assert_eq!(f.value, None);
        isle.shutdown().unwrap();
    }

    #[test]
    fn a_host_error_through_isle_is_a_callback_failure() {
        let isle = Isle::spawn(|lua| {
            let fail = lua.create_function(|_, ()| -> mlua::Result<()> {
                Err(mlua::Error::external(MyErr(3)))
            })?;
            lua.globals().set("fail", fail)
        })
        .unwrap();
        let f = failure(isle.eval("fail()"));
        assert_eq!(f.kind, LuaErrorKind::Callback);
        assert_eq!(f.message, "my error 3");
        isle.shutdown().unwrap();
    }

    #[test]
    fn isle_error_display() {
        let f = LuaFailure::new(LuaErrorKind::Runtime, "boom");
        assert_eq!(IsleError::Lua(f.clone()).to_string(), "lua error: boom");
        assert_eq!(IsleError::Init(f).to_string(), "init error: boom");
        assert_eq!(
            IsleError::NotFound("f".into()).to_string(),
            "function 'f' not found"
        );
        assert_eq!(
            IsleError::ThreadPanic(Some("x".into())).to_string(),
            "lua thread panicked: x"
        );
        assert_eq!(
            IsleError::ThreadPanic(None).to_string(),
            "lua thread panicked"
        );
        assert_eq!(IsleError::RecvFailed.to_string(), "recv failed");
    }
}

#[cfg(feature = "tokio")]
mod runtime_paths {
    use super::*;
    use mlua_isle::runtime::{CancelToken, Config, Vm};
    use mlua_isle::{cancellable, AsyncIsle};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    /// A VM driven in this thread: `task`, `sleep(ms)` (cancellable),
    /// `fail()` (a host error) and `report(...)` (records its arguments as
    /// strings).
    struct Local {
        rt: tokio::runtime::Runtime,
        local: tokio::task::LocalSet,
        lua: mlua::Lua,
        vm: Vm,
        reports: Rc<RefCell<Vec<Vec<String>>>>,
    }

    impl Local {
        fn new() -> Self {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            let lua = mlua::Lua::new();
            let vm = Vm::attach(
                &lua,
                Config {
                    grace: Duration::from_millis(500),
                    ..Default::default()
                },
            )
            .unwrap();
            let g = lua.globals();
            g.set("task", vm.task_lib().unwrap()).unwrap();
            let sleep = lua
                .create_async_function(|_, ms: u64| {
                    cancellable(async move {
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                        Ok(())
                    })
                })
                .unwrap();
            g.set("sleep", sleep).unwrap();
            let fail = lua
                .create_function(|_, ()| -> mlua::Result<()> {
                    Err(mlua::Error::external(MyErr(7)))
                })
                .unwrap();
            g.set("fail", fail).unwrap();
            let reports: Rc<RefCell<Vec<Vec<String>>>> = Default::default();
            let r = reports.clone();
            let report = lua
                .create_function(move |_, args: mlua::Variadic<mlua::Value>| {
                    let row = args
                        .iter()
                        .map(|v| v.to_string().unwrap_or_else(|_| format!("{v:?}")))
                        .collect();
                    r.borrow_mut().push(row);
                    Ok(())
                })
                .unwrap();
            g.set("report", report).unwrap();
            Self {
                rt,
                local,
                lua,
                vm,
                reports,
            }
        }

        /// Run `code` as a root; cancel the token after `cancel_after`.
        fn run(
            &self,
            code: &str,
            cancel_after: Option<Duration>,
        ) -> Result<mlua::MultiValue, IsleError> {
            let f = self.lua.load(code).into_function()?;
            let token = CancelToken::new();
            if let Some(d) = cancel_after {
                // From another thread: a CPU loop never yields to this one.
                let t = token.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(d);
                    t.cancel();
                });
            }
            self.local.block_on(&self.rt, self.vm.run(&token, f, ()))
        }

        fn reports(&self) -> Vec<Vec<String>> {
            self.reports.borrow().clone()
        }
    }

    // ── Vm::run / run_root ───────────────────────────────────────────────

    #[test]
    fn run_of_a_raised_table_keeps_its_message_and_value() {
        let l = Local::new();
        let f = failure(l.run("local t = { code = 42 } shown = tostring(t) error(t)", None));
        let shown: String = l.lua.globals().get("shown").unwrap();
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        assert_eq!(f.message, shown);
        assert!(
            f.traceback
                .as_deref()
                .unwrap_or("")
                .contains("stack traceback"),
            "traceback: {:?}",
            f.traceback
        );
        #[cfg(feature = "serde")]
        assert_eq!(f.value.unwrap()["code"], 42);
    }

    #[test]
    fn run_of_a_table_with_tostring_uses_it() {
        let l = Local::new();
        let f = failure(l.run(
            "error(setmetatable({ code = 7 }, { __tostring = function() return 'custom E7' end }))",
            None,
        ));
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        assert_eq!(f.message, "custom E7");
        #[cfg(feature = "serde")]
        assert_eq!(f.value.unwrap()["code"], 7);
    }

    #[test]
    fn run_of_an_unconvertible_value_has_no_value() {
        let l = Local::new();
        let f = failure(l.run("error(function() end)", None));
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        assert!(f.message.starts_with("function: "), "got: {}", f.message);
        #[cfg(feature = "serde")]
        assert_eq!(f.value, None);
    }

    #[test]
    fn a_chunk_with_a_syntax_error_is_syntax() {
        let l = Local::new();
        let f = failure(l.run("x = = 1", None));
        assert_eq!(f.kind, LuaErrorKind::Syntax);
    }

    #[test]
    fn a_host_error_is_a_callback_failure_with_its_message() {
        let l = Local::new();
        let f = failure(l.run("fail()", None));
        assert_eq!(f.kind, LuaErrorKind::Callback);
        assert_eq!(f.message, "my error 7");
        assert!(f.traceback.is_some());
        #[cfg(feature = "serde")]
        assert_eq!(f.value, None);
    }

    #[test]
    fn a_cancelled_root_is_cancelled() {
        let l = Local::new();
        let r = l.run("sleep(5000)", Some(Duration::from_millis(20)));
        assert!(matches!(r, Err(IsleError::Cancelled)), "got: {r:?}");
    }

    #[test]
    fn a_cancelled_cpu_loop_root_is_cancelled() {
        let l = Local::new();
        let r = l.run("while true do end", Some(Duration::from_millis(20)));
        assert!(matches!(r, Err(IsleError::Cancelled)), "got: {r:?}");
    }

    #[test]
    fn an_error_with_the_old_sentinel_is_not_a_cancel() {
        let l = Local::new();
        let f = failure(l.run("error('__isle_cancelled__')", None));
        assert!(
            f.message.ends_with("__isle_cancelled__"),
            "got: {}",
            f.message
        );
    }

    #[test]
    fn a_host_that_raises_cancelled_is_recognised_by_value() {
        let l = Local::new();
        let raise = l
            .lua
            .create_function(|_, ()| -> mlua::Result<()> {
                Err(mlua::Error::external(mlua_isle::Cancelled))
            })
            .unwrap();
        l.lua.globals().set("raise_cancel", raise).unwrap();
        let r = l.run("raise_cancel()", None);
        assert!(matches!(r, Err(IsleError::Cancelled)), "got: {r:?}");
        // And Lua sees it as a cancel too.
        let out = l
            .run(
                "local ok, e = pcall(raise_cancel) return task.is_cancelled(e)",
                None,
            )
            .unwrap();
        assert_eq!(out[0].as_boolean(), Some(true));
    }

    // ── task.is_cancelled ────────────────────────────────────────────────

    #[test]
    fn pcall_under_a_cancel_gives_a_cancel_value() {
        let l = Local::new();
        let r = l.run(
            "local ok, err = pcall(sleep, 5000)
             report(ok, task.is_cancelled(err))
             local ok2, err2 = pcall(function() while true do end end)
             report(ok2, task.is_cancelled(err2))",
            Some(Duration::from_millis(20)),
        );
        assert!(matches!(r, Err(IsleError::Cancelled)), "got: {r:?}");
        assert_eq!(
            l.reports(),
            vec![
                vec!["false".to_string(), "true".to_string()],
                vec!["false".to_string(), "true".to_string()],
            ]
        );
    }

    #[test]
    fn a_close_handler_receives_a_cancel_value() {
        let l = Local::new();
        let r = l.run(
            "local c <close> = setmetatable({}, { __close = function(_, e)
                 report(e ~= nil, task.is_cancelled(e))
             end })
             sleep(5000)",
            Some(Duration::from_millis(20)),
        );
        assert!(matches!(r, Err(IsleError::Cancelled)), "got: {r:?}");
        assert_eq!(
            l.reports(),
            vec![vec!["true".to_string(), "true".to_string()]]
        );
    }

    #[test]
    fn join_of_a_cancelled_task_still_returns_task_cancelled() {
        let l = Local::new();
        let out = l
            .run(
                "local h = task.spawn(function() sleep(5000) end)
                 sleep(5)
                 h:cancel()
                 local ok, e = h:join()
                 local _, host = pcall(fail)
                 return ok, rawequal(e, task.CANCELLED), task.is_cancelled(e),
                        task.is_cancelled(task.CANCELLED),
                        task.is_cancelled('some string'), task.is_cancelled(nil),
                        task.is_cancelled({}), task.is_cancelled(host)",
                None,
            )
            .unwrap();
        let got: Vec<Option<bool>> = out.iter().map(|v| v.as_boolean()).collect();
        assert_eq!(
            got,
            vec![
                Some(false),
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
            ]
        );
    }

    #[test]
    fn a_task_error_value_is_unchanged() {
        let l = Local::new();
        let out = l
            .run(
                "local h = task.spawn(function() error({ code = 42 }) end)
                 local ok, e = h:join()
                 return ok, e.code",
                None,
            )
            .unwrap();
        assert_eq!(out[0].as_boolean(), Some(false));
        assert_eq!(out[1].as_i64(), Some(42));
    }

    // ── the same LuaFailure through the actors ───────────────────────────

    const RAISE: &str =
        "error(setmetatable({ code = 42 }, { __tostring = function() return 'E42' end }))";

    fn assert_e42(f: &LuaFailure) {
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        assert_eq!(f.message, "E42");
        assert!(f.traceback.is_some());
        #[cfg(feature = "serde")]
        assert_eq!(f.value.as_ref().unwrap()["code"], 42);
    }

    #[tokio::test]
    async fn async_isle_requests_carry_the_same_failure() {
        let (isle, driver) =
            AsyncIsle::spawn(|lua| lua.load(format!("function raise() {RAISE} end")).exec())
                .await
                .unwrap();

        assert_e42(&failure(isle.eval(RAISE).await));
        assert_e42(&failure(isle.coroutine_eval(RAISE).await));
        assert_e42(&failure(isle.call("raise", &[]).await));
        assert_e42(&failure(isle.coroutine_call("raise", &[]).await));
        assert_e42(&failure(isle.spawn_coroutine_eval(RAISE).await));

        let sync = Isle::spawn(|_| Ok(())).unwrap();
        assert_e42(&failure(sync.eval(RAISE)));
        sync.shutdown().unwrap();

        driver.shutdown().await.unwrap();
    }

    #[test]
    fn vm_run_carries_the_same_failure_as_the_actors() {
        let l = Local::new();
        assert_e42(&failure(l.run(RAISE, None)));
    }

    #[tokio::test]
    async fn async_isle_not_found_and_old_sentinel() {
        let (isle, driver) = AsyncIsle::spawn(|_| Ok(())).await.unwrap();
        assert!(matches!(isle.call("nope", &[]).await, Err(IsleError::NotFound(n)) if n == "nope"));
        assert!(matches!(
            isle.coroutine_call("nope", &[]).await,
            Err(IsleError::NotFound(n)) if n == "nope"
        ));
        let f = failure(isle.coroutine_eval("error('__isle_cancelled__')").await);
        assert!(f.message.ends_with("__isle_cancelled__"));
        let f = failure(isle.eval("error('__isle_cancelled__')").await);
        assert!(f.message.ends_with("__isle_cancelled__"));
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn async_isle_init_errors() {
        let err = AsyncIsle::spawn(|_| Err(mlua::Error::runtime("nope")))
            .await
            .err()
            .expect("init must fail");
        assert!(
            matches!(&err, IsleError::Init(f) if f.kind == LuaErrorKind::Runtime && f.message == "nope"),
            "got: {err:?}"
        );
        let err = AsyncIsle::spawn(|_| panic!("async init boom"))
            .await
            .err()
            .expect("init must fail");
        assert!(
            matches!(&err, IsleError::ThreadPanic(Some(m)) if m == "async init boom"),
            "got: {err:?}"
        );
    }

    // ── the protected call on a sandboxed VM ────────────────────────────

    #[tokio::test]
    async fn coroutine_eval_survives_init_removing_xpcall() {
        let (isle, driver) = AsyncIsle::spawn(|lua| lua.globals().set("xpcall", mlua::Value::Nil))
            .await
            .unwrap();
        assert_eq!(isle.coroutine_eval("return 1 + 1").await.unwrap(), "2");
        assert_eq!(isle.eval("return 2 + 2").await.unwrap(), "4");
        let f = failure(isle.coroutine_eval("error({ code = 1 })").await);
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        assert!(f.traceback.is_some());
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn coroutine_eval_survives_whitelisted_globals() {
        let (isle, driver) = AsyncIsle::spawn(sandbox).await.unwrap();
        assert_eq!(isle.coroutine_eval("return 40 + 2").await.unwrap(), "42");
        let f = failure(isle.coroutine_eval("error('x')").await);
        assert_eq!(f.message, "coroutine_eval:1: x");
        assert!(f.traceback.is_some(), "the message handler did not run");
        driver.shutdown().await.unwrap();
    }

    #[test]
    fn vm_run_survives_removing_xpcall_and_whitelisted_globals() {
        let l = Local::new();
        l.lua.globals().set("xpcall", mlua::Value::Nil).unwrap();
        assert_eq!(l.run("return 1 + 1", None).unwrap()[0].as_i64(), Some(2));
        sandbox(&l.lua).unwrap();
        assert_eq!(l.run("return 40 + 2", None).unwrap()[0].as_i64(), Some(42));
        let f = failure(l.run("error({ code = 1 })", None));
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        assert!(f.traceback.is_some(), "the message handler did not run");
        // A re-attach after the sandbox keeps the captured `xpcall`.
        Vm::attach(&l.lua, Config::default()).unwrap();
        assert_eq!(l.run("return 3", None).unwrap()[0].as_i64(), Some(3));
    }

    #[tokio::test]
    async fn coroutine_memory_error_is_memory() {
        let (isle, driver) = AsyncIsle::spawn(|lua| lua.set_memory_limit(1 << 20).map(drop))
            .await
            .unwrap();
        let f = failure(isle.coroutine_eval(ALLOC_LOOP).await);
        assert_eq!(f.kind, LuaErrorKind::Memory, "got: {f:?}");
        let f = failure(isle.eval(ALLOC_LOOP).await);
        assert_eq!(f.kind, LuaErrorKind::Memory, "got: {f:?}");
        assert_eq!(isle.coroutine_eval("return 1").await.unwrap(), "1");
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn coroutine_memory_error_in_close_after_an_error_is_memory() {
        let (isle, driver) = AsyncIsle::spawn(|lua| lua.set_memory_limit(1 << 20).map(drop))
            .await
            .unwrap();
        let f = failure(isle.coroutine_eval(CLOSE_OOM).await);
        assert_eq!(f.kind, LuaErrorKind::Memory, "got: {f:?}");
        assert_eq!(f.traceback, None);
        assert_eq!(isle.coroutine_eval("return 1").await.unwrap(), "1");
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn interleaved_failing_roots_keep_their_own_traceback() {
        let (isle, driver) = AsyncIsle::spawn(|lua| {
            let sleep = lua.create_async_function(|_, ms: u64| async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            })?;
            lua.globals().set("sleep", sleep)
        })
        .await
        .unwrap();
        // A fails at once and its `<close>` handler then awaits 50 ms;
        // B fails at 10 ms, while A is between its message handler and
        // the return of its `xpcall`.
        let a = "local c <close> = setmetatable({}, { __close = function() sleep(50) end })
error('A')";
        let b = "sleep(10)
sleep(0)
error('B')";
        let (ra, rb) = tokio::join!(isle.coroutine_eval(a), isle.coroutine_eval(b));
        let (fa, fb) = (failure(ra), failure(rb));
        assert_eq!(fa.message, "coroutine_eval:2: A");
        assert_eq!(fb.message, "coroutine_eval:3: B");
        let (ta, tb) = (fa.traceback.unwrap(), fb.traceback.unwrap());
        assert!(ta.contains("coroutine_eval:2:"), "A: {ta}");
        assert!(!ta.contains("coroutine_eval:3:"), "A: {ta}");
        assert!(tb.contains("coroutine_eval:3:"), "B: {tb}");
        assert!(!tb.contains("coroutine_eval:2:"), "B: {tb}");
        driver.shutdown().await.unwrap();
    }
}
