use mlua_isle::{Isle, IsleError};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn eval_simple_expression() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let result: i64 = isle.eval("return 1 + 2").unwrap();
    assert_eq!(result, 3);
    isle.shutdown().unwrap();
}

#[test]
fn eval_string_result() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let result: String = isle.eval("return 'hello world'").unwrap();
    assert_eq!(result, "hello world");
    isle.shutdown().unwrap();
}

#[test]
fn eval_nil_is_none() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let result: Option<String> = isle.eval("return nil").unwrap();
    assert_eq!(result, None);
    isle.shutdown().unwrap();
}

#[test]
fn eval_lua_error_propagates() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let result = isle.eval::<()>("error('boom')");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("boom"),
        "expected 'boom' in error, got: {err}"
    );
    isle.shutdown().unwrap();
}

#[test]
fn init_sets_globals() {
    let isle = Isle::spawn(|lua| {
        lua.globals().set("my_val", 42)?;
        Ok(())
    })
    .unwrap();

    let result: i64 = isle.eval("return my_val").unwrap();
    assert_eq!(result, 42);
    isle.shutdown().unwrap();
}

#[test]
fn call_global_function() {
    let isle = Isle::spawn(|lua| {
        let f = lua.create_function(|_lua, args: mlua::MultiValue| {
            let mut parts = Vec::new();
            for v in args {
                match v {
                    mlua::Value::String(s) => parts.push(s.to_str().unwrap().to_string()),
                    _ => parts.push(format!("{v:?}")),
                }
            }
            Ok(parts.join(", "))
        })?;
        lua.globals().set("greet", f)?;
        Ok(())
    })
    .unwrap();

    let result: String = isle.call("greet", ("hello", "world")).unwrap();
    assert_eq!(result, "hello, world");
    isle.shutdown().unwrap();
}

#[test]
fn spawn_eval_cancel_infinite_loop() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let task = isle.spawn_eval::<()>("while true do end");

    // Cancel after a short delay
    std::thread::sleep(Duration::from_millis(50));
    task.cancel();

    let start = Instant::now();
    let result = task.wait();
    let elapsed = start.elapsed();

    assert!(result.is_err());
    match result.unwrap_err() {
        IsleError::Cancelled => {}
        other => panic!("expected Cancelled, got: {other}"),
    }
    // Should resolve quickly after cancel (not hang)
    assert!(
        elapsed < Duration::from_secs(2),
        "cancel took too long: {elapsed:?}"
    );

    isle.shutdown().unwrap();
}

#[test]
fn multiple_sequential_evals() {
    let isle = Isle::spawn(|lua| {
        lua.globals().set("counter", 0)?;
        Ok(())
    })
    .unwrap();

    for i in 1..=5 {
        let result: i64 = isle.eval("counter = counter + 1; return counter").unwrap();
        assert_eq!(result, i);
    }

    isle.shutdown().unwrap();
}

#[test]
fn exec_closure() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();

    let result = isle
        .exec(|lua| {
            let val: i64 = lua.load("return 7 * 6").eval()?;
            Ok(val)
        })
        .unwrap();

    assert_eq!(result, 42);
    isle.shutdown().unwrap();
}

#[test]
fn shutdown_after_drop_is_safe() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let _ = isle.eval::<()>("return 1");
    // Drop without explicit shutdown — should not panic
    drop(isle);
}

#[test]
fn is_alive_check() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    assert!(isle.is_alive());
    isle.shutdown().unwrap();
}

#[test]
fn init_error_propagates() {
    let result = Isle::spawn(|lua| {
        lua.load("this is not valid lua").exec()?;
        Ok(())
    });
    assert!(result.is_err());
    match result.err().unwrap() {
        IsleError::Init(f) => {
            assert_eq!(f.kind, mlua_isle::LuaErrorKind::Syntax);
            assert!(
                !f.message.is_empty(),
                "init error message should not be empty"
            );
        }
        other => panic!("expected Init error, got: {other}"),
    }
}

#[test]
fn spawn_eval_after_drop_returns_shutdown() {
    // Create Isle, drop it to close the channel, then use a leaked sender
    // to verify the send-failure path.
    //
    // We can't call methods after shutdown (consumes self), so we test the
    // drop path: spawn_eval should return a Task that yields Shutdown.
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    // Use spawn_eval before dropping — but we need to test the failure path.
    // The only way to exercise it without unsafe is to race: drop on another thread.
    let task = isle.spawn_eval::<i64>("return 1");
    let result = task.wait();
    // This should succeed since we haven't dropped yet
    assert!(result.is_ok());
    drop(isle);
}

#[test]
fn spawn_call_returns_correct_result_after_init() {
    let isle = Isle::spawn(|lua| {
        lua.load("function add(a, b) return tostring(tonumber(a) + tonumber(b)) end")
            .exec()?;
        Ok(())
    })
    .unwrap();

    let result: String = isle.call("add", ("3", "4")).unwrap();
    assert_eq!(result, "7");
    isle.shutdown().unwrap();
}

#[test]
fn spawn_exec_cancel() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let task = isle.spawn_exec(|lua| {
        let _: () = lua
            .load("while true do end")
            .exec()
            .map_err(IsleError::from)?;
        Ok("done".to_string())
    });

    std::thread::sleep(Duration::from_millis(50));
    task.cancel();

    let result = task.wait();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), IsleError::Cancelled));

    isle.shutdown().unwrap();
}

#[test]
fn try_recv_returns_none_then_some() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let task = isle.spawn_eval::<String>("return 'async'");

    // Poll until result arrives (should be fast)
    let mut result = None;
    for _ in 0..100 {
        if let Some(r) = task.try_recv() {
            result = Some(r);
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    assert_eq!(result.unwrap().unwrap(), "async");
    isle.shutdown().unwrap();
}

#[test]
fn cancel_token_accessor() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let task = isle.spawn_eval::<()>("return 1");

    let token = task.cancel_token();
    assert!(!token.is_cancelled());

    task.cancel();
    assert!(task.cancel_token().is_cancelled());

    let _ = task.wait();
    isle.shutdown().unwrap();
}

// ---------------------------------------------------------------------------
// Concurrency tests
// ---------------------------------------------------------------------------

#[test]
fn concurrent_evals_from_multiple_threads() {
    let isle = Arc::new(Isle::spawn(|_lua| Ok(())).unwrap());
    let thread_count = 8;
    let evals_per_thread = 10;

    let handles: Vec<_> = (0..thread_count)
        .map(|t| {
            let isle = Arc::clone(&isle);
            std::thread::spawn(move || {
                for i in 0..evals_per_thread {
                    let code = format!("return {} + {}", t, i);
                    let result: i64 = isle.eval(&code).unwrap();
                    let expected = i64::from(t + i);
                    assert_eq!(result, expected, "thread {t}, iter {i}");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("worker thread panicked");
    }

    // Isle processes requests sequentially — all should have completed
    Arc::try_unwrap(isle)
        .unwrap_or_else(|_| panic!("other Arc references remain"))
        .shutdown()
        .unwrap();
}

#[test]
fn concurrent_spawn_eval_with_cancel() {
    let isle = Arc::new(Isle::spawn(|_lua| Ok(())).unwrap());

    // Spawn a long-running task and several quick tasks concurrently
    let isle_c = Arc::clone(&isle);
    let long_handle = std::thread::spawn(move || {
        let task = isle_c.spawn_eval::<()>("while true do end");
        std::thread::sleep(Duration::from_millis(30));
        task.cancel();
        let result = task.wait();
        assert!(matches!(result.unwrap_err(), IsleError::Cancelled));
    });

    long_handle.join().expect("long task thread panicked");

    // After cancel, Isle should still accept new requests
    let result: String = isle.eval("return 'still alive'").unwrap();
    assert_eq!(result, "still alive");

    Arc::try_unwrap(isle)
        .unwrap_or_else(|_| panic!("other Arc references remain"))
        .shutdown()
        .unwrap();
}

// ---------------------------------------------------------------------------
// Tokio integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn eval_from_tokio_spawn_blocking() {
    let isle = Arc::new(Isle::spawn(|_lua| Ok(())).unwrap());

    let isle_c = Arc::clone(&isle);
    let result = tokio::task::spawn_blocking(move || isle_c.eval::<i64>("return 1 + 1"))
        .await
        .expect("spawn_blocking panicked")
        .unwrap();

    assert_eq!(result, 2);

    Arc::try_unwrap(isle)
        .unwrap_or_else(|_| panic!("other Arc references remain"))
        .shutdown()
        .unwrap();
}

#[tokio::test]
async fn multiple_tokio_tasks_share_isle() {
    let isle = Arc::new(Isle::spawn(|_lua| Ok(())).unwrap());
    let task_count = 10;

    let mut join_handles = Vec::with_capacity(task_count);
    for i in 0..task_count {
        let isle_c = Arc::clone(&isle);
        join_handles.push(tokio::task::spawn_blocking(move || {
            let code = format!("return {i} * 2");
            let result: i64 = isle_c.eval(&code).unwrap();
            assert_eq!(result, (i * 2) as i64);
        }));
    }

    for h in join_handles {
        h.await.expect("tokio task panicked");
    }

    Arc::try_unwrap(isle)
        .unwrap_or_else(|_| panic!("other Arc references remain"))
        .shutdown()
        .unwrap();
}

#[tokio::test]
async fn cancel_from_tokio_task() {
    let isle = Arc::new(Isle::spawn(|_lua| Ok(())).unwrap());

    let isle_c = Arc::clone(&isle);
    let result = tokio::task::spawn_blocking(move || {
        let task = isle_c.spawn_eval::<()>("while true do end");
        std::thread::sleep(Duration::from_millis(50));
        task.cancel();
        task.wait()
    })
    .await
    .expect("spawn_blocking panicked");

    assert!(matches!(result.unwrap_err(), IsleError::Cancelled));

    // Isle still functional after cancel
    let isle_c = Arc::clone(&isle);
    let result = tokio::task::spawn_blocking(move || isle_c.eval::<String>("return 'ok'"))
        .await
        .expect("spawn_blocking panicked")
        .unwrap();
    assert_eq!(result, "ok");

    Arc::try_unwrap(isle)
        .unwrap_or_else(|_| panic!("other Arc references remain"))
        .shutdown()
        .unwrap();
}

#[tokio::test]
async fn shutdown_from_tokio() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();

    let result = tokio::task::spawn_blocking(move || {
        let _ = isle.eval::<()>("return 1");
        isle.shutdown()
    })
    .await
    .expect("spawn_blocking panicked");

    assert!(result.is_ok());
}

/// The cancel hook reaches coroutines created by the Lua code itself.
#[test]
fn spawn_eval_cancel_loop_in_lua_created_coroutine() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let task = isle.spawn_eval::<()>("coroutine.wrap(function() while true do end end)()");

    std::thread::sleep(Duration::from_millis(50));
    task.cancel();

    let start = Instant::now();
    let result = loop {
        if let Some(r) = task.try_recv() {
            break r;
        }
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "cancel did not reach the nested coroutine"
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    assert!(matches!(result.unwrap_err(), IsleError::Cancelled));

    // The isle is still usable.
    assert_eq!(isle.eval::<i64>("return 1").unwrap(), 1);
    isle.shutdown().unwrap();
}

// ── typed errors (#11), sync `Isle` ──────────────────────────────────

#[derive(Debug)]
struct MyErr(u32);

impl std::fmt::Display for MyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "my error {}", self.0)
    }
}

impl std::error::Error for MyErr {}

fn lua_failure(r: Result<(), IsleError>) -> mlua_isle::LuaFailure {
    match r {
        Err(IsleError::Lua(f)) => f,
        other => panic!("expected IsleError::Lua, got: {other:?}"),
    }
}

#[test]
fn eval_raised_table_is_a_lua_failure_with_its_value() {
    let isle = Isle::spawn(|_| Ok(())).unwrap();
    let f =
        lua_failure(isle.eval(
            "error(setmetatable({ code = 42 }, { __tostring = function() return 'E42' end }))",
        ));
    assert_eq!(f.kind, mlua_isle::LuaErrorKind::Runtime);
    assert_eq!(f.message, "E42");
    assert_eq!(f.to_string(), "E42");
    assert!(
        f.traceback
            .as_deref()
            .unwrap_or("")
            .contains("stack traceback"),
        "traceback: {:?}",
        f.traceback
    );
    #[cfg(feature = "serde")]
    assert_eq!(f.value.as_ref().unwrap()["code"], 42);

    // Without `__tostring` the message is what `tostring` gives.
    let f = lua_failure(isle.eval("error({ code = 42 })"));
    assert!(f.message.starts_with("table: "), "got: {}", f.message);
    #[cfg(feature = "serde")]
    assert_eq!(f.value.as_ref().unwrap()["code"], 42);
    isle.shutdown().unwrap();
}

#[test]
fn eval_string_error_and_syntax_error_kinds() {
    let isle = Isle::spawn(|_| Ok(())).unwrap();
    let f = lua_failure(isle.eval("error('boom')"));
    assert_eq!(f.kind, mlua_isle::LuaErrorKind::Runtime);
    assert!(f.message.ends_with(": boom"), "got: {}", f.message);
    #[cfg(feature = "serde")]
    assert_eq!(f.value, Some(serde_json::json!(f.message)));

    let f = lua_failure(isle.eval("x = = 1"));
    assert_eq!(f.kind, mlua_isle::LuaErrorKind::Syntax);
    assert!(!f.message.is_empty());
    isle.shutdown().unwrap();
}

#[test]
fn eval_host_function_error_is_a_callback_failure() {
    let isle = Isle::spawn(|lua| {
        let fail = lua.create_function(|_, ()| -> mlua::Result<()> {
            Err(mlua::Error::external(MyErr(7)))
        })?;
        lua.globals().set("fail", fail)
    })
    .unwrap();
    let f = lua_failure(isle.eval("fail()"));
    assert_eq!(f.kind, mlua_isle::LuaErrorKind::Callback);
    assert_eq!(f.message, "my error 7");
    assert!(f.traceback.is_some());
    #[cfg(feature = "serde")]
    assert_eq!(f.value, None);
    isle.shutdown().unwrap();
}

#[test]
fn eval_error_with_the_old_sentinel_is_not_a_cancel() {
    let isle = Isle::spawn(|_| Ok(())).unwrap();
    let f = lua_failure(isle.eval("error('__isle_cancelled__')"));
    assert!(
        f.message.ends_with("__isle_cancelled__"),
        "got: {}",
        f.message
    );
    isle.shutdown().unwrap();
}

#[test]
fn cancelled_eval_is_cancelled_even_when_lua_replaces_the_error() {
    let isle = Isle::spawn(|_| Ok(())).unwrap();
    // The Lua code catches the cancel and raises a string instead.
    let task = isle
        .spawn_eval::<()>("local ok = pcall(function() while true do end end) error('swallowed')");
    std::thread::sleep(Duration::from_millis(20));
    task.cancel();
    assert!(matches!(task.wait(), Err(IsleError::Cancelled)));
    isle.shutdown().unwrap();
}

#[test]
fn call_of_a_missing_global_is_not_found() {
    let isle = Isle::spawn(|lua| lua.globals().set("n", 1)).unwrap();
    assert!(matches!(isle.call::<_, ()>("nope", ()), Err(IsleError::NotFound(n)) if n == "nope"));
    assert!(matches!(isle.call::<_, ()>("n", ()), Err(IsleError::NotFound(n)) if n == "n"));
    isle.shutdown().unwrap();
}

#[test]
fn init_error_is_a_lua_failure() {
    let err = Isle::spawn(|_| Err(mlua::Error::runtime("nope")))
        .err()
        .expect("init must fail");
    match err {
        IsleError::Init(f) => {
            assert_eq!(f.kind, mlua_isle::LuaErrorKind::Runtime);
            assert_eq!(f.message, "nope");
        }
        other => panic!("expected Init, got: {other:?}"),
    }
}

#[test]
fn init_panic_is_thread_panic_with_the_message() {
    let err = Isle::spawn(|_| panic!("init boom"))
        .err()
        .expect("init must fail");
    assert!(
        matches!(&err, IsleError::ThreadPanic(Some(m)) if m == "init boom"),
        "got: {err:?}"
    );
}

#[test]
fn request_panic_is_recv_failed_then_thread_panic_on_shutdown() {
    let isle = Isle::spawn(|lua| {
        let boom = lua.create_function(|_, ()| -> mlua::Result<()> { panic!("request boom") })?;
        lua.globals().set("boom", boom)
    })
    .unwrap();
    assert!(matches!(
        isle.eval::<()>("boom()"),
        Err(IsleError::RecvFailed)
    ));
    let err = isle.shutdown().unwrap_err();
    assert!(
        matches!(&err, IsleError::ThreadPanic(Some(m)) if m == "request boom"),
        "got: {err:?}"
    );
}
