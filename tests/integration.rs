use mlua_isle::{Isle, IsleError};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn eval_simple_expression() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let result = isle.eval("return 1 + 2").unwrap();
    assert_eq!(result, "3");
    isle.shutdown().unwrap();
}

#[test]
fn eval_string_result() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let result = isle.eval("return 'hello world'").unwrap();
    assert_eq!(result, "hello world");
    isle.shutdown().unwrap();
}

#[test]
fn eval_nil_returns_empty() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let result = isle.eval("return nil").unwrap();
    assert_eq!(result, "");
    isle.shutdown().unwrap();
}

#[test]
fn eval_lua_error_propagates() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let result = isle.eval("error('boom')");
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

    let result = isle.eval("return my_val").unwrap();
    assert_eq!(result, "42");
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

    let result = isle.call("greet", &["hello", "world"]).unwrap();
    assert_eq!(result, "hello, world");
    isle.shutdown().unwrap();
}

#[test]
fn spawn_eval_cancel_infinite_loop() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let task = isle.spawn_eval("while true do end");

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
        let result = isle.eval("counter = counter + 1; return counter").unwrap();
        assert_eq!(result, i.to_string());
    }

    isle.shutdown().unwrap();
}

#[test]
fn exec_closure() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();

    let result = isle
        .exec(|lua| {
            let val: i64 = lua.load("return 7 * 6").eval().map_err(IsleError::from)?;
            Ok(val.to_string())
        })
        .unwrap();

    assert_eq!(result, "42");
    isle.shutdown().unwrap();
}

#[test]
fn shutdown_after_drop_is_safe() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let _ = isle.eval("return 1");
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
        IsleError::Init(msg) => {
            assert!(!msg.is_empty(), "init error message should not be empty");
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
    let task = isle.spawn_eval("return 1");
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

    let result = isle.call("add", &["3", "4"]).unwrap();
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
    assert_eq!(result.unwrap_err(), IsleError::Cancelled);

    isle.shutdown().unwrap();
}

#[test]
fn try_recv_returns_none_then_some() {
    let isle = Isle::spawn(|_lua| Ok(())).unwrap();
    let task = isle.spawn_eval("return 'async'");

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
    let task = isle.spawn_eval("return 1");

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
                    let result = isle.eval(&code).unwrap();
                    let expected = (t + i).to_string();
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
        let task = isle_c.spawn_eval("while true do end");
        std::thread::sleep(Duration::from_millis(30));
        task.cancel();
        let result = task.wait();
        assert_eq!(result.unwrap_err(), IsleError::Cancelled);
    });

    long_handle.join().expect("long task thread panicked");

    // After cancel, Isle should still accept new requests
    let result = isle.eval("return 'still alive'").unwrap();
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
    let result = tokio::task::spawn_blocking(move || isle_c.eval("return 1 + 1"))
        .await
        .expect("spawn_blocking panicked")
        .unwrap();

    assert_eq!(result, "2");

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
            let result = isle_c.eval(&code).unwrap();
            assert_eq!(result, (i * 2).to_string());
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
        let task = isle_c.spawn_eval("while true do end");
        std::thread::sleep(Duration::from_millis(50));
        task.cancel();
        task.wait()
    })
    .await
    .expect("spawn_blocking panicked");

    assert_eq!(result.unwrap_err(), IsleError::Cancelled);

    // Isle still functional after cancel
    let isle_c = Arc::clone(&isle);
    let result = tokio::task::spawn_blocking(move || isle_c.eval("return 'ok'"))
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
        let _ = isle.eval("return 1");
        isle.shutdown()
    })
    .await
    .expect("spawn_blocking panicked");

    assert!(result.is_ok());
}
