#![cfg(feature = "tokio")]

use mlua_isle::{AsyncIsle, IsleError};
use std::time::{Duration, Instant};

#[tokio::test]
async fn async_eval_simple() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result: i64 = isle.eval("return 1 + 2").await.unwrap();
    assert_eq!(result, 3);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_eval_string() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result: String = isle.eval("return 'hello world'").await.unwrap();
    assert_eq!(result, "hello world");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_eval_nil() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result: Option<String> = isle.eval("return nil").await.unwrap();
    assert_eq!(result, None);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_eval_lua_error() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result = isle.eval::<()>("error('boom')").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("boom"));
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_init_sets_globals() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        lua.globals().set("my_val", 42)?;
        Ok(())
    })
    .await
    .unwrap();

    let result: i64 = isle.eval("return my_val").await.unwrap();
    assert_eq!(result, 42);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_call_global_function() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
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
    .await
    .unwrap();

    let result: String = isle.call("greet", ("hello", "world")).await.unwrap();
    assert_eq!(result, "hello, world");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_exec_closure() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();

    let result = isle
        .exec(|lua| {
            let val: i64 = lua.load("return 7 * 6").eval()?;
            Ok(val)
        })
        .await
        .unwrap();

    assert_eq!(result, 42);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_spawn_eval_cancel() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let task = isle.spawn_eval::<()>("while true do end");

    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
    });

    let start = Instant::now();
    let result = task.await;
    let elapsed = start.elapsed();

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), IsleError::Cancelled));
    assert!(
        elapsed < Duration::from_secs(2),
        "cancel took too long: {elapsed:?}"
    );

    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_spawn_call_cancel() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        lua.load("function spin() while true do end end").exec()?;
        Ok(())
    })
    .await
    .unwrap();

    let task = isle.spawn_call::<_, ()>("spin", ());
    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
    });

    let result = task.await;
    assert!(matches!(result.unwrap_err(), IsleError::Cancelled));
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_spawn_exec_cancel() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let task = isle.spawn_exec(|lua| {
        let _: () = lua
            .load("while true do end")
            .exec()
            .map_err(IsleError::from)?;
        Ok("done".to_string())
    });

    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
    });

    let result = task.await;
    assert!(matches!(result.unwrap_err(), IsleError::Cancelled));
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_multiple_sequential_evals() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        lua.globals().set("counter", 0)?;
        Ok(())
    })
    .await
    .unwrap();

    for i in 1..=5 {
        let result: i64 = isle
            .eval("counter = counter + 1; return counter")
            .await
            .unwrap();
        assert_eq!(result, i);
    }

    driver.shutdown().await.unwrap();
}

/// Clone the handle freely — no Arc needed.
#[tokio::test]
async fn async_concurrent_evals_from_multiple_tasks() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let task_count = 10;

    let mut handles = Vec::with_capacity(task_count);
    for i in 0..task_count {
        let isle = isle.clone();
        handles.push(tokio::spawn(async move {
            let code = format!("return {i} * 3");
            let result: i64 = isle.eval(&code).await.unwrap();
            assert_eq!(result, (i * 3) as i64);
        }));
    }

    for h in handles {
        h.await.expect("tokio task panicked");
    }

    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_init_error_propagates() {
    let result = AsyncIsle::spawn(|lua| {
        lua.load("this is not valid lua").exec()?;
        Ok(())
    })
    .await;

    assert!(result.is_err());
    match result.err().unwrap() {
        IsleError::Init(f) => {
            assert_eq!(f.kind, mlua_isle::LuaErrorKind::Syntax);
            assert!(!f.message.is_empty());
        }
        other => panic!("expected Init error, got: {other}"),
    }
}

#[tokio::test]
async fn async_is_alive_handle() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    assert!(isle.is_alive());
    driver.shutdown().await.unwrap();
    assert!(!isle.is_alive());
}

#[tokio::test]
async fn async_is_alive_driver() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    assert!(driver.is_alive());
    drop(isle);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_drop_without_shutdown() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let _ = isle.eval::<()>("return 1").await;
    drop(isle);
    drop(driver);
    // Should not panic or hang
}

#[tokio::test]
async fn async_still_works_after_cancel() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();

    // Cancel a long-running task
    let task = isle.spawn_eval::<()>("while true do end");
    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        token.cancel();
    });
    let _ = task.await;

    // Isle should still accept new requests
    let result: String = isle.eval("return 'still alive'").await.unwrap();
    assert_eq!(result, "still alive");

    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_channel_full_returns_correct_error() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();

    // Block the Lua thread so it never drains the channel.
    let blocker = isle.spawn_eval::<()>("while true do end");
    let blocker_token = blocker.cancel_token().clone();

    // Give the Lua thread time to start the infinite loop.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Fill the channel (capacity = 256) then expect ChannelFull.
    let mut last_task = None;
    for _ in 0..300 {
        last_task = Some(isle.spawn_eval::<i64>("return 1"));
    }

    // The last task should be ChannelFull (channel was full).
    let result = last_task.unwrap().await;
    assert!(matches!(result, Err(IsleError::ChannelFull)));

    blocker_token.cancel();
    let _ = blocker.await;
    driver.shutdown().await.unwrap();
}

/// Cloned handles work independently; dropping one does not affect others.
#[tokio::test]
async fn async_clone_independence() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let isle2 = isle.clone();

    let r1: i64 = isle.eval("return 1").await.unwrap();
    drop(isle);

    // isle2 still works after isle is dropped.
    let r2: i64 = isle2.eval("return 2").await.unwrap();
    assert_eq!(r1, 1);
    assert_eq!(r2, 2);

    driver.shutdown().await.unwrap();
}

/// Dropping the Driver does NOT kill the Lua thread while Handle clones exist.
/// "In Rust, cancellation is drop" — the thread lives until all senders are gone.
#[tokio::test]
async fn async_driver_drop_does_not_kill_handles() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let isle2 = isle.clone();

    // Drop the driver without shutdown.
    drop(driver);

    // Both handles should still work — the Lua thread is alive.
    let r1: String = isle.eval("return 'from isle'").await.unwrap();
    let r2: String = isle2.eval("return 'from isle2'").await.unwrap();
    assert_eq!(r1, "from isle");
    assert_eq!(r2, "from isle2");

    // Drop all handles → channel disconnects → thread exits naturally.
    drop(isle);
    drop(isle2);
}

/// When all handles AND driver are dropped, the thread exits via channel disconnect.
#[tokio::test]
async fn async_natural_shutdown_via_channel_disconnect() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();

    let _ = isle.eval::<i64>("return 1").await.unwrap();

    // Drop everything — no explicit shutdown.
    // Thread exits because blocking_recv returns None.
    drop(isle);
    drop(driver);

    // Brief pause to let the detached thread clean up.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// ── Builder tests ────────────────────────────────────────────────────

#[tokio::test]
async fn builder_default_works() {
    let (isle, driver) = AsyncIsle::builder().spawn(|_lua| Ok(())).await.unwrap();

    let result: i64 = isle.eval("return 42").await.unwrap();
    assert_eq!(result, 42);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn builder_custom_capacity() {
    let (isle, driver) = AsyncIsle::builder()
        .channel_capacity(8)
        .spawn(|_lua| Ok(()))
        .await
        .unwrap();

    let result: String = isle.eval("return 'ok'").await.unwrap();
    assert_eq!(result, "ok");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn builder_custom_thread_name() {
    let (isle, driver) = AsyncIsle::builder()
        .thread_name("my-lua-worker")
        .spawn(|_lua| Ok(()))
        .await
        .unwrap();

    let result: String = isle.eval("return 'named'").await.unwrap();
    assert_eq!(result, "named");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn builder_small_capacity_triggers_channel_full() {
    let (isle, driver) = AsyncIsle::builder()
        .channel_capacity(2)
        .spawn(|_lua| Ok(()))
        .await
        .unwrap();

    // Block the Lua thread.
    let blocker = isle.spawn_eval::<()>("while true do end");
    let blocker_token = blocker.cancel_token().clone();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // With capacity 2, filling should be fast.
    let mut last_task = None;
    for _ in 0..10 {
        last_task = Some(isle.spawn_eval::<i64>("return 1"));
    }

    let result = last_task.unwrap().await;
    assert!(matches!(result, Err(IsleError::ChannelFull)));

    blocker_token.cancel();
    let _ = blocker.await;
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn builder_all_options() {
    let (isle, driver) = AsyncIsle::builder()
        .channel_capacity(32)
        .thread_name("custom-isle")
        .spawn(|lua| {
            lua.globals().set("x", 99)?;
            Ok(())
        })
        .await
        .unwrap();

    let result: i64 = isle.eval("return x").await.unwrap();
    assert_eq!(result, 99);
    driver.shutdown().await.unwrap();
}

// ── Coroutine tests ─────────────────────────────────────────────────

#[tokio::test]
async fn coroutine_eval_simple() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result: i64 = isle.coroutine_eval("return 1 + 2").await.unwrap();
    assert_eq!(result, 3);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn coroutine_eval_string() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result: String = isle.coroutine_eval("return 'hello'").await.unwrap();
    assert_eq!(result, "hello");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn coroutine_eval_nil() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result: Option<String> = isle.coroutine_eval("return nil").await.unwrap();
    assert_eq!(result, None);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn coroutine_eval_error() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result = isle.coroutine_eval::<()>("error('boom')").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("boom"));
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn coroutine_eval_accesses_globals() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        lua.globals().set("val", 42)?;
        Ok(())
    })
    .await
    .unwrap();

    let result: i64 = isle.coroutine_eval("return val * 2").await.unwrap();
    assert_eq!(result, 84);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn coroutine_call_simple() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        lua.load("function add(a, b) return a .. b end").exec()?;
        Ok(())
    })
    .await
    .unwrap();

    let result = isle
        .coroutine_call::<_, String>("add", ("hello", " world"))
        .await
        .unwrap();
    assert_eq!(result, "hello world");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn coroutine_eval_cancel() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let task = isle.spawn_coroutine_eval::<()>("while true do end");

    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
    });

    let start = Instant::now();
    let result = task.await;

    assert!(matches!(result.unwrap_err(), IsleError::Cancelled));
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "cancel took too long"
    );

    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn coroutine_still_works_after_cancel() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();

    // Cancel a coroutine
    let task = isle.spawn_coroutine_eval::<()>("while true do end");
    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        token.cancel();
    });
    let _ = task.await;

    // Isle should still work
    let result: String = isle.coroutine_eval("return 'ok'").await.unwrap();
    assert_eq!(result, "ok");

    driver.shutdown().await.unwrap();
}

/// Multiple coroutines can interleave on the same VM when one yields.
#[tokio::test]
async fn coroutine_concurrent_with_async_function() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        // Register an async Rust function that sleeps briefly.
        let sleep_fn = lua.create_async_function(|_, ms: u64| async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Ok(ms)
        })?;
        lua.globals().set("async_sleep", sleep_fn)?;
        Ok(())
    })
    .await
    .unwrap();

    let start = Instant::now();

    // Launch two coroutines that each sleep 50ms.
    // If they run sequentially: ~100ms.  If cooperative: ~50ms.
    let t1 = isle.spawn_coroutine_eval::<u64>("return async_sleep(50)");
    let t2 = isle.spawn_coroutine_eval::<u64>("return async_sleep(50)");

    let (r1, r2) = tokio::join!(t1, t2);
    let elapsed = start.elapsed();

    assert_eq!(r1.unwrap(), 50);
    assert_eq!(r2.unwrap(), 50);

    // With cooperative scheduling, both should complete in ~50-80ms,
    // not ~100ms.  Use a generous threshold to avoid flaky CI.
    assert!(
        elapsed < Duration::from_millis(90),
        "coroutines ran sequentially ({elapsed:?}), expected cooperative interleaving"
    );

    driver.shutdown().await.unwrap();
}

/// Mixing sync eval and coroutine eval works correctly.
#[tokio::test]
async fn coroutine_mixed_with_sync() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        lua.globals().set("counter", 0)?;
        Ok(())
    })
    .await
    .unwrap();

    // Sync eval
    let r1: i64 = isle
        .eval("counter = counter + 1; return counter")
        .await
        .unwrap();
    assert_eq!(r1, 1);

    // Coroutine eval
    let r2: i64 = isle
        .coroutine_eval("counter = counter + 10; return counter")
        .await
        .unwrap();
    assert_eq!(r2, 11);

    // Sync eval again — state should persist
    let r3: i64 = isle.eval("return counter").await.unwrap();
    assert_eq!(r3, 11);

    driver.shutdown().await.unwrap();
}

/// Pending coroutines are drained (not aborted) on shutdown.
#[tokio::test]
async fn coroutine_pending_drained_on_shutdown() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        let sleep_fn = lua.create_async_function(|_, ms: u64| async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Ok(ms)
        })?;
        lua.globals().set("async_sleep", sleep_fn)?;
        Ok(())
    })
    .await
    .unwrap();

    // Spawn a coroutine that takes 80ms.
    let task = isle.spawn_coroutine_eval::<u64>("return async_sleep(80)");

    // Immediately request shutdown — the coroutine is still running.
    tokio::time::sleep(Duration::from_millis(10)).await;
    driver.shutdown().await.unwrap();

    // The coroutine should have been drained (completed), not aborted.
    let result = task.await;
    assert_eq!(result.unwrap(), 80);
}

/// Cancel a nested-coroutine CPU loop and check the isle still answers.
async fn assert_nested_loop_cancels(isle: &AsyncIsle, task: mlua_isle::AsyncTask<()>) {
    let token = task.cancel_token().clone();
    tokio::time::sleep(Duration::from_millis(50)).await;
    token.cancel();

    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("cancel did not reach the nested coroutine");
    assert!(matches!(result.unwrap_err(), IsleError::Cancelled));

    let probe = tokio::time::timeout(Duration::from_secs(2), isle.eval::<i64>("return 1"))
        .await
        .expect("isle thread is stuck");
    assert_eq!(probe.unwrap(), 1);
}

const NESTED_LOOP: &str = "coroutine.wrap(function() while true do end end)()";

#[tokio::test]
async fn spawn_eval_cancel_loop_in_lua_created_coroutine() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let task = isle.spawn_eval::<()>(NESTED_LOOP);
    assert_nested_loop_cancels(&isle, task).await;
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn coroutine_eval_cancel_loop_in_lua_created_coroutine() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let task = isle.spawn_coroutine_eval::<()>(NESTED_LOOP);
    assert_nested_loop_cancels(&isle, task).await;
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn coroutine_call_cancel_loop_in_lua_created_coroutine() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        lua.load(format!("function spin() {NESTED_LOOP} end"))
            .exec()
    })
    .await
    .unwrap();
    let task = isle.spawn_coroutine_call::<_, ()>("spin", ());
    assert_nested_loop_cancels(&isle, task).await;
    driver.shutdown().await.unwrap();
}

/// Records when it is dropped.
struct DropProbe(std::sync::Arc<std::sync::Mutex<Option<Instant>>>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        *self.0.lock().unwrap() = Some(Instant::now());
    }
}

/// Cancelling a coroutine request releases the Rust future it awaits
/// right away, not at the next Lua GC cycle or at shutdown.
#[tokio::test]
async fn coroutine_cancel_drops_awaited_future_immediately() {
    let dropped_at = std::sync::Arc::new(std::sync::Mutex::new(None));
    let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (d, f) = (dropped_at.clone(), finished.clone());
    let (isle, driver) = AsyncIsle::spawn(move |lua| {
        let hold = lua.create_async_function(move |_, ()| {
            let (d, f) = (d.clone(), f.clone());
            async move {
                let _probe = DropProbe(d);
                tokio::time::sleep(Duration::from_secs(5)).await;
                f.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        })?;
        lua.globals().set("hold", hold)
    })
    .await
    .unwrap();

    let task = isle.spawn_coroutine_eval::<String>("hold() return 'done'");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cancelled_at = Instant::now();
    task.cancel();
    assert!(matches!(task.await.unwrap_err(), IsleError::Cancelled));

    // No collectgarbage() and no shutdown: the drop must already be done.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let dropped_at = dropped_at
        .lock()
        .unwrap()
        .expect("awaited future was not dropped on cancel");
    assert!(
        dropped_at.duration_since(cancelled_at) < Duration::from_millis(50),
        "drop was delayed: {:?}",
        dropped_at.duration_since(cancelled_at)
    );
    assert!(!finished.load(std::sync::atomic::Ordering::SeqCst));

    driver.shutdown().await.unwrap();
}

/// Cancelling a coroutine request closes its pending to-be-closed variables.
#[tokio::test]
async fn coroutine_cancel_closes_to_be_closed_variables() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        let hold = lua.create_async_function(|_, ()| async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(())
        })?;
        lua.globals().set("hold", hold)?;
        lua.globals().set("closed", false)
    })
    .await
    .unwrap();

    let task = isle.spawn_coroutine_eval::<()>(
        "local guard <close> = setmetatable({}, { __close = function() closed = true end }) \
         hold()",
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    task.cancel();
    assert!(matches!(task.await.unwrap_err(), IsleError::Cancelled));

    assert!(isle.eval::<bool>("return closed == true").await.unwrap());
    driver.shutdown().await.unwrap();
}
