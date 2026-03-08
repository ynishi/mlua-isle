#![cfg(feature = "tokio")]

use mlua_isle::{AsyncIsle, IsleError};
use std::time::{Duration, Instant};

#[tokio::test]
async fn async_eval_simple() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result = isle.eval("return 1 + 2").await.unwrap();
    assert_eq!(result, "3");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_eval_string() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result = isle.eval("return 'hello world'").await.unwrap();
    assert_eq!(result, "hello world");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_eval_nil() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result = isle.eval("return nil").await.unwrap();
    assert_eq!(result, "");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_eval_lua_error() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let result = isle.eval("error('boom')").await;
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

    let result = isle.eval("return my_val").await.unwrap();
    assert_eq!(result, "42");
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

    let result = isle.call("greet", &["hello", "world"]).await.unwrap();
    assert_eq!(result, "hello, world");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_exec_closure() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();

    let result = isle
        .exec(|lua| {
            let val: i64 = lua.load("return 7 * 6").eval().map_err(IsleError::from)?;
            Ok(val.to_string())
        })
        .await
        .unwrap();

    assert_eq!(result, "42");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_spawn_eval_cancel() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let task = isle.spawn_eval("while true do end");

    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
    });

    let start = Instant::now();
    let result = task.await;
    let elapsed = start.elapsed();

    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), IsleError::Cancelled);
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

    let task = isle.spawn_call("spin", &[]);
    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
    });

    let result = task.await;
    assert_eq!(result.unwrap_err(), IsleError::Cancelled);
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
    assert_eq!(result.unwrap_err(), IsleError::Cancelled);
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
        let result = isle
            .eval("counter = counter + 1; return counter")
            .await
            .unwrap();
        assert_eq!(result, i.to_string());
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
            let result = isle.eval(&code).await.unwrap();
            assert_eq!(result, (i * 3).to_string());
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
        IsleError::Init(msg) => {
            assert!(!msg.is_empty());
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
    let _ = isle.eval("return 1").await;
    drop(isle);
    drop(driver);
    // Should not panic or hang
}

#[tokio::test]
async fn async_still_works_after_cancel() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();

    // Cancel a long-running task
    let task = isle.spawn_eval("while true do end");
    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        token.cancel();
    });
    let _ = task.await;

    // Isle should still accept new requests
    let result = isle.eval("return 'still alive'").await.unwrap();
    assert_eq!(result, "still alive");

    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn async_channel_full_returns_correct_error() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();

    // Block the Lua thread so it never drains the channel.
    let blocker = isle.spawn_eval("while true do end");
    let blocker_token = blocker.cancel_token().clone();

    // Give the Lua thread time to start the infinite loop.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Fill the channel (capacity = 256) then expect ChannelFull.
    let mut last_task = None;
    for _ in 0..300 {
        last_task = Some(isle.spawn_eval("return 1"));
    }

    // The last task should be ChannelFull (channel was full).
    let result = last_task.unwrap().await;
    assert_eq!(result, Err(IsleError::ChannelFull));

    blocker_token.cancel();
    let _ = blocker.await;
    driver.shutdown().await.unwrap();
}

/// Cloned handles work independently; dropping one does not affect others.
#[tokio::test]
async fn async_clone_independence() {
    let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await.unwrap();
    let isle2 = isle.clone();

    let r1 = isle.eval("return 1").await.unwrap();
    drop(isle);

    // isle2 still works after isle is dropped.
    let r2 = isle2.eval("return 2").await.unwrap();
    assert_eq!(r1, "1");
    assert_eq!(r2, "2");

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
    let r1 = isle.eval("return 'from isle'").await.unwrap();
    let r2 = isle2.eval("return 'from isle2'").await.unwrap();
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

    let _ = isle.eval("return 1").await.unwrap();

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

    let result = isle.eval("return 42").await.unwrap();
    assert_eq!(result, "42");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn builder_custom_capacity() {
    let (isle, driver) = AsyncIsle::builder()
        .channel_capacity(8)
        .spawn(|_lua| Ok(()))
        .await
        .unwrap();

    let result = isle.eval("return 'ok'").await.unwrap();
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

    let result = isle.eval("return 'named'").await.unwrap();
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
    let blocker = isle.spawn_eval("while true do end");
    let blocker_token = blocker.cancel_token().clone();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // With capacity 2, filling should be fast.
    let mut last_task = None;
    for _ in 0..10 {
        last_task = Some(isle.spawn_eval("return 1"));
    }

    let result = last_task.unwrap().await;
    assert_eq!(result, Err(IsleError::ChannelFull));

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

    let result = isle.eval("return x").await.unwrap();
    assert_eq!(result, "99");
    driver.shutdown().await.unwrap();
}
