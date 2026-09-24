#![cfg(feature = "tokio")]
//! Structured tasks, hook sharing, two-stage cancel and handle drop.

use mlua_isle::hooks::{self, CancelConfig};
use mlua_isle::{cancellable, tasks, AsyncIsle, CancelToken, IsleError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Isle with `task`, `sleep(ms)` (cancellable), `hold(ms)` (not
/// cancellable) and `probe(ms, name)` (records when its future drops).
async fn isle_with(
    config: CancelConfig,
    drops: Arc<Mutex<Vec<(String, Instant)>>>,
) -> (AsyncIsle, mlua_isle::AsyncIsleDriver) {
    AsyncIsle::spawn(move |lua| {
        hooks::configure(lua, config);
        lua.globals().set("task", tasks::install(lua)?)?;
        let sleep = lua.create_async_function(|_, ms: u64| {
            cancellable(async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            })
        })?;
        lua.globals().set("sleep", sleep)?;
        let hold = lua.create_async_function(|_, ms: u64| async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Ok(())
        })?;
        lua.globals().set("hold", hold)?;
        let probe = lua.create_async_function(move |_, (ms, name): (u64, String)| {
            let drops = drops.clone();
            async move {
                struct Probe(Arc<Mutex<Vec<(String, Instant)>>>, String);
                impl Drop for Probe {
                    fn drop(&mut self) {
                        let name = std::mem::take(&mut self.1);
                        self.0.lock().unwrap().push((name, Instant::now()));
                    }
                }
                let _p = Probe(drops, name);
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            }
        })?;
        lua.globals().set("probe", probe)
    })
    .await
    .unwrap()
}

async fn isle() -> (AsyncIsle, mlua_isle::AsyncIsleDriver) {
    isle_with(CancelConfig::default(), Default::default()).await
}

async fn within<F: std::future::Future>(ms: u64, f: F) -> F::Output {
    tokio::time::timeout(Duration::from_millis(ms), f)
        .await
        .expect("timed out")
}

// ── task library ──

#[tokio::test]
async fn join_returns_all_values() {
    let (isle, driver) = isle().await;
    let r = isle
        .coroutine_eval(
            "local h = task.spawn(function(a, b) sleep(5) return a + b, a * b end, 3, 4)
             local ok, s, p = h:join()
             return tostring(ok) .. ' ' .. s .. ' ' .. p",
        )
        .await
        .unwrap();
    assert_eq!(r, "true 7 12");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn join_keeps_the_raw_error_value() {
    let (isle, driver) = isle().await;
    let r = isle
        .coroutine_eval(
            "local h = task.spawn(function() error({ code = 42 }) end)
             local ok, err = h:join()
             return tostring(ok) .. ' ' .. type(err) .. ' ' .. err.code",
        )
        .await
        .unwrap();
    assert_eq!(r, "false table 42");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn tasks_run_concurrently() {
    let (isle, driver) = isle().await;
    let start = Instant::now();
    isle.coroutine_eval(
        "local a = task.spawn(function() sleep(100) end)
         local b = task.spawn(function() sleep(100) end)
         a:join() b:join()",
    )
    .await
    .unwrap();
    assert!(
        start.elapsed() < Duration::from_millis(180),
        "tasks ran sequentially: {:?}",
        start.elapsed()
    );
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn second_join_is_an_error() {
    let (isle, driver) = isle().await;
    let err = isle
        .coroutine_eval("local h = task.spawn(function() end) h:join() h:join()")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already joined"), "got: {err}");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_task_joins_as_cancelled() {
    let (isle, driver) = isle().await;
    let r = isle
        .coroutine_eval(
            "local h = task.spawn(function() sleep(5000) end)
             sleep(10)
             h:cancel()
             local ok, err = h:join()
             return tostring(ok) .. ' ' .. tostring(err == task.CANCELLED) .. ' ' .. tostring(h:done())",
        )
        .await
        .unwrap();
    assert_eq!(r, "false true true");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn spawn_in_a_sync_request_is_an_error() {
    let (isle, driver) = isle().await;
    let err = isle.eval("task.spawn(function() end)").await.unwrap_err();
    assert!(
        err.to_string()
            .contains("not inside a coroutine request or task"),
        "got: {err}"
    );
    driver.shutdown().await.unwrap();
}

// ── structured concurrency ──

#[tokio::test]
async fn unjoined_tasks_are_cancelled_and_awaited_when_the_request_ends() {
    let (isle, driver) = isle().await;
    isle.eval("closed = false").await.unwrap();
    let start = Instant::now();
    let r = within(
        2000,
        isle.coroutine_eval(
            "task.spawn(function()
               local g <close> = setmetatable({}, { __close = function() closed = true end })
               sleep(5000)
             end)
             sleep(10)
             return 'parent done'",
        ),
    )
    .await
    .unwrap();
    assert_eq!(r, "parent done");
    assert!(start.elapsed() < Duration::from_millis(1000));
    // The request resolved only after the child had finished.
    assert_eq!(isle.eval("return closed").await.unwrap(), "true");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn close_on_handle_cancels_and_waits() {
    let (isle, driver) = isle().await;
    let r = isle
        .coroutine_eval(
            "local closed = false
             do
               local h <close> = task.spawn(function()
                 local g <close> = setmetatable({}, { __close = function() closed = true end })
                 sleep(5000)
               end)
               sleep(10)
             end
             return tostring(closed)",
        )
        .await
        .unwrap();
    assert_eq!(r, "true");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_the_request_reaches_grandchildren() {
    let drops = Arc::new(Mutex::new(Vec::new()));
    let (isle, driver) = isle_with(CancelConfig::default(), drops.clone()).await;
    let task = isle.spawn_coroutine_eval(
        "task.spawn(function()
           task.spawn(function() probe(5000, 'grandchild') end)
           probe(5000, 'child')
         end)
         probe(5000, 'parent')",
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cancelled_at = Instant::now();
    task.cancel();
    assert_eq!(within(1000, task).await.unwrap_err(), IsleError::Cancelled);

    let drops = drops.lock().unwrap().clone();
    let mut names: Vec<_> = drops.iter().map(|(n, _)| n.as_str()).collect();
    names.sort();
    assert_eq!(names, ["child", "grandchild", "parent"]);
    for (name, at) in &drops {
        assert!(
            at.duration_since(cancelled_at) < Duration::from_millis(100),
            "{name} dropped late"
        );
    }
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_functions_can_derive_child_tokens() {
    let seen = Arc::new(AtomicBool::new(false));
    let s = seen.clone();
    let (isle, driver) = AsyncIsle::spawn(move |lua| {
        let watch = lua.create_function(move |_, ()| {
            let token = mlua_isle::current_token()
                .expect("no current token")
                .child_token();
            let s = s.clone();
            tokio::task::spawn_local(async move {
                token.cancelled().await;
                s.store(true, Ordering::SeqCst);
            });
            Ok(())
        })?;
        lua.globals().set("watch", watch)?;
        let hold = lua.create_async_function(|_, ms: u64| async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Ok(())
        })?;
        lua.globals().set("hold", hold)
    })
    .await
    .unwrap();
    let task = isle.spawn_coroutine_eval("watch() hold(5000)");
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.cancel();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(seen.load(Ordering::SeqCst));
    driver.shutdown().await.unwrap();
}

// ── preemption ──

#[tokio::test]
async fn preemption_lets_a_sibling_cancel_a_cpu_loop() {
    let config = CancelConfig {
        preempt_every: Some(1),
        ..Default::default()
    };
    let (isle, driver) = isle_with(config, Default::default()).await;
    let r = within(
        2000,
        isle.coroutine_eval(
            "local h = task.spawn(function() while true do end end)
             sleep(20)
             h:cancel()
             local ok, err = h:join()
             return tostring(err == task.CANCELLED)",
        ),
    )
    .await
    .unwrap();
    assert_eq!(r, "true");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn preemption_does_not_yield_lua_created_coroutines() {
    let config = CancelConfig {
        preempt_every: Some(1),
        ..Default::default()
    };
    let (isle, driver) = isle_with(config, Default::default()).await;
    let r = within(
        5000,
        isle.coroutine_eval(
            "local h = task.spawn(function()
               local co = coroutine.wrap(function()
                 local n = 0
                 for i = 1, 3000000 do n = n + 1 end
                 return 'done'
               end)
               return co()
             end)
             local ok, v = h:join()
             return v",
        ),
    )
    .await
    .unwrap();
    assert_eq!(r, "done");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn preemption_interleaves_cpu_bound_requests() {
    let config = CancelConfig {
        preempt_every: Some(1),
        ..Default::default()
    };
    let (isle, driver) = isle_with(config, Default::default()).await;
    let spin = isle.spawn_coroutine_eval("while true do end");
    let r = within(2000, isle.coroutine_eval("return 'still responsive'"))
        .await
        .unwrap();
    assert_eq!(r, "still responsive");
    spin.cancel();
    assert_eq!(within(1000, spin).await.unwrap_err(), IsleError::Cancelled);
    driver.shutdown().await.unwrap();
}

// ── two-stage cancel ──

#[tokio::test]
async fn grace_lets_close_handlers_await() {
    let config = CancelConfig {
        grace: Duration::from_millis(500),
        ..Default::default()
    };
    let (isle, driver) = isle_with(config, Default::default()).await;
    isle.eval("cleaned = false").await.unwrap();
    let task = isle.spawn_coroutine_eval(
        "local g <close> = setmetatable({}, { __close = function()
           hold(20)
           cleaned = true
         end })
         sleep(5000)",
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.cancel();
    assert_eq!(within(1000, task).await.unwrap_err(), IsleError::Cancelled);
    assert_eq!(isle.eval("return cleaned").await.unwrap(), "true");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn without_grace_an_awaiting_close_handler_cannot_finish() {
    let (isle, driver) = isle().await;
    isle.eval("cleaned = false").await.unwrap();
    let task = isle.spawn_coroutine_eval(
        "local g <close> = setmetatable({}, { __close = function()
           hold(20)
           cleaned = true
         end })
         hold(5000)",
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.cancel();
    assert_eq!(within(1000, task).await.unwrap_err(), IsleError::Cancelled);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(isle.eval("return cleaned").await.unwrap(), "false");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn grace_ends_with_a_hard_drop() {
    let config = CancelConfig {
        grace: Duration::from_millis(100),
        ..Default::default()
    };
    let drops = Arc::new(Mutex::new(Vec::new()));
    let (isle, driver) = isle_with(config, drops.clone()).await;
    // `probe` is not cancellable: only the hard drop releases it.
    let task = isle.spawn_coroutine_eval("probe(5000, 'held')");
    tokio::time::sleep(Duration::from_millis(30)).await;
    let cancelled_at = Instant::now();
    task.cancel();
    assert_eq!(within(1000, task).await.unwrap_err(), IsleError::Cancelled);
    let (_, at) = drops.lock().unwrap()[0];
    let waited = at.duration_since(cancelled_at);
    assert!(
        waited >= Duration::from_millis(90) && waited < Duration::from_millis(400),
        "dropped after {waited:?}"
    );
    driver.shutdown().await.unwrap();
}

// ── handle drop ──

#[tokio::test]
async fn dropping_an_async_task_cancels_it() {
    let (isle, driver) = isle().await;
    drop(isle.spawn_eval("while true do end"));
    // Would block forever if the loop were still running.
    assert_eq!(within(2000, isle.eval("return 1")).await.unwrap(), "1");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_detached_async_task_runs_to_completion() {
    let (isle, driver) = isle().await;
    isle.eval("finished = false").await.unwrap();
    isle.spawn_coroutine_eval("sleep(20) finished = true")
        .detach();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(isle.eval("return finished").await.unwrap(), "true");
    driver.shutdown().await.unwrap();
}

#[test]
fn dropping_a_sync_task_cancels_it_and_detach_does_not() {
    let isle = mlua_isle::Isle::spawn(|_| Ok(())).unwrap();
    drop(isle.spawn_eval("while true do end"));
    assert_eq!(isle.eval("return 1").unwrap(), "1");

    isle.spawn_eval("finished = true").detach();
    assert_eq!(isle.eval("return finished").unwrap(), "true");
    isle.shutdown().unwrap();
}

// ── hook sharing ──

#[tokio::test]
async fn user_hooks_coexist_with_cancellation() {
    let lines = Arc::new(Mutex::new(0u64));
    let l = lines.clone();
    let (isle, driver) = AsyncIsle::spawn(move |lua| {
        hooks::add_hook(lua, mlua::HookTriggers::EVERY_LINE, move |_, _| {
            *l.lock().unwrap() += 1;
            Ok(mlua::VmState::Continue)
        })
        .map_err(mlua::Error::external)?;
        Ok(())
    })
    .await
    .unwrap();

    isle.eval("local a = 1\nlocal b = 2").await.unwrap();
    assert!(*lines.lock().unwrap() >= 2);

    let task = isle.spawn_eval("coroutine.wrap(function() while true do end end)()");
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.cancel();
    assert_eq!(within(1000, task).await.unwrap_err(), IsleError::Cancelled);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_hook_replaced_with_set_hook_is_restored_at_the_next_request() {
    let (isle, driver) = isle().await;
    isle.exec(|lua| {
        lua.set_hook(mlua::HookTriggers::EVERY_LINE, |_, _| {
            Ok(mlua::VmState::Continue)
        })
        .map_err(IsleError::from)?;
        Ok(String::new())
    })
    .await
    .unwrap();

    let task = isle.spawn_eval("coroutine.wrap(function() while true do end end)()");
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.cancel();
    assert_eq!(within(1000, task).await.unwrap_err(), IsleError::Cancelled);
    driver.shutdown().await.unwrap();
}

/// The "Structured tasks (async)" example of the README.
#[tokio::test]
async fn readme_structured_tasks_example() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        hooks::configure(
            lua,
            CancelConfig {
                grace: Duration::from_millis(100),
                preempt_every: Some(1),
            },
        );
        lua.globals().set("task", tasks::install(lua)?)?;
        let sleep = lua.create_async_function(|_, ms: u64| {
            cancellable(async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            })
        })?;
        lua.globals().set("sleep", sleep)
    })
    .await
    .unwrap();

    let r = within(
        2000,
        isle.coroutine_eval(
            r#"
            local a = task.spawn(function() sleep(10) return "a" end)
            local b = task.spawn(function() error({ code = 42 }) end)
            local _, va = a:join()
            local _, err = b:join()
            local slow <close> = task.spawn(function() sleep(10000) end)
            return va .. err.code
            "#,
        ),
    )
    .await
    .unwrap();
    assert_eq!(r, "a42");
    driver.shutdown().await.unwrap();
}

// ── outside an isle ──

#[test]
fn run_root_drives_tasks_on_a_vm_you_own() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let lua = mlua::Lua::new();
    hooks::install(&lua).unwrap();
    lua.globals()
        .set("task", tasks::install(&lua).unwrap())
        .unwrap();
    let f: mlua::Function = lua
        .load(
            "return function(x)
               local h = task.spawn(function() return x * 2 end)
               local ok, v = h:join()
               return v
             end",
        )
        .eval()
        .unwrap();

    let out = local.block_on(&rt, async {
        mlua_isle::run_root(
            &lua,
            CancelToken::new(),
            f,
            mlua::MultiValue::from_vec(vec![mlua::Value::Integer(21)]),
        )
        .await
    });
    let v: i64 = out
        .unwrap()
        .into_iter()
        .next()
        .and_then(|v| v.as_i64())
        .unwrap();
    assert_eq!(v, 42);
}
