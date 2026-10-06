#![cfg(feature = "tokio")]
//! Preemption applies to the coroutines the isle runs (roots and tasks)
//! and to nothing else, whatever ran before.

use mlua_isle::runtime::{Config, Vm};
use mlua_isle::AsyncIsle;
use std::time::Duration;

/// Isle with `task` and `run_lua(f)`, a host function that runs `f` in a
/// coroutine of its own (`Function::call_async`) and returns its result.
async fn isle() -> (AsyncIsle, mlua_isle::AsyncIsleDriver) {
    let config = Config {
        preempt_every: Some(1),
        ..Default::default()
    };
    AsyncIsle::spawn(move |lua| {
        let vm = Vm::attach(lua, config)?;
        lua.globals().set("task", vm.task_lib()?)?;
        let run_lua = lua.create_async_function(|_, f: mlua::Function| async move {
            f.call_async::<mlua::Value>(()).await
        })?;
        lua.globals().set("run_lua", run_lua)
    })
    .await
    .unwrap()
}

async fn within<T>(ms: u64, fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_millis(ms), fut)
        .await
        .expect("timed out")
}

/// A CPU loop in the root that stops as soon as a sibling task has run.
/// Returns whether the sibling ran during the loop (the root was
/// preempted) and how many iterations it took.
const ROOT_LOOP: &str = "
    local ran = false
    local h = task.spawn(function() ran = true end)
    local n = 0
    while not ran and n < 3000000 do n = n + 1 end
    h:join()
    return tostring(ran) .. ' ' .. tostring(n < 3000000)";

#[tokio::test]
async fn the_root_is_preempted() {
    let (isle, driver) = isle().await;
    let r = within(5000, isle.coroutine_eval::<String>(ROOT_LOOP))
        .await
        .unwrap();
    assert_eq!(r, "true true");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn the_root_is_still_preempted_after_a_task_it_spawned_finished() {
    let (isle, driver) = isle().await;
    let src = format!(
        "local first = task.spawn(function() return 1 end)
         first:join()
         {ROOT_LOOP}"
    );
    let r = within(5000, isle.coroutine_eval::<String>(&src))
        .await
        .unwrap();
    assert_eq!(r, "true true");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_coroutine_a_host_function_runs_is_not_preempted() {
    // `run_lua` runs its argument with `call_async`: a coroutine the isle
    // did not create, like one from `coroutine.create`.  A sibling task
    // must not run in the middle of it.
    let (isle, driver) = isle().await;
    let r = within(
        5000,
        isle.coroutine_eval::<String>(
            "local ran = false
             local h = task.spawn(function() ran = true end)
             local during = run_lua(function()
               local n = 0
               for i = 1, 3000000 do n = n + 1 end
               return tostring(ran) .. ' ' .. n
             end)
             h:join()
             return during",
        ),
    )
    .await
    .unwrap();
    assert_eq!(r, "false 3000000");
    driver.shutdown().await.unwrap();
}
