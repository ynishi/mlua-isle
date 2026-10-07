#![cfg(feature = "tokio")]
//! AsyncIsle: function handles (`function` / `call_fn` /
//! `coroutine_call_fn`), `AsyncIsleBuilder::spawn_with` and the `lua`
//! factory.

use mlua_isle::runtime::{channel, channel_to_host, Config, Vm};
use mlua_isle::{AsyncIsle, IsleError, LuaErrorKind};
use std::time::Duration;

/// `task`, and a `scheduler` module (in `package.preload`, not a
/// global) whose functions count their calls.
fn setup(lua: &mlua::Lua) -> mlua::Result<()> {
    let vm = Vm::attach(lua, Config::default())?;
    lua.globals().set("task", vm.task_lib()?)?;
    lua.load(
        r#"package.preload.scheduler = function()
             local M = { calls = 0 }
             function M.scan(now)
               M.calls = M.calls + 1
               return now * 2, M.calls
             end
             -- Needs a coroutine request: task.spawn / join.
             function M.scan_async(now)
               M.calls = M.calls + 1
               local _, v = task.spawn(function() return now + 1 end):join()
               return v
             end
             return M
           end"#,
    )
    .exec()
}

// ── 7. IsleFunction ──

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_module_function_is_called_through_call_fn_and_coroutine_call_fn() {
    let (isle, driver) = AsyncIsle::spawn(setup).await.unwrap();
    let scan = isle
        .function(|lua| lua.load("return require('scheduler').scan").eval())
        .await
        .unwrap();
    let scan_async = isle
        .function(|lua| lua.load("return require('scheduler').scan_async").eval())
        .await
        .unwrap();

    let (v, calls): (i64, i64) = isle.call_fn(&scan, 21).await.unwrap();
    assert_eq!((v, calls), (42, 1));
    let (v, calls): (i64, i64) = isle.coroutine_call_fn(&scan, 5).await.unwrap();
    assert_eq!((v, calls), (10, 2));
    let v: i64 = isle.coroutine_call_fn(&scan_async, 9).await.unwrap();
    assert_eq!(v, 10);
    // A sync request cannot spawn tasks.
    let err = isle.call_fn::<_, i64>(&scan_async, 9).await.unwrap_err();
    assert!(
        matches!(&err, IsleError::Lua(f) if f.message.contains("task.spawn")),
        "{err:?}"
    );
    // Not a global: the name-based call does not find it.
    let err = isle.call::<_, i64>("scan", 1).await.unwrap_err();
    assert!(matches!(err, IsleError::NotFound(_)), "{err:?}");
    driver.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cloned_handle_is_used_from_several_tokio_tasks() {
    const TASKS: i64 = 8;
    const CALLS: i64 = 25;
    let (isle, driver) = AsyncIsle::spawn(setup).await.unwrap();
    let scan = isle
        .function(|lua| lua.load("return require('scheduler').scan").eval())
        .await
        .unwrap();
    let mut joins = Vec::new();
    for t in 0..TASKS {
        let isle = isle.clone();
        let scan = scan.clone();
        joins.push(tokio::spawn(async move {
            for i in 0..CALLS {
                let n = t * 1000 + i;
                let (v, _): (i64, i64) = if i % 2 == 0 {
                    isle.call_fn(&scan, n).await.unwrap()
                } else {
                    isle.coroutine_call_fn(&scan, n).await.unwrap()
                };
                assert_eq!(v, n * 2);
            }
        }));
    }
    drop(scan);
    for j in joins {
        j.await.unwrap();
    }
    let calls: i64 = isle
        .eval("return require('scheduler').calls")
        .await
        .unwrap();
    assert_eq!(calls, TASKS * CALLS);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_handle_from_another_isle_is_refused() {
    let (isle, driver) = AsyncIsle::spawn(setup).await.unwrap();
    let (other, other_driver) = AsyncIsle::spawn(setup).await.unwrap();
    let scan = isle
        .function(|lua| lua.load("return require('scheduler').scan").eval())
        .await
        .unwrap();

    let err = other.call_fn::<_, (i64, i64)>(&scan, 1).await.unwrap_err();
    assert!(matches!(err, IsleError::WrongIsle), "{err:?}");
    let err = other
        .coroutine_call_fn::<_, (i64, i64)>(&scan, 1)
        .await
        .unwrap_err();
    assert!(matches!(err, IsleError::WrongIsle), "{err:?}");
    // Nothing ran in either isle.
    let calls: i64 = other
        .eval("return require('scheduler').calls")
        .await
        .unwrap();
    assert_eq!(calls, 0);

    // A clone of the creating isle accepts it.
    let (v, calls): (i64, i64) = isle.clone().call_fn(&scan, 4).await.unwrap();
    assert_eq!((v, calls), (8, 1));
    other_driver.shutdown().await.unwrap();
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn function_reports_lua_errors_and_non_functions() {
    let (isle, driver) = AsyncIsle::spawn(setup).await.unwrap();
    let err = isle
        .function(|lua| lua.load("return require('missing').f").eval())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, IsleError::Lua(f) if f.message.contains("missing")),
        "{err:?}"
    );
    let err = isle
        .function(|lua| lua.load("return 42").eval())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, IsleError::Lua(f) if f.kind == LuaErrorKind::Conversion),
        "{err:?}"
    );
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_function_handle_call_can_be_cancelled() {
    let (isle, driver) = AsyncIsle::spawn(setup).await.unwrap();
    let spin = isle
        .function(|lua| lua.load("return function() while true do end end").eval())
        .await
        .unwrap();
    let task = isle.spawn_coroutine_call_fn::<_, ()>(&spin, ());
    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        token.cancel();
    });
    assert!(matches!(task.await, Err(IsleError::Cancelled)));
    let task = isle.spawn_call_fn::<_, ()>(&spin, ());
    let token = task.cancel_token().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        token.cancel();
    });
    assert!(matches!(task.await, Err(IsleError::Cancelled)));
    driver.shutdown().await.unwrap();
}

// ── 8. spawn_with ──

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_with_returns_a_sender_created_in_init_that_works_from_another_task() {
    let (isle, driver, events) = AsyncIsle::builder()
        .spawn_with(|lua| {
            let vm = Vm::attach(lua, Config::default())?;
            lua.globals().set("task", vm.task_lib()?)?;
            let (tx, ch) = channel::<i64>(lua, 4)?;
            lua.globals().set("events", ch)?;
            Ok(tx)
        })
        .await
        .unwrap();
    let producer = tokio::spawn(async move {
        for i in 1..=20 {
            events.send(i).await.unwrap();
        }
        // `events` dropped: the channel closes after the queued values.
    });
    let sum: i64 = isle
        .coroutine_eval(
            "local sum = 0
             while true do
               local v, ok = events:recv()
               if not ok then return sum end
               sum = sum + v
             end",
        )
        .await
        .unwrap();
    producer.await.unwrap();
    assert_eq!(sum, 210);
    driver.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_with_returns_a_receiver_of_a_channel_to_the_host() {
    let (isle, driver, mut reports) = AsyncIsle::builder()
        .config(Config {
            grace: Duration::from_millis(100),
            preempt_every: None,
        })
        .spawn_with(|lua| {
            let vm = Vm::attach(lua, Config::default())?;
            lua.globals().set("task", vm.task_lib()?)?;
            let (ch, rx) = channel_to_host::<String>(lua, 2)?;
            lua.globals().set("reports", ch)?;
            Ok(rx)
        })
        .await
        .unwrap();
    let consumer = tokio::spawn(async move {
        let mut got = Vec::new();
        while let Some(r) = reports.recv().await {
            got.push(r);
        }
        got
    });
    isle.coroutine_eval::<()>(
        "for i = 1, 10 do reports:send('r' .. i) end
         reports:close()",
    )
    .await
    .unwrap();
    let got = consumer.await.unwrap();
    assert_eq!(got, (1..=10).map(|i| format!("r{i}")).collect::<Vec<_>>());
    let grace: u64 = isle
        .exec(|lua| Ok(Vm::of(lua).unwrap().config().grace.as_millis() as u64))
        .await
        .unwrap();
    assert_eq!(grace, 100, "the builder's config replaces init's");
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_init_error_in_spawn_with_is_init() {
    let r = AsyncIsle::builder()
        .spawn_with(|_| -> mlua::Result<i64> { Err(mlua::Error::runtime("init failed")) })
        .await;
    match r {
        Err(IsleError::Init(f)) => assert!(f.message.contains("init failed"), "{f:?}"),
        Err(other) => panic!("unexpected error: {other:?}"),
        Ok(_) => panic!("expected an error"),
    }
}

// ── 9. the lua factory ──

/// A stripped bytecode chunk returning 42.
fn bytecode() -> Vec<u8> {
    let lua = mlua::Lua::new();
    let f = lua.load("return 40 + 2").into_function().unwrap();
    f.dump(true)
}

/// Load and run the chunk on the isle.
async fn run_bytecode(isle: &AsyncIsle) -> Result<i64, IsleError> {
    let bytes = bytecode();
    isle.exec(move |lua| Ok(lua.load(&bytes[..]).call::<i64>(())?))
        .await
}

/// Load the unsafe `debug` library into the isle's state.
async fn load_debug(isle: &AsyncIsle) -> Result<(), IsleError> {
    isle.exec(|lua| Ok(lua.load_std_libs(mlua::StdLib::DEBUG)?))
        .await
}

// Note: mlua 0.12.2 (Lua 5.4) does not refuse bytecode in a
// `Lua::new()` state (its safe mode blocks the unsafe libraries and C
// modules, not binary chunks), so the difference shown for the default
// state is the refused `debug` library.
#[tokio::test]
async fn the_lua_factory_creates_the_state() {
    // The default state (`Lua::new()`) is safe: no `debug`, and loading
    // it is refused.
    let (plain, plain_driver) = AsyncIsle::spawn(setup).await.unwrap();
    let has_debug: bool = plain.eval("return debug ~= nil").await.unwrap();
    assert!(!has_debug);
    let err = load_debug(&plain).await.unwrap_err();
    assert!(
        matches!(&err, IsleError::Lua(f) if f.message.contains("debug")),
        "{err:?}"
    );
    plain_driver.shutdown().await.unwrap();

    let (isle, driver) = AsyncIsle::builder()
        .lua(|| unsafe { mlua::Lua::unsafe_new() })
        .spawn(setup)
        .await
        .unwrap();
    // The state is the factory's: the unsafe libraries are there.
    let has_debug: bool = isle.eval("return debug ~= nil").await.unwrap();
    assert!(has_debug);
    load_debug(&isle).await.unwrap();
    // It loads a stripped bytecode chunk.
    assert_eq!(run_bytecode(&isle).await.unwrap(), 42);

    // Works as usual: tasks in a coroutine request ...
    let v: i64 = isle
        .coroutine_eval(
            "local h = task.spawn(function(a, b) return a + b end, 20, 22)
             local _, v = h:join()
             return v",
        )
        .await
        .unwrap();
    assert_eq!(v, 42);
    // ... a module function through a handle ...
    let scan = isle
        .function(|lua| lua.load("return require('scheduler').scan_async").eval())
        .await
        .unwrap();
    assert_eq!(isle.coroutine_call_fn::<_, i64>(&scan, 1).await.unwrap(), 2);
    // ... and cancel, of a CPU loop and of a task tree.
    for code in [
        "while true do end",
        "local h = task.spawn(function() while true do end end) h:join()",
    ] {
        let task = isle.spawn_coroutine_eval::<()>(code);
        let token = task.cancel_token().clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            token.cancel();
        });
        let r = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("cancel timed out");
        assert!(matches!(r, Err(IsleError::Cancelled)), "{code}: {r:?}");
    }
    let still: i64 = isle.eval("return 1").await.unwrap();
    assert_eq!(still, 1);
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_panicking_lua_factory_is_thread_panic() {
    let r = AsyncIsle::builder()
        .lua(|| panic!("no state for you"))
        .spawn(|_| Ok(()))
        .await;
    match r {
        Err(IsleError::ThreadPanic(Some(m))) => assert!(m.contains("no state for you"), "{m}"),
        Err(other) => panic!("unexpected error: {other:?}"),
        Ok(_) => panic!("expected an error"),
    }
}
