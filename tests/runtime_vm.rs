//! `runtime::Vm`: attaching, per-VM config, the task table, and the
//! actors built on it.

use mlua_isle::runtime::{Config, Vm};
use std::time::Duration;

const A: Config = Config {
    grace: Duration::from_millis(100),
    preempt_every: None,
};
const B: Config = Config {
    grace: Duration::from_millis(700),
    preempt_every: Some(3),
};

#[test]
fn of_is_none_before_attach_and_some_after() {
    let lua = mlua::Lua::new();
    assert!(Vm::of(&lua).is_none());
    // Touching the hooks module alone does not attach.
    mlua_isle::hooks::configure(&lua, A.into());
    assert!(Vm::of(&lua).is_none());

    Vm::attach(&lua, A).unwrap();
    assert_eq!(Vm::of(&lua).unwrap().config(), A);
}

#[test]
fn a_second_attach_wins_and_shares_the_state() {
    let lua = mlua::Lua::new();
    let first = Vm::attach(&lua, A).unwrap();
    let second = Vm::attach(&lua, B).unwrap();
    assert_eq!(first.config(), B);
    assert_eq!(second.config(), B);
    assert_eq!(Vm::of(&lua).unwrap().config(), B);

    first.set_config(A);
    assert_eq!(second.config(), A);
    assert_eq!(mlua_isle::hooks::config(&lua), A.into());
}

#[test]
fn config_converts_to_and_from_cancel_config() {
    let c: mlua_isle::hooks::CancelConfig = B.into();
    assert_eq!(c.grace, B.grace);
    assert_eq!(c.preempt_every, B.preempt_every);
    assert_eq!(Config::from(c), B);
    assert_eq!(
        Config::default(),
        Config::from(mlua_isle::hooks::CancelConfig::default())
    );
}

#[test]
fn attach_does_not_set_a_task_global() {
    let lua = mlua::Lua::new();
    Vm::attach(&lua, Config::default()).unwrap();
    let task: mlua::Value = lua.globals().get("task").unwrap();
    assert!(task.is_nil(), "got {task:?}");
}

#[test]
fn hooks_added_through_the_vm_run_and_can_be_removed() {
    let lua = mlua::Lua::new();
    let vm = Vm::attach(&lua, Config::default()).unwrap();
    let mut lines = 0u32;
    let seen = std::rc::Rc::new(std::cell::Cell::new(0u32));
    let s = seen.clone();
    let id = vm
        .add_hook(mlua::HookTriggers::EVERY_LINE, move |_, _| {
            lines += 1;
            s.set(lines);
            Ok(mlua::VmState::Continue)
        })
        .unwrap();
    lua.load("local a = 1\nlocal b = 2").exec().unwrap();
    assert!(seen.get() >= 2, "line callback ran {} times", seen.get());

    assert!(vm.remove_hook(id).unwrap());
    assert!(!vm.remove_hook(id).unwrap());
    let before = seen.get();
    lua.load("local a = 1\nlocal b = 2").exec().unwrap();
    assert_eq!(seen.get(), before);

    // A second attach keeps registered callbacks.
    let s = seen.clone();
    vm.add_hook(mlua::HookTriggers::EVERY_LINE, move |_, _| {
        s.set(s.get() + 1);
        Ok(mlua::VmState::Continue)
    })
    .unwrap();
    Vm::attach(&lua, B).unwrap();
    lua.load("local a = 1\nlocal b = 2").exec().unwrap();
    assert!(seen.get() >= before + 2);
}

#[cfg(feature = "tokio")]
mod with_tokio {
    use super::*;
    use mlua_isle::runtime::{cancellable, CancelToken};
    use mlua_isle::{AsyncIsle, IsleError};

    #[test]
    fn task_lib_is_one_table_per_vm() {
        let lua = mlua::Lua::new();
        let first = Vm::attach(&lua, A).unwrap();
        let second = Vm::attach(&lua, B).unwrap();
        let t = first.task_lib().unwrap();
        assert_eq!(t.to_pointer(), second.task_lib().unwrap().to_pointer());
        assert_eq!(
            t.to_pointer(),
            Vm::of(&lua).unwrap().task_lib().unwrap().to_pointer()
        );
        // A later attach keeps the table.
        let third = Vm::attach(&lua, A).unwrap();
        assert_eq!(t.to_pointer(), third.task_lib().unwrap().to_pointer());
        assert!(t.get::<mlua::Function>("spawn").is_ok());
    }

    /// `attach` runs no Lua (an actor that never asks for the task
    /// table behaves as before); `task_lib` does, once.
    #[test]
    fn the_task_table_is_created_on_first_use() {
        let lua = mlua::Lua::new();
        let calls = std::rc::Rc::new(std::cell::Cell::new(0u32));
        let c = calls.clone();
        mlua_isle::hooks::add_hook(&lua, mlua::HookTriggers::EVERY_LINE, move |_, _| {
            c.set(c.get() + 1);
            Ok(mlua::VmState::Continue)
        })
        .unwrap();
        let vm = Vm::attach(&lua, A).unwrap();
        Vm::attach(&lua, B).unwrap();
        assert_eq!(calls.get(), 0, "attach ran Lua code");
        vm.task_lib().unwrap();
        let after_first = calls.get();
        assert!(after_first > 0);
        vm.task_lib().unwrap();
        assert_eq!(calls.get(), after_first, "task_lib created a second table");
    }

    #[test]
    fn run_returns_cancelled_when_the_token_is_cancelled() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let lua = mlua::Lua::new();
        let vm = Vm::attach(&lua, Config::default()).unwrap();
        lua.globals().set("task", vm.task_lib().unwrap()).unwrap();
        let f: mlua::Function = lua
            .load("return function() task.spawn(function() while true do end end):join() end")
            .eval()
            .unwrap();
        vm.set_config(Config {
            preempt_every: Some(1),
            ..Config::default()
        });
        let token = CancelToken::new();
        let t = token.clone();
        let out = local.block_on(&rt, async {
            tokio::task::spawn_local(async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                t.cancel();
            });
            tokio::time::timeout(Duration::from_secs(2), vm.run(&token, f, ()))
                .await
                .expect("timed out")
        });
        assert!(matches!(out.unwrap_err(), IsleError::Cancelled));
    }

    /// Isle with `hold(ms)` (not cancellable) and `sleep(ms)`
    /// (cancellable), configured only through the builder.
    async fn isle_with_builder_config(
        config: Option<Config>,
    ) -> (AsyncIsle, mlua_isle::AsyncIsleDriver) {
        let mut b = AsyncIsle::builder();
        if let Some(c) = config {
            b = b.config(c);
        }
        b.spawn(|lua| {
            let hold = lua.create_async_function(|_, ms: u64| async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            })?;
            lua.globals().set("hold", hold)?;
            let sleep = lua.create_async_function(|_, ms: u64| {
                cancellable(async move {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(())
                })
            })?;
            lua.globals().set("sleep", sleep)
        })
        .await
        .unwrap()
    }

    /// Cancel a coroutine request whose `__close` awaits 20 ms, and
    /// report whether that cleanup finished.
    async fn cleanup_finished(isle: &AsyncIsle) -> bool {
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
        let r = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("timed out");
        assert!(matches!(r.unwrap_err(), IsleError::Cancelled));
        isle.eval("return cleaned").await.unwrap() == "true"
    }

    #[tokio::test]
    async fn builder_config_sets_the_grace_of_coroutine_requests() {
        let (isle, driver) = isle_with_builder_config(Some(Config {
            grace: Duration::from_millis(500),
            ..Config::default()
        }))
        .await;
        assert!(cleanup_finished(&isle).await);
        assert_eq!(
            isle.exec(|lua| Ok(format!("{:?}", Vm::of(lua).map(|vm| vm.config().grace))))
                .await
                .unwrap(),
            "Some(500ms)"
        );
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn without_builder_config_the_grace_is_zero() {
        let (isle, driver) = isle_with_builder_config(None).await;
        assert!(!cleanup_finished(&isle).await);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn builder_config_replaces_the_init_closures_config() {
        let (isle, driver) = AsyncIsle::builder()
            .config(B)
            .spawn(|lua| {
                mlua_isle::hooks::configure(lua, A.into());
                Ok(())
            })
            .await
            .unwrap();
        let grace = isle
            .exec(|lua| Ok(format!("{:?}", Vm::of(lua).unwrap().config())))
            .await
            .unwrap();
        assert_eq!(grace, format!("{B:?}"));
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn without_builder_config_the_init_closures_config_is_kept() {
        let (isle, driver) = AsyncIsle::spawn(|lua| {
            mlua_isle::hooks::configure(lua, A.into());
            Ok(())
        })
        .await
        .unwrap();
        let config = isle
            .exec(|lua| Ok(format!("{:?}", Vm::of(lua).unwrap().config())))
            .await
            .unwrap();
        assert_eq!(config, format!("{A:?}"));
        driver.shutdown().await.unwrap();
    }
}

#[test]
fn an_isle_attaches_its_vm() {
    let isle = mlua_isle::Isle::spawn(|lua| {
        mlua_isle::hooks::configure(lua, A.into());
        Ok(())
    })
    .unwrap();
    let r = isle
        .exec(|lua| {
            let vm = Vm::of(lua).expect("attached");
            let task: mlua::Value = lua.globals().get("task")?;
            Ok(format!("{:?} {}", vm.config() == A, task.is_nil()))
        })
        .unwrap();
    assert_eq!(r, "true true");
    isle.shutdown().unwrap();
}
