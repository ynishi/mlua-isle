//! The 0.7 names deprecated in 0.8.0 still forward to `runtime`.  One
//! test per deprecated entry point; remove this file together with them.

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
#[allow(deprecated)]
fn hooks_install_and_configure_forward_to_the_vm() {
    use mlua_isle::hooks;

    let lua = mlua::Lua::new();
    // `configure` before any attach stores the config without attaching.
    hooks::configure(&lua, A);
    assert!(Vm::of(&lua).is_none());
    // `install` attaches with the stored config.
    hooks::install(&lua).unwrap();
    let vm = Vm::of(&lua).expect("hooks::install attaches");
    assert_eq!(vm.config(), A);
    // `configure` / `config` are `Vm::set_config` / `Vm::config`.
    hooks::configure(&lua, B);
    assert_eq!(vm.config(), B);
    vm.set_config(A);
    assert_eq!(hooks::config(&lua), A);
}

#[test]
#[allow(deprecated)]
fn hooks_add_and_remove_hook_forward_to_the_vm() {
    use mlua_isle::hooks;

    let lua = mlua::Lua::new();
    let seen = std::rc::Rc::new(std::cell::Cell::new(0u32));
    let s = seen.clone();
    let id: hooks::HookId = hooks::add_hook(&lua, mlua::HookTriggers::EVERY_LINE, move |_, _| {
        s.set(s.get() + 1);
        Ok(mlua::VmState::Continue)
    })
    .unwrap();
    let vm = Vm::of(&lua).expect("hooks::add_hook attaches");
    lua.load("local a = 1\nlocal b = 2").exec().unwrap();
    assert!(seen.get() >= 2);
    // The id is a `runtime::HookId`: the Vm removes it.
    assert!(vm.remove_hook(id).unwrap());
    assert!(!hooks::remove_hook(&lua, id).unwrap());
}

#[test]
#[allow(deprecated)]
fn hooks_configure_in_an_isle_init_is_kept() {
    let isle = mlua_isle::Isle::spawn(|lua| {
        mlua_isle::hooks::configure(lua, A);
        Ok(())
    })
    .unwrap();
    let same = isle
        .exec(|lua| Ok(Vm::of(lua).expect("attached").config() == A))
        .unwrap();
    assert!(same);
    isle.shutdown().unwrap();
}

#[test]
#[allow(deprecated)]
fn cancel_config_is_an_alias_of_config() {
    use mlua_isle::hooks::CancelConfig;

    let c = CancelConfig {
        grace: B.grace,
        preempt_every: B.preempt_every,
    };
    // The same type: no conversion needed in either direction (0.7 code
    // that called `.into()` still compiles through the identity `From`).
    let r: Config = c;
    assert_eq!(r, B);
    let back: CancelConfig = B;
    assert_eq!(back, c);
    assert_eq!(CancelConfig::default(), Config::default());
}

#[test]
#[allow(deprecated)]
fn root_current_token_forwards() {
    assert!(mlua_isle::current_token().is_none());
    let isle = mlua_isle::Isle::spawn(|_| Ok(())).unwrap();
    let r = isle
        .exec(|_| {
            Ok((
                mlua_isle::current_token().is_some(),
                mlua_isle::runtime::current_token().is_some(),
            ))
        })
        .unwrap();
    assert_eq!(r, (true, true));
    isle.shutdown().unwrap();
}

#[cfg(feature = "tokio")]
mod with_tokio {
    use super::*;
    use mlua_isle::{CancelToken, IsleError};

    fn rt() -> (tokio::runtime::Runtime, tokio::task::LocalSet) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        (rt, tokio::task::LocalSet::new())
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn builder_config_wins_over_hooks_configure_in_init() {
        let (isle, driver) = mlua_isle::AsyncIsle::builder()
            .config(B)
            .spawn(|lua| {
                mlua_isle::hooks::configure(lua, A);
                Ok(())
            })
            .await
            .unwrap();
        let wins = isle
            .exec(|lua| Ok(Vm::of(lua).expect("attached").config() == B))
            .await
            .unwrap();
        assert!(wins);
        driver.shutdown().await.unwrap();
    }

    #[test]
    #[allow(deprecated)]
    fn tasks_install_returns_the_vms_task_lib() {
        let lua = mlua::Lua::new();
        let t = mlua_isle::tasks::install(&lua).unwrap();
        let vm = Vm::of(&lua).expect("tasks::install attaches");
        assert_eq!(t.to_pointer(), vm.task_lib().unwrap().to_pointer());
        assert_eq!(
            t.to_pointer(),
            mlua_isle::tasks::install(&lua).unwrap().to_pointer()
        );
    }

    #[test]
    #[allow(deprecated)]
    fn root_run_root_runs_on_an_unattached_vm() {
        let (rt, local) = rt();
        let lua = mlua::Lua::new();
        lua.globals()
            .set("task", mlua_isle::tasks::install(&lua).unwrap())
            .unwrap();
        let f: mlua::Function = lua
            .load("return function(x) local _, v = task.spawn(function() return x * 2 end):join() return v end")
            .eval()
            .unwrap();
        let out = local
            .block_on(
                &rt,
                mlua_isle::run_root(
                    &lua,
                    CancelToken::new(),
                    f,
                    mlua::MultiValue::from_vec(vec![mlua::Value::Integer(21)]),
                ),
            )
            .unwrap();
        assert_eq!(out[0].as_i64(), Some(42));
    }

    #[test]
    #[allow(deprecated)]
    fn root_cancellable_forwards() {
        let (rt, local) = rt();
        let lua = mlua::Lua::new();
        let vm = Vm::attach(
            &lua,
            Config {
                grace: Duration::from_secs(1),
                ..Config::default()
            },
        )
        .unwrap();
        lua.globals().set("task", vm.task_lib().unwrap()).unwrap();
        let sleep = lua
            .create_async_function(|_, ms: u64| {
                mlua_isle::cancellable(async move {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(())
                })
            })
            .unwrap();
        lua.globals().set("sleep", sleep).unwrap();
        let f: mlua::Function = lua
            .load("return function() local ok, err = pcall(sleep, 5000) seen = task.is_cancelled(err) end")
            .eval()
            .unwrap();
        let token = CancelToken::new();
        let t = token.clone();
        let out = local.block_on(&rt, async {
            tokio::task::spawn_local(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                t.cancel();
            });
            tokio::time::timeout(Duration::from_secs(2), vm.run(&token, f, ()))
                .await
                .expect("timed out")
        });
        assert!(matches!(out.unwrap_err(), IsleError::Cancelled));
        assert!(lua.globals().get::<bool>("seen").unwrap());
    }
}
