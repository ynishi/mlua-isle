//! `From<IsleError> for mlua::Error`: `?` on an `IsleError` inside a
//! closure or host function that returns `mlua::Result`.

use mlua_isle::{Cancelled, Isle, IsleError, LuaErrorKind, LuaFailure};

#[test]
fn cancelled_converts_to_the_cancel_error_and_back() {
    let e: mlua::Error = IsleError::Cancelled.into();
    assert!(e.downcast_ref::<Cancelled>().is_some());
    assert!(matches!(IsleError::from(e), IsleError::Cancelled));
    // A manual `external(IsleError::Cancelled)` is recognised too.
    let manual = mlua::Error::external(IsleError::Cancelled);
    assert!(matches!(IsleError::from(manual), IsleError::Cancelled));
}

/// A non-cancel `IsleError` from a host function called from Lua comes
/// back as `Lua(f)` with `f.kind == Callback` and its `Display` as the
/// message.
#[test]
fn a_host_function_error_round_trips_as_a_callback_failure() {
    let isle = Isle::spawn(|lua| {
        let fail = lua.create_function(|_, ()| -> mlua::Result<()> {
            Err(IsleError::Lua(LuaFailure::new(
                LuaErrorKind::Runtime,
                "boom",
            )))?
        })?;
        lua.globals().set("fail", fail)
    })
    .unwrap();
    match isle.eval::<()>("fail()") {
        Err(IsleError::Lua(f)) => {
            assert_eq!(f.kind, LuaErrorKind::Callback);
            assert_eq!(f.message, "lua error: boom");
        }
        other => panic!("unexpected: {other:?}"),
    }
    isle.shutdown().unwrap();
}

/// From an init closure (no Lua call in between) the kind is `External`.
#[test]
fn an_init_closure_error_is_external() {
    let r = Isle::spawn(|_| Err(IsleError::NotFound("x".into()))?);
    match r {
        Err(IsleError::Init(f)) => {
            assert_eq!(f.kind, LuaErrorKind::External);
            assert_eq!(f.message, "function 'x' not found");
        }
        other => panic!("unexpected: {:?}", other.map(|_| ())),
    }
}

#[cfg(feature = "tokio")]
mod with_tokio {
    use super::*;
    use mlua_isle::runtime::{CancelToken, Config, Vm};

    /// Stand-in for an awaited isle / VM operation that was cancelled.
    async fn cancelled_op() -> Result<(), IsleError> {
        Err(IsleError::Cancelled)
    }

    /// `op()` does `cancelled_op().await?`; the Lua side catches it and
    /// records `task.is_cancelled(err)`, then re-raises it.
    #[test]
    fn a_cancelled_op_is_a_cancel_on_both_sides() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let lua = mlua::Lua::new();
        let vm = Vm::attach(&lua, Config::default()).unwrap();
        lua.globals().set("task", vm.task_lib().unwrap()).unwrap();
        let op = lua
            .create_async_function(|_, ()| async move {
                cancelled_op().await?;
                Ok(())
            })
            .unwrap();
        lua.globals().set("op", op).unwrap();
        let f: mlua::Function = lua
            .load(
                "return function()
                   local ok, err = pcall(op)
                   seen = task.is_cancelled(err)
                   error(err, 0)
                 end",
            )
            .eval()
            .unwrap();
        let out = local.block_on(&rt, vm.run(&CancelToken::new(), f, ()));
        assert!(matches!(out.unwrap_err(), IsleError::Cancelled));
        assert!(lua.globals().get::<bool>("seen").unwrap());
    }
}
