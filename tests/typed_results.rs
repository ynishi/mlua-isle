//! Typed results and arguments (#10): requests convert on the VM thread
//! and a `Send` value crosses back.
//!
//! `sync_isle` needs no feature; `async_isle` needs `tokio`; the pooled
//! handles need `pool` (and `tokio` for `AsyncPooledIsle`).

use mlua_isle::{IsleError, LuaErrorKind, LuaFailure};

fn failure<T: std::fmt::Debug>(r: Result<T, IsleError>) -> LuaFailure {
    match r {
        Err(IsleError::Lua(f)) => f,
        other => panic!("expected IsleError::Lua, got: {other:?}"),
    }
}

/// `types(a, b, c)` returns the concatenated Lua types of its arguments;
/// `count(...)` returns how many arguments it got.
fn install_probes(lua: &mlua::Lua) -> mlua::Result<()> {
    lua.load(
        "function types(a, b, c) return type(a) .. type(b) .. type(c) end
         function count(...) return select('#', ...) end
         function pair() return 7, 'seven' end
         function noop() end",
    )
    .exec()
}

/// An argument whose `IntoLua` fails, with a conversion error or with an
/// external one.  mlua 0.12 has no into-Lua conversion variant; its own
/// conversion errors are `FromLuaConversionError`.
#[derive(Debug)]
enum BadArg {
    Conversion,
    External,
}

impl mlua::IntoLua for BadArg {
    fn into_lua(self, _: &mlua::Lua) -> mlua::Result<mlua::Value> {
        Err(match self {
            BadArg::Conversion => mlua::Error::FromLuaConversionError {
                from: "BadArg",
                to: "Value".into(),
                message: Some("refuses to convert".into()),
            },
            BadArg::External => mlua::Error::external(std::io::Error::other("no")),
        })
    }
}

/// A `FromLua` that reads field `x` of a table, so a `__index`
/// metamethod runs during the conversion.
#[derive(Debug)]
struct ViaIndex(#[allow(dead_code)] i64);

impl mlua::FromLua for ViaIndex {
    fn from_lua(v: mlua::Value, _: &mlua::Lua) -> mlua::Result<Self> {
        match v {
            mlua::Value::Table(t) => Ok(ViaIndex(t.get("x")?)),
            other => Err(mlua::Error::runtime(format!("got {}", other.type_name()))),
        }
    }
}

/// A `FromLua` that runs a Lua loop that never ends.
#[derive(Debug)]
struct Spin;

impl mlua::FromLua for Spin {
    fn from_lua(_: mlua::Value, lua: &mlua::Lua) -> mlua::Result<Self> {
        lua.load("while true do end").exec()?;
        Ok(Spin)
    }
}

const RAISING_INDEX: &str =
    "return setmetatable({}, { __index = function() error('from __index') end })";

mod sync_isle {
    use super::*;
    use mlua_isle::Isle;

    fn isle() -> Isle {
        Isle::spawn(install_probes).unwrap()
    }

    #[test]
    fn exec_returns_a_caller_chosen_type() {
        let isle = isle();
        let r: (i64, bool) = isle
            .exec(|lua| {
                let n: i64 = lua.load("return 40 + 2").eval()?;
                Ok((n, true))
            })
            .unwrap();
        assert_eq!(r, (42, true));
        isle.shutdown().unwrap();
    }

    #[test]
    fn exec_converts_a_table_on_the_vm_thread() {
        let isle = isle();
        let v: Vec<i64> = isle
            .exec(|lua| {
                let t: mlua::Table = lua.load("return { 1, 2, 3 }").eval()?;
                Ok(t.sequence_values::<i64>().collect::<mlua::Result<_>>()?)
            })
            .unwrap();
        assert_eq!(v, [1, 2, 3]);
        isle.shutdown().unwrap();
    }

    #[test]
    fn eval_converts_to_the_requested_type() {
        let isle = isle();
        assert_eq!(isle.eval::<i64>("return 1 + 1").unwrap(), 2);
        assert_eq!(isle.eval::<Option<String>>("return nil").unwrap(), None);
        assert_eq!(
            isle.eval::<Option<String>>("return 'a'")
                .unwrap()
                .as_deref(),
            Some("a")
        );
        assert_eq!(isle.eval::<String>("return 'a'").unwrap(), "a");
        assert_eq!(isle.eval::<f64>("return 0.5").unwrap(), 0.5);
        assert!(isle.eval::<bool>("return true").unwrap());
        assert_eq!(
            isle.eval::<(i64, String)>("return 1, 'x'").unwrap(),
            (1, "x".to_string())
        );
        isle.shutdown().unwrap();
    }

    #[test]
    fn eval_of_a_statement_chunk_as_unit() {
        let isle = isle();
        isle.eval::<()>("x = 1").unwrap();
        assert_eq!(isle.eval::<i64>("return x").unwrap(), 1);
        isle.shutdown().unwrap();
    }

    #[test]
    fn eval_of_a_value_that_does_not_convert_is_a_conversion_failure() {
        let isle = isle();
        let f = failure(isle.eval::<i64>("return 'not a number'"));
        assert_eq!(f.kind, LuaErrorKind::Conversion, "got: {f:?}");
        // `String` does not take `nil` (the old API returned "").
        let f = failure(isle.eval::<String>("return nil"));
        assert_eq!(f.kind, LuaErrorKind::Conversion, "got: {f:?}");
        // Nor a boolean (the old API returned "true" / "false").
        let f = failure(isle.eval::<String>("return true"));
        assert_eq!(f.kind, LuaErrorKind::Conversion, "got: {f:?}");
        // mlua's own coercions: a number converts to `String`, and
        // `bool` is Lua truthiness (only `nil` and `false` are false).
        assert_eq!(isle.eval::<String>("return 42").unwrap(), "42");
        assert!(isle.eval::<bool>("return 0").unwrap());
        assert!(!isle.eval::<bool>("return nil").unwrap());
        // The VM still serves.
        assert_eq!(isle.eval::<i64>("return 1").unwrap(), 1);
        isle.shutdown().unwrap();
    }

    #[test]
    fn call_passes_typed_arguments() {
        let isle = isle();
        let r: String = isle.call("types", (1, true, "x")).unwrap();
        assert_eq!(r, "numberbooleanstring");
        isle.call::<_, ()>("noop", ()).unwrap();
        let (n, s): (i64, String) = isle.call("pair", ()).unwrap();
        assert_eq!((n, s.as_str()), (7, "seven"));
        isle.shutdown().unwrap();
    }

    #[test]
    fn variadic_spreads_and_vec_is_one_table() {
        let isle = isle();
        let args = mlua::Variadic::from_iter(["a", "b", "c"].map(String::from));
        assert_eq!(isle.call::<_, i64>("count", args).unwrap(), 3);
        // A Vec is one argument: a table.
        let r: String = isle.call("types", vec!["a", "b"]).unwrap();
        assert_eq!(r, "tablenilnil");
        assert_eq!(isle.call::<_, i64>("count", vec![1, 2, 3]).unwrap(), 1);
        isle.shutdown().unwrap();
    }

    #[test]
    fn bstring_holds_the_raw_bytes_of_a_string() {
        let isle = isle();
        let b: mlua::BString = isle.eval("return 'a\\0b'").unwrap();
        assert_eq!(b, &b"a\0b"[..]);
        let b: mlua::BString = isle.eval("return 'a\\255'").unwrap();
        assert_eq!(b, &b"a\xff"[..]);
        // `String` rejects a string that is not UTF-8.
        let f = failure(isle.eval::<String>("return 'a\\255'"));
        assert_eq!(f.kind, LuaErrorKind::Conversion, "got: {f:?}");
        isle.shutdown().unwrap();
    }

    #[test]
    fn registry_key_is_a_send_result() {
        let isle = isle();
        let key: mlua::RegistryKey = isle.eval("return { 1, 2, 3 }").unwrap();
        let n: usize = isle
            .exec(move |lua| {
                let t: mlua::Table = lua.registry_value(&key)?;
                lua.remove_registry_value(key)?;
                Ok(t.raw_len())
            })
            .unwrap();
        assert_eq!(n, 3);
        isle.shutdown().unwrap();
    }

    #[test]
    fn unit_ignores_the_values_and_option_takes_no_value() {
        let isle = isle();
        isle.eval::<()>("return 1, 2").unwrap();
        assert_eq!(isle.eval::<Option<i64>>("x = 1").unwrap(), None);
        isle.shutdown().unwrap();
    }

    #[test]
    fn not_found_wins_over_an_argument_that_fails_to_convert() {
        let isle = isle();
        let r = isle.call::<_, ()>("nope", BadArg::Conversion);
        assert!(
            matches!(&r, Err(IsleError::NotFound(n)) if n == "nope"),
            "got: {r:?}"
        );
        isle.shutdown().unwrap();
    }

    #[test]
    fn an_argument_that_fails_to_convert_keeps_its_kind() {
        let isle = isle();
        let f = failure(isle.call::<_, ()>("noop", BadArg::Conversion));
        assert_eq!(f.kind, LuaErrorKind::Conversion, "got: {f:?}");
        let f = failure(isle.call::<_, ()>("noop", BadArg::External));
        assert_eq!(f.kind, LuaErrorKind::External, "got: {f:?}");
        isle.shutdown().unwrap();
    }

    #[test]
    fn a_from_lua_that_raises_keeps_the_raised_kind() {
        let isle = isle();
        let f = failure(isle.eval::<ViaIndex>(RAISING_INDEX));
        assert_eq!(f.kind, LuaErrorKind::Runtime, "got: {f:?}");
        assert!(f.message.ends_with("from __index"), "got: {}", f.message);
        isle.shutdown().unwrap();
    }

    #[test]
    fn a_cancel_during_the_conversion_is_cancelled() {
        let isle = isle();
        let task = isle.spawn_eval::<Spin>("return 1");
        std::thread::sleep(std::time::Duration::from_millis(30));
        task.cancel();
        assert!(matches!(task.wait(), Err(IsleError::Cancelled)));
        assert_eq!(isle.eval::<i64>("return 1").unwrap(), 1);
        isle.shutdown().unwrap();
    }

    #[test]
    fn spawn_forms_carry_the_type() {
        let isle = isle();
        let t: mlua_isle::Task<i64> = isle.spawn_eval("return 6 * 7");
        assert_eq!(t.wait().unwrap(), 42);
        let t = isle.spawn_call::<_, String>("types", (1, "x", false));
        assert_eq!(t.wait().unwrap(), "numberstringboolean");
        let t = isle.spawn_exec(|_| Ok((1u8, 'c')));
        assert_eq!(t.wait().unwrap(), (1, 'c'));
        isle.shutdown().unwrap();
    }

    #[cfg(feature = "serde")]
    #[test]
    fn exec_deserializes_a_table_with_serde() {
        use mlua::LuaSerdeExt;
        let isle = isle();
        let v: serde_json::Value = isle
            .exec(|lua| Ok(lua.from_value(lua.load("return { a = 1, b = 'x' }").eval()?)?))
            .unwrap();
        assert_eq!(v, serde_json::json!({ "a": 1, "b": "x" }));
        isle.shutdown().unwrap();
    }

    #[cfg(feature = "pool")]
    #[test]
    fn pooled_isle_has_the_typed_calls() {
        use mlua_isle::{IslePool, PoolConfig, PoolStrategy};
        let pool = IslePool::new(
            install_probes,
            PoolConfig {
                max_size: 1,
                strategy: PoolStrategy::Warm,
            },
        )
        .unwrap();
        let isle = pool.checkout().unwrap();
        assert_eq!(isle.eval::<i64>("return 1 + 1").unwrap(), 2);
        let r: String = isle.call("types", (1, true, "x")).unwrap();
        assert_eq!(r, "numberbooleanstring");
        assert_eq!(isle.exec(|_| Ok((3i64, false))).unwrap(), (3, false));
        drop(isle);
        pool.shutdown();
    }
}

#[cfg(feature = "tokio")]
mod async_isle {
    use super::*;
    use mlua_isle::{AsyncIsle, AsyncIsleDriver, AsyncTask};

    async fn isle() -> (AsyncIsle, AsyncIsleDriver) {
        AsyncIsle::spawn(install_probes).await.unwrap()
    }

    #[tokio::test]
    async fn exec_returns_a_caller_chosen_type() {
        let (isle, driver) = isle().await;
        let r: (i64, bool) = isle
            .exec(|lua| {
                let n: i64 = lua.load("return 40 + 2").eval()?;
                Ok((n, true))
            })
            .await
            .unwrap();
        assert_eq!(r, (42, true));
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn eval_converts_to_the_requested_type() {
        let (isle, driver) = isle().await;
        assert_eq!(isle.eval::<i64>("return 1 + 1").await.unwrap(), 2);
        assert_eq!(
            isle.eval::<Option<String>>("return nil").await.unwrap(),
            None
        );
        assert_eq!(isle.eval::<String>("return 'a'").await.unwrap(), "a");
        assert_eq!(
            isle.eval::<(i64, String)>("return 1, 'x'").await.unwrap(),
            (1, "x".to_string())
        );
        isle.eval::<()>("x = 1").await.unwrap();
        assert_eq!(isle.eval::<i64>("return x").await.unwrap(), 1);
        let f = failure(isle.eval::<i64>("return 'not a number'").await);
        assert_eq!(f.kind, LuaErrorKind::Conversion, "got: {f:?}");
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn call_passes_typed_arguments() {
        let (isle, driver) = isle().await;
        let r: String = isle.call("types", (1, true, "x")).await.unwrap();
        assert_eq!(r, "numberbooleanstring");
        isle.call::<_, ()>("noop", ()).await.unwrap();
        let task: AsyncTask<(i64, String)> = isle.spawn_call("pair", ());
        assert_eq!(task.await.unwrap(), (7, "seven".to_string()));
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn coroutine_requests_carry_the_type() {
        let (isle, driver) = isle().await;
        let task = isle.spawn_coroutine_eval::<i64>("return 6 * 7");
        assert_eq!(task.await.unwrap(), 42);
        assert_eq!(
            isle.coroutine_eval::<Option<i64>>("return nil")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            isle.coroutine_eval::<(i64, bool)>("return 1, false")
                .await
                .unwrap(),
            (1, false)
        );
        let r: String = isle.coroutine_call("types", (1, true, "x")).await.unwrap();
        assert_eq!(r, "numberbooleanstring");
        let task: AsyncTask<i64> =
            isle.spawn_coroutine_call("count", mlua::Variadic::from_iter([1, 2]));
        assert_eq!(task.await.unwrap(), 2);
        let f = failure(isle.coroutine_eval::<i64>("return {}").await);
        assert_eq!(f.kind, LuaErrorKind::Conversion, "got: {f:?}");
        driver.shutdown().await.unwrap();
    }

    async fn within<F: std::future::Future>(f: F) -> F::Output {
        tokio::time::timeout(std::time::Duration::from_secs(2), f)
            .await
            .expect("timed out")
    }

    #[tokio::test]
    async fn a_coroutine_argument_that_fails_to_convert_does_not_hang() {
        let (isle, driver) = isle().await;
        let f = failure(within(isle.coroutine_call::<_, ()>("noop", BadArg::Conversion)).await);
        assert_eq!(f.kind, LuaErrorKind::Conversion, "got: {f:?}");
        let f = failure(within(isle.coroutine_call::<_, ()>("noop", BadArg::External)).await);
        assert_eq!(f.kind, LuaErrorKind::External, "got: {f:?}");
        let r = within(isle.coroutine_call::<_, ()>("nope", BadArg::Conversion)).await;
        assert!(
            matches!(&r, Err(IsleError::NotFound(n)) if n == "nope"),
            "got: {r:?}"
        );
        let f = failure(within(isle.call::<_, ()>("noop", BadArg::Conversion)).await);
        assert_eq!(f.kind, LuaErrorKind::Conversion, "got: {f:?}");
        assert_eq!(
            within(isle.coroutine_eval::<i64>("return 1"))
                .await
                .unwrap(),
            1
        );
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_cancelled_coroutine_eval_is_cancelled() {
        let (isle, driver) = isle().await;
        let task = isle.spawn_coroutine_eval::<i64>("while true do end return 1");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        task.cancel();
        assert!(matches!(within(task).await, Err(IsleError::Cancelled)));
        // Dropping the task cancels it too: the next request is served.
        drop(isle.spawn_coroutine_eval::<i64>("while true do end return 1"));
        assert_eq!(
            within(isle.coroutine_eval::<i64>("return 2"))
                .await
                .unwrap(),
            2
        );
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_coroutine_from_lua_that_raises_keeps_the_raised_kind() {
        let (isle, driver) = isle().await;
        let f = failure(within(isle.coroutine_eval::<ViaIndex>(RAISING_INDEX)).await);
        assert_eq!(f.kind, LuaErrorKind::Runtime, "got: {f:?}");
        driver.shutdown().await.unwrap();
    }

    /// The conversion of a coroutine request runs under its token (it
    /// used to run after the token was left, where a cancel could not
    /// reach a loop).
    #[tokio::test]
    async fn a_cancel_during_a_coroutine_conversion_is_cancelled() {
        let (isle, driver) = isle().await;
        let task = isle.spawn_coroutine_eval::<Spin>("return 1");
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        task.cancel();
        assert!(matches!(within(task).await, Err(IsleError::Cancelled)));
        let task = isle.spawn_eval::<Spin>("return 1");
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        task.cancel();
        assert!(matches!(within(task).await, Err(IsleError::Cancelled)));
        assert_eq!(within(isle.eval::<i64>("return 1")).await.unwrap(), 1);
        driver.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_cancelled_typed_request_is_cancelled() {
        let (isle, driver) = isle().await;
        let task = isle.spawn_eval::<i64>("while true do end");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        task.cancel();
        assert!(matches!(task.await, Err(IsleError::Cancelled)));
        driver.shutdown().await.unwrap();
    }

    #[cfg(feature = "pool")]
    #[tokio::test]
    async fn async_pooled_isle_has_the_typed_calls() {
        use mlua_isle::{AsyncIslePool, PoolConfig, PoolStrategy};
        let pool = AsyncIslePool::new(
            install_probes,
            PoolConfig {
                max_size: 1,
                strategy: PoolStrategy::Warm,
            },
        )
        .unwrap();
        let isle = pool.checkout().await.unwrap();
        assert_eq!(isle.eval::<i64>("return 1 + 1").await.unwrap(), 2);
        let r: String = isle.call("types", (1, true, "x")).await.unwrap();
        assert_eq!(r, "numberbooleanstring");
        assert_eq!(isle.coroutine_eval::<i64>("return 2 * 3").await.unwrap(), 6);
        assert_eq!(isle.exec(|_| Ok((3i64, false))).await.unwrap(), (3, false));
        drop(isle);
        pool.shutdown().await;
    }
}
