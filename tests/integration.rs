use mlua_isle::{Isle, IsleError};
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
