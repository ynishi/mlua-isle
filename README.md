# mlua-isle

Thread-isolated Lua VM with cancellation and async bridge for [mlua](https://crates.io/crates/mlua).

## Problem

`mlua::Lua` is `!Send` — it cannot cross thread boundaries. This makes it
difficult to use from async runtimes, UI threads, or any multi-threaded context.

**mlua-isle** solves this by confining the Lua VM to a dedicated thread and
communicating via channels.

## Features

- **Thread isolation** — Lua VM runs on a dedicated thread; callers interact
  via a `Send + Sync` handle
- **Cancellation** — long-running Lua code can be interrupted via `CancelToken`
  using a Lua debug hook
- **Sync API** — blocking `Isle` handle with `Task<T>` for non-blocking usage
- **Async API** (optional, `tokio` feature) — `AsyncIsle` handle with
  Handle/Driver separation, bounded channel backpressure, and `AsyncTask<T>`
  which implements `Future`
- **Coroutine execution** (optional, `tokio` feature) — cooperative
  multitasking via `coroutine_eval` / `coroutine_call`.  Multiple Lua
  coroutines share the same VM and yield when awaiting async Rust functions
- **Connection pool** (optional, `pool` feature) — `IslePool` manages
  multiple `Isle` instances with checkout/return semantics.  Supports
  `Cold` (fresh VM) and `Warm` (reuse) strategies
- **Async connection pool** (optional, `pool` + `tokio` features) —
  `AsyncIslePool` is the async counterpart of `IslePool`: `checkout` /
  `try_checkout` / `checkout_timeout` are async, idle wait uses
  `tokio::sync::Notify`, and each slot owns both an `AsyncIsle` handle
  and its `AsyncIsleDriver` so VMs can be joined on shutdown
- **Typed errors** — one error type, `IsleError`; a Lua error arrives as
  `IsleError::Lua(LuaFailure)` with its kind, message, traceback and
  (`serde` feature) the raised value, and a cancel is recognised by value
- **Zero unsafe in user code** — both `Isle` and `AsyncIsle` are safe to
  share across threads

## Architecture

### Sync (`Isle`)

```text
┌─────────────────┐  std mpsc  ┌──────────────────┐
│  caller thread   │──────────►│  Lua thread       │
│                  │           │  (mlua confined)   │
│  Isle handle     │◄──────────│                    │
│                  │  oneshot   │  Lua VM + hook    │
└─────────────────┘            └──────────────────┘
```

### Async (`AsyncIsle`, requires `tokio` feature)

```text
┌──────────────────┐                ┌──────────────────┐
│  tokio tasks      │  tokio mpsc   │  Lua thread       │
│                   │──────────────►│  (mlua confined)   │
│  AsyncIsle handle │  (bounded,    │                    │
│  (Clone, no Arc)  │  backpressure)│  Lua VM + hook    │
│                   │◄──────────────│                    │
│                   │   oneshot     │                    │
├──────────────────┤                │                    │
│  AsyncIsleDriver  │───done_tx────►│                    │
│  (lifecycle owner)│               └──────────────────┘
└──────────────────┘
```

- **Handle** (`AsyncIsle`) — lightweight, cloneable. Share across tasks
  without `Arc`.
- **Driver** (`AsyncIsleDriver`) — sole lifecycle owner. Call
  `shutdown().await` for clean thread join, or drop to let the channel-close
  mechanism terminate the thread naturally.

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
mlua-isle = "0.7"

# For async support (includes coroutine execution):
# mlua-isle = { version = "0.7", features = ["tokio"] }

# For connection pool:
# mlua-isle = { version = "0.7", features = ["pool"] }

# Both:
# mlua-isle = { version = "0.7", features = ["tokio", "pool"] }

# The value a Lua error raised, as JSON, on `LuaFailure::value`:
# mlua-isle = { version = "0.7", features = ["serde"] }
```

### Sync API

```rust
use mlua_isle::Isle;

let isle = Isle::spawn(|lua| {
    lua.globals().set("greeting", "hello")?;
    Ok(())
}).unwrap();

let result: String = isle.eval("return greeting").unwrap();
assert_eq!(result, "hello");

isle.shutdown().unwrap();
```

### Async API

```rust
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use mlua_isle::AsyncIsle;

let (isle, driver) = AsyncIsle::spawn(|lua| {
    lua.globals().set("greeting", "hello")?;
    Ok(())
}).await?;

// Clone freely — no Arc needed.
let isle2 = isle.clone();

let result: String = isle.eval("return greeting").await?;
assert_eq!(result, "hello");

driver.shutdown().await?;
# Ok(())
# }
```

### Typed results and arguments

A request converts its result on the VM thread and hands back a `Send`
value of the type you ask for (`T: FromLuaMulti + Send + 'static`), so
nothing is flattened to a string on the way:

```rust
use mlua_isle::Isle;

let isle = Isle::spawn(|lua| {
    lua.load("function info(n, flag, s) return n * 2, not flag, s end
              function count(...) return select('#', ...) end").exec()
}).unwrap();

let n: i64 = isle.eval("return 1 + 1").unwrap();                // 2
let pair: (i64, bool) = isle.eval("return 1, true").unwrap();   // several return values
let none: Option<String> = isle.eval("return nil").unwrap();    // `nil` is `None`
isle.eval::<()>("counter = 0").unwrap();                        // ignore the result

// Arguments are `IntoLuaMulti`: a tuple keeps each value's Lua type.
let (d, f, s): (i64, bool, String) = isle.call("info", (21, true, "x")).unwrap();
assert_eq!((n, pair, none, d, f, s.as_str()), (2, (1, true), None, 42, false, "x"));

// A run-time number of arguments: `Variadic` (a `Vec` is one argument, a table).
let args = mlua::Variadic::from_iter(["a", "b"].map(String::from));
let count: i64 = isle.call("count", args).unwrap();
assert_eq!(count, 2);

isle.shutdown().unwrap();
```

A value that does not convert is `IsleError::Lua` with kind
`LuaErrorKind::Conversion`.  The conversions are mlua's own: a number
converts to `String`, `bool` is Lua truthiness, and `mlua::BString` holds
the raw bytes of a Lua string (UTF-8 or not).  A `FromLua` of your own
that fails with an error raised by Lua code it runs is `IsleError::Lua`
with that error's kind.  `Table`, `Function`, `Value` and `MultiValue`
hold references into the VM and, without mlua's `send` feature, are not
`Send`, so asking for them does not compile.  Convert them inside `exec`,
whose closure returns any `T: Send + 'static` as is, or ask for a
`mlua::RegistryKey` (which is `Send`) and read the value in a later
`exec`:

```rust
use mlua_isle::Isle;

let isle = Isle::spawn(|_| Ok(())).unwrap();
let v: Vec<i64> = isle.exec(|lua| {
    let t: mlua::Table = lua.load("return { 1, 2, 3 }").eval()?;
    Ok(t.sequence_values::<i64>().collect::<mlua::Result<_>>()?)
}).unwrap();
assert_eq!(v, [1, 2, 3]);
isle.shutdown().unwrap();
```

With the `serde` feature, a table deserializes the same way.  This is a
fragment: `isle` is an `Isle` as above and `MyStruct` any type that
implements `serde::Deserialize`:

```rust
use mlua::LuaSerdeExt;

let s: MyStruct = isle.exec(|lua| {
    Ok(lua.from_value(lua.load("return { name = 'x', n = 1 }").eval()?)?)
}).unwrap();
```

The async handle has the same signatures (`isle.eval::<i64>(code).await`,
`AsyncTask<T>`), and so do the coroutine requests.

### Coroutine execution (async)

```rust
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use mlua_isle::AsyncIsle;

let (isle, driver) = AsyncIsle::spawn(|lua| {
    // Register an async Rust function — coroutines yield here
    lua.globals().set("sleep_ms", lua.create_async_function(|_, ms: u64| async move {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        Ok(())
    })?)?;
    Ok(())
}).await?;

// Multiple coroutines share the same VM cooperatively
let t1 = isle.spawn_coroutine_eval::<String>("sleep_ms(10) return 'a'");
let t2 = isle.spawn_coroutine_eval::<String>("sleep_ms(10) return 'b'");

let (r1, r2) = tokio::join!(t1, t2);
assert_eq!(r1?, "a");
assert_eq!(r2?, "b");

driver.shutdown().await?;
# Ok(())
# }
```

### Connection pool

```rust
use mlua_isle::{IslePool, PoolConfig, PoolStrategy};

let pool = IslePool::new(
    |lua| {
        lua.globals().set("greeting", "hello")?;
        Ok(())
    },
    PoolConfig {
        max_size: 4,
        strategy: PoolStrategy::Warm,
    },
).unwrap();

{
    let isle = pool.checkout().unwrap();
    let result: String = isle.eval("return greeting").unwrap();
    assert_eq!(result, "hello");
} // isle returned to pool automatically

pool.shutdown();
```

### Cancellation (sync)

```rust
use mlua_isle::Isle;
use std::time::Duration;
use std::thread;

let isle = Isle::spawn(|_| Ok(())).unwrap();

let task = isle.spawn_eval::<()>("while true do end");

thread::sleep(Duration::from_millis(50));
task.cancel();

let result = task.wait();
assert!(result.is_err()); // IsleError::Cancelled
```

### Cancellation (async)

```rust
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use mlua_isle::AsyncIsle;
use std::time::Duration;

let (isle, driver) = AsyncIsle::spawn(|_lua| Ok(())).await?;
let task = isle.spawn_eval::<()>("while true do end");

let token = task.cancel_token().clone();
tokio::spawn(async move {
    tokio::time::sleep(Duration::from_millis(100)).await;
    token.cancel();
});

let result = task.await; // Err(Cancelled)
assert!(result.is_err());
driver.shutdown().await?;
# Ok(())
# }
```

Dropping a `Task` / `AsyncTask` before it resolves cancels the operation.
Call `.detach()` to let it run without keeping the handle.

### Structured tasks (async)

`tasks::install` gives Lua code a `task` library.  Tasks are structured:
cancelling a request cancels every task it spawned (and theirs), and a
request, whether it finishes or is cancelled, does not resolve before
the tasks it did not join have been cancelled and have finished or been
dropped.  The cancel grace period is one deadline for the request and
all of its tasks.  Host code joins the same structure through
`runtime::current_scope()` (see [Host tasks](#host-tasks-in-the-requests-scope)).
Dropping a `run_root` future (rather than cancelling its token and
awaiting it) only schedules its tasks for abort.

```rust
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use mlua_isle::hooks::{self, CancelConfig};
use mlua_isle::{cancellable, tasks, AsyncIsle};
use std::time::Duration;

let (isle, driver) = AsyncIsle::spawn(|lua| {
    hooks::configure(lua, CancelConfig {
        // A cancelled coroutine may run its cleanup for up to 100 ms.
        grace: Duration::from_millis(100),
        // Yield CPU-bound tasks so that siblings can run and cancel them.
        preempt_every: Some(1),
    });
    lua.globals().set("task", tasks::install(lua)?)?;
    // `cancellable` turns a cancel into a Lua error at this await point,
    // so `__close` handlers of the coroutine run and may await.
    let sleep = lua.create_async_function(|_, ms: u64| {
        cancellable(async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Ok(())
        })
    })?;
    lua.globals().set("sleep", sleep)
})
.await?;

let r: String = isle
    .coroutine_eval(
        r#"
        local a = task.spawn(function() sleep(10) return "a" end)
        local b = task.spawn(function() error({ code = 42 }) end)
        local _, va = a:join()           -- true, "a"
        local _, err = b:join()          -- false, { code = 42 }
        local slow <close> = task.spawn(function() sleep(10000) end)
        return va .. err.code            -- `slow` is cancelled and awaited here
        "#,
    )
    .await?;
assert_eq!(r, "a42");
driver.shutdown().await?;
# Ok(())
# }
```

The isle owns the VM's Lua debug hook.  Register your own hook callbacks
with `hooks::add_hook` rather than `Lua::set_hook`, which would replace
the cancel hook.

### Errors

Every function returns `IsleError`.  A Lua error of a request or root is
`IsleError::Lua(LuaFailure)`, built on the VM thread from the raised
value, the same on every path (`Isle`, `AsyncIsle` sync and coroutine
requests, `Vm::run` / `run_root`):

```rust
use mlua_isle::{Isle, IsleError, LuaErrorKind};

let isle = Isle::spawn(|_| Ok(())).unwrap();
match isle.eval::<String>("error({ code = 42 })") {
    Err(IsleError::Lua(f)) => {
        assert_eq!(f.kind, LuaErrorKind::Runtime);
        println!("{}", f.message);          // tostring(err), honours __tostring
        println!("{:?}", f.traceback);      // where it was raised
        // With the `serde` feature: f.value == Some(json!({ "code": 42 }))
    }
    Err(IsleError::Cancelled) => { /* the token was cancelled */ }
    Err(other) => panic!("{other}"),
    Ok(v) => println!("{v}"),
}
```

Other variants: `Init(LuaFailure)` (the init closure failed),
`NotFound(name)` (`call` of a global that is not a function),
`ThreadPanic(Option<String>)` (with the panic message), `RecvFailed`,
`Shutdown`, `ChannelFull`, and the pool errors.  `IsleError` does not
implement `PartialEq`; match with `matches!`.

A cancel reaches Lua code as an error value (the cancel hook raises it
in a CPU loop, `cancellable` at an await point).  Rust recognises it by
value (`mlua::Error::external(Cancelled)`, found with
`downcast_ref::<Cancelled>()`), never by message.  Lua code tells it
from other errors with `task.is_cancelled(err)` from the `task` library,
which is also true for `task.CANCELLED` (what `join` returns for a
cancelled task):

```lua
local ok, err = pcall(sleep, 1000)
if not ok and task.is_cancelled(err) then
  cleanup()
  error(err, 0)  -- let the cancel continue
end
```

### Running Lua on a VM you own

The actors are built on `runtime::Vm`, the in-thread layer.  A host that
owns the `Lua` and drives its own `LocalSet` uses it directly: attach,
put the `task` library where you want it, and run.  `Vm::attach`, the
config and the hook methods need no feature; `vm.run`, `vm.task_lib`
and `cancellable` need `tokio`.

```rust
use mlua_isle::runtime::{CancelToken, Config, Vm};
use std::time::Duration;

let vm = Vm::attach(&lua, Config { grace: Duration::from_secs(1), ..Default::default() })?;
lua.globals().set("task", vm.task_lib()?)?;
let out = local.run_until(vm.run(&token, main, ())).await?;
```

`vm.run` resolves only after every task the root started has ended (Lua
tasks, and host tasks spawned through `current_scope()`, below), and
returns `Err(IsleError::Cancelled)` once `token` is cancelled (Ctrl-C,
a timeout, a hook callback).  The `task` table is never set as a global
by the crate.  `vm.config()` / `vm.set_config()` read and write the one
`Config` of the VM, and `vm.add_hook` registers hook callbacks next to
the cancel check.  An `AsyncIsle` takes the same `Config` through
`AsyncIsle::builder().config(..)`; the pools have no such setting yet,
so configure their VMs from the factory closure.

### Host tasks in the request's scope

A host function that starts work of its own spawns it into the scope of
the running request or task.  The task is then structured like a
`task.spawn` task: it is cancelled when the request ends or is
cancelled, gets the grace (the same deadline as the rest of the tree),
is dropped when the grace ends even if it never looks at its token, and
the request resolves only after it is gone.  This works on the
`AsyncIsle` path and on `vm.run` / `run_root`.

```rust
use mlua_isle::runtime::current_scope;

let bg = lua.create_function(move |_, ()| {
    // Take the handle here, in the synchronous part, and move it into
    // the future.  `None` in a sync request (`eval` / `call` / `exec`).
    let scope = current_scope().expect("inside a coroutine request");
    scope
        .spawn_local(async move {
            // current_token() is this task's token here, and
            // `cancellable` works.
            poll_something().await
        })
        .detach(); // fire and forget; still cancelled and waited for
    Ok(())
})?;
```

`spawn_local` returns a `ScopedTask`, a future of `Result<T, IsleError>`
(`Err(Cancelled)` if the task was cancelled before it finished).  Three
ways to let go of it:

- **await it** to wait for the value;
- **keep it** for as long as the task should run: dropping it cancels
  the task now (without waiting; the scope still waits for it);
- **`detach()` it** to let the task run on without a handle: it is still
  cancelled, given the grace, dropped and waited for when the request or
  task that owns the scope ends or is cancelled.

Inside a host task, `current_scope()` is that task's own scope, so
tasks it spawns are waited for by it.  Tasks spawned into a scope that
is already ending share its remaining time, and a spawn after that time
is gone starts nothing.  A panic in a host future
is caught by tokio and its `ScopedTask` resolves to `Err(Cancelled)`.

`current_token().child_token()` plus a bare `tokio::task::spawn_local`
gives cancellation only: the request neither waits for that task nor
drops it, so a host future that does not watch its token keeps running
next to the next request on the VM, and keeps `driver.shutdown()` from
returning.

## API

### Sync (`Isle`)

| Method | Description |
|--------|-------------|
| `Isle::spawn(init)` | Create a Lua VM on a dedicated thread |
| `isle.eval::<T>(code)` | Evaluate a Lua chunk (blocking), result converted to `T` |
| `isle.call::<A, T>(func, args)` | Call a global Lua function with `args: A` (blocking) |
| `isle.exec(closure)` | Run an arbitrary closure on the Lua thread, returns its `T` |
| `isle.spawn_eval::<T>(code)` | Non-blocking eval, returns a `Task<T>` |
| `isle.spawn_call::<A, T>(func, args)` | Non-blocking call, returns a `Task<T>` |
| `isle.spawn_exec(closure)` | Non-blocking exec, returns a `Task<T>` |
| `isle.shutdown()` | Graceful shutdown and thread join |
| `task.wait()` | Block until the task completes |
| `task.cancel()` | Cancel the running task |

### Async (`AsyncIsle`, `tokio` feature)

| Method | Description |
|--------|-------------|
| `AsyncIsle::spawn(init)` | Create a Lua VM, returns `(AsyncIsle, AsyncIsleDriver)` |
| `AsyncIsle::builder()` | Configure channel capacity / thread name / `Config` |
| `isle.eval::<T>(code)` | Evaluate a Lua chunk (async, exclusive), result converted to `T` |
| `isle.call::<A, T>(func, args)` | Call a global Lua function with `args: A` (async, exclusive) |
| `isle.exec(closure)` | Run a closure on the Lua thread (async, exclusive), returns its `T` |
| `isle.coroutine_eval::<T>(code)` | Evaluate as a cooperative coroutine |
| `isle.coroutine_call::<A, T>(func, args)` | Call a function as a cooperative coroutine |
| `isle.spawn_eval::<T>(code)` | Returns a cancellable `AsyncTask<T>` |
| `isle.spawn_call::<A, T>(func, args)` | Returns a cancellable `AsyncTask<T>` |
| `isle.spawn_exec(closure)` | Returns a cancellable `AsyncTask<T>` |
| `isle.spawn_coroutine_eval::<T>(code)` | Coroutine eval, returns `AsyncTask<T>` |
| `isle.spawn_coroutine_call::<A, T>(func, args)` | Coroutine call, returns `AsyncTask<T>` |
| `driver.shutdown().await` | Graceful shutdown (drains pending coroutines) |
| `task.cancel()` | Cancel the running task |
| `task.cancel_token()` | Access the `CancelToken` for sharing |
| `task.detach()` | Let the task run without the handle (dropping it cancels) |
| `tasks::install(lua)` | Lua `task` library: `spawn` / `join` / `cancel` / `done` / `is_cancelled` |
| `hooks::configure(lua, config)` | Cancel grace period and preemption |
| `hooks::add_hook(lua, triggers, f)` | Register a Lua hook callback next to the cancel hook |
| `cancellable(fut)` | Make an async host function stop at cancel |
| `current_token()` | Token of the running request / task (derive child tokens) |
| `run_root(lua, token, f, args)` | Run a coroutine with task support on a VM you drive |
| `runtime::Vm::attach(lua, config)` | Take over a VM you own: hook, `Config`, `task` table |
| `vm.task_lib()` | The `task` table, created on first call (not set as a global) |
| `vm.run(&token, f, args)` | Run a root coroutine; resolves after its tasks ended |

### Pool (`IslePool`, `pool` feature)

| Method | Description |
|--------|-------------|
| `IslePool::new(factory, config)` | Create a pool with factory closure |
| `pool.checkout()` | Checkout an Isle (blocks until available) |
| `pool.try_checkout()` | Non-blocking checkout, returns `None` at capacity |
| `pool.checkout_timeout(dur)` | Checkout with timeout |
| `pool.active()` | Number of currently checked-out Isles |
| `pool.idle()` | Number of idle Isles |
| `pool.shutdown()` | Shut down all idle Isles |
| `pooled.kill()` | Mark Isle for disposal on drop |

## Minimum Supported Rust Version

Rust 1.88 or later.

## License

Licensed under either of

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
