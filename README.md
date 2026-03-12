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
- **Sync API** — blocking `Isle` handle with `Task` for non-blocking usage
- **Async API** (optional, `tokio` feature) — `AsyncIsle` handle with
  Handle/Driver separation, bounded channel backpressure, and `AsyncTask<T>`
  which implements `Future`
- **Coroutine execution** (optional, `tokio` feature) — cooperative
  multitasking via `coroutine_eval` / `coroutine_call`.  Multiple Lua
  coroutines share the same VM and yield when awaiting async Rust functions
- **Connection pool** (optional, `pool` feature) — `IslePool` manages
  multiple `Isle` instances with checkout/return semantics.  Supports
  `Cold` (fresh VM) and `Warm` (reuse) strategies
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
mlua-isle = "0.4"

# For async support (includes coroutine execution):
# mlua-isle = { version = "0.4", features = ["tokio"] }

# For connection pool:
# mlua-isle = { version = "0.4", features = ["pool"] }

# Both:
# mlua-isle = { version = "0.4", features = ["tokio", "pool"] }
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
let t1 = isle.spawn_coroutine_eval("sleep_ms(10) return 'a'");
let t2 = isle.spawn_coroutine_eval("sleep_ms(10) return 'b'");

let (r1, r2) = tokio::join!(t1, t2);
assert!(r1.is_ok());
assert!(r2.is_ok());

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

let task = isle.spawn_eval("while true do end");

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
let task = isle.spawn_eval("while true do end");

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

## API

### Sync (`Isle`)

| Method | Description |
|--------|-------------|
| `Isle::spawn(init)` | Create a Lua VM on a dedicated thread |
| `isle.eval(code)` | Evaluate a Lua chunk (blocking) |
| `isle.call(func, args)` | Call a global Lua function (blocking) |
| `isle.exec(closure)` | Run an arbitrary closure on the Lua thread |
| `isle.spawn_eval(code)` | Non-blocking eval, returns a `Task` |
| `isle.spawn_call(func, args)` | Non-blocking call, returns a `Task` |
| `isle.spawn_exec(closure)` | Non-blocking exec, returns a `Task` |
| `isle.shutdown()` | Graceful shutdown and thread join |
| `task.wait()` | Block until the task completes |
| `task.cancel()` | Cancel the running task |

### Async (`AsyncIsle`, `tokio` feature)

| Method | Description |
|--------|-------------|
| `AsyncIsle::spawn(init)` | Create a Lua VM, returns `(AsyncIsle, AsyncIsleDriver)` |
| `AsyncIsle::builder()` | Configure channel capacity / thread name |
| `isle.eval(code)` | Evaluate a Lua chunk (async, exclusive) |
| `isle.call(func, args)` | Call a global Lua function (async, exclusive) |
| `isle.exec(closure)` | Run a closure on the Lua thread (async, exclusive) |
| `isle.coroutine_eval(code)` | Evaluate as a cooperative coroutine |
| `isle.coroutine_call(func, args)` | Call a function as a cooperative coroutine |
| `isle.spawn_eval(code)` | Returns a cancellable `AsyncTask` |
| `isle.spawn_call(func, args)` | Returns a cancellable `AsyncTask` |
| `isle.spawn_exec(closure)` | Returns a cancellable `AsyncTask` |
| `isle.spawn_coroutine_eval(code)` | Coroutine eval, returns `AsyncTask` |
| `isle.spawn_coroutine_call(func, args)` | Coroutine call, returns `AsyncTask` |
| `driver.shutdown().await` | Graceful shutdown (drains pending coroutines) |
| `task.cancel()` | Cancel the running task |
| `task.cancel_token()` | Access the `CancelToken` for sharing |

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

Rust 1.77 or later.

## License

Licensed under either of

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
