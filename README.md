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
- **Blocking & async-friendly** — blocking API with `Task` handles for
  non-blocking usage
- **Zero unsafe in user code** — the `Isle` handle is safe to share across threads

## Architecture

```text
┌─────────────────┐   mpsc    ┌──────────────────┐
│  caller thread   │─────────►│  Lua thread       │
│  (UI / async)    │          │  (mlua confined)   │
│                  │◄─────────│                    │
│  Isle handle     │  oneshot  │  Lua VM + hook    │
└─────────────────┘           └──────────────────┘
```

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
mlua-isle = "0.1"
```

### Basic example

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

### Cancellation

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

## API

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
