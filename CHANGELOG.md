# Changelog

## [Unreleased]

### Breaking
Typed errors (#11), per the API policy of #12: payloads change in place.

- `IsleError::Lua(String)` → `IsleError::Lua(LuaFailure)`.  The payload
  is built on the VM thread from the raised value: `kind`, `message`
  (`tostring(err)`, honouring `__tostring`), `traceback`, and with the
  `serde` feature `value`.  Migration: `IsleError::Lua(msg)` →
  `IsleError::Lua(f)` and use `f.message` (or `f.to_string()`).  The
  `Display` of `IsleError::Lua` is now `lua error: <message>`, without
  mlua's `runtime error:` prefix and without the appended traceback
  (that is in `f.traceback`).
- `IsleError::Init(String)` → `IsleError::Init(LuaFailure)`, built from
  the init closure's `mlua::Error`.  Migration: `Init(msg)` → `Init(f)`
  and use `f.message`.  A pool with `max_size == 0` and a VM thread the
  OS refuses to start are `Init` with `LuaErrorKind::External`.
- `IsleError::ThreadPanic` → `IsleError::ThreadPanic(Option<String>)`,
  the panic message when the payload is a `&str` / `String`.  Migration:
  `ThreadPanic` → `ThreadPanic(_)`.  An init closure that panics is now
  `ThreadPanic(Some(msg))` (was `Init("init channel closed: ...")`), from
  `Isle::spawn` and `AsyncIsle::spawn`.
- `IsleError::RecvFailed(String)` → `IsleError::RecvFailed` (the string
  was a fixed discriminator).  Migration: `RecvFailed(_)` → `RecvFailed`.
- New variant `IsleError::NotFound(String)`: `call` / `coroutine_call`
  (and `spawn_*`) of a global that is not a function.  Was
  `IsleError::Lua("function '<name>' not found: ...")`.  Migration: match
  `NotFound(name)`.
- `IsleError` no longer implements `PartialEq`.  Migration:
  `assert_eq!(r, Err(IsleError::Cancelled))` →
  `assert!(matches!(r, Err(IsleError::Cancelled)))`.
- The cancellation error is `mlua::Error::external(Cancelled)` instead of
  a runtime error carrying `__isle_cancelled__`; the sentinel string is
  gone.  Rust detects a cancel by downcast, so a Lua error whose message
  contains `__isle_cancelled__` is no longer reported as `Cancelled`.  In
  Lua, `tostring` of the cancel error is `cancelled` plus a traceback.
  Migration: Lua code that did `tostring(err):find("__isle_cancelled__")`
  uses `task.is_cancelled(err)`; Rust code that matched the message uses
  `e.downcast_ref::<Cancelled>()` (or `IsleError::from(e)`).
- A sync request (`eval` / `call`) whose token was cancelled returns
  `Cancelled` even when the Lua code caught the cancel and raised a
  different error (as coroutine requests and `run_root` already did).
- Sync `eval` / `call` run the Lua code under `xpcall` and `run_root`'s
  root wrapper returns the error as a value instead of re-raising it, so
  a raised table reaches Rust as its `tostring` and (with `serde`) its
  value, not as mlua's flattened `RuntimeError`.  (Results are no longer
  converted to a string at all: see typed results below.)
- Chunk names: a sync `eval` is compiled as chunk `eval` and a
  `coroutine_eval` as `coroutine_eval`, so error positions read
  `eval:1: ...` instead of a crate-internal source path.
- The protected call captures `xpcall` when the VM is set up: the actors
  do it before the init closure, `Vm::attach` on its first call.
  Removing `xpcall` or replacing the globals afterwards no longer
  affects requests.  `Vm::attach` on a VM that has no `xpcall` function
  fails; attach before sandboxing the globals.
- A memory error (`LUA_ERRMEM`) raised in a request is
  `Lua(LuaFailure)` with `LuaErrorKind::Memory` and no traceback.

Typed results and arguments (#10), per #12: signatures change in place,
no `_as` twins.  Results are converted on the VM thread with
`FromLuaMulti` and the `T` crosses back; arguments are converted there
with `IntoLuaMulti`.  `T: FromLuaMulti + Send + 'static` covers a single
value (`String`, `i64`, `f64`, `bool`, `mlua::BString`, ...), `()`,
`Option<T>`, and tuples of return values; `Table` / `Value` /
`MultiValue` are not `Send` without mlua's `send` feature and are then
rejected at compile time (convert them inside `exec`, or ask for a
`RegistryKey`, which is `Send`).

- `Isle::eval(&self, code: &str) -> Result<String, IsleError>` →
  `Isle::eval<T>(&self, code: &str) -> Result<T, IsleError>`
  (`T: FromLuaMulti + Send + 'static`); `spawn_eval(..) -> Task` →
  `spawn_eval<T>(..) -> Task<T>`.  Migration: `isle.eval(code)?` →
  `isle.eval::<String>(code)?` or `let s: String = isle.eval(code)?`.
- `Isle::call(&self, func: &str, args: &[&str]) -> Result<String, IsleError>`
  → `Isle::call<A, T>(&self, func: &str, args: A) -> Result<T, IsleError>`
  (`A: IntoLuaMulti + Send + 'static`); `spawn_call(..) -> Task` →
  `spawn_call<A, T>(..) -> Task<T>`.  Migration: `call("f", &["a", "b"])`
  → `call::<_, String>("f", ("a", "b"))`; `call("f", &[])` →
  `call("f", ())`; a slice built at run time →
  `mlua::Variadic::from_iter(v)` (a `Vec` is passed as one table).
- `Isle::exec<F>(&self, f: F) -> Result<String, IsleError>` with
  `F: FnOnce(&Lua) -> Result<String, IsleError>` →
  `Isle::exec<F, T>(&self, f: F) -> Result<T, IsleError>` with
  `F: FnOnce(&Lua) -> Result<T, IsleError> + Send + 'static`,
  `T: Send + 'static`; `spawn_exec(..) -> Task` →
  `spawn_exec<F, T>(..) -> Task<T>`.  Migration:
  `exec(|lua| Ok(x.to_string()))` → `exec(|lua| Ok(x))`.
- `AsyncIsle`: the same change for `eval` / `spawn_eval`, `call` /
  `spawn_call`, `exec` / `spawn_exec`, `coroutine_eval` /
  `spawn_coroutine_eval` and `coroutine_call` / `spawn_coroutine_call`
  (`async fn .. -> Result<T, IsleError>`, `spawn_* -> AsyncTask<T>`).
  Migration: `isle.eval(code).await?` → `isle.eval::<String>(code).await?`;
  `coroutine_call("f", &["a"])` → `coroutine_call::<_, String>("f", ("a",))`.
  `PooledIsle` / `AsyncPooledIsle` deref to the handles and change with
  them.
- `Task<T = String>` → `Task<T>` and `AsyncTask<T = String>` →
  `AsyncTask<T>`: the `String` default is gone.  Migration: a type that
  named `Task` / `AsyncTask` bare names `Task<String>` (or the new `T`).
- The lossy string conversion is gone.  Code that relied on it gets a
  different result or a `Conversion` error: `nil` was `""` (now `None`
  with `Option<T>`, and a `Conversion` error for `String`); a table was
  `"table: 0x..."` (now rejected at compile time, convert in `exec`); a
  boolean was `"true"` / `"false"` (ask for `bool`; `String` gives a
  `Conversion` error); only the first return value was kept (ask for a
  tuple); a string that is not UTF-8 was an error (ask for
  `mlua::BString` for its raw bytes; `String` still rejects it with a
  `Conversion` error).  Numbers still convert to `String` (mlua coerces
  them), and `bool` follows Lua truthiness.  A value that does not
  convert to `T` is `IsleError::Lua` with `LuaErrorKind::Conversion`; a
  `FromLua` / `IntoLua` of your own that fails keeps the kind of the
  error it returns (a Lua error it raised is `Runtime`).
- The result conversion of every request, coroutine requests included,
  runs under the request's cancel token: a `FromLua` that runs Lua code
  (a metamethod, a loop) can be cancelled and then returns `Cancelled`.
  Before, a coroutine request converted its result (`tostring`, which
  runs `__tostring`) after its token had been left, where a cancel
  could not reach it.
- `call` arguments keep their type: `call("f", (1, true))` passes a
  number and a boolean (the old API could only pass strings).

### Added
- `LuaFailure` (`kind`, `message`, `traceback`, `value` with `serde`;
  `LuaFailure::new`, `LuaFailure::from_mlua`, `From<mlua::Error>`,
  `Display` = the message) and `LuaErrorKind` (`Runtime`, `Syntax`,
  `Memory`, `Callback`, `External`, `Conversion`, `Other`).  A host
  function that returns `Err(mlua::Error::external(e))` and is called
  from Lua is `Callback`, with `e`'s `Display` as the message.
  `LuaFailure` has no `source: mlua::Error`: without mlua's `error-send`
  feature `mlua::Error` is not `Send`.
- `Cancelled`, the error a cancel raises (`mlua::Error::external(Cancelled)`),
  exported at the crate root and in `runtime`.  A host function may
  return it to report a cancel.
- `task.is_cancelled(err)` in the `task` library: true for the error a
  cancel raises (caught with `pcall`, or received by a `__close`
  handler) and for `task.CANCELLED`.  `join` still returns
  `false, task.CANCELLED` for a cancelled task.
- `serde` feature (`mlua/serialize` + `serde_json`): `LuaFailure::value`,
  the raised value as `serde_json::Value` when it converts.
- `runtime` re-exports `IsleError`, `LuaFailure`, `LuaErrorKind` and
  `Cancelled`.
- `runtime` module, the in-thread layer: `runtime::Vm` is the entry point
  for a host that owns the `Lua` and drives its own `LocalSet`.
  `Vm::attach(&lua, Config)` installs the hook and stores the config (a
  second `attach` on the same VM replaces the config); `Vm::of`,
  `config` / `set_config`, `add_hook` / `remove_hook`, and with the
  `tokio` feature `task_lib` (the `task` table, created on first call,
  one per VM, never set as a global) and
  `run(&token, f, args)`.  The module docs state the layer's contracts.
  `runtime` also re-exports `CancelToken`, `current_token`, `HookId` and
  `cancellable`.  The existing free functions are unchanged.
- `runtime::Config` (`grace`, `preempt_every`), converting to and from
  `hooks::CancelConfig`.
- `AsyncIsleBuilder::config(Config)` sets the grace period and preemption
  without the init closure.  It replaces a config the init closure set.
- Host tasks in a request's scope (#8): `runtime::current_scope()`
  returns a `runtime::ScopeHandle` while a coroutine request or task is
  polled (on the `AsyncIsle` path and on `Vm::run` / `run_root`; `None`
  in a sync request), and `ScopeHandle::spawn_local(fut)` starts a host
  future as a task of that scope.  The task gets a child token (its
  `current_token()`, so `cancellable` works inside) and its own scope,
  is cancelled when the request or task ends or is cancelled, shares the
  grace deadline of the tree, is dropped when the grace ends, and is
  waited for before the request resolves, like a `task.spawn` task.  It
  returns a `runtime::ScopedTask<T>`, a future of `Result<T, IsleError>`
  whose drop cancels the task without waiting; `ScopedTask::detach()`
  lets the task run on without the handle (it stays in the scope and is
  still cancelled, dropped and waited for when the scope ends), like
  `AsyncTask::detach`.  `ScopeHandle::token()` is the scope's token.
  `run`'s contract now covers these host tasks.  Tasks spawned into a
  scope that is already ending share its remaining time: a scope whose
  body ended normally takes its grace deadline when it starts waiting
  for its tasks, and a spawn after that deadline starts nothing
  (`Err(Cancelled)`).

### Changed
- The cancel grace period is now one deadline shared by a request or
  task and the tasks it spawns, transitively.  A task spawned during
  cleanup gets the remaining time instead of a fresh grace period, and a
  task that observes the cancel late still ends by its parent's
  deadline, so the total wait no longer grows with how deep cleanup
  spawns tasks or with the order in which tasks run.
- `Isle`, `AsyncIsle` and the pools attach a `runtime::Vm` to their VM
  after the init closure (in place of `hooks::install`) and run coroutine
  requests through `Vm::run`.  No change in behaviour.

### Fixed
- Cancelling a coroutine request, a `run_root` call or a task now waits
  for the tasks it spawned, transitively, to finish or be dropped before
  it resolves.  Previously it resolved first and only scheduled those
  tasks for abort, so their coroutines and the host futures they were
  awaiting could be dropped later, inside the next request on the same
  VM.

## [0.7.0] - 2026-09-24

### Added
- `CancelToken::child_token` — a token cancelled together with its parent.
  Finished children leave the parent without an explicit unregister.
- `current_token()` — the token of the request or task currently running,
  for host functions that start work of their own.
- `hooks` module: the isle owns the VM's Lua debug hook and shares it.
  `hooks::add_hook` / `hooks::remove_hook` register user callbacks next
  to the cancel check (including in coroutines the Lua code creates);
  `hooks::configure` sets a `CancelConfig`:
  - `grace` — a cancelled coroutine first gets the cancellation as a Lua
    error, so its `__close` handlers run and may await; after `grace` it
    is dropped.  Default zero (drop at once, as before).
  - `preempt_every` — yield CPU-bound coroutine requests and tasks
    periodically so other tasks on the thread, including one that
    cancels them, can run.  Coroutines the Lua code creates are never
    yielded.  Default off.
- `cancellable(fut)` — wrap the future of an async host function so that
  a cancel returns a Lua error at that await point.
- `tasks` module: a structured Lua task library (`task.spawn`,
  `h:join()`, `h:cancel()`, `h:done()`, `task.CANCELLED`).  `join`
  returns the raw Lua error value (tables stay tables).  A request or
  task cancels and awaits the tasks it did not join before it resolves;
  a `<close>` handle does the same on scope exit.
- `run_root(lua, token, f, args)` — run a coroutine with task support on
  a VM you drive yourself (a `LocalSet` on your own thread).
- `Task::detach` / `AsyncTask::detach`.

### Changed
- **Breaking**: dropping a `Task` or `AsyncTask` before it resolves now
  cancels the operation (previously it kept running, detached).  Use
  `.detach()` for the old behaviour.  Both types are `#[must_use]`.
- The isle re-installs its hook at the start of a request when
  `Lua::set_hook` replaced it.
- The cancel hook is installed once per VM as a Lua global hook, after the
  init closure, and stays installed.  A hook set with `Lua::set_hook` in
  the init closure is replaced by it.  (Previously every request replaced
  the hook and removed it afterwards, so such a hook did not survive the
  first request either.)  Do not set a hook with `Lua::set_hook` /
  `Lua::set_global_hook` from inside a request (e.g. an `exec` closure):
  it replaces the cancel hook, and cancellation stops working.  Register
  callbacks with `hooks::add_hook` instead.
- Coroutine requests run through `Function::call_async` instead of
  `Thread::into_async`.

### Fixed
- Cancelling a request now reaches coroutines that the Lua code creates
  itself (`coroutine.create` / `coroutine.wrap`).  The cancel hook was a
  per-thread hook, which mlua removes from such coroutines the first time
  it fires, so a CPU loop inside one could not be cancelled and blocked
  the isle thread for good.  Affects `Isle`, `AsyncIsle` (sync and
  coroutine requests) and the pools.
- Cancelling a coroutine request (`spawn_coroutine_eval` /
  `spawn_coroutine_call`) now drops the Rust future the coroutine was
  awaiting at once, as `CancelToken::cancelled` documents, and closes the
  coroutine's pending to-be-closed variables.  Previously the future was
  kept alive until the next Lua GC cycle or shutdown (#1).

## [0.6.0] - 2026-09-05

### Changed
- **Breaking**: `mlua` dependency bumped from `0.11` to `0.12`.  `mlua`
  types appear in the public API (`&mlua::Lua` / `mlua::Error` in exec
  closures), so downstream crates must use `mlua 0.12` as well.
- MSRV raised from 1.77 to 1.88 (required by `mlua 0.12`).
- No source changes were needed: every `mlua` API used by this crate
  (`set_hook` / `remove_hook` / `HookTriggers` / `VmState` /
  `Thread::into_async` / `MultiValue::from_vec` / `Error::runtime`) kept
  its signature and crate-root re-export in `mlua 0.12`.
- Picks up the `mlua 0.12.1` fix for coroutine stack handling after
  yielding from hooks, which affects the `AsyncIsle` cancel-hook path.

## [0.5.0] - 2026-06-14

### Added
- `AsyncIslePool` / `AsyncPooledIsle` — async counterpart of `IslePool`,
  gated behind `pool` + `tokio` features.  Holds `(AsyncIsle, AsyncIsleDriver)`
  slots, supports `Cold` / `Warm` strategies, and exposes
  `checkout` / `try_checkout` / `checkout_timeout` / `active` / `idle` /
  `shutdown` mirrored on the sync `IslePool` API.  Idle wait uses
  `tokio::sync::Notify`; `Drop` is synchronous and dispatches the inner
  driver shutdown to a background `tokio::spawn` when a runtime handle
  is available (otherwise the Lua thread exits via channel-close).

### Fixed
- `tokio` feature now enables the `tokio/macros` and `tokio/time` cargo
  features needed by `tokio::select!` (in `async_isle`) and `tokio::time::*`
  (in `async_pool::checkout_timeout`).  Previously the crate built only
  via dev-dependency feature unification with `--all-features`; building
  with just `--features tokio` failed.

## [0.4.1] - 2026-04-18

### Added
- `CancelToken::cancelled` — async cancellation signal backed by
  `tokio::sync::Notify` (tokio feature only).  Registers the
  `Notified` future via `enable()` before re-checking the flag, so
  a `cancel()` call racing with `cancelled()` is not lost.

### Fixed
- Coroutines suspended inside a Rust `.await` (e.g. a
  `create_async_function` awaiting a tokio child process) can now be
  cancelled.  `execute_coroutine_eval` / `execute_coroutine_call`
  race the coroutine future against `CancelToken::cancelled` in a
  `tokio::select!`; when cancel wins, dropping the `AsyncThread`
  releases the awaited Rust resources via the standard async
  cancellation model.  Previously the Lua debug hook was the only
  cancel path, and it cannot fire while no Lua instructions execute.

## [0.4.0] - 2026-03-12

### Added
- `coroutine_eval` / `coroutine_call` on `AsyncIsle` — cooperative coroutine
  execution via `mlua::Thread::into_async` + `tokio::task::spawn_local`.
  Multiple coroutines share the Lua VM; when one yields (e.g. awaiting an
  async Rust function), others make progress.
- `spawn_coroutine_eval` / `spawn_coroutine_call` — non-blocking variants
  returning cancellable `AsyncTask`.
- `IslePool` — connection pool for `Isle` instances with checkout/return
  semantics via RAII guard (`PooledIsle`).  Gated behind the `pool` feature.
- `PoolConfig` / `PoolStrategy` — configure pool `max_size` and
  `Cold` (fresh VM per checkout) vs `Warm` (reuse) strategies.
- `PooledIsle::kill` — mark a checked-out Isle for disposal instead of return.
- `IslePool::try_checkout` — non-blocking checkout, returns `None` at capacity.
- `IslePool::checkout_timeout` — checkout with deadline.
- `IslePool::active` / `IslePool::idle` — pool metrics.
- `IsleError::PoolExhausted` / `IsleError::PoolPoisoned` — pool-specific
  error variants (pool feature only).
- `hook::install_cancel_hook_on_thread` — install cancel hook on a specific
  Lua `Thread` (internal, supports coroutine cancellation).

### Changed
- Tokio runtime (`Builder::new_current_thread`) is now built **before** the
  init-success signal is sent.  A build failure (e.g. fd exhaustion from
  `epoll_create`/`kqueue`) is reported as `IsleError::Init` instead of
  causing an unrecoverable panic on the Lua thread.
- Shutdown now drains pending coroutines to completion before exiting the
  Lua thread.  The `LocalSet` future is awaited, which completes only after
  all `spawn_local`'d tasks finish.  Coroutines stuck in infinite loops can
  still be cancelled via their `CancelToken`.

## [0.3.0] - 2026-03-09

### Added
- `AsyncIsle` / `AsyncIsleDriver` — async (tokio) API with Handle/Driver
  separation pattern, bounded channel backpressure, and cancellation support.
- `AsyncTask<T>` — `Future`-based task handle for async operations.
- `AsyncIsleBuilder` — builder for configuring channel capacity and thread name.
- `IsleError::ChannelFull` — transient backpressure error (tokio feature only).
- `#[non_exhaustive]` on `IsleError` for forward-compatible matching.
- `HookGuard` (internal) — RAII guard ensuring Lua debug hooks are removed
  even on panic.

### Changed
- `Isle::shutdown` signature: `fn shutdown(mut self)` → `fn shutdown(self)`.
  The `mut` was unnecessary since `JoinHandle` is now behind a `Mutex`.
- `Isle` internal: replaced `unsafe impl Sync` with `Mutex<Option<JoinHandle<()>>>`,
  deriving `Sync` safely through the type system.
- `thread::execute_eval`, `execute_call`, `execute_exec` promoted to
  `pub(crate)` for reuse by `async_isle`.

### Removed
- **BREAKING**: `IsleError::SendFailed` removed.  Channel-send failures are
  now reported as `IsleError::Shutdown` (sync) or `IsleError::ChannelFull`
  (async, tokio feature).  If you were matching on `SendFailed`, update to
  match on `Shutdown` instead.
- `unsafe impl Sync for Isle` — no longer needed.

## [0.2.0] - 2026-03-07

Initial public release.

- Thread-isolated Lua VM with `Isle` handle.
- `Task` with `CancelToken` for cooperative cancellation.
- `eval`, `call`, `exec` APIs.
