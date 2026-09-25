# Changelog

## [Unreleased]

### Changed
- The cancel grace period is now one deadline shared by a request or
  task and the tasks it spawns, transitively.  A task spawned during
  cleanup gets the remaining time instead of a fresh grace period, so
  the total wait no longer grows with how deep cleanup spawns tasks.

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
