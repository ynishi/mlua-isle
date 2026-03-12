# Changelog

## [Unreleased]

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
