# Changelog

## [Unreleased]

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
