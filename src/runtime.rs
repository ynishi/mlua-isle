//! Running Lua on a VM you own: the in-thread layer of the crate.
//!
//! The crate has two layers.  The actors ([`Isle`](crate::Isle),
//! `AsyncIsle`, the pools) put a VM on a thread of their own and take
//! requests over channels.  This module is the layer underneath: a host
//! that owns the [`Lua`] and drives the executor itself (its own thread
//! and [`LocalSet`](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html))
//! uses it directly, and the actors are built on it.
//!
//! [`Vm`] is the entry point: [`Vm::attach`] takes over the VM's debug
//! hook, stores its [`Config`] and creates the `task` library.  The
//! hook part (`attach`, `of`, `config`, `set_config`, `add_hook`,
//! `remove_hook`) is always available; `Vm::run`, `Vm::task_lib`,
//! `cancellable` and `current_scope` need the `tokio` feature.  With
//! it, setup is three calls (see `Vm::run` for the full example):
//!
//! ```text
//! let vm = Vm::attach(&lua, Config { grace: Duration::from_secs(1), ..Default::default() })?;
//! lua.globals().set("task", vm.task_lib()?)?;
//! let out = local.run_until(vm.run(&token, main, ())).await?;
//! ```
//!
//! # Contracts
//!
//! 1. **When `run` resolves, nothing the root started is alive.**  This
//!    holds for Lua tasks (`task.spawn`) and for host tasks spawned
//!    through the scope (`current_scope()` then
//!    `ScopeHandle::spawn_local`), transitively and in any mix,
//!    whether the root finished or was cancelled.  A host task that
//!    ignores its token is dropped when the grace ends.  Tasks spawned
//!    into a scope that is already ending share its remaining time; a
//!    spawn after the remaining time is gone starts nothing.  The layer
//!    does not drain the host's `LocalSet`: a `spawn_local` that bypasses the
//!    scope (for example `current_token().child_token()` plus a bare
//!    `tokio::task::spawn_local`) is cancelled with the request but is
//!    neither waited for nor dropped, and is the host's
//!    responsibility.  This holds when `run` is awaited to the end:
//!    dropping the `run` future instead only schedules the tasks for
//!    abort, and a task in a CPU loop blocks `run` until it yields,
//!    which without [`Config::preempt_every`] it never does.
//! 2. **One error type**, [`IsleError`], on this layer and on the
//!    actors, with one payload for a Lua error: [`IsleError::Lua`]
//!    carries a [`LuaFailure`] (kind, message as Lua prints it,
//!    traceback, and with the `serde` feature the raised value as
//!    JSON), built on the VM thread from the raised value, the same
//!    for `run` and for every actor request.  A cancel is
//!    [`IsleError::Cancelled`], recognised by value: the cancel error
//!    is `mlua::Error::external(`[`Cancelled`]`)`, found by downcast,
//!    never by message.
//! 3. **The layer owns the VM's debug hook.**  Register callbacks with
//!    [`Vm::add_hook`], never with `Lua::set_hook` /
//!    `Lua::set_global_hook`, which replace the hook and stop
//!    cancellation (see [Hooks](#hooks)).
//! 4. **One [`Config`] per VM**, read and written through [`Vm`]
//!    ([`Vm::config`], [`Vm::set_config`]; a second [`Vm::attach`]
//!    replaces it).
//! 5. **Cancellation is a token the host creates** and passes to
//!    `run`; `run` spawns nothing and returns no handle.  Ctrl-C, a
//!    timeout or a hook callback cancel that [`CancelToken`].
//!
//! Two requirements on the VM itself: attach it before sandboxing its
//! globals (the first [`Vm::attach`] captures `xpcall`), and keep mlua's
//! default `LuaOptions::catch_rust_panics = true`, whose `xpcall` is
//! yieldable (with `false` every yield in a `run` root fails; see
//! [`Vm::attach`]).
//!
//! Host functions called from Lua reach the running request or task
//! through the context functions, which read a thread-local and so take
//! no receiver: [`current_token`], `cancellable` and `current_scope`.
//!
//! # Hooks
//!
//! Lua has one hook slot per thread, and mlua's hook setters replace
//! whatever was there, so the layer owns the VM's hook: [`Vm::attach`]
//! installs a single **global** hook (one callback shared by every
//! thread of the VM, including coroutines the Lua code creates) that,
//! in order,
//!
//! 1. raises the cancellation error ([`Cancelled`]) when the token of
//!    the request or task currently executing is cancelled (checked
//!    every 1000 instructions; a check that lands in the code mlua uses
//!    to return an async host function's results is deferred, see
//!    [Channels, timers and select](#channels-timers-and-select)),
//! 2. calls the callbacks registered with [`Vm::add_hook`], each at its
//!    own [`HookTriggers`],
//! 3. yields the running root coroutine or task every N checks when
//!    preemption is enabled ([`Config::preempt_every`]).
//!
//! Calling [`Lua::set_hook`], [`Lua::set_global_hook`] or
//! [`mlua::Thread::set_hook`] on the VM replaces this hook, and
//! cancellation stops working.  The hook is re-installed when a
//! `set_hook` replacement is noticed (at the start of every actor
//! request and of every `Vm::run`), but a replacement through
//! `set_global_hook` cannot be detected.
//!
//! # The `task` library
//!
//! `Vm::task_lib` (`tokio` feature) returns the VM's `task` table,
//! conventionally set as the global `task`:
//!
//! | Lua | meaning |
//! |---|---|
//! | `task.spawn(f, ...)` | Start `f(...)` as a concurrent task of the current coroutine request or task.  Returns a handle. |
//! | `h:join()` | Wait for the task.  Returns `true, ...` (what `f` returned) or `false, err`, where `err` is the raw Lua error value, or `task.CANCELLED` if the task was cancelled.  A handle can be joined once (a chosen `h:on` / `h:arm` case counts as the join). |
//! | `h:cancel()` | Request cancellation.  Does not wait. |
//! | `h:done()` | Whether the task has finished. |
//! | `local h <close> = task.spawn(...)` | On scope exit, a task that was not joined is cancelled and waited for. |
//! | `task.is_cancelled(err)` | Whether `err` is a cancellation: the error a cancel raises (caught with `pcall`, or received by a `__close` handler) or `task.CANCELLED`.  `false` for any other value. |
//! | `task.CANCELLED` | What `join` returns as `err` for a cancelled task. |
//! | `task.channel(cap)` | A local channel holding up to `cap` values (`cap >= 0`; `cap = 0` is a rendezvous channel).  See [Channels, timers and select](#channels-timers-and-select). |
//! | `ch:send(v)` | Push `v`; waits while the channel is full (rendezvous: until a receiver has taken `v`).  Raises when the channel is closed. |
//! | `ch:try_send(v)` | Push `v` without waiting: `true`, or `false` when full (rendezvous: `true` only if a receiver is waiting right now).  Raises when the channel is closed. |
//! | `ch:recv()` | Take the value at the front; waits while the channel is empty.  Returns `v, true`, or `nil, false` when the channel is closed and empty. |
//! | `ch:try_recv()` | Take without waiting.  Returns `v, ok, ready`: `v, true, true` (a value), `nil, false, true` (closed and empty) or `nil, false, false` (empty, not closed). |
//! | `ch:close()` | Close the channel.  Idempotent. |
//! | `ch:closed()`, `ch:len()`, `ch:cap()` | Whether it is closed; how many values it holds; its capacity. |
//! | `task.after(ms)` | A one-shot timer, ready `ms` milliseconds after this call (`ms <= 0`: at once) and from then on. |
//! | `t:wait()` | Wait until the timer is ready. |
//! | `task.ticker(ms)` | A receive-only channel of capacity 1 that receives a tick every `ms` milliseconds (`ms > 0`): the tick's time in milliseconds since the ticker started.  Stops with the scope that created it. |
//! | `tk:stop()` | Stop the ticker and close its channel.  Idempotent. |
//! | `ch:on(f)`, `ch:on_send(v, f)`, `t:on(f)`, `h:on(f)` | A case for `task.select`: receive from `ch` and call `f(v, ok)`; send `v` into `ch` and call `f(sent)` (`sent = false`: the channel was closed); call `f()` when `t` is ready; call `f(ok, ...)` with what `h:join()` returns once the task `h` has finished. |
//! | `task.select(cases, opts)` | Wait until one case is ready, take it, call its handler and return what the handler returns.  `opts.biased` (default `false`): check the cases in order instead of round robin.  `opts.default = f`: if no case is ready, call `f()` instead of waiting. |
//! | `ch:arm_recv()`, `ch:arm_send(v)`, `t:arm()`, `h:arm()` | A case for `task.select_raw`. |
//! | `task.select_raw(arms, opts)` | As `task.select`, without handlers: returns the index of the chosen case and its values (`i, v, ok` for a receive, `i, sent` for a send, `i` for a timer, `i, ok, ...` for a task).  `opts.default = true`: return `0` if no case is ready. |
//! | `req.value`, `req:reply(v)`, `req:replied()`, `local req <close> = ...` | A `Request` received from a host channel.  See [Host channels and requests](#host-channels-and-requests). |
//!
//! A host channel (`channel`) and a ticker are `Channel` objects like
//! `task.channel`'s, receive-only: `send` / `try_send` and send cases
//! raise.  A channel to the host (`channel_to_host`) is one too,
//! send-only: `recv` / `try_recv` / `on` / `arm_recv` and receive cases
//! raise.
//!
//! Tasks are **structured**: when a coroutine request or task finishes,
//! the tasks it spawned and did not join are cancelled, and it waits
//! for them before its own result is delivered.  Cancelling a request
//! or task cancels all of its tasks (their tokens are children of its
//! token, see [`CancelToken::child_token`]), and the cancelled request
//! or task still resolves only after they, and their own tasks, have
//! finished or been dropped.  The [grace](Config::grace) is one deadline
//! for the whole tree: a task spawned during cleanup gets the time that
//! remains, not a fresh grace period.
//!
//! `task.spawn` works inside `Vm::run`, inside coroutine requests
//! (`AsyncIsle::coroutine_eval` / `coroutine_call`) and inside tasks
//! (including host tasks).  Sync requests (`eval` / `call` / `exec`)
//! cannot await, so `task.spawn` raises an error there.
//!
//! A cancel reaches Lua code as an error: the cancel hook raises it
//! while Lua code runs, and an async host function wrapped with
//! `cancellable` returns it while the coroutine awaits.  Its value is
//! `mlua::Error::external(`[`Cancelled`]`)` (a userdata to Lua);
//! `task.is_cancelled(err)` is the test, and it is also true for
//! `task.CANCELLED`, so one predicate covers both:
//!
//! ```lua
//! local ok, err = pcall(sleep, 1000)
//! if not ok and task.is_cancelled(err) then
//!   -- cancelled: clean up and let the cancel continue
//!   error(err, 0)
//! end
//! ```
//!
//! The predicate lives in the library: a VM that runs without the `task`
//! table has no `task.is_cancelled`; Rust code tests an `mlua::Error`
//! with `e.downcast_ref::<Cancelled>()`.
//!
//! A task that runs a CPU loop never yields on its own, so a sibling on
//! the same thread cannot run to cancel it; enable
//! [`Config::preempt_every`] for that.  Cancelling from another thread
//! (an `AsyncTask` handle) works without it.
//!
//! # Channels, timers and select
//!
//! The `task` library passes values between the tasks of one VM with
//! **local channels**, and waits on several of them (and on timers) at
//! once with **`select`**:
//!
//! ```lua
//! local requests = task.channel(16)
//! local worker = task.spawn(function()
//!   while true do
//!     local stop = task.select({
//!       requests:on(function(req, ok)
//!         if not ok then return true end   -- closed and drained
//!         handle(req)
//!         return false
//!       end),
//!       task.after(1000):on(function() idle() return false end),
//!     })
//!     if stop then return end
//!   end
//! end)
//! requests:send(req)
//! ```
//!
//! **Channels**
//!
//! - FIFO, with any number of senders and receivers inside one VM.
//!   Waiting receivers are woken in arrival order.
//! - Values are Lua values as they are (`nil` included); tables are
//!   shared, not copied.
//! - After `close`: `send` / `try_send` raise; `recv` returns what is
//!   left, in order, then `nil, false`.
//! - A channel belongs to no scope: cancelling a task does not close
//!   it.  When the channel is garbage-collected, its contents are
//!   dropped.  The values are held from Rust, out of the collector's
//!   sight, so a value that refers back to its own channel keeps both
//!   alive.
//! - Without [`Config::preempt_every`], a sender that never waits (a
//!   `try_send` loop) keeps receivers on the same thread from running;
//!   `send`, which waits when the channel is full, is the form to use.
//!
//! **Rendezvous channels** (`task.channel(0)`)
//!
//! - A sender posts an *offer* and waits until a receiver takes it:
//!   `send` returns once a receiver has taken the value.  Receivers take
//!   offers in the order they were posted (FIFO among senders).
//! - `try_send` succeeds only when a receiver is waiting right now (a
//!   `recv`, or a select's receive case: the two wait the same way); it
//!   hands the value to the first waiting receiver and posts nothing.
//!   `try_recv` takes the first offer.
//! - `len()` and `cap()` are `0`.  The one exception is a value handed
//!   to a waiting receiver that then did not take it (its select chose
//!   another case, or it was cancelled): the value goes back to the
//!   front of the channel, counts in `len()`, and is received next.
//! - A select never pairs its own send case with its own receive case
//!   on the same channel; two different selects do pair.
//! - A select whose send case's offer was taken chooses that case, even
//!   if another case is ready by the time it runs again: the value is
//!   delivered.  At most one of a select's offers is taken.  When a
//!   select chooses another case (or runs `default`, or is cancelled),
//!   its offers are withdrawn and are never received.
//! - `close`: waiting senders raise "channel is closed" (a send case is
//!   chosen with `sent = false`); waiting receivers get `nil, false`.
//! - With `default`, a send case on a rendezvous channel is ready on
//!   the first check when a receiver is waiting (a `recv`, or another
//!   select's receive case): its value goes to the first such receiver,
//!   as with `try_send`, and the case is chosen with `sent = true`.
//!   With no receiver waiting, `default` runs and nothing is posted, so
//!   the value is never received.
//!
//! **Timers**: `task.after(ms)` is ready once `ms` milliseconds have
//! passed since it was created (`ms <= 0`: at once).  It may be waited
//! on, or used as a case, any number of times; once ready it stays
//! ready.
//!
//! **Tickers**: `task.ticker(ms)` is a receive-only channel of capacity
//! 1 that a host task, spawned into the scope of the request or task
//! that called it (as `ScopeHandle::spawn_local` does), feeds every `ms`
//! milliseconds.
//!
//! - A tick is the tick's scheduled time in milliseconds since the
//!   ticker started: `ms`, `2 * ms`, ... (the tick count is the value
//!   divided by `ms`).
//! - Only the newest tick is kept: an unread tick is replaced, not
//!   queued.  Ticks that a busy VM missed are skipped
//!   (`tokio::time::MissedTickBehavior::Skip`).
//! - `tk:stop()` stops the host task and closes the channel; a tick
//!   already in it can still be received.  The ticker also stops, and
//!   its channel closes, when the scope that created it ends or is
//!   cancelled (within the grace), when Lua closes the channel, and when
//!   the ticker object is collected.
//! - Outside a coroutine request or task (a sync request) `task.ticker`
//!   raises.
//!
//! **Send cases** (`ch:on_send(v, f)` / `ch:arm_send(v)`, local channels
//! and channels to the host)
//!
//! - Ready when `v` can be pushed (room in a buffered channel; for a
//!   rendezvous channel, once a receiver has taken the offer; for a
//!   channel to the host, once room is reserved) or when the channel is
//!   closed (`sent = false`).  The value enters the channel only for the
//!   chosen case.
//!
//! **Task cases** (`h:on(f)` / `h:arm()`, a `task.spawn` handle)
//!
//! - Ready when the task has finished.  The case returns what
//!   `h:join()` would (`true, ...`, `false, err` with the raw error
//!   value, or `false, task.CANCELLED`) and marks the handle joined.
//! - A case that is not chosen leaves the handle unjoined: it can be
//!   joined, or used in another select, later.
//! - Building a case from a joined handle raises, and so does a select
//!   given a case whose handle was joined since.
//! - A handle that is joined elsewhere (`h:join()`, or another select's
//!   case) while a select waits on its case: the join takes the result,
//!   and the waiting select raises "task already joined" once the task
//!   has finished.
//!
//! **select**
//!
//! - Exactly one ready case is chosen.  Cases that are not chosen
//!   consume nothing.
//! - Order: round robin per VM by default (each select starts one
//!   position after the previous one; deterministic, so tests are
//!   reproducible).  `biased = true` checks the cases in the given
//!   order.
//! - `default`: if no case is ready on the first check, `select` runs
//!   `default` (`select_raw` returns `0`) without waiting.
//! - A closed, empty channel is ready: its case is chosen with
//!   `ok = false`.  It stays ready, so a loop that keeps a closed
//!   channel in its case list keeps selecting it.
//! - An empty case list without `default` raises.
//! - A handler runs as Lua code inside the select and may await (and
//!   select again); an error it raises is re-raised by `select` with
//!   its raw value.  While it runs, the other cases are not watched:
//!   move long work into `task.spawn`.  [`Config::preempt_every`] does
//!   not preempt a handler either: it runs in a coroutine that the
//!   select's host call creates, not in a root or task (#22).
//!
//! **Delivery and cancellation**.  The cancel hook can raise at any
//! instruction count check, including the instructions between a host
//! function returning a value and the Lua code that uses it.  The
//! library defines when a value is *delivered*:
//!
//! - A wait (`recv`, `send`, `t:wait()`, `select`, `select_raw`) that is
//!   cancelled before it is ready returns the cancel error and has
//!   consumed nothing (a cancelled `send` sent nothing).  The wait is
//!   wrapped in `cancellable`, which checks the token first, so a poll
//!   that returns the cancel never also polled the cases.
//! - `recv` and `select_raw`: a value is delivered when the call returns
//!   it.  A cancel raised after that, in the caller's code, is the
//!   caller's to handle, as for any other value it holds.
//! - A send (`send`, a send case) is delivered when its value is in the
//!   channel (buffered) or taken by a receiver (rendezvous), and it
//!   cannot be taken back.  A rendezvous `send` whose offer was taken
//!   before the cancel was seen returns normally (sent), and a select
//!   whose send case's offer was taken chooses that case, rather than
//!   returning the cancel; a cancel seen before the offer was taken
//!   withdraws it (nothing sent).  An error raised after a send case was
//!   delivered and before its handler is entered is re-raised; the value
//!   stays sent.
//! - A task case is delivered like a received value: if the handler is
//!   never entered, the handle is left unjoined.
//! - `select`: a value is delivered when the chosen handler is entered
//!   with it.  `select` takes the value and calls the handler inside the
//!   same host call, so a cancel arrives either before the take
//!   (nothing consumed) or inside the handler (the handler has the
//!   value).  An error raised after the take but before the handler is
//!   entered puts the value back at the front of its channel.
//! - Between a host function's future becoming ready and the call
//!   returning, mlua runs a few instructions of Lua code of its own (the
//!   loop that polls the future).  The cancel hook does not raise there:
//!   it raises at the next check outside that code (or the next
//!   `cancellable` wait returns the cancel), so a value `recv` or
//!   `select_raw` took reaches the caller, and a `send` that pushed its
//!   value returns normally.  This holds for every async host function,
//!   not only the library's.  A program that keeps landing its checks in
//!   that code (an async host function that is always ready and not
//!   `cancellable`, called in a tight loop) is still cancelled: the hook
//!   defers at most 16 checks before it raises anyway.  The code is
//!   recognised by its chunk name, which mlua keeps from 0.12.2 on (the
//!   crate requires it).
//!
//! A task waiting in a select (or any of these waits) is cancelled with
//! its scope and ends within the grace like any other wait.
//!
//! # Host channels and requests
//!
//! `channel` (`tokio` feature) creates a channel that `Send` host
//! code feeds into a running Lua loop: a `Sender` (`Send + Clone`)
//! and its Lua side, a `LuaChannel` (the `task` library's `Channel`
//! object).  A channel of `Request`s carries values that Lua answers:
//!
//! ```text
//! let (tx, events) = channel::<Event>(&lua, 1024)?;      // on the VM thread
//! lua.globals().set("events", events)?;
//! tx.send(ev).await?;                                     // any thread or task
//!
//! let (req_tx, requests) = channel::<Request<Call, Answer>>(&lua, 256)?;
//! lua.globals().set("requests", requests)?;
//! let answer = tokio::time::timeout(limit, req_tx.request(call)).await??;
//! ```
//!
//! ```lua
//! task.select({
//!   events:on(function(ev, ok) ... end),
//!   requests:on(function(req, ok)
//!     if ok then req:reply(answer_for(req.value)) end
//!   end),
//! })
//! ```
//!
//! Call `channel` on the VM thread, after [`Vm::attach`] and
//! `Vm::task_lib`; for an `AsyncIsle`, in an
//! `exec` request that returns the `Sender` (example on `channel`).
//!
//! **Channel**
//!
//! - `cap >= 1` (a tokio bounded channel; `cap = 0` is an error: host
//!   channels have no rendezvous form).  `ch:cap()` is `cap`; `ch:len()` counts the values
//!   queued by the host plus any put back (below).
//! - On the Lua side it behaves as a local channel: FIFO, any number of
//!   Lua receivers, the same `recv` / `try_recv` / `close` / `closed` /
//!   `len` / `cap` and the same `select` contract.  `send` / `try_send`
//!   raise ("channel is receive-only").
//! - A value is converted to a Lua value (`T: IntoLua`) when Lua takes
//!   it, on the VM thread.  A conversion error is raised to the
//!   receiver (`recv`, `try_recv`, `select`, `select_raw`), and that
//!   value is dropped; the next receive takes the next value.
//! - Closed when every `Sender` is dropped (receivers get the queued
//!   values first, then `nil, false`), or when Lua calls `close`: the
//!   host's sends then fail with the value given back (`SendError`,
//!   `TrySendError::Closed`) and the values already queued can still
//!   be received.  When the Lua side is collected (or the VM dropped),
//!   the host's sends fail as closed.  `Sender::is_closed` reports
//!   both.
//! - Ordering between several `Sender`s is the order in which their
//!   sends complete.
//! - `Sender::send` waits while the channel is full;
//!   `Sender::try_send` returns `TrySendError::Full`.  There is no
//!   `send_timeout`: wrap `send` (or `request`) in
//!   `tokio::time::timeout`; a send that times out sent nothing.
//! - Several Lua receivers: every value is received exactly once, and no
//!   receiver is left asleep while a value is queued, including when a
//!   woken receiver's `select` chooses another case or the receiver is
//!   cancelled.  (A tokio `Receiver` wakes only the task that polled it
//!   last; the channel polls it with a waker that wakes every waiting
//!   receiver.)
//!
//! **Request**
//!
//! - A `Request<Req, Resp>` carries a `Req` and a one-shot
//!   reply.  `Sender::request` sends it and waits for the reply:
//!   `RequestError::Closed` (with the `Req`) if the channel is closed,
//!   `RequestError::NoReply` if the request is closed or collected
//!   without a reply (or the `Req` failed to convert to a Lua value).
//! - On the Lua side it is a userdata: `req.value` is the `Req`
//!   (converted with `IntoLua` when Lua received it); `req:reply(v)`
//!   converts `v` with `Resp: FromLua` and returns `true`, or `false`
//!   when the requester stopped waiting (its future was dropped, e.g. by
//!   a timeout; not an error).  A second `reply` raises, and so does a
//!   `reply` after the request was closed.  A conversion error raises
//!   and leaves the request unanswered: it can be answered again.
//!   `req:replied()` is whether a `reply` succeeded.
//! - Closing an unanswered request (`local req <close> = ...`, at scope
//!   exit or on an error) answers `NoReply` at once.  A request that is
//!   neither answered nor closed is reported `NoReply` only when Lua
//!   collects it, so a requester should use a timeout, and Lua code that
//!   may fail between receiving and replying should hold the request in
//!   a `<close>` variable.
//! - `select`'s handler form does not close a request its handler
//!   returns without answering: the handler may hand it to a task that
//!   replies later.  Close it in the handler (or let it be collected).
//!
//! **Delivery and cancellation** are as for local channels (above): a
//! wait cancelled before it is ready consumed nothing, and a value
//! `select` took for a handler that was never entered goes back to the
//! front of the channel, ahead of the values the host queued (the Lua
//! side keeps a front buffer for it; the tokio channel itself cannot
//! take a value back).
//!
//! # Channels to the host
//!
//! `channel_to_host` (`tokio` feature) creates a channel that a running
//! Lua loop feeds and `Send` host code drains: its Lua side, a
//! `LuaChannel` (the `task` library's `Channel` object, send-only), and
//! a `Receiver` (`Send`, not `Clone`) for the host:
//!
//! ```text
//! let (reports, mut rx) = channel_to_host::<Report>(&lua, 64)?;   // on the VM thread
//! lua.globals().set("reports", reports)?;
//! while let Some(r) = rx.recv().await { ... }                     // any thread or task
//! rx.try_recv();   // Ok(v), Err(TryRecvError::Empty), Err(TryRecvError::Closed)
//! rx.close();      // Lua's sends raise from now on; queued values can still be received
//! ```
//!
//! ```lua
//! reports:send(r)                    -- waits while full
//! local ok = reports:try_send(r)     -- false when full
//! task.select({ reports:on_send(r, function(sent) ... end) })
//! reports:close()                    -- the host receives the queued values, then None
//! ```
//!
//! Call `channel_to_host` on the VM thread, after [`Vm::attach`] and
//! `Vm::task_lib`; for an `AsyncIsle`, in the init closure of
//! `AsyncIsleBuilder::spawn_with`, which returns the `Receiver` (example
//! on `channel_to_host`), or in an `exec` request that returns it.
//!
//! - `cap >= 1` (a tokio bounded channel; `cap = 0` is an error).  One
//!   host receiver; any number of Lua senders (tasks, selects).
//!   `ch:cap()` is `cap`; `ch:len()` is the number of values queued for
//!   the host (`0` once Lua closed the channel).
//! - Each Lua send first reserves room in the tokio channel (tokio's
//!   `Sender::reserve_owned`; every waiting Lua sender has its own place
//!   in tokio's queue, in the order they started waiting).  Then the
//!   value is converted to `T` (`T: FromLua`) on the VM thread and
//!   queued, in the same step.  A value that fails to convert raises in
//!   the sender (`send`, `try_send`, or the select with the chosen
//!   `on_send` case) and is not queued; its room is given back.
//! - `send` waits while the channel is full; `try_send` returns `false`
//!   when full.  Both raise "channel is closed" when the channel is
//!   closed: by `rx.close()`, by dropping `rx`, or by Lua's `close`.
//!   Waiting senders wake and raise at the close.
//! - A send case (`on_send` / `arm_send`) is ready when room is
//!   reserved, or when the channel is closed (`sent = false`); it
//!   converts and queues the value only when it is chosen.  With
//!   `default`, it is ready only when there is room at the first check
//!   (`try_reserve`).
//! - Lua's `close`: the host receives the values already queued, then
//!   `recv` returns `None` (`try_recv` `Closed`).  When the Lua side is
//!   collected (or the VM dropped), the same.  `rx.close()` keeps the
//!   queued values receivable.
//! - Delivery: a send is delivered when its value is in the tokio
//!   queue.  A send cancelled before that (a cancelled `send`, a select
//!   that chose another case or was cancelled) consumed nothing: its
//!   reservation, if it had one, is released, and the room goes to the
//!   next waiting sender.
//! - `recv`, `try_recv`, `on`, `arm_recv` raise ("channel is
//!   send-only"), and so does a select given a receive case built by
//!   hand on such a channel.
//! - Ordering between several Lua senders is the order in which their
//!   sends complete; each value arrives exactly once.
//!
//! # Host tasks
//!
//! A host function that starts work of its own takes the scope of the
//! running request or task with `current_scope()` and spawns into it
//! with `ScopeHandle::spawn_local`.  Such a task is structured like a
//! Lua task: it is cancelled when the request or task ends or is
//! cancelled, gets the grace (one deadline for the whole tree), is
//! dropped when the grace ends, and is waited for.  Take the handle in
//! the synchronous part of the host function (the `create_function`
//! body, or a `create_async_function` body before its first `.await`)
//! and move it into the future; `current_scope()` is `Some` only while
//! a coroutine request or task is being polled, so it is `None` in a
//! sync request.  `spawn_local` returns a `ScopedTask`; there are three
//! ways to let go of it: await it (wait for the value), keep it for as
//! long as the task should run (dropping it cancels the task now), or
//! `detach()` it (fire and forget: the task runs on without a handle and
//! is still cancelled, given the grace, dropped and waited for when the
//! scope ends).
//!
//! ```text
//! let bg = lua.create_function(|_, ()| {
//!     let scope = current_scope().expect("inside a coroutine request");
//!     scope.spawn_local(async move { /* host work */ }).detach();
//!     Ok(())
//! })?;
//! ```

use crate::hub;
use mlua::debug::Debug;
use mlua::{HookTriggers, Lua, VmState};
use std::cell::RefCell;
use std::fmt;
use std::time::Duration;

pub use crate::error::{Cancelled, IsleError, LuaErrorKind, LuaFailure};
pub use crate::hook::{current_token, CancelToken};
#[cfg(feature = "tokio")]
pub use crate::host_chan::{
    channel, channel_to_host, LuaChannel, Receiver, Request, RequestError, SendError, Sender,
    TryRecvError, TrySendError,
};
#[cfg(feature = "tokio")]
pub use crate::scope::{cancellable, current_scope, ScopeHandle, ScopedTask};

/// Settings of a VM.  One per VM, read and written through [`Vm`]
/// ([`Vm::attach`], [`Vm::config`], [`Vm::set_config`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Config {
    /// How long a cancelled coroutine request or task may keep running
    /// to finish its cleanup before it is dropped.
    ///
    /// On cancel, the coroutine first receives the cancellation as a Lua
    /// error (from the cancel hook while it runs, or from an async
    /// function wrapped with `cancellable` while it awaits).  That error
    /// unwinds normally, so `__close` handlers run and may await.  If
    /// the coroutine has not finished when the grace period ends, it is
    /// dropped: the awaited Rust future is released, and pending
    /// `__close` handlers run without being able to yield (a Lua 5.4
    /// restriction).
    ///
    /// The grace period is one deadline for the cancelled request or
    /// task and every task it spawned, transitively: a task started
    /// during cleanup (from a `__close` handler, say) gets the time that
    /// remains, not a fresh grace period.
    ///
    /// Default: zero (drop at once).
    pub grace: Duration,
    /// Yield the running coroutine request or task every this many
    /// cancel checks (a check runs every 1000 instructions), so that
    /// other tasks on the same thread, including one that cancels it,
    /// get to run while it is in a CPU loop.
    ///
    /// Only the coroutine that the crate created for the request or task
    /// is yielded, never a coroutine the Lua code created itself (that
    /// yield would reach the Lua code's own `coroutine.resume` as a
    /// spurious yield).  Code in a non-yieldable context (a metamethod,
    /// a C function boundary) is not yielded.
    ///
    /// Yielding lets other tasks interleave at points the Lua code did
    /// not mark, so Lua code that updates shared state across such a
    /// point can observe changes made by another task.
    ///
    /// Default: `None` (never preempt).
    pub preempt_every: Option<u32>,
}

/// Handle of a callback registered with [`Vm::add_hook`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HookId(pub(crate) u64);

/// Marks a VM as attached; holds the per-VM state that is not in the
/// hook hub.
#[derive(Default)]
struct Attached {
    /// The `task` table, created on the first [`Vm::task_lib`] call, so
    /// that a VM that never asks for it (an actor whose init closure
    /// does not) runs no extra Lua.  Kept in the registry, not as a
    /// global.
    #[cfg(feature = "tokio")]
    task: std::cell::OnceCell<TaskLib>,
}

/// The registry keys of the `task` table and of the library's channel
/// constructor (used by [`channel`]).
#[cfg(feature = "tokio")]
struct TaskLib {
    table: mlua::RegistryKey,
    wrap_channel: mlua::RegistryKey,
}

/// The in-thread handle of a Lua VM run by this crate.
///
/// A `Vm` is a clone of the [`Lua`] handle; its state lives in the VM,
/// so every `Vm` of the same VM (from [`Vm::attach`] or [`Vm::of`]) sees
/// the same state.  It holds the VM strongly: do not store it in
/// something the VM owns (a Lua function's captured state, app data),
/// or the VM is never freed.
#[derive(Clone)]
pub struct Vm {
    lua: Lua,
}

impl fmt::Debug for Vm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Vm")
            .field("config", &self.config())
            .finish_non_exhaustive()
    }
}

impl Vm {
    /// Install the hook and store `config`.
    ///
    /// Calling it again on the same VM re-installs the hook, replaces
    /// the config (last wins) and returns a `Vm` for the same state; the
    /// task table, if `Vm::task_lib` created it, is kept.  Runs no Lua
    /// code.
    ///
    /// Hook callbacks registered before (with [`Vm::add_hook`]) are
    /// kept.
    ///
    /// The first `attach` also captures the `xpcall` global, which
    /// [`Vm::run`] uses to bring a raised Lua value back as a value (see
    /// [`LuaFailure`]).  **Attach before sandboxing the globals**
    /// (removing `xpcall`, or [`Lua::set_globals`] with a whitelist):
    /// after that the capture is kept, and later changes to the globals
    /// or a re-attach do not affect it.
    ///
    /// **The coroutine path needs mlua's default
    /// `LuaOptions::catch_rust_panics = true`.**  With `false`, mlua
    /// replaces the global `xpcall` with a version that is not
    /// yieldable, so every yield in a root run by [`Vm::run`] (an async
    /// host function, `task.join`, preemption) fails with "attempt to
    /// yield across a C-call boundary".  [`Lua::new`] uses the default.
    ///
    /// # Errors
    ///
    /// [`IsleError::Init`] with [`LuaErrorKind::External`] when the VM's
    /// `xpcall` global is not a function (it was sandboxed away before
    /// the first attach).
    pub fn attach(lua: &Lua, config: Config) -> Result<Vm, IsleError> {
        crate::protect::install(lua)?;
        hub::install(lua)?;
        hub::set_config(lua, config);
        let attached = lua.app_data_ref::<Attached>().is_some();
        if !attached {
            lua.set_app_data(Attached::default());
        }
        Ok(Vm { lua: lua.clone() })
    }

    /// The `Vm` of `lua`, if [`Vm::attach`] was called on it.
    pub fn of(lua: &Lua) -> Option<Vm> {
        let attached = lua.app_data_ref::<Attached>().is_some();
        attached.then(|| Vm { lua: lua.clone() })
    }

    /// The VM's settings.
    pub fn config(&self) -> Config {
        hub::config(&self.lua)
    }

    /// Replace the VM's settings.  Takes effect for requests and tasks
    /// that start afterwards (and, for `preempt_every`, at once).
    pub fn set_config(&self, config: Config) {
        hub::set_config(&self.lua, config);
    }

    /// Register a hook callback, run from the VM's hook after the cancel
    /// check at `triggers` (see [Hooks](self#hooks)).
    ///
    /// Instruction counts are approximated to the hook's step (the
    /// smallest count among all registrations and the cancel check).
    /// Returning [`VmState::Yield`] yields the running coroutine,
    /// including a coroutine the Lua code created itself, where the
    /// yield reaches the Lua code's `coroutine.resume`.
    ///
    /// The callback applies to the main thread and to coroutines created
    /// afterwards; coroutines that already exist keep the instruction
    /// count and events they were created with.  It is kept across a
    /// second [`Vm::attach`].
    ///
    /// The callback is not re-entered.  A callback that runs Lua code
    /// can be hooked again from inside itself: resuming a coroutine is
    /// the usual case, because the new thread has hooks enabled while
    /// the hooked thread does not.  That inner call fails with
    /// [`mlua::Error::RecursiveMutCallback`].
    ///
    /// Known limit: a request's Lua error goes through the crate's
    /// message handler (a C function under `xpcall`).  Count and line
    /// callbacks do not fire inside it, but a callback with `on_calls` /
    /// `on_returns` fires for the handler's own call and return.  If
    /// that callback returns `Err`, its error replaces the original Lua
    /// error (the request fails with "error in error handling" or the
    /// callback's error).  Do not fail from call / return callbacks if
    /// the original error matters.
    pub fn add_hook<F>(&self, triggers: HookTriggers, f: F) -> Result<HookId, IsleError>
    where
        F: FnMut(&Lua, &Debug) -> mlua::Result<VmState> + 'static,
    {
        let f = RefCell::new(f);
        hub::add_hook(&self.lua, triggers, move |lua, debug| {
            let mut f = f
                .try_borrow_mut()
                .map_err(|_| mlua::Error::RecursiveMutCallback)?;
            f(lua, debug)
        })
    }

    /// Remove a callback registered with [`Vm::add_hook`].  Returns
    /// whether it was registered.
    pub fn remove_hook(&self, id: HookId) -> Result<bool, IsleError> {
        hub::remove_hook(&self.lua, id)
    }

    /// The `task` library table (see [The `task` library](self#the-task-library)).
    ///
    /// It is not set as a global: the host decides where it lives,
    /// e.g. `lua.globals().set("task", vm.task_lib()?)`.  The table is
    /// created on the first call (this runs the library's Lua chunk);
    /// every later call, through any `Vm` of the same VM, returns the
    /// same table.
    ///
    /// In an actor's init closure the VM is not attached yet: attach it
    /// there (the actor re-attaches after the closure and keeps the
    /// config and the table).  `?` converts the [`IsleError`] into the
    /// closure's `mlua::Error`.
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use mlua_isle::runtime::{Config, Vm};
    /// use mlua_isle::AsyncIsle;
    ///
    /// let (isle, driver) = AsyncIsle::spawn(|lua| {
    ///     let vm = Vm::attach(lua, Config::default())?;
    ///     lua.globals().set("task", vm.task_lib()?)
    /// })
    /// .await?;
    /// let r: i64 = isle
    ///     .coroutine_eval(
    ///         "local h = task.spawn(function(a, b) return a + b end, 1, 2)
    ///          local ok, sum = h:join()
    ///          return sum",
    ///     )
    ///     .await?;
    /// assert_eq!(r, 3);
    /// driver.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Fails only when creating the table fails (e.g. the VM's memory
    /// limit is reached).
    #[cfg(feature = "tokio")]
    pub fn task_lib(&self) -> Result<mlua::Table, IsleError> {
        if let Some(t) = self.cached_task_lib()? {
            return Ok(t);
        }
        // Created without holding the app data borrow: the chunk runs
        // under the hook, whose callbacks may touch app data.
        let (table, wrap_channel) = crate::task_lib::create(&self.lua)?;
        let lib = TaskLib {
            table: self.lua.create_registry_value(table)?,
            wrap_channel: self.lua.create_registry_value(wrap_channel)?,
        };
        {
            let a = self.attached();
            // A table stored meanwhile wins; `lib` is then dropped.
            let _ = a.task.set(lib);
        }
        Ok(self
            .cached_task_lib()?
            .expect("the task table was just stored"))
    }

    #[cfg(feature = "tokio")]
    fn cached_task_lib(&self) -> Result<Option<mlua::Table>, IsleError> {
        let a = self.attached();
        match a.task.get() {
            Some(lib) => Ok(Some(self.lua.registry_value(&lib.table)?)),
            None => Ok(None),
        }
    }

    /// The `task` library's channel constructor, if `Vm::task_lib`
    /// created the library.
    #[cfg(feature = "tokio")]
    pub(crate) fn channel_ctor(&self) -> Result<Option<mlua::Function>, IsleError> {
        let a = self.attached();
        match a.task.get() {
            Some(lib) => Ok(Some(self.lua.registry_value(&lib.wrap_channel)?)),
            None => Ok(None),
        }
    }

    #[cfg(feature = "tokio")]
    fn attached(&self) -> mlua::AppDataRef<'_, Attached> {
        self.lua
            .app_data_ref::<Attached>()
            .expect("a Vm exists only for an attached VM")
    }

    /// Run `f(args)` as a root coroutine under `token`.
    ///
    /// Resolves only after everything the root started has ended: the
    /// Lua tasks it spawned and the host tasks spawned through
    /// [`current_scope`], transitively (contract 1 of the
    /// [module docs](self)).  Resolves to `Err(IsleError::Cancelled)` if
    /// `token` was cancelled, whatever the coroutine returned; the
    /// coroutine and its tasks then get the VM's [`Config::grace`], as
    /// one deadline, before they are dropped.  The cancellation error
    /// raised by some other token (a host function that returned
    /// `Err(mlua::Error::external(Cancelled))`) also resolves to
    /// `Err(IsleError::Cancelled)`.
    ///
    /// A Lua error resolves to `Err(IsleError::Lua(f))`, where the
    /// [`LuaFailure`] is built from the raised value itself on this
    /// thread: `f.message` is `tostring(err)` and, with the `serde`
    /// feature, `f.value` is the value as JSON (so `error({ code = 42 })`
    /// gives `f.value["code"] == 42`).
    ///
    /// A task in a CPU loop cannot be dropped until it yields; without
    /// [`Config::preempt_every`] the wait blocks on it.
    ///
    /// Await it inside a [`tokio::task::LocalSet`].
    ///
    /// # Dropping the future
    ///
    /// Dropping the returned future before it resolves (wrapping it in
    /// [`tokio::time::timeout`], or a losing [`tokio::select!`] arm)
    /// drops the coroutine at once but only schedules the tasks it
    /// spawned for abort: tokio drops them on a later poll of the
    /// `LocalSet`, as with
    /// [`AbortHandle::abort`](tokio::task::AbortHandle::abort).  To have
    /// the tasks gone before you continue, [cancel](CancelToken::cancel)
    /// the token and await the future instead of dropping it.
    ///
    /// ```rust
    /// use mlua_isle::runtime::{CancelToken, Config, Vm};
    /// use std::time::Duration;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// let local = tokio::task::LocalSet::new();
    /// let lua = mlua::Lua::new();
    ///
    /// let vm = Vm::attach(&lua, Config { grace: Duration::from_secs(1), ..Default::default() })?;
    /// lua.globals().set("task", vm.task_lib()?)?;
    /// let main: mlua::Function = lua
    ///     .load("return function(x) local _, v = task.spawn(function() return x * 2 end):join() return v end")
    ///     .eval()?;
    ///
    /// let token = CancelToken::new();
    /// let out = local.block_on(&rt, vm.run(&token, main, 21))?;
    /// assert_eq!(out[0].as_i64(), Some(42));
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "tokio")]
    pub async fn run(
        &self,
        token: &CancelToken,
        f: mlua::Function,
        args: impl mlua::IntoLuaMulti,
    ) -> Result<mlua::MultiValue, IsleError> {
        let args = args.into_lua_multi(&self.lua)?;
        crate::scope::run_root(&self.lua, token.clone(), f, args).await
    }
}

/// Attach an actor's VM after its init closure ran.
///
/// With `config`, it replaces whatever the init closure configured;
/// without, the VM keeps the init closure's settings (the default if it
/// set none).  The hook is (re-)installed either way, so a hook the init
/// closure set with `Lua::set_hook` is replaced, as before.
pub(crate) fn attach_after_init(lua: &Lua, config: Option<Config>) -> Result<Vm, IsleError> {
    let config = config.unwrap_or_else(|| hub::config(lua));
    Vm::attach(lua, config)
}

/// Make sure the VM is attached and its hook is in place, before a
/// request runs.  Re-installs the hook if `Lua::set_hook` replaced it;
/// keeps the stored config.
pub(crate) fn ensure_attached(lua: &Lua) -> Result<(), IsleError> {
    let attached = lua.app_data_ref::<Attached>().is_some();
    if attached {
        hub::ensure_installed(lua)
    } else {
        // Unreachable for the actors (they attach before reporting a
        // successful spawn); kept as a defensive path.
        attach_after_init(lua, None).map(drop)
    }
}

/// The [`Vm`] of `lua`, attaching it with its stored config (the
/// default if none was set) when it is not attached yet.
#[cfg(feature = "tokio")]
pub(crate) fn of_or_attach(lua: &Lua) -> Result<Vm, IsleError> {
    match Vm::of(lua) {
        Some(vm) => Ok(vm),
        None => attach_after_init(lua, None),
    }
}
