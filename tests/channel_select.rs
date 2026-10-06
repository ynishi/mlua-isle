#![cfg(feature = "tokio")]
//! Local channels, timers and `select` in the `task` library, run
//! under `Vm::run`.

use mlua_isle::runtime::{cancellable, CancelToken, Config, Vm};
use mlua_isle::IsleError;
use std::time::{Duration, Instant};

/// A VM on a current-thread runtime, with `task`, `sleep(ms)`
/// (cancellable), `now_ms()` and `call_in_host(f, ...)`, an async host
/// function that calls `f(...)` with `Function::call_async` and returns
/// its results.
struct Env {
    rt: tokio::runtime::Runtime,
    local: tokio::task::LocalSet,
    lua: mlua::Lua,
    vm: Vm,
}

fn env(config: Config) -> Env {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let lua = mlua::Lua::new();
    let vm = Vm::attach(&lua, config).unwrap();
    let g = lua.globals();
    g.set("task", vm.task_lib().unwrap()).unwrap();
    let sleep = lua
        .create_async_function(|_, ms: u64| {
            cancellable(async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            })
        })
        .unwrap();
    g.set("sleep", sleep).unwrap();
    let call_in_host = lua
        .create_async_function(
            |_, (f, args): (mlua::Function, mlua::Variadic<mlua::Value>)| async move {
                f.call_async::<mlua::MultiValue>(mlua::MultiValue::from_iter(args))
                    .await
            },
        )
        .unwrap();
    g.set("call_in_host", call_in_host).unwrap();
    let t0 = Instant::now();
    let now_ms = lua
        .create_function(move |_, ()| Ok(t0.elapsed().as_secs_f64() * 1000.0))
        .unwrap();
    g.set("now_ms", now_ms).unwrap();
    Env { rt, local, lua, vm }
}

impl Env {
    /// Run `src` (a chunk) as the root under `token`, within `limit`.
    fn run_with(
        &self,
        token: &CancelToken,
        limit: Duration,
        src: &str,
    ) -> Result<mlua::MultiValue, IsleError> {
        let f: mlua::Function = self.lua.load(src).into_function().unwrap();
        self.local.block_on(&self.rt, async {
            tokio::time::timeout(limit, self.vm.run(token, f, ()))
                .await
                .expect("timed out")
        })
    }

    fn run(&self, src: &str) -> Result<mlua::MultiValue, IsleError> {
        self.run_with(&CancelToken::new(), Duration::from_secs(5), src)
    }

    fn string(&self, src: &str) -> String {
        let out = self.run(src).unwrap_or_else(|e| panic!("run failed: {e}"));
        match out.front() {
            Some(mlua::Value::String(s)) => s.to_str().unwrap().to_string(),
            other => panic!("expected a string, got {other:?}"),
        }
    }

    fn global<T: mlua::FromLua>(&self, name: &str) -> T {
        self.lua.globals().get(name).unwrap()
    }
}

/// Cancel `token` from another OS thread after `ms` (works while the
/// VM thread is in a CPU loop).
fn cancel_after(token: &CancelToken, ms: u64) {
    let t = token.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(ms));
        t.cancel();
    });
}

const GRACE: Config = Config {
    grace: Duration::from_millis(300),
    preempt_every: None,
};

// ── a Lua function called from an async host function ──
//
// `task.select` calls the chosen handler with `Function::call_async`
// inside its own host call.  These tests check that such a call runs
// under the caller's scope and token.

#[test]
fn a_function_called_from_a_host_function_can_spawn_and_join() {
    let e = env(GRACE);
    let r = e.string(
        "return call_in_host(function(x)
           local ok, v = task.spawn(function() sleep(5) return x * 2 end):join()
           return tostring(ok) .. ' ' .. v
         end, 21)",
    );
    assert_eq!(r, "true 42");
}

#[test]
fn a_cancel_reaches_a_function_called_from_a_host_function_while_it_awaits() {
    let e = env(GRACE);
    e.lua.globals().set("seen", false).unwrap();
    let token = CancelToken::new();
    cancel_after(&token, 30);
    let start = Instant::now();
    let r = e.run_with(
        &token,
        Duration::from_secs(2),
        "call_in_host(function()
           local ok, err = pcall(sleep, 5000)
           seen = not ok and task.is_cancelled(err)
           error(err, 0)
         end)",
    );
    assert!(matches!(r, Err(IsleError::Cancelled)), "got {r:?}");
    assert!(
        e.global::<bool>("seen"),
        "the cancel did not reach the handler"
    );
    assert!(
        start.elapsed() < Duration::from_millis(300),
        "{:?}",
        start.elapsed()
    );
}

#[test]
fn a_cancel_reaches_a_function_called_from_a_host_function_in_a_cpu_loop() {
    let e = env(GRACE);
    e.lua.globals().set("seen", false).unwrap();
    let token = CancelToken::new();
    cancel_after(&token, 30);
    let start = Instant::now();
    let r = e.run_with(
        &token,
        Duration::from_secs(2),
        "call_in_host(function()
           local ok, err = pcall(function() while true do end end)
           seen = not ok and task.is_cancelled(err)
           error(err, 0)
         end)",
    );
    assert!(matches!(r, Err(IsleError::Cancelled)), "got {r:?}");
    assert!(
        e.global::<bool>("seen"),
        "the cancel did not reach the handler"
    );
    assert!(
        start.elapsed() < Duration::from_millis(300),
        "{:?}",
        start.elapsed()
    );
}

/// The error message of a failed run.
fn err_message(r: Result<mlua::MultiValue, IsleError>) -> String {
    match r {
        Err(IsleError::Lua(f)) => f.message,
        other => panic!("expected a Lua error, got {other:?}"),
    }
}

// ── 1. FIFO and capacity ──

#[test]
fn values_keep_their_order_across_senders_and_receivers() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(2)
         local senders = {}
         for s = 1, 3 do
           senders[s] = task.spawn(function()
             for i = 1, 5 do ch:send(s * 100 + i) end
           end)
         end
         local got = {}
         local receivers = {}
         for r = 1, 2 do
           receivers[r] = task.spawn(function()
             while true do
               local v, ok = ch:recv()
               if not ok then return end
               got[#got + 1] = v
               if v % 2 == 0 then sleep(1) end
             end
           end)
         end
         for _, h in ipairs(senders) do assert(h:join()) end
         ch:close()
         for _, h in ipairs(receivers) do assert(h:join()) end
         -- every value once, and each sender's values in its order
         local last = { 0, 0, 0 }
         for _, v in ipairs(got) do
           local s, i = v // 100, v % 100
           assert(i == last[s] + 1, 'out of order: ' .. v)
           last[s] = i
         end
         return #got .. ' ' .. table.concat(last, ',')",
    );
    assert_eq!(r, "15 5,5,5");
}

#[test]
fn waiting_receivers_are_served_in_arrival_order() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(3)
         local got = {}
         local hs = {}
         for r = 1, 3 do
           hs[r] = task.spawn(function() got[r] = ch:recv() end)
           sleep(2)
         end
         assert(ch:try_send('a') and ch:try_send('b') and ch:try_send('c'))
         for _, h in ipairs(hs) do h:join() end
         return table.concat(got, ',')",
    );
    assert_eq!(r, "a,b,c");
}

#[test]
fn try_send_returns_false_when_full() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(1)
         local a = ch:try_send(1)
         local b = ch:try_send(2)
         return tostring(a) .. ' ' .. tostring(b) .. ' ' .. ch:len() .. ' ' .. ch:cap()",
    );
    assert_eq!(r, "true false 1 1");
}

#[test]
fn send_waits_while_full_and_resumes_when_a_value_is_taken() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(1)
         ch:send(1)
         local sent = false
         local h = task.spawn(function() ch:send(2) sent = true end)
         sleep(20)
         local before = sent
         local a = ch:recv()
         h:join()
         local b = ch:recv()
         return tostring(before) .. ' ' .. tostring(sent) .. ' ' .. a .. ' ' .. b",
    );
    assert_eq!(r, "false true 1 2");
}

#[test]
fn try_recv_returns_value_ok_and_ready() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(2)
         local function show(v, ok, ready)
           return tostring(v) .. '/' .. tostring(ok) .. '/' .. tostring(ready)
         end
         local empty = show(ch:try_recv())
         ch:send('x')
         local value = show(ch:try_recv())
         ch:close()
         local closed = show(ch:try_recv())
         return empty .. ' ' .. value .. ' ' .. closed",
    );
    assert_eq!(r, "nil/false/false x/true/true nil/false/true");
}

#[test]
fn values_are_shared_not_copied_and_nil_is_a_value() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(2)
         local t = { n = 1 }
         ch:send(t)
         ch:send(nil)
         local got = ch:recv()
         got.n = 2
         local v, ok = ch:recv()
         return tostring(rawequal(got, t)) .. ' ' .. t.n .. ' ' .. tostring(v) .. ' ' .. tostring(ok)",
    );
    assert_eq!(r, "true 2 nil true");
}

#[test]
fn channel_capacity_must_be_at_least_one() {
    let e = env(GRACE);
    let m = err_message(e.run("task.channel(0)"));
    assert!(m.contains("cap = 0"), "got: {m}");
    let m = err_message(e.run("task.channel(-1)"));
    assert!(m.contains("cap must be an integer >= 1"), "got: {m}");
    let m = err_message(e.run("task.channel()"));
    assert!(m.contains("cap must be an integer >= 1"), "got: {m}");
}

// ── 2. close ──

#[test]
fn close_lets_queued_values_drain_then_reports_closed() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(3)
         ch:send(1) ch:send(2)
         ch:close()
         ch:close()
         local a, oka = ch:recv()
         local b, okb = ch:recv()
         local c, okc = ch:recv()
         local d, okd = ch:recv()
         local s1, e1 = pcall(ch.send, ch, 3)
         local s2, e2 = pcall(ch.try_send, ch, 3)
         return table.concat({
           tostring(ch:closed()), a, tostring(oka), b, tostring(okb),
           tostring(c), tostring(okc), tostring(d), tostring(okd),
           tostring(s1), tostring(e1):match('channel is closed') or tostring(e1),
           tostring(s2), tostring(e2):match('channel is closed') or tostring(e2),
         }, ' ')",
    );
    assert_eq!(
        r,
        "true 1 true 2 true nil false nil false false channel is closed false channel is closed"
    );
}

#[test]
fn close_wakes_waiting_receivers_and_senders() {
    let e = env(GRACE);
    let r = e.string(
        "local empty = task.channel(1)
         local full = task.channel(1)
         full:send(0)
         local r = task.spawn(function() return empty:recv() end)
         local s = task.spawn(function() full:send(1) end)
         sleep(10)
         empty:close()
         full:close()
         local rok, v, ok = r:join()
         local sok, err = s:join()
         return table.concat({
           tostring(rok), tostring(v), tostring(ok),
           tostring(sok), tostring(err):match('channel is closed') or tostring(err),
         }, ' ')",
    );
    assert_eq!(r, "true nil false false channel is closed");
}

// ── 3. select picks exactly one case ──

#[test]
fn select_raw_takes_from_one_case_and_leaves_the_others() {
    let e = env(GRACE);
    let r = e.string(
        "local a, b = task.channel(1), task.channel(1)
         a:send('A') b:send('B')
         local i, v, ok = task.select_raw({ a:arm_recv(), b:arm_recv() }, { biased = true })
         return i .. ' ' .. v .. ' ' .. tostring(ok) .. ' ' .. a:len() .. ' ' .. b:len()",
    );
    assert_eq!(r, "1 A true 0 1");
}

#[test]
fn select_runs_one_handler_and_returns_its_results() {
    let e = env(GRACE);
    let r = e.string(
        "local a, b = task.channel(1), task.channel(1)
         a:send('A') b:send('B')
         local calls = 0
         local x, y = task.select({
           a:on(function(v, ok) calls = calls + 1 return 'a:' .. v, ok end),
           b:on(function(v, ok) calls = calls + 1 return 'b:' .. v, ok end),
         }, { biased = true })
         return x .. ' ' .. tostring(y) .. ' ' .. calls .. ' ' .. a:len() .. ' ' .. b:len()",
    );
    assert_eq!(r, "a:A true 1 0 1");
}

#[test]
fn select_waits_for_the_first_case_that_becomes_ready() {
    let e = env(GRACE);
    let r = e.string(
        "local a, b = task.channel(1), task.channel(1)
         task.spawn(function() sleep(10) b:send('late') end)
         local i, v = task.select_raw({ a:arm_recv(), b:arm_recv() })
         return i .. ' ' .. v .. ' ' .. a:len()",
    );
    assert_eq!(r, "2 late 0");
}

#[test]
fn a_handler_error_keeps_its_raw_value() {
    let e = env(GRACE);
    let r = e.string(
        "local a = task.channel(1)
         a:send(1)
         local ok, err = pcall(task.select, { a:on(function() error({ code = 42 }) end) })
         return tostring(ok) .. ' ' .. type(err) .. ' ' .. err.code",
    );
    assert_eq!(r, "false table 42");
}

#[test]
fn a_handler_may_await_and_select_again() {
    let e = env(GRACE);
    let r = e.string(
        "local a, b = task.channel(1), task.channel(1)
         a:send('A')
         task.spawn(function() sleep(5) b:send('B') end)
         return task.select({
           a:on(function(v)
             sleep(1)
             return v .. task.select({ b:on(function(w) return w end) })
           end),
         })",
    );
    assert_eq!(r, "AB");
}

#[test]
fn malformed_cases_raise() {
    let e = env(GRACE);
    let m = err_message(e.run("task.select({ 1 })"));
    assert!(m.contains("task.select: case 1 is not a case"), "got: {m}");
    let m = err_message(e.run("local ch = task.channel(1) task.select({ ch:arm_recv() })"));
    assert!(m.contains("task.select: case 1 has no handler"), "got: {m}");
    let m = err_message(e.run("task.select_raw({ { kind = 'nope' } })"));
    assert!(m.contains("unknown kind 'nope'"), "got: {m}");
    let m = err_message(e.run("task.select_raw({ { kind = 'recv', target = 1 } })"));
    assert!(m.contains("target is not a channel"), "got: {m}");
    let m = err_message(e.run("local ch = task.channel(1) ch:on(1)"));
    assert!(m.contains("handler must be a function"), "got: {m}");
    let m = err_message(e.run(
        "local ch = task.channel(1) task.select_raw({ ch:arm_recv() }, { default = function() end })",
    ));
    assert!(m.contains("opts.default must be a boolean"), "got: {m}");
    let m = err_message(
        e.run("local ch = task.channel(1) task.select({ ch:on(print) }, { default = true })"),
    );
    assert!(m.contains("opts.default must be a function"), "got: {m}");
}

// ── 4. round robin and biased ──

#[test]
fn round_robin_alternates_between_ready_cases() {
    let e = env(GRACE);
    let r = e.string(
        "local a, b = task.channel(10), task.channel(10)
         for i = 1, 10 do a:send(i) b:send(i) end
         local picks = {}
         for n = 1, 6 do
           picks[n] = task.select_raw({ a:arm_recv(), b:arm_recv() })
         end
         local h = {}
         for n = 1, 6 do
           h[n] = task.select({ a:on(function() return 'a' end), b:on(function() return 'b' end) })
         end
         return table.concat(picks, '') .. ' ' .. table.concat(h, '')",
    );
    let (raw, handlers) = r.split_once(' ').unwrap();
    assert!(raw == "121212" || raw == "212121", "got {raw}");
    assert!(
        handlers == "ababab" || handlers == "bababa",
        "got {handlers}"
    );
}

#[test]
fn biased_always_checks_the_first_case_first() {
    let e = env(GRACE);
    let r = e.string(
        "local a, b = task.channel(10), task.channel(10)
         for i = 1, 10 do a:send(i) b:send(i) end
         local picks = {}
         for n = 1, 4 do
           picks[n] = task.select_raw({ a:arm_recv(), b:arm_recv() }, { biased = true })
         end
         for n = 5, 8 do
           picks[n] = task.select({
             a:on(function() return 1 end), b:on(function() return 2 end),
           }, { biased = true })
         end
         return table.concat(picks, '')",
    );
    assert_eq!(r, "11111111");
}

// ── 5. default ──

#[test]
fn default_runs_only_when_nothing_is_ready() {
    let e = env(GRACE);
    let r = e.string(
        "local a = task.channel(1)
         local i0 = task.select_raw({ a:arm_recv() }, { default = true })
         local d0 = task.select({ a:on(function() return 'case' end) },
                                { default = function() return 'default' end })
         a:send('x')
         local i1, v1 = task.select_raw({ a:arm_recv() }, { default = true })
         a:send('y')
         local d1 = task.select({ a:on(function(v) return 'case ' .. v end) },
                                { default = function() return 'default' end })
         return table.concat({ i0, d0, i1, v1, d1 }, ' ')",
    );
    assert_eq!(r, "0 default 1 x case y");
}

// ── 6. closed channel, empty case list ──

#[test]
fn a_closed_channel_case_is_chosen_with_ok_false() {
    let e = env(GRACE);
    let r = e.string(
        "local a, b = task.channel(1), task.channel(1)
         b:close()
         local i, v, ok = task.select_raw({ a:arm_recv(), b:arm_recv() })
         local h = task.select({
           a:on(function() return 'a' end),
           b:on(function(v, ok) return 'b ' .. tostring(v) .. ' ' .. tostring(ok) end),
         })
         -- it stays ready
         local again = task.select_raw({ b:arm_recv() })
         return table.concat({ i, tostring(v), tostring(ok), h, again }, ' ')",
    );
    assert_eq!(r, "2 nil false b nil false 1");
}

#[test]
fn an_empty_case_list_raises_without_default() {
    let e = env(GRACE);
    let m = err_message(e.run("task.select({})"));
    assert!(
        m.contains("task.select: no cases and no default"),
        "got: {m}"
    );
    let m = err_message(e.run("task.select_raw({})"));
    assert!(
        m.contains("task.select_raw: no cases and no default"),
        "got: {m}"
    );
    let r = e.string(
        "local i = task.select_raw({}, { default = true })
         return i .. ' ' .. task.select({}, { default = function() return 'd' end })",
    );
    assert_eq!(r, "0 d");
}

// ── 7. timers ──

#[test]
fn a_timer_case_is_chosen_after_its_delay() {
    let e = env(GRACE);
    let out = e
        .run(
            "local a = task.channel(1)
             local t0 = now_ms()
             local i = task.select_raw({ a:arm_recv(), task.after(50):arm() })
             local t1 = now_ms()
             local h = task.select({ a:on(function() return 'a' end),
                                     task.after(20):on(function() return 'timer' end) })
             return i, t1 - t0, h, now_ms() - t1",
        )
        .unwrap();
    let v: Vec<mlua::Value> = out.into_iter().collect();
    assert_eq!(v[0].as_i64(), Some(2));
    let ms = v[1].as_f64().unwrap();
    assert!((45.0..400.0).contains(&ms), "timer fired after {ms} ms");
    assert_eq!(v[2].as_string().unwrap().to_str().unwrap(), "timer");
    let ms = v[3].as_f64().unwrap();
    assert!((15.0..400.0).contains(&ms), "timer fired after {ms} ms");
}

#[test]
fn a_timer_with_ms_le_0_is_ready_at_once_and_stays_ready() {
    let e = env(GRACE);
    let r = e.string(
        "local z, n = task.after(0), task.after(-5)
         local a = task.select_raw({ z:arm() }, { default = true })
         local b = task.select_raw({ n:arm() }, { default = true })
         local t = task.after(10)
         local c = task.select_raw({ t:arm() }, { default = true })
         t:wait()
         t:wait()
         local d = task.select_raw({ t:arm() }, { default = true })
         local e = task.select({ t:on(function(...) return select('#', ...) end) })
         return table.concat({ a, b, c, d, e }, ' ')",
    );
    assert_eq!(r, "1 1 0 1 0");
}

// ── 8. a cancelled wait consumes nothing ──

#[test]
fn a_cancelled_select_consumes_nothing() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(1)
         local entered = false
         local waiters = {
           task.spawn(function()
             return task.select({ ch:on(function() entered = true end) })
           end),
           task.spawn(function() return task.select_raw({ ch:arm_recv() }) end),
           task.spawn(function() return ch:recv() end),
         }
         sleep(10)
         for _, h in ipairs(waiters) do h:cancel() end
         local res = {}
         for _, h in ipairs(waiters) do
           local ok, err = h:join()
           res[#res + 1] = tostring(ok) .. '/' .. tostring(err == task.CANCELLED)
         end
         ch:send('x')
         local v, ok = ch:recv()
         return table.concat(res, ' ') .. ' ' .. tostring(entered) .. ' ' .. v .. ' ' .. tostring(ok)",
    );
    assert_eq!(r, "false/true false/true false/true false x true");
}

#[test]
fn a_cancelled_send_does_not_send() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(1)
         ch:send(1)
         local h = task.spawn(function() ch:send(2) end)
         sleep(10)
         h:cancel()
         local ok, err = h:join()
         local a = ch:recv()
         local b, okb, ready = ch:try_recv()
         return table.concat({ tostring(ok), tostring(err == task.CANCELLED), a,
                               tostring(b), tostring(ready) }, ' ')",
    );
    assert_eq!(r, "false true 1 nil false");
}

#[test]
fn a_cancelled_timer_wait_returns_the_cancel() {
    let e = env(GRACE);
    let r = e.string(
        "local h = task.spawn(function() task.after(5000):wait() end)
         sleep(10)
         h:cancel()
         local ok, err = h:join()
         return tostring(ok) .. ' ' .. tostring(err == task.CANCELLED)",
    );
    assert_eq!(r, "false true");
}

// ── 9. delivery to a handler ──

#[test]
fn a_handler_cancelled_while_running_had_the_value() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(1)
         local seen = {}
         local closed = false
         local h = task.spawn(function()
           return task.select({
             ch:on(function(v, ok)
               seen[#seen + 1] = v
               local g <close> = setmetatable({}, { __close = function() closed = true end })
               sleep(5000)
             end),
           })
         end)
         sleep(5)
         ch:send('v1')
         sleep(10)
         h:cancel()
         local ok, err = h:join()
         return table.concat({ tostring(ok), tostring(err == task.CANCELLED),
                               table.concat(seen, ','), tostring(closed), ch:len() }, ' ')",
    );
    assert_eq!(r, "false true v1 true 0");
}

// ── 10. structure ──

#[test]
fn a_select_in_a_task_is_cancelled_with_its_parent_within_the_grace() {
    let e = env(GRACE);
    e.lua
        .load("closed_child = false closed_grandchild = false")
        .exec()
        .unwrap();
    let token = CancelToken::new();
    cancel_after(&token, 30);
    let start = Instant::now();
    let r = e.run_with(
        &token,
        Duration::from_secs(2),
        "local ch = task.channel(1)
         local function waiter(flag)
           return function()
             local g <close> = setmetatable({}, { __close = function() _G[flag] = true end })
             task.select({ ch:on(function() end), task.after(60000):on(function() end) })
           end
         end
         task.spawn(waiter('closed_child'))
         task.spawn(function()
           task.spawn(waiter('closed_grandchild'))
           ch:recv()
         end)
         sleep(60000)",
    );
    assert!(matches!(r, Err(IsleError::Cancelled)), "got {r:?}");
    assert!(
        start.elapsed() < Duration::from_millis(300),
        "{:?}",
        start.elapsed()
    );
    assert!(e.global::<bool>("closed_child"));
    assert!(e.global::<bool>("closed_grandchild"));
}

#[test]
fn a_select_in_a_task_ends_when_its_parent_task_is_cancelled() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(1)
         local closed = false
         local parent = task.spawn(function()
           task.spawn(function()
             local g <close> = setmetatable({}, { __close = function() closed = true end })
             task.select({ ch:on(function() end) })
           end)
           sleep(60000)
         end)
         sleep(10)
         parent:cancel()
         local ok, err = parent:join()
         return tostring(ok) .. ' ' .. tostring(err == task.CANCELLED) .. ' ' .. tostring(closed)",
    );
    assert_eq!(r, "false true true");
}

#[test]
fn a_channel_outlives_a_cancelled_task_and_is_not_closed_by_it() {
    let e = env(GRACE);
    let r = e.string(
        "local h = task.spawn(function()
           local ch = task.channel(1)
           chan = ch
           ch:send('kept')
           sleep(60000)
         end)
         sleep(5)
         h:cancel()
         h:join()
         local v, ok = chan:recv()
         return tostring(chan:closed()) .. ' ' .. v .. ' ' .. tostring(ok)",
    );
    assert_eq!(r, "false kept true");
}

/// An error raised after the take but before the handler is entered
/// (here from a hook callback on the call into `select`'s handler
/// wrapper; in practice the cancel hook) gives the value back to the
/// front of its channel.
#[test]
fn a_value_taken_for_a_handler_that_was_never_entered_goes_back() {
    let e = env(GRACE);
    let armed = std::rc::Rc::new(std::cell::Cell::new(false));
    let a = armed.clone();
    e.vm.add_hook(mlua::HookTriggers::ON_CALLS, move |_, debug| {
        let wrapper = debug.source().source.as_deref() == Some("=mlua_isle.select");
        if wrapper && a.replace(false) {
            return Err(mlua::Error::runtime("injected before the handler"));
        }
        Ok(mlua::VmState::Continue)
    })
    .unwrap();
    e.lua
        .globals()
        .set(
            "arm_failure",
            e.lua
                .create_function(move |_, ()| {
                    armed.set(true);
                    Ok(())
                })
                .unwrap(),
        )
        .unwrap();
    let r = e.string(
        "local ch = task.channel(2)
         ch:send('first') ch:send('second')
         local entered = 0
         arm_failure()
         local ok, err = pcall(task.select, { ch:on(function() entered = entered + 1 end) })
         local len = ch:len()
         local a = ch:recv()
         local b = ch:recv()
         return table.concat({ tostring(ok), tostring(err):match('injected before the handler') or tostring(err),
                               entered, len, a, b }, ' ')",
    );
    assert_eq!(r, "false injected before the handler 0 2 first second");
}

// ── the cancel hook and mlua's async poll chunk ──
//
// mlua runs every async host function through a Lua chunk of its own
// (source `=__mlua_async_poll`), which returns the future's results with a few
// instructions of its own.  The hook does not raise a cancel there; it
// raises it at the next check outside the chunk.
//
// To test this deterministically, the cancel check has to land on one
// of those instructions with the token already cancelled.  A hook
// callback registered with `every_nth_instruction(1)` makes the hook
// fire on every instruction, and the cancel check then runs on every
// 1000th of them (`hub.rs`: the check interval divided by the step).
// The callback counts the events, so its count equals the hub's.  A
// probe run finds the index of the chunk's last instruction (the one
// that returns to the caller); the real run inserts padding
// instructions (`x = x + 1`, one instruction each) before the call so
// that this instruction becomes a cancel check, cancels the token on
// the instruction before it, and records that the check really landed
// there with the token cancelled.  Both runs use a fresh VM and the
// same code, so the instruction indices match.

/// Whether `debug` is in mlua's async poll chunk (mlua >= 0.12.2 keeps
/// its name as the source).
fn in_async_poll(debug: &mlua::debug::Debug) -> bool {
    debug.source().source.as_deref() == Some("=__mlua_async_poll")
}

#[derive(Default)]
struct Probe {
    /// Count events since the callback was registered.
    n: std::cell::Cell<u64>,
    /// Set by `arm()` just before the call under test.
    armed: std::cell::Cell<bool>,
    /// Index of the latest event in the poll chunk after `arm()`.
    last_in_chunk: std::cell::Cell<u64>,
    /// Index of the chunk's last instruction (set once it is left).
    target: std::cell::Cell<u64>,
    /// Real run: cancel on this event.
    cancel_at: std::cell::Cell<u64>,
    cancel_was_in_chunk: std::cell::Cell<bool>,
    /// Real run: the check at `cancel_at + 1` ran in the chunk with the
    /// token cancelled and did not raise.
    deferred_in_chunk: std::cell::Cell<bool>,
}

/// Run `src` (with `--PAD--` replaced by `pad` padding instructions)
/// in a fresh VM whose hook fires on every instruction.
fn run_probed(
    src: &str,
    pad: u64,
    probe: &std::rc::Rc<Probe>,
    token: &CancelToken,
    spin: bool,
) -> (Env, Result<mlua::MultiValue, IsleError>, Duration) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let lua = mlua::Lua::new();
    let vm = Vm::attach(&lua, GRACE).unwrap();
    // Registered before any Lua runs, so the count matches the hub's.
    let p = probe.clone();
    let t = token.clone();
    vm.add_hook(
        mlua::HookTriggers::new().every_nth_instruction(1),
        move |_, debug| {
            let n = p.n.get() + 1;
            p.n.set(n);
            if !p.armed.get() {
                return Ok(mlua::VmState::Continue);
            }
            let in_chunk = in_async_poll(debug);
            if in_chunk {
                p.last_in_chunk.set(n);
            } else if p.last_in_chunk.get() != 0 && p.target.get() == 0 {
                p.target.set(p.last_in_chunk.get());
            }
            if n == p.cancel_at.get() {
                p.cancel_was_in_chunk.set(in_chunk);
                t.cancel();
            }
            if n == p.cancel_at.get() + 1 && n.is_multiple_of(1000) && in_chunk && t.is_cancelled()
            {
                p.deferred_in_chunk.set(true);
            }
            Ok(mlua::VmState::Continue)
        },
    )
    .unwrap();
    let g = lua.globals();
    g.set("task", vm.task_lib().unwrap()).unwrap();
    let p = probe.clone();
    g.set(
        "arm",
        lua.create_function(move |_, ()| {
            p.armed.set(true);
            Ok(())
        })
        .unwrap(),
    )
    .unwrap();
    g.set("spin", spin).unwrap();
    let padding = "x = x + 1\n".repeat(pad as usize);
    let src = src.replace("--PAD--", &padding);
    let f: mlua::Function = lua.load(&src).into_function().unwrap();
    let start = Instant::now();
    let r = local.block_on(&rt, async {
        tokio::time::timeout(Duration::from_secs(2), vm.run(token, f, ()))
            .await
            .expect("timed out")
    });
    let elapsed = start.elapsed();
    let e = Env { rt, local, lua, vm };
    (e, r, elapsed)
}

/// Probe `src`, then run it with the cancel check aligned on the poll
/// chunk's last instruction and the token cancelled just before it.
/// `src` sets the global `after` right after the call under test.
/// Asserts that the check landed there and that the run still ended
/// as cancelled within the grace (the `while spin do end` loop after
/// the call is cancelled at a later check).
fn run_with_cancel_in_poll_chunk(src: &str) -> Env {
    let probe = std::rc::Rc::new(Probe::default());
    let (_, r, _) = run_probed(src, 0, &probe, &CancelToken::new(), false);
    r.unwrap_or_else(|e| panic!("probe run failed: {e}"));
    let target = probe.target.get();
    assert!(target > 0, "the probe saw no poll chunk");
    let pad = (1000 - target % 1000) % 1000;

    let real = std::rc::Rc::new(Probe::default());
    real.cancel_at.set(target + pad - 1);
    let token = CancelToken::new();
    let (e, r, elapsed) = run_probed(src, pad, &real, &token, true);
    assert!(
        real.cancel_was_in_chunk.get(),
        "cancelled outside the chunk"
    );
    // The hub checks the cancel before it calls user callbacks, so when it
    // raises at the aligned check this callback never sees that event (and
    // counts one event short from there on).  Whether the statement after
    // the call ran (`after`) tells a raise apart from a moved chunk.
    if !real.deferred_in_chunk.get() {
        let after = e.lua.globals().get::<bool>("after").unwrap_or(false);
        assert!(
            after,
            "the cancel was raised at the check at {} in the poll chunk: the \
             deferral did not apply",
            target + pad
        );
        panic!(
            "the check at {} did not run in the poll chunk with the token \
             cancelled: the chunk moved",
            target + pad
        );
    }
    assert_eq!(
        real.target.get(),
        target + pad,
        "padding did not shift the chunk"
    );
    assert!(matches!(r, Err(IsleError::Cancelled)), "got {r:?}");
    assert!(elapsed < Duration::from_millis(300), "{elapsed:?}");
    e
}

#[test]
fn a_cancel_in_the_poll_chunk_does_not_lose_the_value_recv_returned() {
    let e = run_with_cancel_in_poll_chunk(
        "local ch = task.channel(2)
         ch:send('x')
         local x = 0
         --PAD--
         arm()
         local v, ok = ch:recv()
         after = true
         seen = tostring(v) .. ' ' .. tostring(ok)
         while spin do end",
    );
    assert_eq!(e.global::<String>("seen"), "x true");
}

#[test]
fn a_cancel_in_the_poll_chunk_does_not_lose_the_value_select_raw_returned() {
    let e = run_with_cancel_in_poll_chunk(
        "local ch = task.channel(2)
         ch:send('x')
         local arms = { ch:arm_recv() }
         local x = 0
         --PAD--
         arm()
         local i, v, ok = task.select_raw(arms)
         after = true
         seen = i .. ' ' .. tostring(v) .. ' ' .. tostring(ok)
         while spin do end",
    );
    assert_eq!(e.global::<String>("seen"), "1 x true");
}

#[test]
fn a_cancel_in_the_poll_chunk_lets_a_completed_send_return() {
    let e = run_with_cancel_in_poll_chunk(
        "local ch = task.channel(2)
         chan = ch
         local x = 0
         --PAD--
         arm()
         ch:send('y')
         after = true
         sent = true
         while spin do end",
    );
    assert!(e.global::<bool>("sent"));
    let r: String = e
        .lua
        .load("return chan:len() .. ' ' .. chan:try_recv()")
        .eval()
        .unwrap();
    assert_eq!(r, "1 y");
}
