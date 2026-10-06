#![cfg(feature = "tokio")]
//! Send cases, rendezvous channels, task-finish cases and tickers in
//! the `task` library (#26, part 3 of #19), run under `Vm::run`.
//!
//! The rendezvous tests (sections 2 to 5) do not depend on timing.
//! The VM runs on a current-thread runtime and a `LocalSet`, which polls
//! the tasks that are ready in the order they became ready.  `yield()`
//! (`tokio::task::yield_now`) lets every task that is ready run until it
//! waits again before the caller resumes, so after `task.spawn(f)` and
//! `yield()` the task has posted its offer (or registered as a waiter)
//! and is waiting; the caller then acts (`try_send`, `try_recv`,
//! `cancel`) synchronously, before the task is polled again.  Each
//! precondition is checked where it can be observed (for example
//! `try_recv` returning the offered value), so a scheduling assumption
//! that did not hold fails the test instead of passing it by luck.
//!
//! Outcomes of a wait that a cancel may interrupt are recorded from Rust
//! (`record(f, ...)`), not by Lua code after the call: the cancel hook
//! may raise at any instruction count check in the caller's code, so a
//! Lua statement after the call would not reliably run.
//!
//! The ticker tests (section 7) run on a runtime with paused time
//! (`env_paused`): the clock advances only when every task is waiting,
//! straight to the next timer, so the ticks, the reads and `clock()`
//! (milliseconds on that clock) happen at exact, repeatable times.

use mlua_isle::runtime::{cancellable, CancelToken, Config, Vm};
use mlua_isle::{Cancelled, IsleError};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

/// A VM on a current-thread runtime, with `task`, `sleep(ms)`
/// (cancellable), `yield()`, `record(f, ...)` and `clock()` (milliseconds
/// on the runtime's clock since the environment was created).
struct Env {
    rt: tokio::runtime::Runtime,
    local: tokio::task::LocalSet,
    lua: mlua::Lua,
    vm: Vm,
    /// What `record` saw, in order.
    log: Rc<RefCell<Vec<String>>>,
}

/// A value as `record` logs it.
fn show(v: &mlua::Value) -> String {
    match v {
        mlua::Value::Nil => "nil".into(),
        mlua::Value::Boolean(b) => b.to_string(),
        mlua::Value::Integer(n) => n.to_string(),
        mlua::Value::Number(n) => n.to_string(),
        mlua::Value::String(s) => s.to_string_lossy(),
        other => other.type_name().into(),
    }
}

fn env(config: Config) -> Env {
    env_with(config, false)
}

/// As [`env`], on a runtime whose time is paused and auto-advances.
fn env_paused(config: Config) -> Env {
    env_with(config, true)
}

fn env_with(config: Config, paused: bool) -> Env {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(paused)
        .build()
        .unwrap();
    let t0 = rt.block_on(async { tokio::time::Instant::now() });
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
    let yield_now = lua
        .create_async_function(|_, ()| async {
            tokio::task::yield_now().await;
            Ok(())
        })
        .unwrap();
    g.set("yield", yield_now).unwrap();
    // `record(f, ...)`: call `f(...)` from Rust and log
    // `ok <values>`, `cancelled` or `err <message>`.
    let log = Rc::new(RefCell::new(Vec::new()));
    let l = log.clone();
    let record = lua
        .create_async_function(
            move |_, (f, args): (mlua::Function, mlua::Variadic<mlua::Value>)| {
                let log = l.clone();
                async move {
                    let out = f
                        .call_async::<mlua::MultiValue>(mlua::MultiValue::from_iter(args))
                        .await;
                    let entry = match out {
                        Ok(values) => {
                            let mut s = String::from("ok");
                            for v in values.iter() {
                                s.push(' ');
                                s.push_str(&show(v));
                            }
                            s
                        }
                        Err(e) if e.downcast_ref::<Cancelled>().is_some() => "cancelled".into(),
                        Err(e) => format!("err {e}"),
                    };
                    log.borrow_mut().push(entry);
                    Ok(())
                }
            },
        )
        .unwrap();
    g.set("record", record).unwrap();
    let clock = lua
        .create_function(
            move |_, ()| Ok((tokio::time::Instant::now() - t0).as_nanos() as f64 / 1e6),
        )
        .unwrap();
    g.set("clock", clock).unwrap();
    Env {
        rt,
        local,
        lua,
        vm,
        log,
    }
}

impl Env {
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

    fn log(&self) -> Vec<String> {
        self.log.borrow().clone()
    }
}

fn err_message(r: Result<mlua::MultiValue, IsleError>) -> String {
    match r {
        Err(IsleError::Lua(f)) => f.message,
        other => panic!("expected a Lua error, got {other:?}"),
    }
}

const GRACE: Config = Config {
    grace: Duration::from_millis(300),
    preempt_every: None,
};

// ── 1. send cases on buffered channels ──

#[test]
fn a_send_case_is_chosen_only_when_there_is_room() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(1)
         ch:send('a')
         local full = task.select_raw({ ch:arm_send('b') }, { default = true })
         local len_full = ch:len()
         local a = ch:recv()
         local i, sent = task.select_raw({ ch:arm_send('b') }, { default = true })
         local b = ch:recv()
         return table.concat({ full, len_full, a, i, tostring(sent), b }, ' ')",
    );
    assert_eq!(r, "0 1 a 1 true b");
}

#[test]
fn a_send_case_waits_for_room_and_sends_when_chosen() {
    let e = env(GRACE);
    let r = e.string(
        "local out = task.channel(1)
         out:send('a')
         local h = task.spawn(function()
           return task.select({ out:on_send('b', function(sent) return 'handler', sent end) })
         end)
         yield()
         local still_full = out:len()
         local a = out:recv()
         local ok, who, sent = h:join()
         local b = out:recv()
         return table.concat({ still_full, a, tostring(ok), who, tostring(sent), b }, ' ')",
    );
    assert_eq!(r, "1 a true handler true b");
}

#[test]
fn a_send_case_that_is_not_chosen_leaves_the_channel_unchanged() {
    let e = env(GRACE);
    let r = e.string(
        "local out = task.channel(2)
         local other = task.channel(1)
         other:send('x')
         -- ready together: the receive (first, biased) is chosen
         local i, v = task.select_raw({ other:arm_recv(), out:arm_send('y') }, { biased = true })
         local len1 = out:len()
         -- the send case waits (out is full), another case is chosen
         out:send('k1') out:send('k2')
         local h = task.spawn(function()
           return task.select_raw({ out:arm_send('lost'), other:arm_recv() })
         end)
         yield()
         other:send('go')
         local ok, j, w = h:join()
         local a, b = out:recv(), out:recv()
         local _, _, ready = out:try_recv()
         return table.concat({ i, v, len1, j, w, a, b, tostring(ready) }, ' ')",
    );
    assert_eq!(r, "1 x 0 2 go k1 k2 false");
}

#[test]
fn a_send_case_on_a_closed_channel_reports_sent_false() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(1)
         ch:close()
         local h = task.select({ ch:on_send('v', function(sent) return sent end) })
         local i, sent = task.select_raw({ ch:arm_send('v') })
         -- a waiting send case is woken by the close
         local full = task.channel(1)
         full:send('a')
         local w = task.spawn(function()
           return task.select({ full:on_send('b', function(sent) return sent end) })
         end)
         yield()
         full:close()
         local ok, wsent = w:join()
         local a = full:recv()
         local _, more = full:recv()
         return table.concat({ tostring(h), i, tostring(sent), tostring(ok), tostring(wsent),
                               a, tostring(more) }, ' ')",
    );
    assert_eq!(r, "false 1 false true false a false");
}

#[test]
fn a_send_case_on_a_receive_only_channel_raises() {
    let e = env(GRACE);
    let (_tx, events) = mlua_isle::runtime::channel::<i64>(&e.lua, 4).unwrap();
    e.lua.globals().set("events", events).unwrap();
    let m = err_message(e.run("task.select_raw({ events:arm_send(1) })"));
    assert!(m.contains("receive-only (a host channel"), "got: {m}");
    let m = err_message(e.run("task.select({ events:on_send(1, function() end) })"));
    assert!(m.contains("receive-only (a host channel"), "got: {m}");
    let m = err_message(e.run(
        "local tk = task.ticker(1000)
         task.select_raw({ tk:arm_send(1) })",
    ));
    assert!(m.contains("receive-only (a ticker)"), "got: {m}");
    let m = err_message(e.run("task.channel(1):on_send(1, 'not a function')"));
    assert!(m.contains("handler must be a function"), "got: {m}");
}

#[test]
fn a_send_case_value_may_be_nil() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(1)
         local i, sent = task.select_raw({ ch:arm_send(nil) })
         local v, ok = ch:recv()
         return table.concat({ i, tostring(sent), tostring(v), tostring(ok) }, ' ')",
    );
    assert_eq!(r, "1 true nil true");
}

// ── 2. rendezvous channels ──

#[test]
fn a_rendezvous_send_returns_only_after_a_receiver_took_the_value() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local log = {}
         local h = task.spawn(function()
           ch:send('v')
           log[#log + 1] = 'sent'
         end)
         yield() yield() yield()
         local before = #log
         local len, cap = ch:len(), ch:cap()
         local v, ok = ch:recv()
         h:join()
         return table.concat({ before, len, cap, v, tostring(ok), table.concat(log, ',') }, ' ')",
    );
    assert_eq!(r, "0 0 0 v true sent");
}

#[test]
fn rendezvous_senders_are_served_in_order() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local hs = {}
         for i = 1, 3 do hs[i] = task.spawn(function() ch:send(i) end) end
         yield()
         local got = {}
         for i = 1, 3 do got[i] = ch:recv() end
         for i = 1, 3 do hs[i]:join() end
         return table.concat(got, ' ')",
    );
    assert_eq!(r, "1 2 3");
}

#[test]
fn a_rendezvous_try_send_succeeds_only_with_a_waiting_receiver() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local alone = ch:try_send('nobody')
         local _, _, posted = ch:try_recv()   -- try_send posted nothing
         local r = task.spawn(function() return ch:recv() end)
         yield()
         local to_recv = ch:try_send('to recv')
         local _, v = r:join()
         -- a select's receive case waits like recv (decided in #26)
         local s = task.spawn(function() return task.select_raw({ ch:arm_recv() }) end)
         yield()
         local to_select = ch:try_send('to select')
         local _, i, w = s:join()
         return table.concat({ tostring(alone), tostring(posted), tostring(to_recv), v,
                               tostring(to_select), i, w, ch:len() }, ' ')",
    );
    assert_eq!(r, "false false true to recv true 1 to select 0");
}

#[test]
fn a_rendezvous_try_recv_takes_a_waiting_sender_offer() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local _, _, ready0 = ch:try_recv()
         local h = task.spawn(function() ch:send('offer') return 'done' end)
         yield()
         local v, ok, ready = ch:try_recv()
         local _, done = h:join()
         return table.concat({ tostring(ready0), v, tostring(ok), tostring(ready), done }, ' ')",
    );
    assert_eq!(r, "false offer true true done");
}

#[test]
fn a_value_try_send_gave_a_receiver_that_was_cancelled_is_received_next() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local h = task.spawn(function() return ch:recv() end)
         yield()
         h:cancel()
         local sent = ch:try_send('kept')
         local ok, err = h:join()
         local len = ch:len()
         local v = ch:recv()
         return table.concat({ tostring(sent), tostring(ok), tostring(err == task.CANCELLED),
                               len, v }, ' ')",
    );
    assert_eq!(r, "true false true 1 kept");
}

// ── 3. rendezvous in select (deterministic) ──

#[test]
fn a_select_does_not_pair_its_own_send_and_receive_cases() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local a = task.select_raw({ ch:arm_send('self'), ch:arm_recv() },
                                   { biased = true, default = true })
         local b = task.select_raw({ ch:arm_recv(), ch:arm_send('self') },
                                   { biased = true, default = true })
         local _, _, left = ch:try_recv()
         -- waiting: only the third case can end it
         local stop = task.channel(1)
         local h = task.spawn(function()
           return task.select_raw({ ch:arm_send('self'), ch:arm_recv(), stop:arm_recv() },
                                  { biased = true })
         end)
         yield()
         stop:send('stop')
         local _, i, v = h:join()
         local _, _, left2 = ch:try_recv()
         return table.concat({ a, b, tostring(left), i, v, tostring(left2) }, ' ')",
    );
    assert_eq!(r, "0 0 false 3 stop false");
}

#[test]
fn two_selects_with_send_and_receive_cases_pair_with_each_other() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local function both(v)
           return function()
             return task.select_raw({ ch:arm_send(v), ch:arm_recv() }, { biased = true })
           end
         end
         local s1 = task.spawn(both('from s1'))
         yield()
         local s2 = task.spawn(both('from s2'))
         local _, i1, x1 = s1:join()
         local _, i2, x2, ok2 = s2:join()
         local _, _, left = ch:try_recv()
         return table.concat({ i1, tostring(x1), i2, x2, tostring(ok2), tostring(left) }, ' ')",
    );
    // s2's receive case takes s1's offer; s1's send case is chosen
    // (sent); s2 withdraws its own offer.
    assert_eq!(r, "1 true 2 from s1 true false");
}

#[test]
fn a_select_that_chooses_another_case_withdraws_its_offer() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local other = task.channel(1)
         local h = task.spawn(function()
           return task.select_raw({ ch:arm_send('withdrawn'), other:arm_recv() }, { biased = true })
         end)
         yield()
         other:try_send('go')
         local _, i, v = h:join()
         local _, _, ready = ch:try_recv()
         -- a receiver that comes later does not get it either
         local r = task.spawn(function() return ch:recv() end)
         yield()
         ch:close()
         local _, rv, rok = r:join()
         return table.concat({ i, v, tostring(ready), tostring(rv), tostring(rok) }, ' ')",
    );
    assert_eq!(r, "2 go false nil false");
}

#[test]
fn a_taken_offer_is_the_case_chosen_even_when_another_is_ready() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local other = task.channel(1)
         local h = task.spawn(function()
           return task.select_raw({ other:arm_recv(), ch:arm_send('w') }, { biased = true })
         end)
         yield()
         other:try_send('go')          -- the first case is ready
         local v = ch:try_recv()       -- and the offer is taken
         local _, i, sent = h:join()
         local left = other:try_recv()
         return table.concat({ v, i, tostring(sent), left }, ' ')",
    );
    assert_eq!(r, "w 2 true go");
}

#[test]
fn a_select_delivers_at_most_one_of_its_offers() {
    let e = env(GRACE);
    let r = e.string(
        "local a = task.channel(0)
         local b = task.channel(0)
         local h = task.spawn(function()
           return task.select_raw({ a:arm_send('to a'), b:arm_send('to b') })
         end)
         yield()
         local va = a:try_recv()
         local vb, okb, readyb = b:try_recv()   -- passed over: the select has its case
         local _, i, sent = h:join()
         return table.concat({ va, tostring(vb), tostring(readyb), i, tostring(sent) }, ' ')",
    );
    assert_eq!(r, "to a nil false 1 true");
}

#[test]
fn a_filled_waiter_slot_that_is_not_chosen_is_received_next() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local other = task.channel(1)
         local h = task.spawn(function()
           return task.select_raw({ other:arm_recv(), ch:arm_recv() }, { biased = true })
         end)
         yield()
         local filled = ch:try_send('slot')   -- fills the select's waiter
         other:try_send('go')                 -- the first case is ready too
         local _, i, v = h:join()
         local len = ch:len()
         local w, ok = ch:recv()
         return table.concat({ tostring(filled), i, v, len, w, tostring(ok) }, ' ')",
    );
    assert_eq!(r, "true 1 go 1 slot true");
}

#[test]
fn a_default_select_sends_to_a_waiting_receiver() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         -- a plain recv waits
         local r1 = task.spawn(function() return ch:recv() end)
         yield()
         local i, sent = task.select_raw({ ch:arm_send('to recv') }, { default = true })
         local _, v1, ok1 = r1:join()
         -- another select's receive case waits; handler form
         local r2 = task.spawn(function() return task.select_raw({ ch:arm_recv() }) end)
         yield()
         local h = task.select({ ch:on_send('to select', function(s) return 'sent ' .. tostring(s) end) },
                               { default = function() return 'default' end })
         local _, j, v2 = r2:join()
         return table.concat({ i, tostring(sent), v1, tostring(ok1), h, j, v2, ch:len() }, ' ')",
    );
    assert_eq!(r, "1 true to recv true sent true 1 to select 0");
}

#[test]
fn a_default_select_runs_default_when_no_receiver_waits_and_delivers_nothing() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local seen
         local d = task.select({ ch:on_send('x', function() return 'sent' end) }, {
           default = function()
             local _, _, ready = ch:try_recv()
             seen = tostring(ready)
             return 'default'
           end,
         })
         local i = task.select_raw({ ch:arm_send('y') }, { default = true })
         -- a receiver that comes later gets neither value
         local r = task.spawn(function() return ch:recv() end)
         yield()
         local _, _, ready = ch:try_recv()
         ch:try_send('later')
         local _, v = r:join()
         return table.concat({ d, seen, i, tostring(ready), v }, ' ')",
    );
    assert_eq!(r, "default false 0 false later");
}

#[test]
fn a_default_select_does_not_send_to_its_own_receive_case() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         -- the receive case registers a waiter first; the send case must
         -- not fill it
         local i = task.select_raw({ ch:arm_recv(), ch:arm_send('self') },
                                   { biased = true, default = true })
         local _, _, left = ch:try_recv()
         -- a select that already has its case (its offer was taken) is
         -- passed over too: only the plain receiver gets the value
         local other = task.channel(0)
         local s = task.spawn(function()
           return task.select_raw({ other:arm_send('o'), ch:arm_recv() }, { biased = true })
         end)
         local r = task.spawn(function() return ch:recv() end)
         yield()
         local o = other:try_recv()           -- s is claimed now
         local j = task.select_raw({ ch:arm_send('z') }, { default = true })
         -- r's slot holds 'z' and s is passed over: no receiver is left
         local more = ch:try_send('w')
         local _, rv = r:join()
         local _, si, ssent = s:join()
         return table.concat({ i, tostring(left), o, j, tostring(more), rv, si, tostring(ssent) }, ' ')",
    );
    assert_eq!(r, "0 false o 1 false z 1 true");
}

// ── 4. cancel after delivery (deterministic) ──

#[test]
fn a_sender_cancelled_after_its_offer_was_taken_returns_as_sent() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local h = task.spawn(function() record(ch.send, ch, 'v') end)
         yield()
         local v = ch:try_recv()   -- takes the offer: delivered
         h:cancel()                -- before the sender is polled again
         h:join()
         return v",
    );
    assert_eq!(r, "v");
    assert_eq!(e.log(), vec!["ok"]);
}

#[test]
fn a_sender_cancelled_before_its_offer_was_taken_is_cancelled() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local h = task.spawn(function() record(ch.send, ch, 'v') end)
         yield()
         h:cancel()
         h:join()
         local _, _, ready = ch:try_recv()
         local r = task.spawn(function() return ch:recv() end)
         yield()
         ch:close()
         local _, rv, rok = r:join()
         return table.concat({ tostring(ready), tostring(rv), tostring(rok) }, ' ')",
    );
    assert_eq!(r, "false nil false");
    assert_eq!(e.log(), vec!["cancelled"]);
}

#[test]
fn a_select_cancelled_after_its_offer_was_taken_reports_the_send() {
    let e = env(GRACE);
    let r = e.string(
        "local ch = task.channel(0)
         local other = task.channel(1)
         local h = task.spawn(function()
           record(task.select_raw, { other:arm_recv(), ch:arm_send('v') })
         end)
         yield()
         local v = ch:try_recv()
         h:cancel()
         h:join()
         local h2 = task.spawn(function()
           record(task.select_raw, { other:arm_recv(), ch:arm_send('w') })
         end)
         yield()
         h2:cancel()
         h2:join()
         local _, _, ready = ch:try_recv()
         return v .. ' ' .. tostring(ready)",
    );
    assert_eq!(r, "v false");
    assert_eq!(e.log(), vec!["ok 2 true", "cancelled"]);
}

// ── 5. close with offers and waiters ──

#[test]
fn close_fails_open_offers_and_ends_waiting_receivers() {
    let e = env(GRACE);
    let r = e.string(
        "local out = task.channel(0)
         local inc = task.channel(0)
         local hs = {
           task.spawn(function() record(out.send, out, 'a') end),
           task.spawn(function()
             return task.select({ out:on_send('b', function(sent) return sent end) })
           end),
           task.spawn(function() return inc:recv() end),
           task.spawn(function()
             return task.select({ inc:on(function(v, ok) return tostring(v) .. '/' .. tostring(ok) end) })
           end),
         }
         yield()
         out:close()
         inc:close()
         local res = {}
         for k, h in ipairs(hs) do
           local ok, a, b = h:join()
           res[#res + 1] = tostring(ok) .. ':' .. tostring(a) .. ':' .. tostring(b)
         end
         local t_ok, t_err = pcall(out.try_send, out, 'x')
         local s_ok, s_err = pcall(out.send, out, 'x')
         local v, ok = out:recv()
         return table.concat(res, ' ') .. ' | ' ..
                table.concat({ tostring(t_ok), tostring(s_ok), tostring(v), tostring(ok),
                               tostring(out:closed()) }, ' ')",
    );
    assert_eq!(
        r,
        "true:nil:nil true:false:nil true:nil:false true:nil/false:nil | false false nil false true"
    );
    let log = e.log();
    assert_eq!(log.len(), 1);
    assert!(log[0].contains("ch:send: channel is closed"), "{log:?}");
}

// ── 6. task-finish cases ──

#[test]
fn a_task_case_returns_what_join_returns() {
    let e = env(GRACE);
    let r = e.string(
        "local ok_h = task.spawn(function() return 1, 'two' end)
         local a = { task.select({ ok_h:on(function(...) return ... end) }) }
         local err_h = task.spawn(function() error({ code = 7 }) end)
         local i, ok, err = task.select_raw({ err_h:arm() })
         local c_h = task.spawn(function() sleep(60000) end)
         c_h:cancel()
         local j, cok, cerr = task.select_raw({ c_h:arm() })
         return table.concat({ tostring(a[1]), a[2], a[3], i, tostring(ok), err.code,
                               j, tostring(cok), tostring(rawequal(cerr, task.CANCELLED)) }, ' ')",
    );
    assert_eq!(r, "true 1 two 1 false 7 1 false true");
}

#[test]
fn a_task_case_waits_for_the_finish() {
    let e = env(GRACE);
    let r = e.string(
        "local gate = task.channel(1)
         local h = task.spawn(function() gate:recv() return 'finished' end)
         local opener = task.spawn(function() yield() gate:send(1) end)
         local i, ok, v = task.select_raw({ h:arm(), task.after(60000):arm() })
         opener:join()
         return table.concat({ i, tostring(ok), v }, ' ')",
    );
    assert_eq!(r, "1 true finished");
}

#[test]
fn a_chosen_task_case_marks_the_handle_joined() {
    let e = env(GRACE);
    let r = e.string(
        "local h = task.spawn(function() return 'v' end)
         task.select_raw({ h:arm() })
         local done = h:done()
         local j_ok, j_err = pcall(h.join, h)
         local o_ok, o_err = pcall(h.on, h, function() end)
         local a_ok, a_err = pcall(h.arm, h)
         return table.concat({ tostring(done), tostring(j_ok), j_err, tostring(o_ok), o_err,
                               tostring(a_ok), a_err }, ' | ')",
    );
    assert!(r.starts_with("true | false | "), "{r}");
    assert_eq!(r.matches("task already joined").count(), 3, "{r}");
}

#[test]
fn a_task_case_that_is_not_chosen_leaves_the_handle_joinable() {
    let e = env(GRACE);
    let r = e.string(
        "local gate = task.channel(1)
         local h = task.spawn(function() gate:recv() return 'late' end)
         local other = task.channel(1)
         other:send('first')
         local i, v = task.select_raw({ h:arm(), other:arm_recv() })
         local done = h:done()
         gate:send(1)
         local again = task.spawn(function() return task.select_raw({ h:arm() }) end)
         local _, j, ok, w = again:join()
         return table.concat({ i, v, tostring(done), j, tostring(ok), w }, ' ')",
    );
    assert_eq!(r, "2 first false 1 true late");

    // Not chosen, then joined.
    let r = e.string(
        "local h = task.spawn(function() yield() return 'joined' end)
         local i = task.select_raw({ h:arm() }, { default = true })
         local ok, v = h:join()
         return table.concat({ i, tostring(ok), v }, ' ')",
    );
    assert_eq!(r, "0 true joined");
}

#[test]
fn a_task_case_of_a_joined_handle_raises() {
    let e = env(GRACE);
    let r = e.string(
        "local h = task.spawn(function() return 1 end)
         h:join()
         local a_ok, a_err = pcall(h.arm, h)
         local o_ok, o_err = pcall(h.on, h, function() end)
         -- a case built before the join is refused by select
         local h2 = task.spawn(function() return 2 end)
         local case = h2:arm()
         h2:join()
         local s_ok, s_err = pcall(task.select_raw, { case })
         return table.concat({ tostring(a_ok), a_err, tostring(o_ok), o_err,
                               tostring(s_ok), tostring(s_err) }, ' | ')",
    );
    assert!(r.starts_with("false | "), "{r}");
    assert_eq!(r.matches("task already joined").count(), 3, "{r}");
    assert!(!r.contains("true"), "{r}");
}

/// The handler of a chosen task case is never entered (an error is
/// injected on the call into `select`'s handler wrapper, as in
/// tests/channel_select.rs): the join is undone.
#[test]
fn a_task_case_whose_handler_was_never_entered_leaves_the_handle_joinable() {
    let e = env(GRACE);
    let armed = Rc::new(std::cell::Cell::new(false));
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
        "local h = task.spawn(function() return 'kept' end)
         yield()
         local entered = false
         arm_failure()
         local ok, err = pcall(task.select, { h:on(function() entered = true end) })
         local jok, v = h:join()
         return table.concat({ tostring(ok), tostring(entered), tostring(jok), v }, ' ')",
    );
    assert_eq!(r, "false false true kept");
}

#[test]
fn a_task_case_whose_handle_is_joined_elsewhere_raises() {
    let e = env(GRACE);
    let r = e.string(
        "local gate = task.channel(1)
         local h = task.spawn(function() gate:recv() return 'v' end)
         local s = task.spawn(function() return task.select_raw({ h:arm() }) end)
         yield()                        -- s waits on h's case
         local j = task.spawn(function() return h:join() end)
         yield()                        -- j is joining h
         gate:send(1)
         local s_ok, s_err = s:join()
         local _, j_ok, j_v = j:join()
         return table.concat({ tostring(s_ok), tostring(s_err), tostring(j_ok), j_v }, ' | ')",
    );
    let parts: Vec<&str> = r.split(" | ").collect();
    assert_eq!(parts[0], "false", "{r}");
    assert!(parts[1].contains("task already joined"), "{r}");
    assert_eq!(&parts[2..], ["true", "v"], "{r}");

    // Two selects wait on the same handle: one joins it, the other
    // raises.
    let r = e.string(
        "local gate = task.channel(1)
         local h = task.spawn(function() gate:recv() return 'v' end)
         local function waiter() return task.select_raw({ h:arm() }) end
         local s1, s2 = task.spawn(waiter), task.spawn(waiter)
         yield()
         gate:send(1)
         local res = {}
         for _, s in ipairs({ s1, s2 }) do
           local ok, a, b, c = s:join()
           if ok then
             res[#res + 1] = 'joined ' .. a .. ' ' .. tostring(b) .. ' ' .. c
           else
             res[#res + 1] = tostring(a):match('task already joined') or tostring(a)
           end
         end
         table.sort(res)
         return table.concat(res, ' / ')",
    );
    assert_eq!(r, "joined 1 true v / task already joined");
}

// ── 7. tickers (paused time) ──

#[test]
fn ticks_are_the_scheduled_times_since_the_start() {
    let e = env_paused(GRACE);
    let r = e.string(
        "sleep(3)                         -- the ticker starts at 3 on the clock
         local t0 = clock()
         local tk = task.ticker(20)
         local t, at = {}, {}
         for i = 1, 3 do
           local v, ok = tk:recv()
           assert(ok)
           t[i] = v
           at[i] = clock() - t0
         end
         tk:stop()
         return table.concat(t, ' ') .. ' | ' .. table.concat(at, ' ')",
    );
    assert_eq!(r, "20.0 40.0 60.0 | 20.0 40.0 60.0");
}

#[test]
fn an_unread_tick_is_replaced_by_the_newest() {
    let e = env_paused(GRACE);
    let r = e.string(
        "local tk = task.ticker(10)
         sleep(65)                        -- ticks at 10, 20, ..., 60 arrive unread
         local len, cap = tk:len(), tk:cap()
         local a = tk:recv()
         local b = tk:recv()
         tk:stop()
         return table.concat({ len, cap, a, b }, ' ')",
    );
    assert_eq!(r, "1 1 60.0 70.0");
}

#[test]
fn stop_closes_the_ticker_and_is_idempotent() {
    let e = env_paused(GRACE);
    let r = e.string(
        "local tk = task.ticker(5)
         local v = tk:recv()
         tk:stop()
         local closed = tk:closed()
         local _, ok, ready = tk:try_recv()  -- closed and empty
         sleep(20)                           -- no tick after the stop
         local _, ok2 = tk:recv()
         tk:stop()
         -- a tick already in the channel can still be received
         local tk2 = task.ticker(5)
         sleep(7)
         tk2:stop()
         local w, okw = tk2:recv()
         local _, okw2 = tk2:recv()
         return table.concat({ v, tostring(closed), tostring(ok), tostring(ready), tostring(ok2),
                               w, tostring(okw), tostring(okw2) }, ' ')",
    );
    assert_eq!(r, "5.0 true false true false 5.0 true false");
}

#[test]
fn a_ticker_stops_with_its_scope() {
    let e = env_paused(GRACE);
    // The task that created it is cancelled: the channel is closed when
    // the cancelled task has been joined, with no time passing (the
    // host task stops at the cancel, it does not wait for the grace).
    let r = e.string(
        "local t0 = clock()
         local parent = task.spawn(function()
           tk = task.ticker(5)
           sleep(60000)
         end)
         yield()
         local v = tk:recv()
         parent:cancel()
         parent:join()
         local at = clock() - t0
         local _, ok = tk:recv()
         return table.concat({ v, at, tostring(tk:closed()), tostring(ok) }, ' ')",
    );
    assert_eq!(r, "5.0 5.0 true false");

    // The task that created it ends: same.
    let r = e.string(
        "local h = task.spawn(function()
           tk2 = task.ticker(5)
           return 'ended'
         end)
         local _, v = h:join()
         return v .. ' ' .. tostring(tk2:closed())",
    );
    assert_eq!(r, "ended true");

    // The root that created it is cancelled: `run` ends and the ticker
    // is closed.
    let token = CancelToken::new();
    let t = token.clone();
    e.lua
        .globals()
        .set(
            "cancel_root",
            e.lua
                .create_function(move |_, ()| {
                    t.cancel();
                    Ok(())
                })
                .unwrap(),
        )
        .unwrap();
    let out = e.run_with(
        &token,
        Duration::from_secs(5),
        "t0 = clock()
         tk3 = task.ticker(5)
         tk3:recv()
         cancel_root()
         while true do tk3:recv() end",
    );
    assert!(matches!(out, Err(IsleError::Cancelled)), "{out:?}");
    // `clock()` reads the runtime's (paused) clock: enter it.
    let _rt = e.rt.enter();
    let r: String = e
        .lua
        .load("return tostring(tk3:closed()) .. ' ' .. (clock() - t0)")
        .eval()
        .unwrap();
    assert_eq!(r, "true 5.0");
}

#[test]
fn a_ticker_is_receive_only_and_checks_its_interval() {
    let e = env_paused(GRACE);
    let m = err_message(e.run("local tk = task.ticker(1000) tk:send(1)"));
    assert!(
        m.contains("ch:send: channel is receive-only (a ticker)"),
        "got: {m}"
    );
    let m = err_message(e.run("local tk = task.ticker(1000) tk:try_send(1)"));
    assert!(
        m.contains("ch:try_send: channel is receive-only (a ticker)"),
        "got: {m}"
    );
    for bad in ["0", "-5", "0/0"] {
        let m = err_message(e.run(&format!("task.ticker({bad})")));
        assert!(m.contains("task.ticker: ms must be > 0"), "{bad}: {m}");
    }
    // Outside a coroutine request or task there is no scope to run in.
    let m = e
        .lua
        .load("task.ticker(10)")
        .exec()
        .unwrap_err()
        .to_string();
    assert!(
        m.contains("task.ticker: not inside a coroutine request or task"),
        "got: {m}"
    );
    // A ticker is a channel: it works as a receive case and closes.
    let r = e.string(
        "local tk = task.ticker(5)
         local i, v, ok = task.select_raw({ tk:arm_recv() })
         tk:close()
         sleep(15)
         local _, more = tk:recv()
         return table.concat({ i, v, tostring(ok), tostring(more) }, ' ')",
    );
    assert_eq!(r, "1 5.0 true false");
}
