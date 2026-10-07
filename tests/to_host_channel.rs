#![cfg(feature = "tokio")]
//! Channels to the host (`runtime::channel_to_host`), run under
//! `Vm::run`.
//!
//! The deterministic tests (full channel, cancelled sends) run on a
//! current-thread runtime with paused time, and the host's receiver
//! lives on the VM thread behind `host_recv()` (a sync host function
//! that does `try_recv`).  Every step is ordered by the Lua code: a
//! spawned task runs when the root yields (`yield()`, one
//! `tokio::task::yield_now`), and receiving on the host side frees room
//! synchronously, inside `host_recv()`.  The timers in those tests only
//! bound a failure ("stuck"): with paused time they fire as soon as
//! nothing else can run, so a failure is reported at once and a pass
//! never depends on timing.

use mlua::IntoLua;
use mlua_isle::runtime::{
    channel_to_host, CancelToken, Config, LuaChannel, Receiver, TryRecvError, Vm,
};
use mlua_isle::{IsleError, LuaErrorKind};
use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::time::Duration;

/// A VM on a current-thread runtime, with `task` and `yield()`.
struct Env {
    rt: tokio::runtime::Runtime,
    local: tokio::task::LocalSet,
    lua: mlua::Lua,
    vm: Vm,
}

fn env_with(paused: bool) -> Env {
    let mut b = tokio::runtime::Builder::new_current_thread();
    b.enable_all();
    if paused {
        b.start_paused(true);
    }
    let rt = b.build().unwrap();
    let local = tokio::task::LocalSet::new();
    let lua = mlua::Lua::new();
    let vm = Vm::attach(
        &lua,
        Config {
            grace: Duration::from_millis(300),
            preempt_every: None,
        },
    )
    .unwrap();
    let g = lua.globals();
    g.set("task", vm.task_lib().unwrap()).unwrap();
    let yield_fn = lua
        .create_async_function(|_, ()| async {
            tokio::task::yield_now().await;
            Ok(())
        })
        .unwrap();
    g.set("yield", yield_fn).unwrap();
    Env { rt, local, lua, vm }
}

fn env() -> Env {
    env_with(false)
}

/// For the deterministic tests: paused time.
fn paused_env() -> Env {
    env_with(true)
}

impl Env {
    /// A channel to the host of `T`, its Lua side set as the global
    /// `name`.
    fn channel<T: mlua::FromLua + Send + 'static>(&self, name: &str, cap: usize) -> Receiver<T> {
        let (ch, rx) = channel_to_host::<T>(&self.lua, cap).unwrap();
        self.lua.globals().set(name, ch).unwrap();
        rx
    }

    /// As `channel`, with the receiver behind the host functions
    /// `host_recv()` (`try_recv`: the value, `'empty'` or `'closed'`),
    /// `host_close()` and `host_drop()`, all on the VM thread.
    fn local_channel<T>(&self, name: &str, cap: usize)
    where
        T: mlua::FromLua + mlua::IntoLua + Send + 'static,
    {
        let rx = Rc::new(RefCell::new(Some(self.channel::<T>(name, cap))));
        let r = rx.clone();
        self.func("host_recv", move |lua, ()| {
            let mut slot = r.borrow_mut();
            let Some(rx) = slot.as_mut() else {
                return "dropped".into_lua(lua);
            };
            match rx.try_recv() {
                Ok(v) => v.into_lua(lua),
                Err(TryRecvError::Empty) => "empty".into_lua(lua),
                Err(TryRecvError::Closed) => "closed".into_lua(lua),
            }
        });
        let r = rx.clone();
        self.func("host_close", move |_, ()| {
            if let Some(rx) = r.borrow_mut().as_mut() {
                rx.close();
            }
            Ok(())
        });
        self.func("host_drop", move |_, ()| {
            rx.borrow_mut().take();
            Ok(())
        });
    }

    /// The root future for `src`, within 5 s.
    fn root(&self, src: &str) -> impl Future<Output = Result<mlua::MultiValue, IsleError>> + '_ {
        let f: mlua::Function = self.lua.load(src).into_function().unwrap();
        let vm = self.vm.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(5), vm.run(&CancelToken::new(), f, ()))
                .await
                .expect("timed out")
        }
    }

    fn run(&self, src: &str) -> Result<mlua::MultiValue, IsleError> {
        self.local.block_on(&self.rt, self.root(src))
    }

    fn string(&self, src: &str) -> String {
        let out = self.run(src).unwrap_or_else(|e| panic!("run failed: {e}"));
        match out.front() {
            Some(mlua::Value::String(s)) => s.to_str().unwrap().to_string(),
            other => panic!("expected a string, got {other:?}"),
        }
    }

    /// Set the global `name` to a sync host function.
    fn func<A, R, F>(&self, name: &str, f: F)
    where
        A: mlua::FromLuaMulti,
        R: mlua::IntoLuaMulti,
        F: Fn(&mlua::Lua, A) -> mlua::Result<R> + 'static,
    {
        let f = self.lua.create_function(f).unwrap();
        self.lua.globals().set(name, f).unwrap();
    }
}

/// Collect everything `rx` receives until it returns `None`, on a
/// thread of its own.
fn drain_on_another_thread<T: Send + 'static>(
    mut rx: Receiver<T>,
) -> std::thread::JoinHandle<Vec<T>> {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async move {
            let mut got = Vec::new();
            while let Some(v) = rx.recv().await {
                got.push(v);
            }
            got
        })
    })
}

// ── 1. values arrive on another thread, in order, exactly once ──

#[test]
fn values_sent_from_lua_arrive_in_order_on_another_thread() {
    const N: i64 = 600;
    let e = env();
    let rx = e.channel::<i64>("ch", 4);
    let consumer = drain_on_another_thread(rx);
    let r = e.string(&format!(
        "local by = {{ send = 0, try_send = 0, select = 0 }}
         for i = 1, {N} do
           local turn = i % 3
           if turn == 0 then
             ch:send(i) by.send = by.send + 1
           elseif turn == 1 then
             if ch:try_send(i) then by.try_send = by.try_send + 1 else ch:send(i) by.send = by.send + 1 end
           else
             local sent = task.select({{ ch:on_send(i, function(sent) return sent end) }})
             if not sent then error('closed') end
             by.select = by.select + 1
           end
         end
         ch:close()
         return table.concat({{ tostring(by.send > 0), tostring(by.try_send > 0),
                                tostring(by.select > 0) }}, ' ')"
    ));
    assert_eq!(r, "true true true");
    let got = consumer.join().unwrap();
    assert_eq!(got, (1..=N).collect::<Vec<_>>());
}

#[test]
fn several_lua_senders_deliver_every_value_exactly_once() {
    const SENDERS: i64 = 4;
    const PER: i64 = 400;
    let e = env();
    let rx = e.channel::<i64>("ch", 3);
    let consumer = drain_on_another_thread(rx);
    e.run(&format!(
        "local hs = {{}}
         for s = 1, {SENDERS} do
           hs[#hs + 1] = task.spawn(function()
             for i = 1, {PER} do
               if i % 2 == 0 then
                 ch:send(s * 100000 + i)
               else
                 task.select({{ ch:on_send(s * 100000 + i, function() end) }})
               end
             end
           end)
         end
         for _, h in ipairs(hs) do
           local ok, err = h:join()
           if not ok then error(err, 0) end
         end
         ch:close()"
    ))
    .unwrap();
    let got = consumer.join().unwrap();
    assert_eq!(got.len() as i64, SENDERS * PER);
    for s in 1..=SENDERS {
        // Every value of each sender, exactly once, in that sender's order.
        let mine: Vec<i64> = got.iter().copied().filter(|v| v / 100000 == s).collect();
        assert_eq!(mine, (1..=PER).map(|i| s * 100000 + i).collect::<Vec<_>>());
    }
}

// ── 2. full channel: send waits, try_send false, on_send not chosen ──

#[test]
fn a_full_channel_makes_send_wait_try_send_fail_and_on_send_unready() {
    let e = paused_env();
    e.local_channel::<i64>("ch", 1);
    let r = e.string(
        "local out = {}
         local function add(x) out[#out + 1] = tostring(x) end
         add(ch:try_send(1))                       -- true: room for one
         add(ch:try_send(2))                       -- false: full
         add(ch:len() .. '/' .. ch:cap())
         -- `default`: the send case is not ready.
         add(task.select({ ch:on_send(2, function() return 'sent' end) },
                         { default = function() return 'default' end }))
         -- Waiting: another ready case is chosen, not the send case.
         local other = task.channel(1)
         other:try_send('x')
         add(task.select({ ch:on_send(2, function() return 'sent' end),
                           other:on(function() return 'other' end) }))
         -- send waits until the host receives.
         local started = false
         local h = task.spawn(function() started = true ch:send(2) return 'resumed' end)
         yield() yield()
         add(started and not h:done())             -- true: waiting
         add(host_recv())                          -- 1, frees the room
         local _, resumed = h:join()
         add(resumed)
         add(host_recv())                          -- 2
         -- A waiting select's send case is chosen once the host receives.
         add(ch:try_send(3))
         local s = task.spawn(function()
           return task.select({ ch:on_send(4, function(sent) return sent end),
                                task.after(1000):on(function() return 'stuck' end) })
         end)
         yield() yield()
         add(not s:done())                         -- true: waiting
         add(host_recv())                          -- 3
         local _, sent = s:join()
         add(sent)                                 -- true
         add(host_recv())                          -- 4
         add(host_recv())                          -- empty: nothing else was sent
         return table.concat(out, ' ')",
    );
    assert_eq!(
        r,
        "true false 1/1 default other true 1 resumed 2 true true 3 true 4 empty"
    );
}

// ── 3. a value that does not convert ──

/// Converts from a non-negative integer; fails otherwise.
#[derive(Debug, PartialEq)]
struct Report(i64);

impl mlua::FromLua for Report {
    fn from_lua(v: mlua::Value, lua: &mlua::Lua) -> mlua::Result<Self> {
        let n = i64::from_lua(v, lua)?;
        if n < 0 {
            return Err(mlua::Error::runtime(format!("bad report {n}")));
        }
        Ok(Report(n))
    }
}

impl mlua::IntoLua for Report {
    fn into_lua(self, lua: &mlua::Lua) -> mlua::Result<mlua::Value> {
        self.0.into_lua(lua)
    }
}

#[test]
fn a_value_that_does_not_convert_raises_and_holds_no_room() {
    let e = paused_env();
    e.local_channel::<Report>("ch", 1);
    let r = e.string(
        "local out = {}
         local function add(x) out[#out + 1] = tostring(x) end
         local function failed(ok, err) add(ok) add(tostring(err):match('bad report %-%d') or tostring(err)) end
         failed(pcall(ch.send, ch, -1))
         failed(pcall(ch.try_send, ch, -2))
         failed(pcall(task.select, { ch:on_send(-3, function() return 'entered' end) }))
         add(ch:len())                             -- 0: no room held
         add(ch:try_send(5))                       -- true
         -- A send that waited for room and then fails to convert.
         local h = task.spawn(function() return pcall(ch.send, ch, -4) end)
         yield() yield()
         add(host_recv())                          -- 5
         local _, ok, err = h:join()
         failed(ok, err)
         add(ch:try_send(6))                       -- true: the room was given back
         add(host_recv())                          -- 6
         add(host_recv())                          -- empty
         return table.concat(out, ' ')",
    );
    assert_eq!(
        r,
        "false bad report -1 false bad report -2 false bad report -3 0 true 5 false bad report -4 true 6 empty"
    );
}

// ── 4. closing ──

/// `close` is `host_close` or `host_drop`.
fn host_side_close(close: &str) -> String {
    let e = paused_env();
    e.local_channel::<i64>("ch", 1);
    e.string(&format!(
        "local out = {{}}
         local function add(x) out[#out + 1] = tostring(x) end
         local function closed(ok, err) add(ok) add(tostring(err):match('channel is closed') or tostring(err)) end
         add(ch:try_send(1))
         local waiting = task.spawn(function() return pcall(ch.send, ch, 2) end)
         local selecting = task.spawn(function()
           return task.select({{ ch:on_send(3, function(sent) return sent end) }})
         end)
         yield() yield()
         {close}()
         local _, ok, err = waiting:join()
         closed(ok, err)                           -- the waiting send raises
         local _, sent = selecting:join()
         add(sent)                                 -- false
         closed(pcall(ch.send, ch, 4))
         closed(pcall(ch.try_send, ch, 5))
         add(task.select({{ ch:on_send(6, function(sent) return sent end) }}))
         add(ch:closed())
         add(host_recv())
         add(host_recv())
         return table.concat(out, ' ')"
    ))
}

#[test]
fn closing_the_receiver_makes_lua_sends_raise_and_keeps_queued_values() {
    assert_eq!(
        host_side_close("host_close"),
        "true false channel is closed false false channel is closed false channel is closed false true 1 closed"
    );
}

#[test]
fn dropping_the_receiver_makes_lua_sends_raise() {
    assert_eq!(
        host_side_close("host_drop"),
        "true false channel is closed false false channel is closed false channel is closed false true dropped dropped"
    );
}

#[test]
fn lua_close_lets_the_host_receive_the_queued_values_then_none() {
    let e = paused_env();
    e.local_channel::<i64>("ch", 2);
    let r = e.string(
        "local out = {}
         local function add(x) out[#out + 1] = tostring(x) end
         ch:send(1) ch:send(2)
         local waiting = task.spawn(function() return pcall(ch.send, ch, 3) end)
         yield() yield()
         ch:close() ch:close()
         local _, ok, err = waiting:join()
         add(ok) add(tostring(err):match('ch:send: channel is closed') or tostring(err))
         add(pcall(ch.try_send, ch, 4))
         add(ch:closed())
         add(host_recv()) add(host_recv()) add(host_recv())
         return table.concat(out, ' ')",
    );
    assert_eq!(r, "false ch:send: channel is closed false true 1 2 closed");
}

#[test]
fn lua_close_ends_the_hosts_recv_on_another_thread() {
    let e = env();
    let rx = e.channel::<String>("ch", 8);
    let consumer = drain_on_another_thread(rx);
    e.run("ch:send('a') ch:send('b') ch:close()").unwrap();
    assert_eq!(consumer.join().unwrap(), ["a", "b"]);
}

#[test]
fn collecting_the_lua_side_closes_the_channel_for_the_host() {
    let lua = mlua::Lua::new();
    let vm = Vm::attach(&lua, Config::default()).unwrap();
    vm.task_lib().unwrap();
    let (ch, mut rx) = channel_to_host::<i64>(&lua, 4).unwrap();
    assert!(!rx.is_closed());
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    drop(ch);
    lua.gc_collect().unwrap();
    lua.gc_collect().unwrap();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
}

// ── 5. a cancelled waiting send gives its reservation back ──
//
// cap 1, full.  A starts waiting first (first in tokio's queue), B
// second.  A is cancelled (or its select chooses another case), then
// the host receives: the room must go to B.  If A's reservation were
// kept, tokio would hand the room to A's (abandoned) place and B would
// stay stuck.

fn second_sender_after(first_waiter: &str, stop_first: &str) -> String {
    let e = paused_env();
    e.local_channel::<String>("ch", 1);
    e.string(&format!(
        "local other = task.channel(1)
         ch:send('a')                              -- full
         local a = task.spawn(function() {first_waiter} end)
         yield() yield()                           -- A waits, first in the queue
         local b = task.spawn(function() ch:send('c') return 'b sent' end)
         yield() yield()                           -- B waits, second
         local a_out = (function() {stop_first} end)()
         local first = host_recv()                 -- 'a': frees the room
         local b_out = task.select({{
           b:on(function(ok, v) return v end),
           task.after(1000):on(function() return 'stuck' end),
         }})
         return table.concat({{ a_out, first, b_out, host_recv(), host_recv() }}, ' ')"
    ))
}

#[test]
fn a_cancelled_waiting_send_releases_its_reservation() {
    let r = second_sender_after(
        "ch:send('b') return 'a sent'",
        "a:cancel() local ok, err = a:join() return tostring(ok) .. ',' .. tostring(task.is_cancelled(err))",
    );
    assert_eq!(r, "false,true a b sent c empty");
}

#[test]
fn a_cancelled_select_waiting_on_on_send_releases_its_reservation() {
    let r = second_sender_after(
        "return task.select({ ch:on_send('b', function(sent) return 'a sent' end) })",
        "a:cancel() local ok, err = a:join() return tostring(ok) .. ',' .. tostring(task.is_cancelled(err))",
    );
    assert_eq!(r, "false,true a b sent c empty");
}

#[test]
fn a_select_that_chooses_another_case_releases_its_reservation() {
    let r = second_sender_after(
        "return task.select({ ch:on_send('b', function() return 'a sent' end),
                              other:on(function(v) return 'a took ' .. v end) })",
        "other:try_send('x') local _, v = a:join() return (v:gsub(' ', '_'))",
    );
    assert_eq!(r, "a_took_x a b sent c empty");
}

// ── 6. receiving from a send-only channel raises ──

#[test]
fn receiving_from_a_send_only_channel_raises() {
    let e = env();
    let _rx = e.channel::<i64>("ch", 4);
    let r = e.string(
        "local out = {}
         local function add(ok, err)
           out[#out + 1] = tostring(ok) .. ':' .. (tostring(err):gsub('^runtime error: ', ''):match('^(.-channel is send%-only)') or tostring(err))
         end
         add(pcall(ch.recv, ch))
         add(pcall(ch.try_recv, ch))
         add(pcall(ch.on, ch, function() end))
         add(pcall(ch.arm_recv, ch))
         -- Past the send-only object: the base `Channel` methods and a
         -- case built by hand.
         local Channel = getmetatable(task.channel(1))
         add(pcall(Channel.recv, ch))
         add(pcall(Channel.try_recv, ch))
         add(pcall(task.select, { { kind = 'recv', target = ch._c, handler = function() end } }))
         add(pcall(task.select_raw, { Channel.arm_recv(ch) }))
         return table.concat(out, ' ')",
    );
    assert_eq!(
        r,
        "false:ch:recv: channel is send-only false:ch:try_recv: channel is send-only \
         false:ch:on: channel is send-only false:ch:arm_recv: channel is send-only \
         false:ch:recv: channel is send-only false:ch:try_recv: channel is send-only \
         false:task.select: case 1: channel is send-only \
         false:task.select_raw: case 1: channel is send-only"
    );
}

// ── setup errors ──

fn setup_message(r: Result<(LuaChannel, Receiver<i64>), IsleError>) -> String {
    match r {
        Err(IsleError::Init(f)) => {
            assert_eq!(f.kind, LuaErrorKind::External);
            f.message
        }
        Err(other) => panic!("unexpected error: {other:?}"),
        Ok(_) => panic!("expected an error"),
    }
}

#[test]
fn channel_to_host_errors_without_an_attached_vm_or_task_library_or_with_cap_0() {
    let lua = mlua::Lua::new();
    let m = setup_message(channel_to_host::<i64>(&lua, 4));
    assert!(m.contains("the VM is not attached"), "{m}");
    let vm = Vm::attach(&lua, Config::default()).unwrap();
    let m = setup_message(channel_to_host::<i64>(&lua, 4));
    assert!(m.contains("task library was not created"), "{m}");
    vm.task_lib().unwrap();
    let m = setup_message(channel_to_host::<i64>(&lua, 0));
    assert!(m.contains("cap must be >= 1"), "{m}");
    let m = setup_message(channel_to_host::<i64>(&lua, usize::MAX));
    assert!(m.contains("cap is too large"), "{m}");
}

#[test]
fn the_lua_side_is_a_channel_with_send_len_cap_and_closed() {
    let e = env();
    let mut rx = e.channel::<i64>("ch", 3);
    let r = e.string(
        "ch:send(1) ch:try_send(2)
         return table.concat({ ch:len(), ch:cap(), tostring(ch:closed()) }, ' ')",
    );
    assert_eq!(r, "2 3 false");
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    rx.close();
    assert!(rx.is_closed());
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
}
