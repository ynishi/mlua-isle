#![cfg(feature = "tokio")]
//! Host channels and requests (`runtime::channel`), run under
//! `Vm::run` and on an `AsyncIsle`.

use mlua_isle::runtime::{
    cancellable, channel, CancelToken, Config, Request, RequestError, SendError, Sender,
    TrySendError, Vm,
};
use mlua_isle::{AsyncIsle, IsleError, LuaErrorKind};
use std::cell::RefCell;
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

/// A VM on a current-thread runtime, with `task` and `sleep(ms)`
/// (cancellable).
struct Env {
    rt: tokio::runtime::Runtime,
    local: tokio::task::LocalSet,
    lua: mlua::Lua,
    vm: Vm,
}

fn env() -> Env {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
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
    let sleep = lua
        .create_async_function(|_, ms: u64| {
            cancellable(async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            })
        })
        .unwrap();
    g.set("sleep", sleep).unwrap();
    Env { rt, local, lua, vm }
}

impl Env {
    /// A host channel of `T`, its Lua side set as the global `name`.
    fn channel<T: mlua::IntoLua + Send + 'static>(&self, name: &str, cap: usize) -> Sender<T> {
        let (tx, ch) = channel::<T>(&self.lua, cap).unwrap();
        self.lua.globals().set(name, ch).unwrap();
        tx
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

    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.local.block_on(&self.rt, fut)
    }

    fn run(&self, src: &str) -> Result<mlua::MultiValue, IsleError> {
        self.block_on(self.root(src))
    }

    fn string(&self, src: &str) -> String {
        to_string(self.run(src))
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

fn to_string(out: Result<mlua::MultiValue, IsleError>) -> String {
    let out = out.unwrap_or_else(|e| panic!("run failed: {e}"));
    match out.front() {
        Some(mlua::Value::String(s)) => s.to_str().unwrap().to_string(),
        other => panic!("expected a string, got {other:?}"),
    }
}

/// Make `push(v)` (a sync host function) `try_send` on `tx` and raise
/// if that fails.
fn push_fn<T: mlua::FromLua + Send + 'static>(e: &Env, tx: Sender<T>) {
    e.func("push", move |_, v: T| {
        tx.try_send(v)
            .map_err(|err| mlua::Error::runtime(format!("push: {err}")))
    });
}

// ── 1. values from another thread, through recv / try_recv / select ──

#[test]
fn values_sent_from_another_thread_arrive_in_order_through_every_receive() {
    const N: i64 = 300;
    let e = env();
    let tx = e.channel::<i64>("ch", 16);
    let producer = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async move {
            for i in 1..=N {
                tx.send(i).await.unwrap();
            }
        });
    });
    let r = e.string(
        "-- Let the producer queue a few, so that try_recv finds a value.
         while ch:len() < 3 do sleep(1) end
         local by = { recv = 0, try_recv = 0, select = 0 }
         local expect, k = 1, 0
         local function check(v, how)
           if v ~= expect then error(how .. ': got ' .. tostring(v) .. ', expected ' .. expect) end
           expect = expect + 1
           by[how] = by[how] + 1
         end
         while true do
           local v, ok, how
           local turn = k % 3
           k = k + 1
           if turn == 0 then
             local ready
             v, ok, ready = ch:try_recv()
             how = 'try_recv'
             if not ready then v, ok = ch:recv() how = 'recv' end
           elseif turn == 1 then
             v, ok = ch:recv() how = 'recv'
           else
             v, ok = task.select({ ch:on(function(v, ok) return v, ok end) })
             how = 'select'
           end
           if not ok then break end
           check(v, how)
         end
         return table.concat({ expect - 1, tostring(by.recv > 0), tostring(by.try_recv > 0),
                               tostring(by.select > 0) }, ' ')",
    );
    producer.join().unwrap();
    assert_eq!(r, format!("{N} true true true"));
}

// ── 2. try_send Full / Closed; send waits while full ──

#[test]
fn try_send_reports_full_and_closed_and_send_waits_while_full() {
    let e = env();
    let tx = e.channel::<i64>("ch", 2);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert!(matches!(tx.try_send(3), Err(TrySendError::Full(3))));

    let r = e.block_on(async {
        let tx2 = tx.clone();
        let sending = tokio::task::spawn_local(async move { tx2.send(3).await });
        // The send task gets to run; the channel is full, so it waits.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(!sending.is_finished(), "send did not wait while full");
        let r = to_string(
            e.root(
                "local a = ch:recv() local b = ch:recv() local c = ch:recv()
                 return table.concat({ a, b, c, ch:len(), ch:cap() }, ' ')",
            )
            .await,
        );
        sending.await.unwrap().unwrap();
        r
    });
    assert_eq!(r, "1 2 3 0 2");

    assert!(!tx.is_closed());
    e.run("ch:close()").unwrap();
    assert!(tx.is_closed());
    assert!(matches!(tx.try_send(9), Err(TrySendError::Closed(9))));
}

// ── 3. dropping every Sender closes after the queued values ──

#[test]
fn dropping_every_sender_closes_the_channel_after_the_queued_values() {
    let e = env();
    let tx = e.channel::<i64>("ch", 4);
    let tx2 = tx.clone();
    tx.try_send(1).unwrap();
    tx2.try_send(2).unwrap();
    drop(tx);
    drop(tx2);
    let r = e.string(
        "local closed_before = ch:closed()
         local a, oka = ch:recv()
         local b, okb = ch:recv()
         local c, okc = ch:recv()
         local d, okd, ready = ch:try_recv()
         return table.concat({ tostring(closed_before), a, tostring(oka), b, tostring(okb),
                               tostring(c), tostring(okc), tostring(d), tostring(okd),
                               tostring(ready), tostring(ch:closed()) }, ' ')",
    );
    assert_eq!(r, "true 1 true 2 true nil false nil false true true");
}

#[test]
fn dropping_the_last_sender_wakes_a_waiting_receiver() {
    let e = env();
    let tx = e.channel::<i64>("ch", 4);
    let slot = Rc::new(RefCell::new(Some(tx)));
    e.func("drop_sender", move |_, ()| {
        slot.borrow_mut().take();
        Ok(())
    });
    let r = e.string(
        "local waiting = task.channel(1)
         local h = task.spawn(function()
           waiting:try_send(true)
           local v, ok = ch:recv()
           return tostring(v) .. ' ' .. tostring(ok)
         end)
         waiting:recv()
         drop_sender()
         local _, r = h:join()
         return r",
    );
    assert_eq!(r, "nil false");
}

// ── 4. Lua close: the host's send fails; queued values stay ──

#[test]
fn lua_close_fails_the_next_host_send_and_keeps_the_queued_values() {
    let e = env();
    let tx = e.channel::<String>("ch", 4);
    tx.try_send("a".into()).unwrap();
    tx.try_send("b".into()).unwrap();
    e.run("ch:close() ch:close()").unwrap();
    let back = e.block_on(tx.send("c".into()));
    assert!(matches!(&back, Err(SendError(v)) if v == "c"), "{back:?}");
    let r = e.string(
        "local a = ch:recv()
         local b = ch:recv()
         local c, ok = ch:recv()
         return table.concat({ a, b, tostring(c), tostring(ok), tostring(ch:closed()) }, ' ')",
    );
    assert_eq!(r, "a b nil false true");
}

// ── 5. send / try_send from Lua raise ──

#[test]
fn send_and_try_send_from_lua_raise() {
    let e = env();
    let _tx = e.channel::<i64>("ch", 4);
    let r = e.string(
        "local ok1, e1 = pcall(ch.send, ch, 1)
         local ok2, e2 = pcall(ch.try_send, ch, 1)
         return table.concat({ tostring(ok1), tostring(e1):match('ch:send: channel is receive%-only') or tostring(e1),
                               tostring(ok2), tostring(e2):match('ch:try_send: channel is receive%-only') or tostring(e2),
                               ch:len() }, ' ')",
    );
    assert_eq!(
        r,
        "false ch:send: channel is receive-only false ch:try_send: channel is receive-only 0"
    );
}

// ── 6. several Lua receivers on one host channel ──
//
// A tokio `Receiver` keeps one receiver waker, replaced by each poll.
// These tests put receiver B to sleep on the host channel first and
// receiver A second, so that A's poll is the last one; then the value
// is pushed (synchronously, on the VM thread) at a moment when A will
// not take it.  The order is fixed by handshakes over a local channel
// (`ready`): each receiver signals and then, in the same resume,
// starts its wait on the host channel, and the root only goes on once
// it has the signal.  Everything runs on one thread, so the outcome
// does not depend on timing; the 1 s timer only bounds the failure
// ("stuck": B was never woken although a value was queued).

#[test]
fn a_receiver_is_woken_when_the_last_one_to_wait_selects_another_case() {
    let e = env();
    let tx = e.channel::<String>("ch", 4);
    push_fn(&e, tx);
    let r = e.string(
        "local ready, got, other = task.channel(4), task.channel(4), task.channel(1)
         local b = task.spawn(function()
           ready:try_send('b')
           local v = ch:recv()
           got:try_send(v)
         end)
         ready:recv()
         local a = task.spawn(function()
           ready:try_send('a')
           return task.select({
             other:on(function() return 'other' end),
             ch:on(function(v) return 'ch:' .. tostring(v) end),
           }, { biased = true })
         end)
         ready:recv()
         -- A is woken by both; its biased select takes `other`.
         other:try_send('x')
         push('v1')
         local _, a_took = a:join()
         local b_took = task.select({
           got:on(function(v) return v end),
           task.after(1000):on(function() return 'stuck' end),
         })
         b:cancel()
         return a_took .. ' ' .. b_took .. ' ' .. ch:len()",
    );
    assert_eq!(r, "other v1 0");
}

#[test]
fn a_receiver_is_woken_when_the_last_one_to_wait_was_cancelled() {
    let e = env();
    let tx = e.channel::<String>("ch", 4);
    push_fn(&e, tx);
    let r = e.string(
        "local ready, got = task.channel(4), task.channel(4)
         local b = task.spawn(function()
           ready:try_send('b')
           local v = ch:recv()
           got:try_send(v)
         end)
         ready:recv()
         local a = task.spawn(function()
           ready:try_send('a')
           return ch:recv()
         end)
         ready:recv()
         a:cancel()
         local a_ok, a_err = a:join()
         push('v1')
         local b_took = task.select({
           got:on(function(v) return v end),
           task.after(1000):on(function() return 'stuck' end),
         })
         b:cancel()
         return tostring(a_ok) .. ' ' .. tostring(task.is_cancelled(a_err)) .. ' ' .. b_took",
    );
    assert_eq!(r, "false true v1");
}

#[test]
fn several_receivers_get_every_value_exactly_once() {
    const N: i64 = 2000;
    let e = env();
    let tx = e.channel::<i64>("ch", 8);
    let producer = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async move {
            for i in 1..=N {
                tx.send(i).await.unwrap();
            }
        });
    });
    let r = e.string(&format!(
        "local seen, total = {{}}, 0
         local function take(v)
           seen[v] = (seen[v] or 0) + 1
           total = total + 1
         end
         local never = task.channel(1)
         local hs = {{}}
         for i = 1, 3 do
           hs[#hs + 1] = task.spawn(function()
             while true do
               local v, ok = ch:recv()
               if not ok then return end
               take(v)
             end
           end)
         end
         for i = 1, 2 do
           hs[#hs + 1] = task.spawn(function()
             while true do
               local stop = task.select({{
                 never:on(function() return true end),
                 ch:on(function(v, ok)
                   if not ok then return true end
                   take(v)
                   return false
                 end),
               }})
               if stop then return end
             end
           end)
         end
         for _, h in ipairs(hs) do h:join() end
         for v = 1, {N} do
           if seen[v] ~= 1 then return 'value ' .. v .. ' seen ' .. tostring(seen[v]) end
         end
         return 'ok ' .. total"
    ));
    producer.join().unwrap();
    assert_eq!(r, format!("ok {N}"));
}

// ── 7. a value taken for a handler never entered is received next ──

/// As `a_value_taken_for_a_handler_that_was_never_entered_goes_back`
/// in `channel_select.rs`: a hook callback fails the call into
/// `select`'s handler wrapper, after the take.
#[test]
fn a_value_taken_for_a_handler_that_was_never_entered_is_received_next() {
    let e = env();
    let tx = e.channel::<String>("ch", 4);
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
    e.func("arm_failure", move |_, ()| {
        armed.set(true);
        Ok(())
    });
    tx.try_send("first".into()).unwrap();
    tx.try_send("second".into()).unwrap();
    push_fn(&e, tx);
    let r = e.string(
        "local entered = 0
         arm_failure()
         local ok, err = pcall(task.select, { ch:on(function() entered = entered + 1 end) })
         push('third')
         local len = ch:len()
         local a = ch:recv()
         local b = ch:try_recv()
         local c = task.select({ ch:on(function(v) return v end) })
         return table.concat({ tostring(ok), tostring(err):match('injected before the handler') or tostring(err),
                               entered, len, a, b, c, ch:len() }, ' ')",
    );
    assert_eq!(
        r,
        "false injected before the handler 0 3 first second third 0"
    );
}

// ── 8. a conversion error is raised to the receiver ──

/// Converts to its number; fails for a negative one.
struct Event(i64);

impl mlua::IntoLua for Event {
    fn into_lua(self, lua: &mlua::Lua) -> mlua::Result<mlua::Value> {
        if self.0 < 0 {
            return Err(mlua::Error::runtime(format!("bad event {}", self.0)));
        }
        self.0.into_lua(lua)
    }
}

#[test]
fn a_conversion_error_is_raised_to_the_receiver_and_the_value_dropped() {
    let e = env();
    let tx = e.channel::<Event>("ch", 8);
    for v in [1, -1, 2, -2, 3, -3, 4] {
        tx.try_send(Event(v)).unwrap();
    }
    let r = e.string(
        "local out = {}
         local function add(x) out[#out + 1] = tostring(x) end
         add(ch:recv())
         local ok, err = pcall(ch.recv, ch)
         add(ok) add(tostring(err):match('bad event %-1'))
         add(ch:recv())
         ok, err = pcall(ch.try_recv, ch)
         add(ok) add(tostring(err):match('bad event %-2'))
         add(ch:recv())
         ok, err = pcall(task.select, { ch:on(function(v) return v end) })
         add(ok) add(tostring(err):match('bad event %-3'))
         add(ch:recv())
         return table.concat(out, ' ')",
    );
    assert_eq!(
        r,
        "1 false bad event -1 2 false bad event -2 3 false bad event -3 4"
    );
}

// ── 9. requests ──

#[test]
fn a_request_is_answered() {
    let e = env();
    let tx = e.channel::<Request<i64, String>>("requests", 4);
    let (lua_out, answer) = e.block_on(async {
        tokio::join!(
            e.root(
                "local req, ok = requests:recv()
                 local delivered = req:reply('answer ' .. req.value * 2)
                 return tostring(ok) .. ' ' .. tostring(delivered) .. ' ' .. tostring(req:replied())"
            ),
            tx.request(21),
        )
    });
    assert_eq!(to_string(lua_out), "true true true");
    assert_eq!(answer.unwrap(), "answer 42");
}

/// `wait_answer()` waits for the requester's outcome (the root cannot
/// return before it: the requester must see it while Lua still runs).
fn request_outcome(e: &Env, src: &str) -> (String, Result<String, RequestError<i64>>) {
    let tx = e.channel::<Request<i64, String>>("requests", 4);
    let (out_tx, out_rx) = tokio::sync::oneshot::channel::<String>();
    let out_rx = Rc::new(RefCell::new(Some(out_rx)));
    let wait = e
        .lua
        .create_async_function(move |_, ()| {
            let rx = out_rx.borrow_mut().take();
            async move {
                let rx = rx.ok_or_else(|| mlua::Error::runtime("wait_answer called twice"))?;
                Ok(rx.await.unwrap_or_else(|_| "dropped".into()))
            }
        })
        .unwrap();
    e.lua.globals().set("wait_answer", wait).unwrap();
    let (lua_out, answer) = e.block_on(async {
        tokio::join!(e.root(src), async {
            let r = tokio::time::timeout(Duration::from_secs(5), tx.request(5))
                .await
                .expect("request timed out");
            let _ = out_tx.send(format!("{r:?}"));
            r
        })
    });
    (to_string(lua_out), answer)
}

#[test]
fn closing_an_unanswered_request_answers_no_reply_at_once() {
    let e = env();
    let (lua, answer) = request_outcome(
        &e,
        "collectgarbage('stop')
         do
           local req <close> = requests:recv()
         end
         return wait_answer()",
    );
    assert_eq!(lua, "Err(NoReply)");
    assert!(matches!(answer, Err(RequestError::NoReply)));
}

#[test]
fn closing_an_answered_request_does_nothing() {
    let e = env();
    let (lua, answer) = request_outcome(
        &e,
        "local delivered
         do
           local req <close> = requests:recv()
           delivered = req:reply('yes')
         end
         return tostring(delivered) .. ' ' .. wait_answer()",
    );
    assert_eq!(lua, "true Ok(\"yes\")");
    assert_eq!(answer.unwrap(), "yes");
}

#[test]
fn a_request_dropped_unanswered_answers_no_reply_when_collected() {
    let e = env();
    let (lua, answer) = request_outcome(
        &e,
        "local function take() local req = requests:recv() end
         take()
         collectgarbage() collectgarbage()
         return wait_answer()",
    );
    assert_eq!(lua, "Err(NoReply)");
    assert!(matches!(answer, Err(RequestError::NoReply)));
}

#[test]
fn a_reply_after_the_requester_stopped_waiting_returns_false() {
    let e = env();
    let tx = e.channel::<Request<i64, String>>("requests", 4);
    e.block_on(async {
        // Poll the request once: it is sent (there is room) and then
        // waits for the reply.  Then drop it, as a timeout would.
        let mut fut = pin!(tx.request(7));
        std::future::poll_fn(|cx| {
            assert!(fut.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    });
    let r = e.string(
        "local n = requests:len()
         local req = requests:recv()
         local delivered = req:reply('late')
         return table.concat({ n, req.value, tostring(delivered), tostring(req:replied()) }, ' ')",
    );
    assert_eq!(r, "1 7 false true");
}

#[test]
fn a_second_reply_raises() {
    let e = env();
    let (lua, answer) = request_outcome(
        &e,
        "local req = requests:recv()
         req:reply('once')
         local ok, err = pcall(req.reply, req, 'twice')
         return tostring(ok) .. ' ' .. (tostring(err):match('req:reply: request already answered') or tostring(err))
                .. ' ' .. wait_answer()",
    );
    assert_eq!(
        lua,
        "false req:reply: request already answered Ok(\"once\")"
    );
    assert_eq!(answer.unwrap(), "once");
}

#[test]
fn a_reply_that_fails_to_convert_raises_and_can_be_answered_again() {
    let e = env();
    let tx = e.channel::<Request<i64, i64>>("requests", 4);
    let (lua_out, answer) = e.block_on(async {
        tokio::join!(
            e.root(
                "local req = requests:recv()
                 local ok = pcall(req.reply, req, {})
                 local replied = req:replied()
                 local delivered = req:reply(req.value + 1)
                 return tostring(ok) .. ' ' .. tostring(replied) .. ' ' .. tostring(delivered)"
            ),
            tx.request(41),
        )
    });
    assert_eq!(to_string(lua_out), "false false true");
    assert_eq!(answer.unwrap(), 42);
}

#[test]
fn a_reply_to_a_closed_request_raises() {
    let e = env();
    let (lua, answer) = request_outcome(
        &e,
        "local req = requests:recv()
         do local closing <close> = req end
         local ok, err = pcall(req.reply, req, 'late')
         return tostring(ok) .. ' ' .. (tostring(err):match('req:reply: request is closed') or tostring(err))
                .. ' ' .. wait_answer()",
    );
    assert_eq!(lua, "false req:reply: request is closed Err(NoReply)");
    assert!(matches!(answer, Err(RequestError::NoReply)));
}

#[test]
fn a_request_on_a_closed_channel_gives_the_value_back() {
    let e = env();
    let tx = e.channel::<Request<String, String>>("requests", 4);
    e.run("requests:close()").unwrap();
    let r = e.block_on(tx.request("hello".into()));
    assert!(
        matches!(&r, Err(RequestError::Closed(v)) if v == "hello"),
        "{r:?}"
    );
}

#[test]
fn a_request_is_received_through_select() {
    let e = env();
    let tx = e.channel::<Request<i64, i64>>("requests", 4);
    let (lua_out, answer) = e.block_on(async {
        tokio::join!(
            e.root(
                "return task.select({ requests:on(function(req, ok)
                   if ok then return tostring(req:reply(req.value * 10)) end
                 end) })"
            ),
            tx.request(4),
        )
    });
    assert_eq!(to_string(lua_out), "true");
    assert_eq!(answer.unwrap(), 40);
}

// ── 10. runtime::channel needs an attached VM with its task library ──

fn setup_message(r: Result<(Sender<i64>, mlua_isle::runtime::LuaChannel), IsleError>) -> String {
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
fn channel_errors_without_an_attached_vm_or_task_library() {
    let lua = mlua::Lua::new();
    let m = setup_message(channel::<i64>(&lua, 4));
    assert!(m.contains("the VM is not attached"), "{m}");

    let vm = Vm::attach(&lua, Config::default()).unwrap();
    let m = setup_message(channel::<i64>(&lua, 4));
    assert!(m.contains("task library was not created"), "{m}");

    vm.task_lib().unwrap();
    let m = setup_message(channel::<i64>(&lua, 0));
    assert!(m.contains("cap = 0"), "{m}");
    let m = setup_message(channel::<i64>(&lua, usize::MAX));
    assert!(m.contains("cap is too large"), "{m}");

    let (tx, ch) = channel::<i64>(&lua, 4).unwrap();
    assert!(!tx.is_closed());
    assert!(ch.table().get::<mlua::Function>("recv").is_ok());
    // The Lua side going away closes the channel for the host.
    drop(ch);
    lua.gc_collect().unwrap();
    lua.gc_collect().unwrap();
    assert!(tx.is_closed());
    assert!(matches!(tx.try_send(1), Err(TrySendError::Closed(1))));
}

#[test]
fn the_lua_side_has_the_task_channel_methods_and_len_and_cap() {
    let e = env();
    let tx = e.channel::<i64>("ch", 3);
    tx.try_send(10).unwrap();
    tx.try_send(20).unwrap();
    let r = e.string(
        "local same = getmetatable(ch) == getmetatable(task.channel(1))
         return table.concat({ tostring(same), ch:len(), ch:cap(), tostring(ch:closed()) }, ' ')",
    );
    assert_eq!(r, "true 2 3 false");
}

// ── 11. AsyncIsle: created in exec, fed from another tokio task ──

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_async_isle_receives_from_another_tokio_task_and_answers_requests() {
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        let vm = Vm::attach(lua, Config::default())?;
        lua.globals().set("task", vm.task_lib()?)
    })
    .await
    .unwrap();
    let (events, requests) = isle
        .exec(|lua| {
            let (events, ch) = channel::<String>(lua, 16)?;
            lua.globals().set("events", ch)?;
            let (requests, ch) = channel::<Request<i64, i64>>(lua, 16)?;
            lua.globals().set("requests", ch)?;
            Ok((events, requests))
        })
        .await
        .unwrap();

    let producer = tokio::spawn(async move {
        for i in 1..=5 {
            events.send(format!("e{i}")).await.unwrap();
        }
        // `events` dropped: the channel closes after the queued values.
    });
    let requester = tokio::spawn(async move {
        let mut answers = Vec::new();
        for i in 1..=3 {
            answers.push(requests.request(i).await.unwrap());
        }
        answers
    });

    let seen: String = isle
        .coroutine_eval(
            "local seen, answered, events_open = {}, 0, true
             while events_open or answered < 3 do
               task.select({
                 events:on(function(ev, ok)
                   if ok then seen[#seen + 1] = ev else events_open = false end
                 end),
                 requests:on(function(req, ok)
                   if ok then req:reply(req.value * 100) answered = answered + 1 end
                 end),
               })
               if not events_open then
                 -- Drained and closed: wait only for the requests.
                 while answered < 3 do
                   local req = requests:recv()
                   req:reply(req.value * 100)
                   answered = answered + 1
                 end
               end
             end
             return table.concat(seen, ',')",
        )
        .await
        .unwrap();
    producer.await.unwrap();
    assert_eq!(seen, "e1,e2,e3,e4,e5");
    assert_eq!(requester.await.unwrap(), vec![100, 200, 300]);
    driver.shutdown().await.unwrap();
}
