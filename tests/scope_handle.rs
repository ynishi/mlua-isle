#![cfg(feature = "tokio")]
//! Host tasks in a request's scope: `runtime::current_scope` and
//! `ScopeHandle::spawn_local` (issue #8).

use mlua_isle::runtime::{
    cancellable, current_scope, current_token, CancelToken, Config, ScopeHandle, ScopedTask, Vm,
};
use mlua_isle::{AsyncIsle, IsleError};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn within<F: std::future::Future>(ms: u64, f: F) -> F::Output {
    tokio::time::timeout(Duration::from_millis(ms), f)
        .await
        .expect("timed out")
}

/// Where a host function keeps the handles of the tasks it spawned:
/// dropping a `ScopedTask` cancels its task.
type Keep = Rc<RefCell<Vec<ScopedTask<()>>>>;

/// Sets its flag when dropped.
struct Guard(Rc<Cell<bool>>);

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

/// A VM on its own current-thread runtime and `LocalSet`, with `task`,
/// `sleep(ms)` (cancellable) and `hold(ms)` (not cancellable).
struct Local {
    rt: tokio::runtime::Runtime,
    local: tokio::task::LocalSet,
    lua: mlua::Lua,
    vm: Vm,
}

impl Local {
    fn new(config: Config) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let lua = mlua::Lua::new();
        let vm = Vm::attach(&lua, config).unwrap();
        lua.globals().set("task", vm.task_lib().unwrap()).unwrap();
        let sleep = lua
            .create_async_function(|_, ms: u64| {
                cancellable(async move {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(())
                })
            })
            .unwrap();
        lua.globals().set("sleep", sleep).unwrap();
        let hold = lua
            .create_async_function(|_, ms: u64| async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            })
            .unwrap();
        lua.globals().set("hold", hold).unwrap();
        Self {
            rt,
            local: tokio::task::LocalSet::new(),
            lua,
            vm,
        }
    }

    fn set_fn<F>(&self, name: &str, f: F)
    where
        F: Fn(&mlua::Lua, ()) -> mlua::Result<()> + 'static,
    {
        let f = self.lua.create_function(f).unwrap();
        self.lua.globals().set(name, f).unwrap();
    }

    /// Run `src` under `Vm::run` (failing the test after 10 s); when
    /// `cancel_on` is given, cancel the token as soon as that notify
    /// fires.
    fn run(
        &self,
        src: &str,
        cancel_on: Option<Rc<tokio::sync::Notify>>,
    ) -> Result<mlua::MultiValue, IsleError> {
        let f = self.lua.load(src).into_function().unwrap();
        let token = CancelToken::new();
        self.local.block_on(&self.rt, async {
            if let Some(n) = cancel_on {
                let t = token.clone();
                tokio::task::spawn_local(async move {
                    n.notified().await;
                    t.cancel();
                });
            }
            tokio::time::timeout(Duration::from_secs(10), self.vm.run(&token, f, ()))
                .await
                .expect("timed out")
        })
    }
}

fn first_int(out: Result<mlua::MultiValue, IsleError>) -> i64 {
    out.unwrap()
        .into_iter()
        .next()
        .and_then(|v| v.as_i64())
        .unwrap()
}

// ── 1. the request waits for a host task to drop ──

#[test]
fn cancelling_vm_run_waits_for_a_host_task_to_drop() {
    let l = Local::new(Config::default());
    let dropped = Rc::new(Cell::new(false));
    let started = Rc::new(tokio::sync::Notify::new());
    let keep: Keep = Default::default();
    let (d, s, k) = (dropped.clone(), started.clone(), keep.clone());
    l.set_fn("bg", move |_, ()| {
        let (d, s) = (d.clone(), s.clone());
        let task = current_scope().unwrap().spawn_local(async move {
            let _g = Guard(d);
            s.notify_one();
            // Never looks at its token: only the drop ends it.
            std::future::pending::<()>().await;
        });
        k.borrow_mut().push(task);
        Ok(())
    });

    let out = l.run("bg() return sleep(5000)", Some(started));
    assert!(matches!(out.unwrap_err(), IsleError::Cancelled));
    assert!(dropped.get(), "run resolved before the host task dropped");
    let task = keep.borrow_mut().pop().unwrap();
    assert!(matches!(
        l.local.block_on(&l.rt, task),
        Err(IsleError::Cancelled)
    ));
}

#[test]
fn a_host_task_spawned_from_a_host_task_is_waited_for_too() {
    let l = Local::new(Config::default());
    let outer = Rc::new(Cell::new(false));
    let inner = Rc::new(Cell::new(false));
    let started = Rc::new(tokio::sync::Notify::new());
    let keep: Keep = Default::default();
    let (o, i, s, k) = (outer.clone(), inner.clone(), started.clone(), keep.clone());
    l.set_fn("bg", move |_, ()| {
        let (o, i, s) = (o.clone(), i.clone(), s.clone());
        let task = current_scope().unwrap().spawn_local(async move {
            let _g = Guard(o);
            // Inside a host task, `current_scope` is that task's scope.
            let _inner = current_scope().unwrap().spawn_local(async move {
                let _g = Guard(i);
                s.notify_one();
                std::future::pending::<()>().await;
            });
            std::future::pending::<()>().await;
        });
        k.borrow_mut().push(task);
        Ok(())
    });

    let out = l.run("bg() return sleep(5000)", Some(started));
    assert!(matches!(out.unwrap_err(), IsleError::Cancelled));
    assert!(outer.get() && inner.get(), "a host task outlived run");
}

#[test]
fn an_unjoined_host_task_is_cancelled_and_waited_for_when_the_request_ends() {
    let l = Local::new(Config::default());
    let dropped = Rc::new(Cell::new(false));
    // Keeps the handle alive (not awaited, not dropped) past the request.
    let held: Rc<RefCell<Vec<ScopedTask<()>>>> = Default::default();
    let (d, h) = (dropped.clone(), held.clone());
    l.set_fn("bg", move |_, ()| {
        let d = d.clone();
        let task = current_scope().unwrap().spawn_local(async move {
            let _g = Guard(d);
            std::future::pending::<()>().await;
        });
        h.borrow_mut().push(task);
        Ok(())
    });

    assert_eq!(first_int(l.run("bg() return 7", None)), 7);
    assert!(dropped.get(), "run resolved before the host task dropped");
    assert!(matches!(
        l.local.block_on(&l.rt, held.borrow_mut().pop().unwrap()),
        Err(IsleError::Cancelled)
    ));
}

// ── 2. case A: nothing runs into the next request ──

/// How `ticker()` lets go of its `ScopedTask`.
#[derive(Clone, Copy)]
enum Handle {
    /// Kept in a holder for the life of the VM.
    Held,
    /// `detach()`ed at once: the fire-and-forget shape.
    Detached,
}

/// An isle with `ticker()`, which spawns through the scope a loop that
/// increments `ticks` every 5 ms and never looks at its token, and
/// `hold(ms)` (not cancellable).
async fn ticker_isle(
    grace: Duration,
    ticks: Arc<AtomicUsize>,
    handle: Handle,
) -> (AsyncIsle, mlua_isle::AsyncIsleDriver) {
    AsyncIsle::builder()
        .config(Config {
            grace,
            ..Default::default()
        })
        .spawn(move |lua| {
            let keep: Keep = Default::default();
            let ticker = lua.create_function(move |_, ()| {
                let ticks = ticks.clone();
                let task = current_scope().unwrap().spawn_local(async move {
                    loop {
                        ticks.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                });
                match handle {
                    Handle::Held => keep.borrow_mut().push(task),
                    Handle::Detached => task.detach(),
                }
                Ok(())
            })?;
            lua.globals().set("ticker", ticker)?;
            let hold = lua.create_async_function(|_, ms: u64| async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            })?;
            lua.globals().set("hold", hold)
        })
        .await
        .unwrap()
}

async fn wait_for_ticks(ticks: &AtomicUsize, n: usize) {
    within(5000, async {
        while ticks.load(Ordering::SeqCst) < n {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn a_cancelled_requests_host_task_is_gone_before_the_next_request() {
    case_a(Handle::Held).await;
}

#[tokio::test]
async fn a_cancelled_requests_detached_host_task_is_gone_before_the_next_request() {
    case_a(Handle::Detached).await;
}

async fn case_a(handle: Handle) {
    let ticks = Arc::new(AtomicUsize::new(0));
    let (isle, driver) = ticker_isle(Duration::ZERO, ticks.clone(), handle).await;
    let task = isle.spawn_coroutine_eval::<()>("ticker() hold(5000)");
    // With a zero grace a cancelled task gets one poll, so one tick; a
    // second tick shows the handle (held or detached) did not cancel it.
    wait_for_ticks(&ticks, 2).await;
    task.cancel();
    assert!(matches!(
        within(5000, task).await.unwrap_err(),
        IsleError::Cancelled
    ));
    let at_resolve = ticks.load(Ordering::SeqCst);

    within(5000, isle.coroutine_eval::<()>("hold(50)"))
        .await
        .unwrap();
    assert_eq!(
        ticks.load(Ordering::SeqCst),
        at_resolve,
        "the host task ran alongside the next request"
    );
    within(5000, driver.shutdown()).await.unwrap();
}

// ── 3. case B: shutdown returns although the host future ignores its token ──

#[tokio::test]
async fn shutdown_returns_after_cancelling_a_request_with_a_token_blind_host_task() {
    case_b(Handle::Held).await;
}

#[tokio::test]
async fn shutdown_returns_after_cancelling_a_request_with_a_detached_token_blind_host_task() {
    case_b(Handle::Detached).await;
}

async fn case_b(handle: Handle) {
    let ticks = Arc::new(AtomicUsize::new(0));
    let (isle, driver) = ticker_isle(Duration::from_millis(100), ticks.clone(), handle).await;
    let task = isle.spawn_coroutine_eval::<()>("ticker() hold(5000)");
    wait_for_ticks(&ticks, 2).await;
    task.cancel();
    assert!(matches!(
        within(5000, task).await.unwrap_err(),
        IsleError::Cancelled
    ));
    // Without the scope (`current_token().child_token()` + a bare
    // `spawn_local`) this loop keeps the driver's `LocalSet` busy and
    // shutdown never returns; that reference case is not run here.
    within(5000, driver.shutdown()).await.unwrap();
}

// ── 4. the task's token: cancellable and current_token ──

#[test]
fn cancellable_inside_a_host_task_returns_the_cancel_error() {
    let l = Local::new(Config::default());
    let started = Rc::new(tokio::sync::Notify::new());
    let seen_token: Rc<RefCell<Option<CancelToken>>> = Default::default();
    let result: Rc<RefCell<Option<Result<(), IsleError>>>> = Default::default();
    let request_token: Rc<RefCell<Option<CancelToken>>> = Default::default();
    let keep: Keep = Default::default();
    let (s, st, r, rt, k) = (
        started.clone(),
        seen_token.clone(),
        result.clone(),
        request_token.clone(),
        keep.clone(),
    );
    l.set_fn("bg", move |_, ()| {
        let (s, st, r) = (s.clone(), st.clone(), r.clone());
        let scope = current_scope().unwrap();
        *rt.borrow_mut() = Some(scope.token().clone());
        let task = scope.spawn_local(async move {
            *st.borrow_mut() = current_token();
            assert!(current_scope().is_some());
            s.notify_one();
            let out = cancellable(std::future::pending::<mlua::Result<()>>()).await;
            *r.borrow_mut() = Some(out.map_err(IsleError::from));
        });
        k.borrow_mut().push(task);
        Ok(())
    });

    let out = l.run("bg() return sleep(5000)", Some(started));
    assert!(matches!(out.unwrap_err(), IsleError::Cancelled));
    assert!(
        matches!(result.borrow_mut().take(), Some(Err(IsleError::Cancelled))),
        "cancellable did not return the cancel error"
    );
    let seen = seen_token.borrow_mut().take().expect("no current token");
    assert!(seen.is_cancelled());
    assert!(request_token.borrow().as_ref().unwrap().is_cancelled());
    // The future itself finished (with the cancel error as its value).
    let task = keep.borrow_mut().pop().unwrap();
    assert!(matches!(l.local.block_on(&l.rt, task), Ok(())));
}

// ── 5. where current_scope is Some ──

#[tokio::test]
async fn current_scope_is_none_in_a_sync_request_and_some_in_a_coroutine_request() {
    assert!(current_scope().is_none());
    let (isle, driver) = AsyncIsle::spawn(|lua| {
        let has = lua.create_function(|_, ()| Ok(current_scope().is_some()))?;
        lua.globals().set("has_scope", has)
    })
    .await
    .unwrap();
    assert!(isle
        .eval::<bool>("return has_scope() == false")
        .await
        .unwrap());
    assert!(isle
        .coroutine_eval::<bool>("return has_scope() == true")
        .await
        .unwrap());
    assert!(current_scope().is_none());
    driver.shutdown().await.unwrap();
}

#[test]
fn current_scope_is_some_inside_vm_run_and_none_outside() {
    let l = Local::new(Config::default());
    let has = l
        .lua
        .create_function(|_, ()| Ok(current_scope().is_some()))
        .unwrap();
    l.lua.globals().set("has_scope", has).unwrap();
    let out = l.run("return has_scope()", None).unwrap();
    assert_eq!(
        out.into_iter().next().and_then(|v| v.as_boolean()),
        Some(true)
    );
    assert!(current_scope().is_none());
    // Outside a request but on the LocalSet.
    l.local
        .block_on(&l.rt, async { assert!(current_scope().is_none()) });
}

// ── 6. the ScopedTask handle ──

#[test]
fn an_awaited_scoped_task_yields_its_value() {
    let l = Local::new(Config::default());
    let compute = l
        .lua
        .create_async_function(|_, x: i64| {
            // Taken before the first await, moved into the future.
            let scope = current_scope().unwrap();
            async move {
                let task = scope.spawn_local(async move {
                    tokio::task::yield_now().await;
                    x * 2
                });
                task.await.map_err(mlua::Error::external)
            }
        })
        .unwrap();
    l.lua.globals().set("compute", compute).unwrap();
    assert_eq!(first_int(l.run("return compute(21)", None)), 42);
}

#[test]
fn dropping_a_scoped_task_cancels_it_and_the_request_still_waits() {
    let l = Local::new(Config {
        grace: Duration::from_millis(50),
        ..Default::default()
    });
    let dropped = Rc::new(Cell::new(false));
    let task_token: Rc<RefCell<Option<CancelToken>>> = Default::default();
    let request_token: Rc<RefCell<Option<CancelToken>>> = Default::default();
    // (task token cancelled, request token cancelled), read while the
    // request's body is still running.
    let seen: Rc<Cell<Option<(bool, bool)>>> = Default::default();
    let (d, tt, rt) = (dropped.clone(), task_token.clone(), request_token.clone());
    l.set_fn("bg", move |_, ()| {
        let (d, tt) = (d.clone(), tt.clone());
        let scope = current_scope().unwrap();
        *rt.borrow_mut() = Some(scope.token().clone());
        let task = scope.spawn_local(async move {
            let _g = Guard(d);
            let token = current_token().unwrap();
            *tt.borrow_mut() = Some(token.clone());
            token.cancelled().await;
            // Ignores the cancel: only the drop at the end of the grace
            // ends it.
            std::future::pending::<()>().await;
        });
        drop(task);
        Ok(())
    });
    let (tt, rt, sn) = (task_token.clone(), request_token.clone(), seen.clone());
    l.set_fn("check", move |_, ()| {
        let t = tt
            .borrow()
            .as_ref()
            .expect("task not polled yet")
            .is_cancelled();
        let r = rt.borrow().as_ref().unwrap().is_cancelled();
        sn.set(Some((t, r)));
        Ok(())
    });

    // `hold(30)` yields, so the task is polled while the body still runs;
    // only the drop of the handle can have cancelled it by `check()`.
    assert_eq!(first_int(l.run("bg() hold(30) check() return 1", None)), 1);
    assert_eq!(
        seen.get(),
        Some((true, false)),
        "(task cancelled, request cancelled) while the body ran"
    );
    assert!(dropped.get(), "run resolved before the host task dropped");
    assert!(!request_token.borrow().as_ref().unwrap().is_cancelled());
}

#[test]
fn a_detached_host_task_runs_on_and_is_still_cancelled_and_waited_for() {
    let l = Local::new(Config::default());
    let dropped = Rc::new(Cell::new(false));
    let count = Rc::new(Cell::new(0usize));
    let ran_on = Rc::new(tokio::sync::Notify::new());
    let (d, c, r) = (dropped.clone(), count.clone(), ran_on.clone());
    l.set_fn("bg", move |_, ()| {
        let (d, c, r) = (d.clone(), c.clone(), r.clone());
        current_scope()
            .unwrap()
            .spawn_local(async move {
                let _g = Guard(d);
                loop {
                    c.set(c.get() + 1);
                    if c.get() == 3 {
                        r.notify_one();
                    }
                    // Never looks at its token.
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .detach();
        Ok(())
    });

    // The request is cancelled only once the detached task has looped
    // three times, i.e. well after its handle was gone.
    let out = l.run("bg() return sleep(5000)", Some(ran_on));
    assert!(matches!(out.unwrap_err(), IsleError::Cancelled));
    assert!(count.get() >= 3);
    assert!(
        dropped.get(),
        "run resolved before the detached task dropped"
    );
}

#[test]
fn a_host_task_spawned_while_its_scope_drains_is_cancelled_and_waited_for() {
    // A grace, so that the drain has remaining time when the late task
    // is spawned (with a zero grace the spawn would start nothing).
    let l = Local::new(Config {
        grace: Duration::from_millis(200),
        ..Default::default()
    });
    let late = Rc::new(Cell::new(false));
    let keep: Keep = Default::default();
    let (d, k) = (late.clone(), keep.clone());
    l.set_fn("bg", move |_, ()| {
        let (d, k2) = (d.clone(), k.clone());
        let parent = current_scope().unwrap();
        let p = parent.clone();
        let task = parent.spawn_local(async move {
            // Cancelled when the request's body has ended.
            current_token().unwrap().cancelled().await;
            let task = p.spawn_local(async move {
                let _g = Guard(d);
                std::future::pending::<()>().await;
            });
            k2.borrow_mut().push(task);
        });
        k.borrow_mut().push(task);
        Ok(())
    });

    assert_eq!(first_int(l.run("bg() return 3", None)), 3);
    assert!(late.get(), "a task added while draining outlived run");
}

#[test]
fn spawning_through_a_handle_after_its_request_ended_starts_nothing() {
    let l = Local::new(Config::default());
    let stash: Rc<RefCell<Option<ScopeHandle>>> = Default::default();
    let s = stash.clone();
    l.set_fn("keep", move |_, ()| {
        *s.borrow_mut() = current_scope();
        Ok(())
    });
    assert_eq!(first_int(l.run("keep() return 5", None)), 5);

    let handle = stash.borrow_mut().take().unwrap();
    let ran = Rc::new(Cell::new(false));
    let r = ran.clone();
    let out = l.local.block_on(&l.rt, async move {
        handle
            .spawn_local(async move {
                r.set(true);
            })
            .await
    });
    assert!(matches!(out.unwrap_err(), IsleError::Cancelled));
    assert!(!ran.get());
}

// ── 7. one grace deadline, host tasks included ──

#[test]
fn a_host_task_spawned_during_cleanup_gets_the_remaining_grace() {
    let l = Local::new(Config {
        grace: Duration::from_millis(1000),
        ..Default::default()
    });
    let dropped_at: Rc<Cell<Option<Instant>>> = Default::default();
    let keep: Keep = Default::default();
    let (d, k) = (dropped_at.clone(), keep.clone());
    l.set_fn("hspawn", move |_, ()| {
        struct At(Rc<Cell<Option<Instant>>>);
        impl Drop for At {
            fn drop(&mut self) {
                self.0.set(Some(Instant::now()));
            }
        }
        let d = d.clone();
        let task = current_scope().unwrap().spawn_local(async move {
            let _g = At(d);
            tokio::time::sleep(Duration::from_millis(5000)).await;
        });
        k.borrow_mut().push(task);
        Ok(())
    });
    let src = "local cleanup <close> = setmetatable({}, { __close = function()
           hold(600)
           hspawn()
           hold(3000)
         end })
         return sleep(5000)";
    let f = l.lua.load(src).into_function().unwrap();
    let token = CancelToken::new();
    let (out, cancelled_at, resolved_at) = l.local.block_on(&l.rt, async {
        let t = token.clone();
        let canceller = tokio::task::spawn_local(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let at = Instant::now();
            t.cancel();
            at
        });
        let out = l.vm.run(&token, f, ()).await;
        let resolved_at = Instant::now();
        (out, canceller.await.unwrap(), resolved_at)
    });
    assert!(matches!(out.unwrap_err(), IsleError::Cancelled));
    let dropped_at = dropped_at.get().expect("the host task was not dropped");
    assert!(dropped_at <= resolved_at);
    let waited = dropped_at.duration_since(cancelled_at);
    // One deadline at cancel + 1000 ms.  A fresh grace from the host
    // task's start would drop it at about cancel + 1600 ms.
    assert!(
        waited >= Duration::from_millis(990) && waited < Duration::from_millis(1400),
        "dropped after {waited:?}"
    );
    let total = resolved_at.duration_since(cancelled_at);
    assert!(
        total < Duration::from_millis(1400),
        "resolved after {total:?}"
    );
}

/// One link of a chain: counts itself, ignores its cancel, and after
/// 150 ms spawns the next link into `scope` (up to `max` links).
fn link(
    scope: ScopeHandle,
    n: usize,
    max: usize,
    links: Rc<Cell<usize>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> {
    Box::pin(async move {
        links.set(links.get() + 1);
        tokio::time::sleep(Duration::from_millis(150)).await;
        if n + 1 < max {
            scope
                .spawn_local(link(scope.clone(), n + 1, max, links))
                .detach();
        }
        std::future::pending::<()>().await;
    })
}

#[test]
fn tasks_spawned_into_a_draining_scope_share_its_remaining_time() {
    // Grace 300 ms; each link spawns the next after 150 ms.  With one
    // deadline for the drain, the chain is dropped about 300 ms after the
    // body ended.  With a fresh grace per link it would run for about
    // 20 links x 150 ms = 3 s.
    let l = Local::new(Config {
        grace: Duration::from_millis(300),
        ..Default::default()
    });
    let links = Rc::new(Cell::new(0usize));
    let body_end: Rc<Cell<Option<Instant>>> = Default::default();
    let lk = links.clone();
    l.set_fn("chain", move |_, ()| {
        let scope = current_scope().unwrap();
        scope
            .spawn_local(link(scope.clone(), 0, 20, lk.clone()))
            .detach();
        Ok(())
    });
    let be = body_end.clone();
    l.set_fn("mark", move |_, ()| {
        be.set(Some(Instant::now()));
        Ok(())
    });

    assert_eq!(first_int(l.run("chain() mark() return 1", None)), 1);
    let drained = body_end.get().unwrap().elapsed();
    assert!(
        drained < Duration::from_millis(600),
        "the drain took {drained:?} after the body ended ({} links)",
        links.get()
    );
    assert!(links.get() < 20, "{} links ran", links.get());
}

#[test]
fn a_spawn_after_the_drain_deadline_starts_nothing() {
    // Each link spawns its replacement on its first poll, with no await
    // in between.  A task started after the deadline still gets one
    // poll, so without the rule the chain would never end.
    let l = Local::new(Config {
        grace: Duration::from_millis(50),
        ..Default::default()
    });
    let links = Rc::new(Cell::new(0usize));
    let last: Rc<RefCell<Option<ScopedTask<()>>>> = Default::default();

    fn sync_link(
        scope: ScopeHandle,
        links: Rc<Cell<usize>>,
        last: Rc<RefCell<Option<ScopedTask<()>>>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> {
        Box::pin(async move {
            links.set(links.get() + 1);
            let next = scope.spawn_local(sync_link(scope.clone(), links, last.clone()));
            // Replacing the previous handle drops (cancels) it; every
            // link is cancelled already, so that changes nothing.
            *last.borrow_mut() = Some(next);
            // Ignores its cancel: only the drop ends it.
            std::future::pending::<()>().await;
        })
    }

    let (lk, ls) = (links.clone(), last.clone());
    l.set_fn("chain", move |_, ()| {
        let scope = current_scope().unwrap();
        scope
            .spawn_local(sync_link(scope.clone(), lk.clone(), ls.clone()))
            .detach();
        Ok(())
    });

    let start = Instant::now();
    assert_eq!(first_int(l.run("chain() return 1", None)), 1);
    let took = start.elapsed();
    assert!(
        took < Duration::from_millis(500),
        "run resolved after {took:?}"
    );
    let n = links.get();
    assert!((1..1_000_000).contains(&n), "{n} links ran");
    // The last spawn came after the deadline: it started nothing.
    let refused = last.borrow_mut().take().expect("no spawn recorded");
    assert!(matches!(
        l.local.block_on(&l.rt, refused),
        Err(IsleError::Cancelled)
    ));
    assert_eq!(links.get(), n, "the refused spawn ran its future");
}
