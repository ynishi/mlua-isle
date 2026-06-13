#![cfg(all(feature = "pool", feature = "tokio"))]

use mlua_isle::{AsyncIslePool, IsleError, PoolConfig, PoolStrategy};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

// ---------------------------------------------------------------------------
// Construction & validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pool_new_with_valid_config() {
    let pool = AsyncIslePool::new(
        |lua| {
            lua.globals().set("x", 1)?;
            Ok(())
        },
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Cold,
        },
    )
    .unwrap();

    pool.shutdown().await;
}

#[tokio::test]
async fn pool_max_size_zero_returns_error() {
    let result = AsyncIslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 0,
            strategy: PoolStrategy::Cold,
        },
    );
    assert!(matches!(result, Err(IsleError::Init(_))));
}

// ---------------------------------------------------------------------------
// Cold strategy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cold_checkout_eval() {
    let pool = AsyncIslePool::new(
        |lua| {
            lua.globals().set("base", 10)?;
            Ok(())
        },
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Cold,
        },
    )
    .unwrap();

    let isle = pool.checkout().await.unwrap();
    let result = isle.eval("return base + 5").await.unwrap();
    assert_eq!(result, "15");
    drop(isle);

    pool.shutdown().await;
}

#[tokio::test]
async fn cold_does_not_preserve_state() {
    let pool = Arc::new(
        AsyncIslePool::new(
            |_lua| Ok(()),
            PoolConfig {
                max_size: 1,
                strategy: PoolStrategy::Cold,
            },
        )
        .unwrap(),
    );

    {
        let isle = pool.checkout().await.unwrap();
        isle.eval("my_global = 42").await.unwrap();
    }

    // Give the background driver shutdown task a chance to release the
    // active slot before retry-driven checkout.
    for _ in 0..50 {
        if pool.active() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    {
        let isle = pool.checkout().await.unwrap();
        let result = isle.eval("return type(my_global)").await.unwrap();
        assert_eq!(result, "nil", "cold pool must not preserve state");
    }

    pool.shutdown().await;
}

// ---------------------------------------------------------------------------
// Warm strategy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn warm_checkout_eval() {
    let pool = AsyncIslePool::new(
        |lua| {
            lua.globals().set("base", 10)?;
            Ok(())
        },
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let isle = pool.checkout().await.unwrap();
    let result = isle.eval("return base + 5").await.unwrap();
    assert_eq!(result, "15");
    drop(isle);

    pool.shutdown().await;
}

#[tokio::test]
async fn warm_preserves_state() {
    let pool = AsyncIslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    {
        let isle = pool.checkout().await.unwrap();
        isle.eval("my_global = 42").await.unwrap();
    }

    {
        let isle = pool.checkout().await.unwrap();
        let result = isle.eval("return my_global").await.unwrap();
        assert_eq!(result, "42", "warm pool must preserve state");
    }

    pool.shutdown().await;
}

// ---------------------------------------------------------------------------
// try_checkout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn try_checkout_returns_none_when_exhausted() {
    let pool = AsyncIslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let _isle = pool.checkout().await.unwrap();
    assert!(
        pool.try_checkout().await.unwrap().is_none(),
        "should return None when pool exhausted"
    );

    pool.shutdown().await;
}

#[tokio::test]
async fn try_checkout_succeeds_when_available() {
    let pool = AsyncIslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let isle = pool.try_checkout().await.unwrap();
    assert!(isle.is_some(), "should return Some when available");
    let isle = isle.unwrap();
    assert_eq!(isle.eval("return 1").await.unwrap(), "1");
    drop(isle);

    pool.shutdown().await;
}

// ---------------------------------------------------------------------------
// checkout_timeout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn checkout_timeout_returns_pool_exhausted() {
    let pool = AsyncIslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let _isle = pool.checkout().await.unwrap();

    let result = pool.checkout_timeout(Duration::from_millis(50)).await;
    match result {
        Err(IsleError::PoolExhausted(1)) => {}
        other => panic!("expected PoolExhausted(1), got: {other:?}"),
    }

    pool.shutdown().await;
}

#[tokio::test]
async fn checkout_blocks_then_succeeds_after_return() {
    let pool = Arc::new(
        AsyncIslePool::new(
            |_lua| Ok(()),
            PoolConfig {
                max_size: 1,
                strategy: PoolStrategy::Warm,
            },
        )
        .unwrap(),
    );

    let isle = pool.checkout().await.unwrap();

    let pool_c = Arc::clone(&pool);
    let waiter = tokio::spawn(async move {
        let start = Instant::now();
        let isle = pool_c
            .checkout_timeout(Duration::from_secs(5))
            .await
            .unwrap();
        let elapsed = start.elapsed();
        let result = isle.eval("return 'waited'").await.unwrap();
        (result, elapsed)
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(isle);

    let (result, elapsed) = waiter.await.unwrap();
    assert_eq!(result, "waited");
    assert!(
        elapsed >= Duration::from_millis(50),
        "should have waited: {elapsed:?}"
    );

    pool.shutdown().await;
}

// ---------------------------------------------------------------------------
// Concurrency: multiple tasks checking out simultaneously
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_checkouts_all_succeed() {
    let pool = Arc::new(
        AsyncIslePool::new(
            |_lua| Ok(()),
            PoolConfig {
                max_size: 4,
                strategy: PoolStrategy::Warm,
            },
        )
        .unwrap(),
    );

    let mut handles = Vec::new();
    for i in 0..8 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let isle = pool.checkout().await.unwrap();
            let code = format!("return {} * 2", i);
            let result = isle.eval(&code).await.unwrap();
            assert_eq!(result, (i * 2).to_string());
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    pool.shutdown().await;
}

// ---------------------------------------------------------------------------
// Coroutine through pooled isle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pooled_coroutine_eval() {
    let pool = AsyncIslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let isle = pool.checkout().await.unwrap();
    let result = isle.coroutine_eval("return 1 + 2").await.unwrap();
    assert_eq!(result, "3");
    drop(isle);

    pool.shutdown().await;
}

// ---------------------------------------------------------------------------
// kill(): force-discard a poisoned isle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn kill_discards_and_next_checkout_gets_fresh_isle() {
    let pool = AsyncIslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    {
        let mut isle = pool.checkout().await.unwrap();
        isle.eval("sentinel = 'old'").await.unwrap();
        isle.kill();
    }

    // Wait for background driver shutdown to free the active slot.
    for _ in 0..50 {
        if pool.active() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    {
        let isle = pool.checkout().await.unwrap();
        let result = isle.eval("return type(sentinel)").await.unwrap();
        assert_eq!(
            result, "nil",
            "kill() must discard the isle; fresh checkout should have clean state"
        );
    }

    pool.shutdown().await;
}

// ---------------------------------------------------------------------------
// Shutdown behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn checkout_after_shutdown_returns_error() {
    let pool = AsyncIslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    {
        let _isle = pool.checkout().await.unwrap();
    }

    pool.shutdown().await;

    let result = pool.checkout().await;
    assert!(
        matches!(result, Err(IsleError::Shutdown)),
        "checkout after shutdown must return Shutdown error, got: {result:?}"
    );
}

#[tokio::test]
async fn shutdown_cleans_up_idle_isles() {
    let pool = AsyncIslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 4,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    for _ in 0..3 {
        let isle = pool.checkout().await.unwrap();
        isle.eval("return 1").await.unwrap();
    }

    pool.shutdown().await;
    // Should not hang.
}

// ---------------------------------------------------------------------------
// Factory failure
// ---------------------------------------------------------------------------

#[tokio::test]
async fn factory_failure_returns_init_error() {
    let pool = AsyncIslePool::new(
        |_lua| Err(mlua::Error::runtime("factory exploded")),
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Cold,
        },
    )
    .unwrap();

    let result = pool.checkout().await;
    assert!(
        matches!(result, Err(IsleError::Init(_))),
        "factory failure should surface as Init error, got: {result:?}"
    );

    pool.shutdown().await;
}

// ---------------------------------------------------------------------------
// Pool metrics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pool_metrics_track_active_and_idle() {
    let pool = AsyncIslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 3,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    assert_eq!(pool.active(), 0);
    assert_eq!(pool.idle(), 0);

    let isle1 = pool.checkout().await.unwrap();
    assert_eq!(pool.active(), 1);
    assert_eq!(pool.idle(), 0);

    let isle2 = pool.checkout().await.unwrap();
    assert_eq!(pool.active(), 2);
    assert_eq!(pool.idle(), 0);

    drop(isle1);
    assert_eq!(pool.active(), 1);
    assert_eq!(pool.idle(), 1); // warm → returned to idle

    drop(isle2);
    assert_eq!(pool.active(), 0);
    assert_eq!(pool.idle(), 2);

    pool.shutdown().await;
}

// ---------------------------------------------------------------------------
// Scenario: VM isolation — distinct VMs do not share runtime state
// ---------------------------------------------------------------------------

/// With `max_size > 1` and `Warm`, two simultaneously-checked-out VMs
/// must have isolated globals.  Writes on one slot's `Isle` must not be
/// visible from the other slot's `Isle`, even after a warm round-trip.
#[tokio::test]
async fn scenario_vm_isolation_across_concurrent_checkouts() {
    let pool = AsyncIslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let isle_a = pool.checkout().await.unwrap();
    let isle_b = pool.checkout().await.unwrap();

    isle_a.eval("tag = 'A'").await.unwrap();
    isle_b.eval("tag = 'B'").await.unwrap();

    assert_eq!(isle_a.eval("return tag").await.unwrap(), "A");
    assert_eq!(isle_b.eval("return tag").await.unwrap(), "B");

    drop(isle_a);
    drop(isle_b);

    // After warm return, each slot still carries its own tag.  The
    // exact checkout order is not guaranteed, so we collect both
    // observed tags into a set.
    let isle1 = pool.checkout().await.unwrap();
    let isle2 = pool.checkout().await.unwrap();
    let t1 = isle1.eval("return tag").await.unwrap();
    let t2 = isle2.eval("return tag").await.unwrap();
    let mut tags = [t1, t2];
    tags.sort();
    assert_eq!(
        tags,
        ["A".to_string(), "B".to_string()],
        "distinct warm slots must carry their own state"
    );

    drop(isle1);
    drop(isle2);
    pool.shutdown().await;
}

// ---------------------------------------------------------------------------
// Scenario: mixed sync + coroutine workload across the pool
// ---------------------------------------------------------------------------

/// Many tokio tasks checkout from a small pool and exercise both
/// `eval` (sync) and `coroutine_eval` (cooperative) paths.  All tasks
/// must complete and the pool must end with all slots idle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_mixed_workload_eval_and_coroutine() {
    let pool = Arc::new(
        AsyncIslePool::new(
            |lua| {
                lua.load(
                    r#"
                    function double(x)
                        return tonumber(x) * 2
                    end
                    "#,
                )
                .exec()?;
                Ok(())
            },
            PoolConfig {
                max_size: 3,
                strategy: PoolStrategy::Warm,
            },
        )
        .unwrap(),
    );

    let mut handles = Vec::new();
    for i in 0..12 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let isle = pool.checkout().await.unwrap();
            // Alternate: even → coroutine path, odd → sync path.
            if i % 2 == 0 {
                let code = format!("return {} + 100", i);
                let r = isle.coroutine_eval(&code).await.unwrap();
                assert_eq!(r, (i + 100).to_string());
            } else {
                let r = isle.call("double", &[&i.to_string()]).await.unwrap();
                assert_eq!(r, (i * 2).to_string());
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // All 12 tasks completed; pool must converge to active=0 with idle
    // bounded by max_size.
    assert_eq!(pool.active(), 0);
    assert!(pool.idle() <= 3);

    pool.shutdown().await;
}

// ---------------------------------------------------------------------------
// Scenario: lifecycle — kill mid-run, pool respawns, warm reuse resumes
// ---------------------------------------------------------------------------

/// A pool of size 1 with `Warm`: poison the VM, `kill()` it, observe the
/// next checkout get a fresh VM, then verify subsequent warm round-trips
/// preserve state on the new VM.
#[tokio::test]
async fn scenario_lifecycle_kill_then_warm_reuse() {
    let pool = AsyncIslePool::new(
        |lua| {
            lua.globals().set("base", 7)?;
            Ok(())
        },
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    // Step 1: poison and kill.
    {
        let mut isle = pool.checkout().await.unwrap();
        isle.eval("scratch = 'dirty'").await.unwrap();
        isle.kill();
    }

    // Wait for the background driver shutdown to release the slot.
    for _ in 0..50 {
        if pool.active() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Step 2: fresh VM — factory ran again (base=7), scratch is gone.
    {
        let isle = pool.checkout().await.unwrap();
        assert_eq!(isle.eval("return base").await.unwrap(), "7");
        assert_eq!(isle.eval("return type(scratch)").await.unwrap(), "nil");
        isle.eval("scratch = 'clean'").await.unwrap();
    }

    // Step 3: warm round-trip on the new VM preserves state.
    {
        let isle = pool.checkout().await.unwrap();
        assert_eq!(isle.eval("return scratch").await.unwrap(), "clean");
    }

    pool.shutdown().await;
}

