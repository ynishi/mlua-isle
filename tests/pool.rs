#![cfg(feature = "pool")]

use mlua_isle::{IsleError, IslePool, PoolConfig, PoolStrategy};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Construction & validation
// ---------------------------------------------------------------------------

#[test]
fn pool_new_with_valid_config() {
    let pool = IslePool::new(
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

    pool.shutdown();
}

#[test]
fn pool_max_size_zero_returns_error() {
    let result = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 0,
            strategy: PoolStrategy::Cold,
        },
    );
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Cold strategy: clean VM on every checkout
// ---------------------------------------------------------------------------

#[test]
fn cold_checkout_eval() {
    let pool = IslePool::new(
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

    let isle = pool.checkout().unwrap();
    let result = isle.eval("return base + 5").unwrap();
    assert_eq!(result, "15");

    pool.shutdown();
}

#[test]
fn cold_does_not_preserve_state() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Cold,
        },
    )
    .unwrap();

    // First checkout: set a global
    {
        let isle = pool.checkout().unwrap();
        isle.eval("my_global = 42").unwrap();
    }
    // PooledIsle dropped → Isle destroyed (cold)

    // Second checkout: global must NOT exist
    {
        let isle = pool.checkout().unwrap();
        let result = isle.eval("return type(my_global)").unwrap();
        assert_eq!(result, "nil", "cold pool must not preserve state");
    }

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// Warm strategy: reuse VM across checkouts
// ---------------------------------------------------------------------------

#[test]
fn warm_checkout_eval() {
    let pool = IslePool::new(
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

    let isle = pool.checkout().unwrap();
    let result = isle.eval("return base + 5").unwrap();
    assert_eq!(result, "15");

    pool.shutdown();
}

#[test]
fn warm_preserves_state() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    // First checkout: set a global
    {
        let isle = pool.checkout().unwrap();
        isle.eval("my_global = 42").unwrap();
    }
    // PooledIsle dropped → Isle returned to pool (warm)

    // Second checkout: global must still exist
    {
        let isle = pool.checkout().unwrap();
        let result = isle.eval("return my_global").unwrap();
        assert_eq!(result, "42", "warm pool must preserve state");
    }

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// try_checkout
// ---------------------------------------------------------------------------

#[test]
fn try_checkout_returns_none_when_exhausted() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let _isle = pool.checkout().unwrap();
    assert!(
        pool.try_checkout().unwrap().is_none(),
        "should return None when pool exhausted"
    );

    pool.shutdown();
}

#[test]
fn try_checkout_succeeds_when_available() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let isle = pool.try_checkout().unwrap();
    assert!(isle.is_some(), "should return Some when available");
    let isle = isle.unwrap();
    assert_eq!(isle.eval("return 1").unwrap(), "1");

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// checkout_timeout
// ---------------------------------------------------------------------------

#[test]
fn checkout_timeout_returns_pool_exhausted() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let _isle = pool.checkout().unwrap();

    let result = pool.checkout_timeout(Duration::from_millis(50));
    match result {
        Err(IsleError::PoolExhausted(1)) => {}
        other => panic!("expected PoolExhausted(1), got: {other:?}"),
    }

    pool.shutdown();
}

#[test]
fn checkout_blocks_then_succeeds_after_return() {
    let pool = Arc::new(
        IslePool::new(
            |_lua| Ok(()),
            PoolConfig {
                max_size: 1,
                strategy: PoolStrategy::Warm,
            },
        )
        .unwrap(),
    );

    let isle = pool.checkout().unwrap();

    let pool_c = Arc::clone(&pool);
    let waiter = std::thread::spawn(move || {
        let start = Instant::now();
        let isle = pool_c.checkout_timeout(Duration::from_secs(5)).unwrap();
        let elapsed = start.elapsed();
        let result = isle.eval("return 'waited'").unwrap();
        (result, elapsed)
    });

    // Hold for 100ms, then drop to return it
    std::thread::sleep(Duration::from_millis(100));
    drop(isle);

    let (result, elapsed) = waiter.join().unwrap();
    assert_eq!(result, "waited");
    assert!(
        elapsed >= Duration::from_millis(50),
        "should have blocked: {elapsed:?}"
    );

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// Concurrency: multiple threads checking out simultaneously
// ---------------------------------------------------------------------------

#[test]
fn concurrent_checkouts_all_succeed() {
    let pool = Arc::new(
        IslePool::new(
            |_lua| Ok(()),
            PoolConfig {
                max_size: 4,
                strategy: PoolStrategy::Warm,
            },
        )
        .unwrap(),
    );

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let pool = Arc::clone(&pool);
            std::thread::spawn(move || {
                let isle = pool.checkout().unwrap();
                let code = format!("return {} * 2", i);
                let result = isle.eval(&code).unwrap();
                assert_eq!(result, (i * 2).to_string());
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// Task API: spawn_eval with cancellation through pool
// ---------------------------------------------------------------------------

#[test]
fn pooled_isle_spawn_eval_and_cancel() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let isle = pool.checkout().unwrap();
    let task = isle.spawn_eval("while true do end");

    // Cancel after short delay
    std::thread::sleep(Duration::from_millis(20));
    task.cancel();

    let result = task.wait();
    assert!(
        matches!(result, Err(IsleError::Cancelled)),
        "expected Cancelled, got: {result:?}"
    );

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// call / exec through pooled isle
// ---------------------------------------------------------------------------

#[test]
fn pooled_isle_call() {
    let pool = IslePool::new(
        |lua| {
            lua.load(
                r#"
                function add(a, b)
                    return tonumber(a) + tonumber(b)
                end
                "#,
            )
            .exec()?;
            Ok(())
        },
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let isle = pool.checkout().unwrap();
    let result = isle.call("add", &["3", "4"]).unwrap();
    assert_eq!(result, "7");

    pool.shutdown();
}

#[test]
fn pooled_isle_exec() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    let isle = pool.checkout().unwrap();
    let result = isle
        .exec(|lua| {
            let val: i64 = lua.load("return 99").eval()?;
            Ok(val.to_string())
        })
        .unwrap();
    assert_eq!(result, "99");

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// kill(): force-discard a poisoned isle
// ---------------------------------------------------------------------------

#[test]
fn kill_discards_and_next_checkout_gets_fresh_isle() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    {
        let isle = pool.checkout().unwrap();
        isle.eval("sentinel = 'old'").unwrap();
        isle.kill();
    }
    // Killed → discarded, not returned to pool

    {
        let isle = pool.checkout().unwrap();
        let result = isle.eval("return type(sentinel)").unwrap();
        assert_eq!(
            result, "nil",
            "kill() must discard the isle; fresh checkout should have clean state"
        );
    }

    pool.shutdown();
}

// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Shutdown behavior
// ---------------------------------------------------------------------------

#[test]
fn checkout_after_shutdown_returns_error() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    // Create some isles first
    {
        let _isle = pool.checkout().unwrap();
    }

    pool.shutdown();

    let result = pool.checkout();
    assert!(
        matches!(result, Err(IsleError::Shutdown)),
        "checkout after shutdown must return Shutdown error, got: {result:?}"
    );
}

#[test]
fn shutdown_cleans_up_idle_isles() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 4,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    // Spawn 3 isles, return them all
    for _ in 0..3 {
        let isle = pool.checkout().unwrap();
        isle.eval("return 1").unwrap();
    }

    // All 3 are now idle in the pool
    pool.shutdown();
    // Should not hang — all idle isles shut down
}

// ---------------------------------------------------------------------------
// Factory failure
// ---------------------------------------------------------------------------

#[test]
fn factory_failure_returns_init_error() {
    let pool = IslePool::new(
        |_lua| Err(mlua::Error::runtime("factory exploded")),
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Cold,
        },
    )
    .unwrap();

    let result = pool.checkout();
    assert!(
        matches!(result, Err(IsleError::Init(_))),
        "factory failure should surface as Init error, got: {result:?}"
    );

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// Pool metrics: active / idle counts
// ---------------------------------------------------------------------------

#[test]
fn pool_metrics_track_active_and_idle() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 3,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    assert_eq!(pool.active(), 0);
    assert_eq!(pool.idle(), 0);

    let isle1 = pool.checkout().unwrap();
    assert_eq!(pool.active(), 1);
    assert_eq!(pool.idle(), 0);

    let isle2 = pool.checkout().unwrap();
    assert_eq!(pool.active(), 2);
    assert_eq!(pool.idle(), 0);

    drop(isle1);
    assert_eq!(pool.active(), 1);
    assert_eq!(pool.idle(), 1); // warm → returned to idle

    drop(isle2);
    assert_eq!(pool.active(), 0);
    assert_eq!(pool.idle(), 2);

    pool.shutdown();
}

#[test]
fn cold_pool_metrics_idle_stays_zero() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Cold,
        },
    )
    .unwrap();

    let isle = pool.checkout().unwrap();
    assert_eq!(pool.active(), 1);

    drop(isle);
    assert_eq!(pool.active(), 0);
    assert_eq!(pool.idle(), 0); // cold → destroyed on return

    pool.shutdown();
}
