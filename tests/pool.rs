#![cfg(feature = "pool")]

use mlua_isle::{IsleError, IslePool, PoolConfig, PoolStrategy};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

#[test]
fn pool_cold_creates_successfully() {
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
fn pool_warm_creates_successfully() {
    let pool = IslePool::new(
        |lua| {
            lua.globals().set("x", 1)?;
            Ok(())
        },
        PoolConfig {
            max_size: 2,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// Cold strategy
// ---------------------------------------------------------------------------

#[test]
fn cold_pool_eval_returns_correct_result() {
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
fn cold_pool_does_not_reuse_state() {
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

    // Second checkout: global should NOT exist
    {
        let isle = pool.checkout().unwrap();
        let result = isle.eval("return type(my_global)").unwrap();
        assert_eq!(result, "nil", "cold pool must not preserve state");
    }

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// Warm strategy
// ---------------------------------------------------------------------------

#[test]
fn warm_pool_eval_returns_correct_result() {
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
fn warm_pool_preserves_state() {
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

    // Second checkout: global should still exist
    {
        let isle = pool.checkout().unwrap();
        let result = isle.eval("return my_global").unwrap();
        assert_eq!(result, "42", "warm pool must preserve state");
    }

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// Pool exhaustion — try_checkout
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
    // Pool is now empty and max_size reached
    let result = pool.try_checkout();
    assert!(result.is_none(), "should return None when pool exhausted");

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// Pool exhaustion — checkout with timeout (blocking wait)
// ---------------------------------------------------------------------------

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
        let isle = pool_c.checkout().unwrap();
        let elapsed = start.elapsed();
        let result = isle.eval("return 'waited'").unwrap();
        (result, elapsed)
    });

    // Hold the isle for a bit, then drop to return it
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
        Err(other) => panic!("expected PoolExhausted(1), got: {other}"),
        Ok(_) => panic!("expected PoolExhausted, got Ok"),
    }

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

#[test]
fn concurrent_checkouts() {
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
                // PooledIsle drops here → returned to pool
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// is_alive / dead Isle handling
// ---------------------------------------------------------------------------

#[test]
fn dead_isle_is_replaced_on_checkout() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 1,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    // Checkout, set a sentinel, then kill
    {
        let isle = pool.checkout().unwrap();
        assert_eq!(isle.eval("return 1").unwrap(), "1");
        isle.eval("sentinel = 'old_isle'").unwrap();
        isle.kill();
    }
    // PooledIsle dropped → Isle discarded (killed)

    // Next checkout must spawn a fresh Isle without the sentinel
    {
        let isle = pool.checkout().unwrap();
        let result = isle.eval("return type(sentinel)").unwrap();
        assert_eq!(
            result, "nil",
            "kill() must discard the isle; new checkout should have clean state"
        );
    }

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

#[test]
fn shutdown_cleans_up_all_isles() {
    let pool = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 4,
            strategy: PoolStrategy::Warm,
        },
    )
    .unwrap();

    // Create some isles
    for _ in 0..3 {
        let isle = pool.checkout().unwrap();
        isle.eval("return 1").unwrap();
    }

    pool.shutdown();
}

// ---------------------------------------------------------------------------
// Config validation
// ---------------------------------------------------------------------------

#[test]
fn max_size_zero_is_error() {
    let result = IslePool::new(
        |_lua| Ok(()),
        PoolConfig {
            max_size: 0,
            strategy: PoolStrategy::Cold,
        },
    );
    assert!(result.is_err());
}
