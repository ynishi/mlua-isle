//! Connection pool for thread-isolated Lua VMs.
//!
//! [`IslePool`] manages a set of [`Isle`] instances and provides
//! checkout/return semantics via RAII guards ([`PooledIsle`]).
//!
//! # Strategies
//!
//! - [`PoolStrategy::Cold`] — destroy the Isle on return, spawn fresh
//!   on next checkout.  Guarantees a clean VM state every time.
//! - [`PoolStrategy::Warm`] — return the Isle to the idle list for
//!   reuse.  Previous Lua global state is preserved.
//!
//! # Example
//!
//! ```rust
//! use mlua_isle::{IslePool, PoolConfig, PoolStrategy};
//!
//! let pool = IslePool::new(
//!     |lua| {
//!         lua.globals().set("greeting", "hello")?;
//!         Ok(())
//!     },
//!     PoolConfig {
//!         max_size: 4,
//!         strategy: PoolStrategy::Warm,
//!     },
//! ).unwrap();
//!
//! {
//!     let isle = pool.checkout().unwrap();
//!     assert_eq!(isle.eval("return greeting").unwrap(), "hello");
//! } // isle returned to pool
//!
//! pool.shutdown();
//! ```

use crate::error::IsleError;
use crate::handle::Isle;
use std::cell::Cell;
use std::ops::Deref;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Factory function type for creating new Lua VMs.
type Factory = dyn Fn(&mlua::Lua) -> Result<(), mlua::Error> + Send + Sync;

/// Pool strategy controls what happens when a [`PooledIsle`] is returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolStrategy {
    /// Destroy the Isle on return and spawn a fresh one on next checkout.
    /// Guarantees a clean VM state for every checkout.
    Cold,
    /// Return the Isle to the pool for reuse.
    /// Previous Lua global state is preserved (caller must be aware).
    Warm,
}

/// Configuration for [`IslePool`].
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of Isles that can exist simultaneously.
    pub max_size: usize,
    /// Strategy for handling returned Isles.
    pub strategy: PoolStrategy,
}

/// Shared inner state of the pool.
struct PoolInner {
    /// Available (idle) Isles ready for checkout.
    idle: Vec<Isle>,
    /// Number of Isles currently checked out.
    active: usize,
    /// Whether the pool has been shut down.
    closed: bool,
}

/// A pool of thread-isolated Lua VMs.
///
/// Provides checkout/return semantics with configurable reuse strategy.
/// Thread-safe — can be shared via `Arc` across multiple threads.
pub struct IslePool {
    inner: Mutex<PoolInner>,
    condvar: Condvar,
    factory: Arc<Factory>,
    config: PoolConfig,
}

impl IslePool {
    /// Create a new pool with the given factory and configuration.
    ///
    /// The `factory` closure is called each time a new Isle needs to be
    /// spawned.  It receives `&Lua` and should set up globals, functions, etc.
    ///
    /// No Isles are created eagerly — they are spawned on first checkout.
    ///
    /// # Errors
    ///
    /// Returns [`IsleError::Init`] if `max_size` is zero.
    pub fn new<F>(factory: F, config: PoolConfig) -> Result<Self, IsleError>
    where
        F: Fn(&mlua::Lua) -> Result<(), mlua::Error> + Send + Sync + 'static,
    {
        if config.max_size == 0 {
            return Err(IsleError::Init("max_size must be > 0".into()));
        }

        Ok(Self {
            inner: Mutex::new(PoolInner {
                idle: Vec::with_capacity(config.max_size),
                active: 0,
                closed: false,
            }),
            condvar: Condvar::new(),
            factory: Arc::new(factory),
            config,
        })
    }

    /// Checkout an Isle from the pool, blocking until one is available.
    ///
    /// 1. If the pool has idle Isles, one is returned immediately.
    /// 2. If the pool is below `max_size`, a new Isle is spawned.
    /// 3. Otherwise, this blocks until an Isle is returned by another thread.
    ///
    /// # Errors
    ///
    /// - [`IsleError::Shutdown`] if the pool has been shut down.
    /// - [`IsleError::Init`] if spawning a new Isle fails.
    /// - [`IsleError::PoolPoisoned`] if the internal lock is poisoned.
    pub fn checkout(&self) -> Result<PooledIsle<'_>, IsleError> {
        let mut inner = self.lock_inner()?;

        loop {
            if inner.closed {
                return Err(IsleError::Shutdown);
            }

            match self.try_acquire(inner)? {
                Acquired::Isle(pooled) => return Ok(pooled),
                Acquired::NeedWait(guard) => {
                    inner = self
                        .condvar
                        .wait(guard)
                        .map_err(|e| IsleError::PoolPoisoned(e.to_string()))?;
                }
            }
        }
    }

    /// Try to checkout an Isle without blocking.
    ///
    /// Returns `Ok(None)` if no Isle is immediately available and the
    /// pool is at capacity.
    ///
    /// # Errors
    ///
    /// - [`IsleError::Shutdown`] if the pool has been shut down.
    /// - [`IsleError::Init`] if spawning a new Isle fails.
    /// - [`IsleError::PoolPoisoned`] if the internal lock is poisoned.
    pub fn try_checkout(&self) -> Result<Option<PooledIsle<'_>>, IsleError> {
        let inner = self.lock_inner()?;

        if inner.closed {
            return Err(IsleError::Shutdown);
        }

        match self.try_acquire(inner)? {
            Acquired::Isle(pooled) => Ok(Some(pooled)),
            Acquired::NeedWait(_) => Ok(None),
        }
    }

    /// Checkout with a timeout.
    ///
    /// Returns [`IsleError::PoolExhausted`] if the timeout expires
    /// before an Isle becomes available.
    pub fn checkout_timeout(&self, timeout: Duration) -> Result<PooledIsle<'_>, IsleError> {
        let mut inner = self.lock_inner()?;
        let deadline = std::time::Instant::now() + timeout;

        loop {
            if inner.closed {
                return Err(IsleError::Shutdown);
            }

            match self.try_acquire(inner)? {
                Acquired::Isle(pooled) => return Ok(pooled),
                Acquired::NeedWait(guard) => {
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(IsleError::PoolExhausted(self.config.max_size));
                    }

                    let (guard, _) = self
                        .condvar
                        .wait_timeout(guard, remaining)
                        .map_err(|e| IsleError::PoolPoisoned(e.to_string()))?;
                    inner = guard;
                }
            }
        }
    }

    /// Number of currently checked-out Isles.
    pub fn active(&self) -> usize {
        self.inner.lock().map(|g| g.active).unwrap_or(0)
    }

    /// Number of idle Isles available for checkout.
    pub fn idle(&self) -> usize {
        self.inner.lock().map(|g| g.idle.len()).unwrap_or(0)
    }

    /// Shut down all idle Isles in the pool.
    ///
    /// Active (checked-out) Isles will be shut down when their
    /// [`PooledIsle`] guards are dropped.
    pub fn shutdown(&self) {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        inner.closed = true;
        for isle in inner.idle.drain(..) {
            let _ = isle.shutdown();
        }
        self.condvar.notify_all();
    }

    // ── internal ────────────────────────────────────────────────────

    /// Attempt to acquire an Isle from idle list or by growing the pool.
    ///
    /// On success, returns `Acquired::Isle`.  If no Isle is available
    /// and the pool is at capacity, returns `Acquired::NeedWait` with
    /// the lock guard so the caller can decide how to wait.
    fn try_acquire<'a>(
        &'a self,
        mut inner: std::sync::MutexGuard<'a, PoolInner>,
    ) -> Result<Acquired<'a>, IsleError> {
        if let Some(isle) = self.take_alive_isle(&mut inner) {
            inner.active += 1;
            return Ok(Acquired::Isle(PooledIsle::new(self, isle)));
        }

        if self.can_grow(&inner) {
            inner.active += 1;
            drop(inner);
            match self.spawn_isle() {
                Ok(isle) => Ok(Acquired::Isle(PooledIsle::new(self, isle))),
                Err(e) => {
                    self.dec_active();
                    Err(e)
                }
            }
        } else {
            Ok(Acquired::NeedWait(inner))
        }
    }

    /// Return an Isle to the pool (called by `PooledIsle::drop`).
    fn return_isle(&self, isle: Isle) {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };

        inner.active = inner.active.saturating_sub(1);

        if inner.closed {
            let _ = isle.shutdown();
            self.condvar.notify_one();
            return;
        }

        match self.config.strategy {
            PoolStrategy::Cold => {
                let _ = isle.shutdown();
            }
            PoolStrategy::Warm => {
                if isle.is_alive() {
                    inner.idle.push(isle);
                }
            }
        }

        self.condvar.notify_one();
    }

    /// Discard an Isle without returning it (called when killed).
    fn discard_isle(&self, isle: Isle) {
        let _ = isle.shutdown();
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        inner.active = inner.active.saturating_sub(1);
        self.condvar.notify_one();
    }

    /// Take an alive Isle from the idle list, skipping dead ones.
    fn take_alive_isle(&self, inner: &mut PoolInner) -> Option<Isle> {
        while let Some(isle) = inner.idle.pop() {
            if isle.is_alive() {
                return Some(isle);
            }
        }
        None
    }

    /// Whether the pool can grow (total < max_size).
    fn can_grow(&self, inner: &PoolInner) -> bool {
        inner.active + inner.idle.len() < self.config.max_size
    }

    /// Spawn a new Isle using the factory.
    fn spawn_isle(&self) -> Result<Isle, IsleError> {
        let factory = Arc::clone(&self.factory);
        Isle::spawn(move |lua| factory(lua))
    }

    /// Lock the inner state.
    fn lock_inner(&self) -> Result<std::sync::MutexGuard<'_, PoolInner>, IsleError> {
        self.inner
            .lock()
            .map_err(|e| IsleError::PoolPoisoned(e.to_string()))
    }

    /// Decrement active count after a failed spawn.
    fn dec_active(&self) {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        inner.active = inner.active.saturating_sub(1);
        self.condvar.notify_one();
    }
}

/// Result of [`IslePool::try_acquire`].
enum Acquired<'pool> {
    /// Successfully acquired an Isle.
    Isle(PooledIsle<'pool>),
    /// No Isle available; caller should wait or return.
    NeedWait(std::sync::MutexGuard<'pool, PoolInner>),
}

/// RAII guard for a checked-out [`Isle`].
///
/// Dereferences to `Isle` for direct use of `eval`, `call`, `exec`,
/// `spawn_eval`, etc.  When dropped, the Isle is either returned to
/// the pool (warm) or destroyed (cold), depending on the pool's
/// [`PoolStrategy`].
pub struct PooledIsle<'pool> {
    pool: &'pool IslePool,
    isle: Option<Isle>,
    killed: Cell<bool>,
}

impl std::fmt::Debug for PooledIsle<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledIsle")
            .field("alive", &self.isle.as_ref().map(|i| i.is_alive()))
            .field("killed", &self.killed.get())
            .finish()
    }
}

impl<'pool> PooledIsle<'pool> {
    fn new(pool: &'pool IslePool, isle: Isle) -> Self {
        Self {
            pool,
            isle: Some(isle),
            killed: Cell::new(false),
        }
    }

    /// Mark the inner Isle for disposal.
    ///
    /// On drop, the Isle will be shut down and discarded rather than
    /// returned to the pool.  A fresh Isle will be spawned on the
    /// next checkout.  Use this when the VM is in a bad state
    /// (e.g. corrupted globals, resource leak).
    pub fn kill(&self) {
        self.killed.set(true);
    }
}

impl Deref for PooledIsle<'_> {
    type Target = Isle;

    fn deref(&self) -> &Isle {
        self.isle.as_ref().expect("PooledIsle used after drop")
    }
}

impl Drop for PooledIsle<'_> {
    fn drop(&mut self) {
        if let Some(isle) = self.isle.take() {
            if self.killed.get() {
                self.pool.discard_isle(isle);
            } else {
                self.pool.return_isle(isle);
            }
        }
    }
}
