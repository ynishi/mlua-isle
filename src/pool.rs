//! Connection pool for thread-isolated Lua VMs.
//!
//! [`IslePool`] manages a set of [`Isle`] instances and provides
//! checkout/return semantics via RAII guards ([`PooledIsle`]).

use crate::error::IsleError;
use crate::handle::Isle;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Available (idle) Isles.
    idle: Vec<Isle>,
    /// Number of Isles currently checked out.
    active: usize,
    /// Whether the pool has been shut down.
    closed: bool,
}

/// A pool of thread-isolated Lua VMs.
///
/// Provides checkout/return semantics with configurable reuse strategy.
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
    /// # Errors
    ///
    /// Returns an error if `max_size` is zero.
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
    /// If the pool has idle Isles, one is returned immediately.
    /// If the pool is below `max_size`, a new Isle is spawned.
    /// Otherwise, this blocks until an Isle is returned by another thread.
    pub fn checkout(&self) -> Result<PooledIsle<'_>, IsleError> {
        let mut inner = self.lock_inner()?;

        loop {
            if inner.closed {
                return Err(IsleError::Shutdown);
            }

            // Try to get an idle Isle
            if let Some(isle) = self.take_alive_isle(&mut inner) {
                inner.active += 1;
                return Ok(PooledIsle::new(self, isle));
            }

            // Can we spawn a new one?
            if inner.active + inner.idle.len() < self.config.max_size {
                inner.active += 1;
                drop(inner); // release lock before blocking spawn
                match self.spawn_isle() {
                    Ok(isle) => return Ok(PooledIsle::new(self, isle)),
                    Err(e) => {
                        self.undo_active_reservation();
                        return Err(e);
                    }
                }
            }

            // Wait for a return
            inner = self
                .condvar
                .wait(inner)
                .map_err(|e| IsleError::Init(format!("pool condvar poisoned: {e}")))?;
        }
    }

    /// Try to checkout an Isle without blocking.
    ///
    /// Returns `None` if no Isle is available and the pool is at capacity.
    pub fn try_checkout(&self) -> Option<PooledIsle<'_>> {
        let mut inner = self.inner.lock().ok()?;

        if inner.closed {
            return None;
        }

        if let Some(isle) = self.take_alive_isle(&mut inner) {
            inner.active += 1;
            return Some(PooledIsle::new(self, isle));
        }

        if inner.active + inner.idle.len() < self.config.max_size {
            inner.active += 1;
            drop(inner); // release lock before blocking spawn
            match self.spawn_isle() {
                Ok(isle) => return Some(PooledIsle::new(self, isle)),
                Err(_) => {
                    self.undo_active_reservation();
                    return None;
                }
            }
        }

        None
    }

    /// Checkout with a timeout.
    ///
    /// Returns [`IsleError::PoolExhausted`] if the timeout expires.
    pub fn checkout_timeout(&self, timeout: Duration) -> Result<PooledIsle<'_>, IsleError> {
        let mut inner = self.lock_inner()?;
        let deadline = std::time::Instant::now() + timeout;

        loop {
            if inner.closed {
                return Err(IsleError::Shutdown);
            }

            if let Some(isle) = self.take_alive_isle(&mut inner) {
                inner.active += 1;
                return Ok(PooledIsle::new(self, isle));
            }

            if inner.active + inner.idle.len() < self.config.max_size {
                inner.active += 1;
                drop(inner); // release lock before blocking spawn
                match self.spawn_isle() {
                    Ok(isle) => return Ok(PooledIsle::new(self, isle)),
                    Err(e) => {
                        self.undo_active_reservation();
                        return Err(e);
                    }
                }
            }

            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(IsleError::PoolExhausted(self.config.max_size));
            }

            let (guard, wait_result) = self
                .condvar
                .wait_timeout(inner, remaining)
                .map_err(|e| IsleError::Init(format!("pool condvar poisoned: {e}")))?;
            inner = guard;

            if wait_result.timed_out() {
                // One more attempt before giving up
                if let Some(isle) = self.take_alive_isle(&mut inner) {
                    inner.active += 1;
                    return Ok(PooledIsle::new(self, isle));
                }
                if inner.active + inner.idle.len() < self.config.max_size {
                    inner.active += 1;
                    drop(inner);
                    match self.spawn_isle() {
                        Ok(isle) => return Ok(PooledIsle::new(self, isle)),
                        Err(e) => {
                            self.undo_active_reservation();
                            return Err(e);
                        }
                    }
                }
                return Err(IsleError::PoolExhausted(self.config.max_size));
            }
        }
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

    /// Return an Isle to the pool (called by PooledIsle::drop).
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
                // Destroy — don't return to pool
                let _ = isle.shutdown();
            }
            PoolStrategy::Warm => {
                if isle.is_alive() {
                    inner.idle.push(isle);
                }
                // Dead isles are simply dropped (not returned)
            }
        }

        self.condvar.notify_one();
    }

    /// Discard an Isle without returning it to the pool (called when killed).
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
            // Dead isle — drop it (shutdown sent by Isle::drop)
        }
        None
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
            .map_err(|e| IsleError::Init(format!("pool mutex poisoned: {e}")))
    }

    /// Undo an active slot reservation after a failed spawn.
    fn undo_active_reservation(&self) {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        inner.active = inner.active.saturating_sub(1);
        self.condvar.notify_one();
    }
}

/// RAII guard for a checked-out [`Isle`].
///
/// Dereferences to `Isle` for direct use.  When dropped, the Isle is
/// either returned to the pool (warm) or destroyed (cold), depending
/// on the pool's [`PoolStrategy`].
pub struct PooledIsle<'pool> {
    pool: &'pool IslePool,
    isle: Option<Isle>,
    killed: AtomicBool,
}

impl<'pool> PooledIsle<'pool> {
    fn new(pool: &'pool IslePool, isle: Isle) -> Self {
        Self {
            pool,
            isle: Some(isle),
            killed: AtomicBool::new(false),
        }
    }

    /// Mark the inner Isle for disposal.
    ///
    /// On drop, the Isle will be shut down and discarded rather than
    /// returned to the pool.  A fresh Isle will be spawned on the
    /// next checkout.
    pub fn kill(&self) {
        self.killed.store(true, Ordering::Release);
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
            if self.killed.load(Ordering::Acquire) {
                self.pool.discard_isle(isle);
            } else {
                self.pool.return_isle(isle);
            }
        }
    }
}
