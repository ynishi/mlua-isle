//! Async connection pool for thread-isolated Lua VMs.
//!
//! [`AsyncIslePool`] is the async counterpart of [`IslePool`](crate::IslePool).
//! It manages a set of [`AsyncIsle`] instances and provides
//! checkout/return semantics via RAII guards ([`AsyncPooledIsle`]).
//!
//! # Why a pool of `AsyncIsle` instead of `AsyncIsle::clone`?
//!
//! A single [`AsyncIsle`] already accepts concurrent requests from any
//! number of clones — they queue on the channel and run sequentially on
//! the Lua thread (single-VM actor model).  A pool is needed when you
//! want **multiple Lua VMs with VM-bound state** to run concurrently:
//!
//! - Each VM has its own `RegistryKey` / `Thread` / globals — these are
//!   not portable across VMs.
//! - Checkout grants exclusive access to one VM for the lifetime of
//!   the [`AsyncPooledIsle`] guard.
//! - The pool size caps memory and OS-thread count.
//!
//! # Strategies
//!
//! - [`PoolStrategy::Cold`] — shut down the VM on return, spawn fresh
//!   on next checkout.  Guarantees clean state every time.
//! - [`PoolStrategy::Warm`] — return the VM to the idle list for reuse.
//!   Previous Lua global state is preserved.
//!
//! # Example
//!
//! ```rust
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use mlua_isle::{AsyncIslePool, PoolConfig, PoolStrategy};
//!
//! let pool = AsyncIslePool::new(
//!     |lua| {
//!         lua.globals().set("greeting", "hello")?;
//!         Ok(())
//!     },
//!     PoolConfig {
//!         max_size: 4,
//!         strategy: PoolStrategy::Warm,
//!     },
//! )?;
//!
//! {
//!     let isle = pool.checkout().await?;
//!     assert_eq!(isle.eval("return greeting").await?, "hello");
//! } // returned to pool on drop
//!
//! pool.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Drop semantics
//!
//! [`AsyncPooledIsle::drop`] is synchronous.  When the guard is dropped
//! inside a tokio runtime context:
//!
//! - **Warm + alive**: returned to the idle list synchronously (short
//!   `std::sync::Mutex` critical section, no await).
//! - **Cold or killed**: the inner [`AsyncIsleDriver`] is moved into a
//!   background task via [`tokio::spawn`] to await its `shutdown()`.
//!
//! If the guard is dropped **outside** a tokio runtime (no current
//! handle), the driver is dropped without awaiting — the Lua thread
//! exits naturally when all channel senders are released (see
//! [`AsyncIsleDriver`] docs for the channel-close mechanism).

use crate::async_isle::{AsyncIsle, AsyncIsleDriver};
use crate::error::IsleError;
use crate::pool::{PoolConfig, PoolStrategy};
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::Notify;

/// Factory closure type for creating new Lua VMs.
type Factory = dyn Fn(&mlua::Lua) -> Result<(), mlua::Error> + Send + Sync;

/// One slot in the pool — keeps the handle and the lifecycle driver
/// together so the OS thread can be cleanly joined on shutdown.
struct Slot {
    isle: AsyncIsle,
    driver: AsyncIsleDriver,
}

/// Shared inner state of the pool.
struct PoolInner {
    idle: Vec<Slot>,
    active: usize,
    closed: bool,
}

/// Async pool of thread-isolated Lua VMs.
///
/// Provides checkout/return semantics with configurable reuse strategy.
/// Thread-safe — wrap in `Arc` to share across tokio tasks.
pub struct AsyncIslePool {
    inner: Mutex<PoolInner>,
    notify: Notify,
    factory: Arc<Factory>,
    config: PoolConfig,
}

impl AsyncIslePool {
    /// Create a new pool with the given factory and configuration.
    ///
    /// No VMs are created eagerly — they are spawned on first checkout.
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
            notify: Notify::new(),
            factory: Arc::new(factory),
            config,
        })
    }

    /// Checkout an Isle from the pool, awaiting until one is available.
    ///
    /// 1. If the pool has idle VMs, one is returned immediately.
    /// 2. If the pool is below `max_size`, a new VM is spawned.
    /// 3. Otherwise, awaits a return notification.
    ///
    /// # Errors
    ///
    /// - [`IsleError::Shutdown`] if the pool has been shut down.
    /// - [`IsleError::Init`] if spawning a new VM fails.
    /// - [`IsleError::PoolPoisoned`] if the internal lock is poisoned.
    pub async fn checkout(&self) -> Result<AsyncPooledIsle<'_>, IsleError> {
        loop {
            let notified = self.notify.notified();
            match self.try_acquire()? {
                AcquireOutcome::Acquired(pooled) => return Ok(pooled),
                AcquireOutcome::SpawnNeeded => {
                    let slot = self.spawn_slot().await.inspect_err(|_| {
                        self.dec_active();
                    })?;
                    return Ok(AsyncPooledIsle::new(self, slot));
                }
                AcquireOutcome::Wait => {
                    notified.await;
                }
            }
        }
    }

    /// Try to checkout an Isle without awaiting.
    ///
    /// Returns `Ok(None)` if no VM is immediately available and the
    /// pool is at capacity.  When the pool is below `max_size`, a new
    /// VM is spawned (this `await`s the spawn but returns once the VM
    /// is initialized).
    ///
    /// # Errors
    ///
    /// - [`IsleError::Shutdown`] if the pool has been shut down.
    /// - [`IsleError::Init`] if spawning a new VM fails.
    /// - [`IsleError::PoolPoisoned`] if the internal lock is poisoned.
    pub async fn try_checkout(&self) -> Result<Option<AsyncPooledIsle<'_>>, IsleError> {
        match self.try_acquire()? {
            AcquireOutcome::Acquired(pooled) => Ok(Some(pooled)),
            AcquireOutcome::SpawnNeeded => {
                let slot = self.spawn_slot().await.inspect_err(|_| {
                    self.dec_active();
                })?;
                Ok(Some(AsyncPooledIsle::new(self, slot)))
            }
            AcquireOutcome::Wait => Ok(None),
        }
    }

    /// Checkout with a timeout.
    ///
    /// Returns [`IsleError::PoolExhausted`] if the timeout elapses
    /// before a VM becomes available.
    pub async fn checkout_timeout(
        &self,
        timeout: Duration,
    ) -> Result<AsyncPooledIsle<'_>, IsleError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.notify.notified();
            match self.try_acquire()? {
                AcquireOutcome::Acquired(pooled) => return Ok(pooled),
                AcquireOutcome::SpawnNeeded => {
                    let slot = self.spawn_slot().await.inspect_err(|_| {
                        self.dec_active();
                    })?;
                    return Ok(AsyncPooledIsle::new(self, slot));
                }
                AcquireOutcome::Wait => {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(IsleError::PoolExhausted(self.config.max_size));
                    }
                    match tokio::time::timeout(remaining, notified).await {
                        Ok(()) => continue,
                        Err(_) => return Err(IsleError::PoolExhausted(self.config.max_size)),
                    }
                }
            }
        }
    }

    /// Number of currently checked-out VMs.
    pub fn active(&self) -> usize {
        self.inner.lock().map(|g| g.active).unwrap_or(0)
    }

    /// Number of idle VMs available for checkout.
    pub fn idle(&self) -> usize {
        self.inner.lock().map(|g| g.idle.len()).unwrap_or(0)
    }

    /// Shut down all idle VMs in the pool.
    ///
    /// Active (checked-out) VMs are shut down asynchronously when their
    /// [`AsyncPooledIsle`] guards are dropped (in a background task —
    /// see module-level Drop semantics).
    ///
    /// This consumes the idle list and `await`s `driver.shutdown()` on
    /// each entry sequentially.
    pub async fn shutdown(&self) {
        let drained = {
            let mut inner = match self.inner.lock() {
                Ok(g) => g,
                Err(e) => e.into_inner(),
            };
            inner.closed = true;
            std::mem::take(&mut inner.idle)
        };
        self.notify.notify_waiters();

        for slot in drained {
            // The handle's tx clone goes out of scope here; driver.shutdown
            // sends an explicit Shutdown message and joins the OS thread.
            let _ = slot.driver.shutdown().await;
            drop(slot.isle);
        }
    }

    // ── internal ────────────────────────────────────────────────────

    /// Attempt to acquire a slot from idle list or by growing the pool.
    ///
    /// On `SpawnNeeded`, `active` has already been pre-incremented.
    fn try_acquire(&self) -> Result<AcquireOutcome<'_>, IsleError> {
        let mut inner = self.lock_inner()?;

        if inner.closed {
            return Err(IsleError::Shutdown);
        }

        if let Some(slot) = self.take_alive_slot(&mut inner) {
            inner.active += 1;
            return Ok(AcquireOutcome::Acquired(AsyncPooledIsle::new(self, slot)));
        }

        if self.can_grow(&inner) {
            inner.active += 1;
            Ok(AcquireOutcome::SpawnNeeded)
        } else {
            Ok(AcquireOutcome::Wait)
        }
    }

    /// Spawn a new VM and wrap it in a [`Slot`].
    async fn spawn_slot(&self) -> Result<Slot, IsleError> {
        let factory = Arc::clone(&self.factory);
        let (isle, driver) = AsyncIsle::spawn(move |lua| factory(lua)).await?;
        Ok(Slot { isle, driver })
    }

    /// Take an alive slot from the idle list, skipping dead ones.
    fn take_alive_slot(&self, inner: &mut PoolInner) -> Option<Slot> {
        while let Some(slot) = inner.idle.pop() {
            if slot.isle.is_alive() {
                return Some(slot);
            }
        }
        None
    }

    /// Whether the pool can grow (active + idle < max_size).
    fn can_grow(&self, inner: &PoolInner) -> bool {
        inner.active + inner.idle.len() < self.config.max_size
    }

    /// Return a slot to the pool (called by `AsyncPooledIsle::drop` /
    /// the warm path).
    fn return_slot_warm(&self, slot: Slot) {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };

        inner.active = inner.active.saturating_sub(1);

        if inner.closed {
            drop(inner);
            self.spawn_driver_shutdown(slot);
            self.notify.notify_one();
            return;
        }

        if slot.isle.is_alive() {
            inner.idle.push(slot);
            self.notify.notify_one();
        } else {
            drop(inner);
            self.spawn_driver_shutdown(slot);
            self.notify.notify_one();
        }
    }

    /// Discard a slot without returning it (called for Cold strategy
    /// and `kill()`).  Driver shutdown happens in the background so the
    /// caller's sync `Drop` does not block.
    fn discard_slot(&self, slot: Slot) {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        inner.active = inner.active.saturating_sub(1);
        drop(inner);
        self.spawn_driver_shutdown(slot);
        self.notify.notify_one();
    }

    /// Spawn a background task to await the driver's shutdown.
    ///
    /// Best-effort: when no tokio runtime is current (drop happened on
    /// a non-tokio thread), the slot is dropped without explicit join.
    /// The Lua thread will exit on its own once the channel senders
    /// are released.
    fn spawn_driver_shutdown(&self, slot: Slot) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let Slot { isle, driver } = slot;
                drop(isle); // release the handle's tx clone first
                let _ = driver.shutdown().await;
            });
        }
        // else: drop occurs here, driver tx goes out of scope, channel
        // disconnects, Lua thread exits naturally.
    }

    /// Lock the inner state, mapping poison to [`IsleError::PoolPoisoned`].
    fn lock_inner(&self) -> Result<MutexGuard<'_, PoolInner>, IsleError> {
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
        self.notify.notify_one();
    }
}

/// Result of [`AsyncIslePool::try_acquire`].
enum AcquireOutcome<'pool> {
    /// Successfully picked up an idle slot.
    Acquired(AsyncPooledIsle<'pool>),
    /// `active` was pre-incremented; the caller must spawn a new slot.
    SpawnNeeded,
    /// Pool at capacity; caller should `await` the notify.
    Wait,
}

/// RAII guard for a checked-out [`AsyncIsle`].
///
/// Dereferences to [`AsyncIsle`] for direct use of `eval`, `call`,
/// `exec`, `coroutine_eval`, etc.  When dropped, the VM is returned to
/// the pool (warm) or shut down (cold / killed) per the pool's
/// [`PoolStrategy`].
pub struct AsyncPooledIsle<'pool> {
    pool: &'pool AsyncIslePool,
    slot: Option<Slot>,
    killed: bool,
}

impl std::fmt::Debug for AsyncPooledIsle<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncPooledIsle")
            .field("alive", &self.slot.as_ref().map(|s| s.isle.is_alive()))
            .field("killed", &self.killed)
            .finish()
    }
}

impl<'pool> AsyncPooledIsle<'pool> {
    fn new(pool: &'pool AsyncIslePool, slot: Slot) -> Self {
        Self {
            pool,
            slot: Some(slot),
            killed: false,
        }
    }

    /// Mark the inner VM for disposal.
    ///
    /// On drop, the VM is shut down and discarded rather than returned
    /// to the pool.  A fresh VM is spawned on the next checkout.  Use
    /// this when the VM is in a bad state (corrupted globals, leaked
    /// resources, etc.).
    pub fn kill(&mut self) {
        self.killed = true;
    }
}

impl Deref for AsyncPooledIsle<'_> {
    type Target = AsyncIsle;

    fn deref(&self) -> &AsyncIsle {
        &self
            .slot
            .as_ref()
            .expect("AsyncPooledIsle used after drop")
            .isle
    }
}

impl Drop for AsyncPooledIsle<'_> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            if self.killed {
                self.pool.discard_slot(slot);
                return;
            }
            match self.pool.config.strategy {
                PoolStrategy::Cold => self.pool.discard_slot(slot),
                PoolStrategy::Warm => self.pool.return_slot_warm(slot),
            }
        }
    }
}
