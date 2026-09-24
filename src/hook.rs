//! Cancellation tokens and the "current token" of a Lua thread.
//!
//! A [`CancelToken`] is a shared `AtomicBool` that can be checked from
//! both Rust code and a Lua debug hook.  When the token of the request
//! (or task) currently executing is cancelled, the cancel hook (see
//! [`hooks`](crate::hooks)) raises a Lua error containing the sentinel
//! `__isle_cancelled__`, which is recognized by
//! [`IsleError::from(mlua::Error)`](crate::IsleError).

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// Sentinel message carried by the Lua error that cancellation raises.
pub(crate) const CANCELLED_SENTINEL: &str = "__isle_cancelled__";

/// Cancellation signal shared between caller and Lua thread.
///
/// Clone is cheap (Arc).
///
/// Two cancellation pathways are wired:
///
/// 1. A Lua debug hook checks [`is_cancelled`](Self::is_cancelled) every
///    `N` Lua instructions.  This interrupts pure-Lua CPU-bound loops
///    (`while true do end` etc), including loops inside coroutines
///    that the Lua code itself creates.
///
/// 2. When the feature `tokio` is enabled, [`cancelled`](Self::cancelled)
///    provides an async signal that fires as soon as [`cancel`](Self::cancel)
///    is called.  Coroutine executors use it to stop a Lua coroutine
///    even when it is suspended inside a Rust `.await` (e.g. a
///    `create_async_function` awaiting a tokio child process), a state
///    in which the debug hook cannot fire because no Lua instructions
///    execute.
///
/// # Hierarchy
///
/// [`child_token`](Self::child_token) derives a token that is cancelled
/// whenever its parent is.  A parent holds its children weakly and a
/// child holds its parent strongly: a finished child disappears from
/// its parent without an explicit unregister, and a live grandchild
/// keeps the chain between it and the root alive.
#[derive(Clone)]
pub struct CancelToken {
    inner: Arc<Inner>,
}

struct Inner {
    flag: AtomicBool,
    #[cfg(feature = "tokio")]
    notify: tokio::sync::Notify,
    // Kept only to hold the chain to the root alive; never read.
    _parent: Option<Arc<Inner>>,
    children: Mutex<Children>,
}

#[derive(Default)]
struct Children {
    list: Vec<Weak<Inner>>,
    /// `list.len()` at which dead entries are swept next.
    sweep_at: usize,
}

impl Inner {
    fn new(parent: Option<Arc<Inner>>) -> Self {
        Self {
            flag: AtomicBool::new(false),
            #[cfg(feature = "tokio")]
            notify: tokio::sync::Notify::new(),
            _parent: parent,
            children: Mutex::new(Children::default()),
        }
    }

    fn cancel(&self) {
        if self.flag.swap(true, Ordering::AcqRel) {
            return;
        }
        #[cfg(feature = "tokio")]
        self.notify.notify_waiters();
        let children = std::mem::take(&mut lock(&self.children).list);
        for child in children.iter().filter_map(Weak::upgrade) {
            child.cancel();
        }
    }
}

fn lock(m: &Mutex<Children>) -> std::sync::MutexGuard<'_, Children> {
    // A panic while holding this lock cannot leave `Children` in an
    // inconsistent state, so a poisoned lock is still usable.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl CancelToken {
    /// Create a new token (not cancelled).
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner::new(None)),
        }
    }

    /// Derive a child token.
    ///
    /// The child is cancelled when this token is cancelled (at once if
    /// it already is).  Cancelling the child does not affect this
    /// token.
    pub fn child_token(&self) -> CancelToken {
        let child = Arc::new(Inner::new(Some(self.inner.clone())));
        let mut children = lock(&self.inner.children);
        // Checked under the lock: `Inner::cancel` sets the flag before
        // taking the lock, so either it sees this child or we see the flag.
        if self.is_cancelled() {
            drop(children);
            child.cancel();
        } else {
            if children.list.len() >= children.sweep_at {
                children.list.retain(|w| w.strong_count() > 0);
                children.sweep_at = (children.list.len() * 2).max(16);
            }
            children.list.push(Arc::downgrade(&child));
        }
        CancelToken { inner: child }
    }

    /// Signal cancellation.
    ///
    /// Sets the atomic flag (observed by the Lua debug hook), notifies
    /// all waiters of the async [`cancelled`](Self::cancelled) signal
    /// when the `tokio` feature is enabled, and cancels every live
    /// child token.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// Check whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.flag.load(Ordering::Acquire)
    }

    /// Await cancellation (async).
    ///
    /// Returns immediately if already cancelled; otherwise resolves
    /// when [`cancel`](Self::cancel) is called on this token or one of
    /// its ancestors.
    ///
    /// Race-free: the returned future is registered with the
    /// underlying [`tokio::sync::Notify`] via
    /// [`Notified::enable`](tokio::sync::futures::Notified::enable)
    /// before the flag is re-checked, so a `cancel()` call that
    /// happens between `cancelled()` being constructed and awaited
    /// is not lost.
    #[cfg(feature = "tokio")]
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = self.inner.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        // Re-check after enabling: a cancel() that happened between
        // the initial is_cancelled() check and enable() would have
        // called notify_waiters() without us being registered, so
        // this second read catches it.
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }

    #[cfg(test)]
    fn live_children(&self) -> usize {
        lock(&self.inner.children)
            .list
            .iter()
            .filter(|w| w.strong_count() > 0)
            .count()
    }

    #[cfg(test)]
    fn stored_children(&self) -> usize {
        lock(&self.inner.children).list.len()
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CancelToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

thread_local! {
    /// Token of the request (or task) currently executing on this Lua
    /// thread.  The cancel hook reads it.
    static CURRENT: RefCell<Option<CancelToken>> = const { RefCell::new(None) };
}

/// Token of the request or task currently executing on this thread.
///
/// Inside a host function called from Lua code that an isle runs, this
/// is the token of that request (for coroutine requests and tasks, the
/// one being polled).  Use it to derive a [`child_token`](CancelToken::child_token)
/// for work the host function starts, so that cancelling the request
/// reaches that work too.  Returns `None` outside such a context.
pub fn current_token() -> Option<CancelToken> {
    CURRENT.with(|c| c.borrow().clone())
}

/// Whether the current token is cancelled.
pub(crate) fn current_is_cancelled() -> bool {
    CURRENT.with(|c| c.borrow().as_ref().is_some_and(CancelToken::is_cancelled))
}

/// RAII guard that makes `token` the current token of this thread.
///
/// The previous token is restored on drop, so guards nest.
pub(crate) struct EnterGuard {
    prev: Option<CancelToken>,
}

impl EnterGuard {
    pub(crate) fn new(token: &CancelToken) -> Self {
        let prev = CURRENT.with(|c| c.replace(Some(token.clone())));
        Self { prev }
    }
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        let prev = self.prev.take();
        CURRENT.with(|c| *c.borrow_mut() = prev);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_default_not_cancelled() {
        let token = CancelToken::new();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn token_cancel_sets_flag() {
        let token = CancelToken::new();
        let clone = token.clone();
        token.cancel();
        assert!(clone.is_cancelled());
    }

    #[test]
    fn cancel_reaches_descendants_not_ancestors() {
        let root = CancelToken::new();
        let child = root.child_token();
        let grandchild = child.child_token();
        let sibling = root.child_token();

        child.cancel();
        assert!(child.is_cancelled());
        assert!(grandchild.is_cancelled());
        assert!(!root.is_cancelled());
        assert!(!sibling.is_cancelled());

        root.cancel();
        assert!(sibling.is_cancelled());
    }

    #[test]
    fn child_of_cancelled_token_starts_cancelled() {
        let root = CancelToken::new();
        root.cancel();
        assert!(root.child_token().is_cancelled());
    }

    #[test]
    fn grandchild_stays_reachable_after_middle_handle_is_dropped() {
        let root = CancelToken::new();
        let grandchild = root.child_token().child_token();
        root.cancel();
        assert!(grandchild.is_cancelled());
    }

    #[test]
    fn dropped_children_are_swept() {
        let root = CancelToken::new();
        let keep = root.child_token();
        for _ in 0..10_000 {
            let _ = root.child_token();
        }
        assert_eq!(root.live_children(), 1);
        assert!(
            root.stored_children() <= 32,
            "dead children were not swept: {}",
            root.stored_children()
        );
        root.cancel();
        assert!(keep.is_cancelled());
    }

    #[test]
    fn current_token_follows_enter_guards() {
        assert!(current_token().is_none());
        let outer = CancelToken::new();
        let inner = CancelToken::new();
        {
            let _o = EnterGuard::new(&outer);
            {
                let _i = EnterGuard::new(&inner);
                inner.cancel();
                assert!(current_is_cancelled());
            }
            assert!(!current_is_cancelled());
        }
        assert!(current_token().is_none());
    }
}
