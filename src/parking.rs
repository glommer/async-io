//! Thread parking and unparking.
//!
//! This module exposes the exact same API as [`parking`][docs-parking]. The only
//! difference is that [`Parker`] in this module will wait on epoll/kqueue/wepoll and wake tasks
//! blocked on I/O or timers.
//!
//! Executors may use this mechanism to go to sleep when idle and wake up when more work is
//! scheduled. By waking tasks blocked on I/O and then running those tasks on the same thread,
//! no thread context switch is necessary when going between task execution and I/O.
//!
//! You can treat this module as merely an optimization over the [`parking`][docs-parking] crate.
//!
//! [docs-parking]: https://docs.rs/parking
//!
//! # Examples
//!
//! A simple `block_on()` that runs a single future and waits on I/O:
//!
//! ```
//! use std::future::Future;
//! use std::task::{Context, Poll};
//!
//! use async_io::parking;
//! use futures_lite::{future, pin};
//! use waker_fn::waker_fn;
//!
//! // Blocks on a future to complete, waiting on I/O when idle.
//! fn block_on<T>(future: impl Future<Output = T>) -> T {
//!     // Create a waker that notifies through I/O when done.
//!     let (p, u) = parking::pair();
//!     let waker = waker_fn(move || u.unpark());
//!     let cx = &mut Context::from_waker(&waker);
//!
//!     pin!(future);
//!     loop {
//!         match future.as_mut().poll(cx) {
//!             Poll::Ready(t) => return t, // Done!
//!             Poll::Pending => p.park(),  // Wait for an I/O event.
//!         }
//!     }
//! }
//!
//! block_on(async {
//!     println!("Hello world!");
//!     future::yield_now().await;
//!     println!("Hello again!");
//! });
//! ```

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::mem;
use std::path::Path;
use std::os::unix::ffi::OsStrExt;
use std::ffi::CString;
use std::rc::Rc;
use std::cell::RefCell;
use std::os::unix::io::RawFd;
use std::panic::{self, RefUnwindSafe, UnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};
use std::pin::Pin;

use concurrent_queue::ConcurrentQueue;
use futures_lite::*;

use crate::sys;
use crate::sys::{DmaBuffer, Source, SourceType};

thread_local!(static LOCAL_REACTOR: Reactor = Reactor::new());

/// Creates a parker and an associated unparker.
///
/// # Examples
///
/// ```
/// use async_io::parking;
///
/// let (p, u) = parking::pair();
/// ```
pub fn pair() -> (Parker, Unparker) {
    let p = Parker::new();
    let u = p.unparker();
    (p, u)
}

/// Waits for a notification.
pub struct Parker {
    unparker: Unparker,
}

impl UnwindSafe for Parker {}
impl RefUnwindSafe for Parker {}

impl Parker {
    /// Creates a new parker.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::parking::Parker;
    ///
    /// let p = Parker::new();
    /// ```
    ///
    pub fn new() -> Parker {
        // Ensure `Reactor` is initialized now to prevent it from being initialized in `Drop`.
        let parker = Parker {
            unparker: Unparker {
                inner: Rc::new(Inner {
                }),
            },
        };
        parker
    }

    /// Blocks until notified and then goes back into unnotified state.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::parking::Parker;
    ///
    /// let p = Parker::new();
    /// let u = p.unparker();
    ///
    /// // Notify the parker.
    /// u.unpark();
    ///
    /// // Wakes up immediately because the parker is notified.
    /// p.park();
    /// ```
    pub fn park(&self) {
        self.unparker.inner.park(None);
    }

    /// Blocks until notified and then goes back into unnotified state, or times out after
    /// `duration`.
    ///
    /// Returns `true` if notified before the timeout.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::parking::Parker;
    /// use std::time::Duration;
    ///
    /// let p = Parker::new();
    ///
    /// // Wait for a notification, or time out after 500 ms.
    /// p.park_timeout(Duration::from_millis(500));
    /// ```
    pub fn park_timeout(&self, timeout: Duration) -> bool {
        self.unparker.inner.park(Some(timeout))
    }

    /// Blocks until notified and then goes back into unnotified state, or times out at `instant`.
    ///
    /// Returns `true` if notified before the deadline.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::parking::Parker;
    /// use std::time::{Duration, Instant};
    ///
    /// let p = Parker::new();
    ///
    /// // Wait for a notification, or time out after 500 ms.
    /// p.park_deadline(Instant::now() + Duration::from_millis(500));
    /// ```
    pub fn park_deadline(&self, deadline: Instant) -> bool {
        self.unparker
            .inner
            .park(Some(deadline.saturating_duration_since(Instant::now())))
    }

    /// Notifies the parker.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::parking::Parker;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let p = Parker::new();
    /// let u = p.unparker();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_millis(500));
    ///     u.unpark();
    /// });
    ///
    /// // Wakes up when `u.unpark()` notifies and then goes back into unnotified state.
    /// p.park();
    /// ```
    pub fn unpark(&self) {
        self.unparker.unpark()
    }

    /// Returns a handle for unparking.
    ///
    /// The returned [`Unparker`] can be cloned and shared among threads.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::parking::Parker;
    ///
    /// let p = Parker::new();
    /// let u = p.unparker();
    ///
    /// // Notify the parker.
    /// u.unpark();
    ///
    /// // Wakes up immediately because the parker is notified.
    /// p.park();
    /// ```
    pub fn unparker(&self) -> Unparker {
        self.unparker.clone()
    }
}

impl Drop for Parker {
    fn drop(&mut self) {}
}

impl Default for Parker {
    fn default() -> Parker {
        Parker::new()
    }
}

impl fmt::Debug for Parker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Parker { .. }")
    }
}

/// Notifies a parker.
pub struct Unparker {
    inner: Rc<Inner>,
}

impl UnwindSafe for Unparker {}
impl RefUnwindSafe for Unparker {}

impl Unparker {
    /// Notifies the associated parker.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::parking::Parker;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let p = Parker::new();
    /// let u = p.unparker();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_millis(500));
    ///     u.unpark();
    /// });
    ///
    /// // Wakes up when `u.unpark()` notifies and then goes back into unnotified state.
    /// p.park();
    /// ```
    pub fn unpark(&self) {
        self.inner.unpark()
    }
}

impl fmt::Debug for Unparker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Unparker { .. }")
    }
}

impl Clone for Unparker {
    fn clone(&self) -> Unparker {
        Unparker {
            inner: self.inner.clone(),
        }
    }
}

struct Inner {
}

impl Inner {
    fn park(&self, timeout: Option<Duration>) -> bool {
        // If the timeout is zero, then there is no need to actually block.
        // Process available I/O events.
        let reactor_lock = Reactor::get().lock();
        let _ = reactor_lock.react(timeout);
        return false;
    }

    // Because this is a Thread-per-core system it is impossible to unpark.
    // We can only be awaken by some external event, and that will wake up the
    // blocking system call in park().
    pub fn unpark(&self) {}
}

/// The reactor.
///
/// Every async I/O handle and every timer is registered here. Invocations of
/// [`run()`][`crate::run()`] poll the reactor to check for new events every now and then.
///
/// There is only one global instance of this type, accessible by [`Reactor::get()`].
pub(crate) struct Reactor {
    /// Raw bindings to epoll/kqueue/wepoll.
    sys: sys::Reactor,

    /// Ticker bumped before polling.
    ticker: AtomicUsize,

    /// An ordered map of registered timers.
    ///
    /// Timers are in the order in which they fire. The `usize` in this type is a timer ID used to
    /// distinguish timers that fire at the same time. The `Waker` represents the task awaiting the
    /// timer.
    timers: RefCell<BTreeMap<(Instant, usize), Waker>>,

    /// A queue of timer operations (insert and remove).
    ///
    /// When inserting or removing a timer, we don't process it immediately - we just push it into
    /// this queue. Timers actually get processed when the queue fills up or the reactor is polled.
    timer_ops: ConcurrentQueue<TimerOp>,
}

impl<'a> Reactor {
    fn new() -> Reactor {
        Reactor {
            sys: sys::Reactor::new().expect("cannot initialize I/O event notification"),
            ticker: AtomicUsize::new(0),
            timers: RefCell::new(BTreeMap::new()),
            timer_ops: ConcurrentQueue::bounded(1000),
        }
    }

    pub(crate) fn get() -> &'static Reactor {
        unsafe {
            LOCAL_REACTOR.with(|r| {
                let rc = r as *const Reactor;
                &*rc
            })
        }
    }

    /// Notifies the thread blocked on the reactor.
    pub(crate) fn notify(&self) {
        self.sys.notify().expect("failed to notify reactor");
    }

    pub(crate) fn alloc_dma_buffer(&self, size: usize) -> DmaBuffer<'_> {
        self.sys.alloc_dma_buffer(size)
    }

    pub(crate) fn write_dma(&self, raw : RawFd, buf : &'a [u8], pos : u64) -> Pin<Box<Source>> {
        let source = sys::Source::new(raw, SourceType::DmaWrite);
        self.sys.write_dma(&source.as_ref(), buf, pos);
        source
    }

    pub(crate) fn read_dma(&self, raw : RawFd, buf : &'a mut [u8], pos : u64) -> Pin<Box<Source>> {
        let source = sys::Source::new(raw, SourceType::DmaRead);
        self.sys.read_dma(&source.as_ref(), buf, pos);
        source
    }

    pub(crate) fn fdatasync(&self, raw : RawFd) -> Pin<Box<Source>> {
        let source = sys::Source::new(raw, SourceType::FdataSync);
        self.sys.fdatasync(&source.as_ref());
        source
    }

    pub(crate) fn fallocate(&self, raw : RawFd,
                            position: u64, size: u64,
                            flags: libc::c_int)
        -> Pin<Box<Source>>     {
        let source = sys::Source::new(raw, SourceType::Fallocate);
        self.sys.fallocate(&source.as_ref(), position, size, flags);
        source
    }

    pub(crate) fn close(&self, raw : RawFd) -> Pin<Box<Source>> {
        let source = sys::Source::new(raw, SourceType::Close);
        self.sys.close(&source.as_ref());
        source
    }

    pub(crate) fn open_at(&self,
                          dir   : RawFd,
                          path  : &Path,
                          flags : libc::c_int,
                          mode  : libc::c_int
    ) -> Pin<Box<Source>> {

        let path = CString::new(path.as_os_str().as_bytes())
                        .expect("path contained null!");

        let source = sys::Source::new(dir, SourceType::Open(path));
        self.sys.open_at(&source.as_ref(), flags, mode);
        source
    }

    pub(crate) fn insert_pollable_io(&self, raw: RawFd) -> io::Result<Pin<Box<Source>>>  {
        let source = sys::Source::new(raw, SourceType::PollableFd);
        self.sys.insert(raw)?;
        Ok(source)
    }

    pub(crate) fn register_file(&self, raw: RawFd) {
        let source = sys::Source::new(raw, SourceType::FilesUpdate(Vec::new()));
        self.sys.register_file(&source, raw);
    }

    pub(crate) fn unregister_file(&self, fd: RawFd) {
        self.sys.unregister_file(fd);
    }
    /*
    /// Deregisters an I/O source from the reactor.
    pub(crate) fn cancel_io(&self, source: &Source) {
        self.sys.cancel_io(source)
    }
    */

    /// Registers a timer in the reactor.
    ///
    /// Returns the inserted timer's ID.
    pub(crate) fn insert_timer(&self, when: Instant, waker: &Waker) -> usize {
        // Generate a new timer ID.
        static ID_GENERATOR: AtomicUsize = AtomicUsize::new(1);
        let id = ID_GENERATOR.fetch_add(1, Ordering::Relaxed);

        // Push an insert operation.
        while self
            .timer_ops
            .push(TimerOp::Insert(when, id, waker.clone()))
            .is_err()
        {
            // If the queue is full, drain it and try again.
            let mut timers = self.timers.borrow_mut();
            self.process_timer_ops(&mut timers);
        }

        // Notify that a timer has been inserted.
        self.notify();

        id
    }

    /// Deregisters a timer from the reactor.
    pub(crate) fn remove_timer(&self, when: Instant, id: usize) {
        // Push a remove operation.
        while self.timer_ops.push(TimerOp::Remove(when, id)).is_err() {
            // If the queue is full, drain it and try again.
            let mut timers = self.timers.borrow_mut();
            self.process_timer_ops(&mut timers);
        }
    }

    /// Locks the reactor, potentially blocking if the lock is held by another thread.
    fn lock(&self) -> ReactorLock<'_> {
        let reactor = self;
        ReactorLock { reactor }
    }

    /// Processes ready timers and extends the list of wakers to wake.
    ///
    /// Returns the duration until the next timer before this method was called.
    fn process_timers(&self, wakers: &mut Vec<Waker>) -> Option<Duration> {
        let mut timers = self.timers.borrow_mut();
        self.process_timer_ops(&mut timers);

        let now = Instant::now();

        // Split timers into ready and pending timers.
        let pending = timers.split_off(&(now, 0));
        let ready = mem::replace(&mut *timers, pending);

        // Calculate the duration until the next event.
        let dur = if ready.is_empty() {
            // Duration until the next timer.
            timers
                .keys()
                .next()
                .map(|(when, _)| when.saturating_duration_since(now))
        } else {
            // Timers are about to fire right now.
            Some(Duration::from_secs(0))
        };

        // Drop the lock before waking.
        drop(timers);

        // Add wakers to the list.
        for (_, waker) in ready {
            wakers.push(waker);
        }

        dur
    }

    /// Processes queued timer operations.
    fn process_timer_ops(&self, timers: &mut BTreeMap<(Instant, usize), Waker>) {
        // Process only as much as fits into the queue, or else this loop could in theory run
        // forever.
        for _ in 0..self.timer_ops.capacity().unwrap() {
            match self.timer_ops.pop() {
                Ok(TimerOp::Insert(when, id, waker)) => {
                    timers.insert((when, id), waker);
                }
                Ok(TimerOp::Remove(when, id)) => {
                    timers.remove(&(when, id));
                }
                Err(_) => break,
            }
        }
    }
}

/// A lock on the reactor.
struct ReactorLock<'a> {
    reactor: &'a Reactor,
}

impl ReactorLock<'_> {
    /// Processes new events, blocking until the first event or the timeout.
    fn react(self, timeout: Option<Duration>) -> io::Result<()> {
        // FIXME: there must be a way to avoid this allocation
        // Indeed it just showed in a profiler. We can cap the number of
        // cqes produced, but this is used for timers as well. Need to
        // be more careful, but doable.
        let mut wakers = Vec::new();

        // Process ready timers.
        let next_timer = self.reactor.process_timers(&mut wakers);

        // compute the timeout for blocking on I/O events.
        let timeout = match (next_timer, timeout) {
            (None, None) => None,
            (Some(t), None) | (None, Some(t)) => Some(t),
            (Some(a), Some(b)) => Some(a.min(b)),
        };

        // Bump the ticker before polling I/O.
        let _ = self
            .reactor
            .ticker
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1);

        // Block on I/O events.
        let res = match self.reactor.sys.wait(&mut wakers, timeout) {
            // No I/O events occurred.
            Ok(0) => {
                if timeout != Some(Duration::from_secs(0)) {
                    // The non-zero timeout was hit so fire ready timers.
                    self.reactor.process_timers(&mut wakers);
                }
                Ok(())
            }

            // At least one I/O event occurred.
            Ok(_) => { Ok(()) }

            // The syscall was interrupted.
            Err(err) if err.kind() == io::ErrorKind::Interrupted => Ok(()),

            // An actual error occureed.
            Err(err) => Err(err),
        };

        // Wake up ready tasks.
        for waker in wakers {
            // Don't let a panicking waker blow everything up.
            let _ = panic::catch_unwind(|| waker.wake() );
        }

        res
    }
}

/// A single timer operation.
enum TimerOp {
    Insert(Instant, usize, Waker),
    Remove(Instant, usize),
}

// FIXME: source should be partitioned in two, write_dma and read_dma should not be allowed
// in files that don't support it, and same for readable() writable()
impl Source {
    pub(crate) async fn collect_rw(&self) -> io::Result<usize> {
        future::poll_fn(|cx| {
            let mut w = self.wakers.borrow_mut();

            if let Some(result) = w.result.take() {
                return Poll::Ready(result);
            }

            w.waiters.push(cx.waker().clone());
            Poll::Pending
        }).await
    }

    /// Waits until the I/O source is readable.
    pub(crate) async fn readable(&self) -> io::Result<()> {
        future::poll_fn(|cx| {
            let mut w = self.wakers.borrow_mut();

            if let Some(_) = w.result.take() {
                return Poll::Ready(Ok(()));
            }

            Reactor::get().sys.interest(self, true, false);
            w.waiters.push(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    /// Waits until the I/O source is writable.
    pub(crate) async fn writable(&self) -> io::Result<()> {
        future::poll_fn(|cx| {
            let mut w = self.wakers.borrow_mut();

            if let Some(_) = w.result.take() {
                return Poll::Ready(Ok(()));
            }

            Reactor::get().sys.interest(self, false, true);
            w.waiters.push(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}
