use std::io;
use std::mem::ManuallyDrop;
use std::net::{Shutdown, TcpStream};
use std::os::unix::io::{FromRawFd, RawFd};
use std::cell::RefCell;
use std::task::Waker;

mod uring;
pub use self::uring::*;

#[derive(Debug)]
pub(crate) enum SourceType {
    DmaWrite,
    DmaRead,
    PollableFd,
    Open,
    FdataSync,
    Fallocate,
    Close,
}

/// Tasks interested in events on a source.
#[derive(Debug)]
pub(crate) struct Wakers {
    /// Raw result of the operation.
    pub(crate) result : Option<io::Result<usize>>,

    /// Tasks waiting for the next event.
    pub(crate) waiters: Vec<Waker>,
}

impl Wakers {
    pub(crate) fn new() -> Self {
        Wakers {
            result : None,
            waiters: Vec::new(),
        }
    }
}

/// A registered source of I/O events.
#[derive(Debug)]
pub struct Source {
    /// Raw file descriptor on Unix platforms.
    pub(crate) raw: RawFd,

    /// Tasks interested in events on this source.
    pub(crate) wakers: RefCell<Wakers>,

    pub(crate) source_type : SourceType,
}

impl Drop for Source {
    fn drop(&mut self) {
        let w = self.wakers.get_mut();
        if !w.waiters.is_empty() {
            panic!("Attempting to release a source with pending waiters!");
            // This cancellation will be problematic, because
            // the operation may still be in-flight. If it returns
            // later it will point to garbage memory.
        //    crate::parking::Reactor::get().cancel_io(self);
        }
    }
}

/// Shuts down the write side of a socket.
///
/// If this source is not a socket, the `shutdown()` syscall error is ignored.
pub fn shutdown_write(raw: RawFd) -> io::Result<()> {
    // This may not be a TCP stream, but that's okay. All we do is call `shutdown()` on the raw
    // descriptor and ignore errors if it's not a socket.
    let res = unsafe {
        let stream = ManuallyDrop::new(TcpStream::from_raw_fd(raw));
        stream.shutdown(Shutdown::Write)
    };

    // The only actual error may be ENOTCONN, ignore everything else.
    match res {
        Err(err) if err.kind() == io::ErrorKind::NotConnected => Err(err),
        _ => Ok(()),
    }
}
