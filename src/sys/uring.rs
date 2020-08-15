use std::io;
use std::os::unix::io::RawFd;
use std::time::Duration;
use std::cell::RefCell;
use std::task::Waker;
use std::collections::VecDeque;
use nix::poll::PollFlags;
use std::ffi::CString;

use crate::sys;
use crate::sys::Source;

macro_rules! syscall {
    ($fn:ident $args:tt) => {{
        let res = unsafe { libc::$fn $args };
        if res == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(res)
        }
    }};
}

pub fn add_flag(fd : RawFd, flag : libc::c_int) -> io::Result<()> {
    let flags = syscall!(fcntl(fd, libc::F_GETFL))?;
    syscall!(fcntl(fd, libc::F_SETFL, flags | flag))?;
    Ok(())
}

#[allow(dead_code)]
enum UringOpDescriptor {
    PollAdd(PollFlags),
    PollRemove(*const u8),
    Cancel(*const u8),
    Write(*const u8, usize, u64), 
    Read(*mut u8, usize, u64), 
    Open(*const u8, libc::c_int, libc::c_int),
    Close,
    FDataSync,
    Fallocate(u64, u64, libc::c_int),
}

struct UringDescriptor {
    fd        : RawFd,
    user_data : u64,
    args      : UringOpDescriptor,
}

pub struct Reactor {
    main_ring : RefCell<iou::IoUring>,
    submission_queue : RefCell<VecDeque<UringDescriptor>>,
}

fn common_flags() -> PollFlags {
    PollFlags::POLLERR | PollFlags::POLLHUP | PollFlags::POLLNVAL
}

/// Epoll flags for all possible readability events.
fn read_flags() -> PollFlags {
    PollFlags::POLLIN | PollFlags::POLLPRI
}

/// Epoll flags for all possible writability events.
fn write_flags() -> PollFlags {
    PollFlags::POLLOUT
}

impl Reactor {
    pub fn new() -> io::Result<Reactor> {
        Ok(Reactor {
            main_ring        : RefCell::new(iou::IoUring::new(128)?),
            submission_queue : RefCell::new(VecDeque::with_capacity(512)),
        })
    }

    fn add_to_submission_queue(&self, source : &sys::Source, descriptor : UringOpDescriptor) {
        self.submission_queue
                .borrow_mut()
                .push_back(UringDescriptor {
                    args : descriptor,
                    fd: source.raw,
                    user_data : source as *const sys::Source as _,
                });
    }

    pub(crate) fn interest(&self, source : &sys::Source, read: bool, write: bool) {
        let mut flags = common_flags();
        if read {
            flags |= read_flags();
        }
        if write {
            flags |= write_flags();
        }

        self.add_to_submission_queue(source, UringOpDescriptor::PollAdd(flags));
    }

    pub fn write_dma(&self, source : &Source, buf : &[u8], pos : u64) {
        let op = UringOpDescriptor::Write(buf.as_ptr() as *const u8, buf.len(), pos);
        self.add_to_submission_queue(source, op);
    }

    pub fn read_dma(&self, source : &Source, buf : &mut [u8], pos : u64) {
        let op = UringOpDescriptor::Read(buf.as_mut_ptr() as *mut u8, buf.len(), pos);
        self.add_to_submission_queue(source, op);
    }

    pub fn fdatasync(&self, source : &Source) {
        self.add_to_submission_queue(source, UringOpDescriptor::FDataSync);
    }

    pub fn fallocate(&self, source : &Source,
                     offset: u64, size: u64,
                     flags: libc::c_int) {
        let op = UringOpDescriptor::Fallocate(offset, size, flags);
        self.add_to_submission_queue(source, op);
    }

    pub fn close(&self, source : &Source) {
        self.add_to_submission_queue(source, UringOpDescriptor::Close);
    }

    pub fn open_at(&self,
                   source : &Source,
                   path   : &CString,
                   flags  : libc::c_int,
                   mode   : libc::c_int)
    {
        let op = UringOpDescriptor::Open(path.as_ptr() as _, flags, mode);
        self.add_to_submission_queue(source, op);
    }

    pub fn insert(&self, fd: RawFd) -> io::Result<()> {
        add_flag(fd, libc::O_NONBLOCK)
    }

    /*
    pub fn cancel_io(&self, source : &Source) {
        let source_ptr = source as *const sys::Source; 
        let op = match source.source_type {
            sys::SourceType::PollableFd => UringOpDescriptor::PollRemove(source_ptr as _),
            _ => UringOpDescriptor::Cancel(source_ptr as _),
        };
        self.add_to_submission_queue(source, op);
    }
    */

    fn consume_one_event(value : iou::CompletionQueueEvent, wakers : &mut Vec<Waker>) {
        // No user data is POLL_REMOVE or CANCEL, we won't process.
        if value.user_data() == 0 {
            return;
        }

        let source = unsafe {
            let s = value.user_data() as *const sys::Source;
            &*s
        };

        let mut w = source.wakers.borrow_mut();
        w.result = Some(value.result());
        wakers.append(&mut w.waiters);
    }

    fn consume_completion_queue(ring : &mut iou::IoUring, wakers : &mut Vec<Waker>) -> io::Result<usize>  {
        let mut completed : usize = 0;
        loop {
            if let Some(cqe) = ring.peek_for_cqe() {
                completed += 1;
                Self::consume_one_event(cqe, wakers);
            } else {
                break;
            }
        }
        Ok(completed)
    }

    fn fill_sqe(sqe : &mut iou::SubmissionQueueEvent<'_>, op : &UringDescriptor) {
        unsafe {
            match op.args {
                UringOpDescriptor::PollAdd(events) => {
                    sqe.prep_poll_add(op.fd, events); 
                },
                UringOpDescriptor::PollRemove(to_remove) => {
                    sqe.prep_poll_remove(to_remove as u64); 
                }
                UringOpDescriptor::Cancel(_) => {
                    println!("Don't yet know how to cancel. NEed to teach iou");
                    //sqe.prep_cancel(to_remove as u64); 
                }
                UringOpDescriptor::Write(ptr, len, pos) => {
                    let buf = std::slice::from_raw_parts(ptr, len);
                    sqe.prep_write(op.fd, buf, pos); 
                }
                UringOpDescriptor::Read(ptr, len, pos) => {
                    let buf = std::slice::from_raw_parts_mut(ptr, len);
                    sqe.prep_read(op.fd, buf, pos); 
                }
                UringOpDescriptor::Open(path, flags, mode) => {
                    sqe.prep_openat(op.fd, path, flags as _, mode as _); 
                }
                UringOpDescriptor::FDataSync => {
                    sqe.prep_fsync(op.fd, iou::FsyncFlags::FSYNC_DATASYNC); 
                }
                UringOpDescriptor::Fallocate(offset, size, flags) => {
                    let flags = iou::FallocateFlags::from_bits_truncate(flags);
                    sqe.prep_fallocate(op.fd, offset, size, flags);
                }
                UringOpDescriptor::Close=> {
                    sqe.prep_close(op.fd);
                }
            }
        }

        match op.args {
            UringOpDescriptor::PollRemove(_) | UringOpDescriptor::Cancel(_) => {
                sqe.set_user_data(0);
            }
            _ => { sqe.set_user_data(op.user_data); }
        }
    }

    fn consume_submission_queue(&self, ring : &mut iou::IoUring) {
        let mut queue = self.submission_queue.borrow_mut();
        while !queue.is_empty() {
            if let Some(mut sqe) = ring.next_sqe() {
                let op = queue.pop_front().unwrap();
                Self::fill_sqe(&mut sqe, &op);
            } else {
                break;
            }
        }
    }

    pub fn wait(&self, wakers : &mut Vec<Waker>, timeout: Option<Duration>) -> io::Result<usize> {
        let mut ring = self.main_ring.borrow_mut();
        self.consume_submission_queue(&mut ring);

        match timeout {
            None => ring.submit_sqes_and_wait(1)?,
            Some(dur) => {
                if dur == Duration::from_millis(0) {
                    ring.submit_sqes()?
                } else {
                    ring.submit_sqes_and_wait_with_timeout(1, dur)?
                }
            }
        };

        Self::consume_completion_queue(&mut ring, wakers)
    }

    pub fn notify(&self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
    }
}
