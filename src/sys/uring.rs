use std::io;
use std::os::unix::io::RawFd;
use std::time::Duration;
use std::cell::RefCell;
use std::task::Waker;
use std::pin::Pin;
use std::collections::VecDeque;
use nix::poll::PollFlags;
use std::ffi::CStr;

use crate::sys::{Source, SourceType};

pub(crate) fn add_flag(fd : RawFd, flag : libc::c_int) -> io::Result<()> {
    let flags = syscall!(fcntl(fd, libc::F_GETFL))?;
    syscall!(fcntl(fd, libc::F_SETFL, flags | flag))?;
    Ok(())
}

#[allow(dead_code)]
#[derive(Debug)]
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

#[derive(Debug)]
struct UringDescriptor {
    fd        : RawFd,
    user_data : u64,
    args      : UringOpDescriptor,
}

fn fill_sqe(sqe : &mut iou::SubmissionQueueEvent<'_>,
            op : &UringDescriptor)
{
    let mut user_data = op.user_data;
    unsafe {
        match op.args {
            UringOpDescriptor::PollAdd(events) => {
                sqe.prep_poll_add(op.fd, events); 
            },
            UringOpDescriptor::PollRemove(to_remove) => {
                user_data = 0;
                sqe.prep_poll_remove(to_remove as u64); 
            }
            UringOpDescriptor::Cancel(_) => {
                user_data = 0;
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
                let path = CStr::from_ptr(path as _);
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

    sqe.set_user_data(user_data);
}

trait UringCommon {
    fn submission_queue(&mut self) -> &mut VecDeque<UringDescriptor>;
    fn submit_sqes(&mut self) -> io::Result<usize>;
    fn submit_one_event(&mut self) -> Option<()>;
    fn consume_one_event(&mut self, wakers: &mut Vec<Waker>) -> Option<()>;

    fn add_to_submission_queue(&mut self, source : &Source, descriptor : UringOpDescriptor) {
        self.submission_queue()
                .push_back(UringDescriptor {
                    args : descriptor,
                    fd: source.raw,
                    user_data : source as *const Source as _,
                });
    }

    fn consume_submission_queue(&mut self) -> io::Result<usize> {
        loop {
            if let None = self.submit_one_event() {
                break;
            }
        }

        self.submit_sqes()
    }

    fn consume_completion_queue(&mut self, wakers : &mut Vec<Waker>) -> usize  {
        let mut completed : usize = 0;
        loop {
            if let None = self.consume_one_event(wakers) {
                break;
            }
            completed += 1;
        }
        completed
    }
}

struct PollRing {
    ring: iou::IoUring,
    submission_queue: VecDeque<UringDescriptor>,
    submitted: u64,
    completed: u64,
}

impl PollRing {
    fn new(size: usize) -> io::Result<Self> {
        Ok(PollRing {
            submitted: 0,
            completed: 0,
            ring: iou::IoUring::new_with_flags(size as _, iou::SetupFlags::IOPOLL)?,
            submission_queue : VecDeque::with_capacity(size * 4),
        })
    }    

    fn can_sleep(&self) -> bool {
        return self.submitted == self.completed
    }
}

impl UringCommon for PollRing {
    fn submission_queue(&mut self) -> &mut VecDeque<UringDescriptor> {
        &mut self.submission_queue
    }
    
    fn submit_sqes(&mut self) -> io::Result<usize> {
        if self.submitted != self.completed {
            return self.ring.submit_sqes()
        }
        Ok(0)
    }

    fn consume_one_event(&mut self, wakers : &mut Vec<Waker>) -> Option<()> {
        process_one_event(self.ring.peek_for_cqe(), |source| { None }, wakers)
    }

    fn submit_one_event(&mut self) -> Option<()> {
        if self.submission_queue.is_empty() {
            return None;
        }

        if let Some(mut sqe) = self.ring.next_sqe() {
            self.submitted += 1;
            let op = self.submission_queue.pop_front().unwrap();
            fill_sqe(&mut sqe, &op);
            return Some(())
        }
        None
    }
}

struct SleepableRing {
    ring: iou::IoUring,
    submission_queue: VecDeque<UringDescriptor>,
}

impl SleepableRing {
    fn new(size: usize) -> io::Result<Self> {
        Ok(SleepableRing {
       //     ring: iou::IoUring::new_with_flags(size as _, iou::SetupFlags::IOPOLL)?,
            ring: iou::IoUring::new(size as _)?,
            submission_queue : VecDeque::with_capacity(size * 4),
        })
    }    

    fn ring_fd(&self) -> RawFd {
        self.ring.raw().ring_fd
    }

    fn sleep(&mut self, link: &mut Pin<Box<Source>>, timeout: Option<Duration>) -> io::Result<usize> {
        match link.source_type {
            SourceType::LinkRings(true) => {}, // nothing to do
            SourceType::LinkRings(false) => {
                if let Some(mut sqe) = self.ring.next_sqe() {
                    link.as_mut().update_source_type(SourceType::LinkRings(true));

                    let op = UringDescriptor {
                        fd: link.as_ref().raw,
                        user_data: link.as_ref().as_ptr() as *const Source as u64,
                        args: UringOpDescriptor::PollAdd(common_flags() | read_flags()),
                    };
                    fill_sqe(&mut sqe, &op);
                }
            }, 
            _ => panic!("Unexpected source type when linking rings"),
        }

        match timeout {
            None => self.ring.submit_sqes_and_wait(1),
            Some(dur) => self.ring.submit_sqes_and_wait_with_timeout(1, dur),
        }
    }

}

fn process_one_event<F>(cqe: Option<iou::CompletionQueueEvent>,
                        try_process: F,
                        wakers : &mut Vec<Waker>) -> Option<()> where
    F : FnOnce(&mut Source) -> Option<()>
{
    if let Some(value) = cqe {
        // No user data is POLL_REMOVE or CANCEL, we won't process.
        if value.user_data() == 0 {
            return Some(());
        }

        let source = unsafe {
            let s = value.user_data() as *mut Source;
            &mut *s
        };
        
        if let None = try_process(source) {
            let mut w = source.wakers.borrow_mut();
            w.result = Some(value.result());
            wakers.append(&mut w.waiters);
        }
        return Some(());
    }
    None
}

impl UringCommon for SleepableRing {
    fn submission_queue(&mut self) -> &mut VecDeque<UringDescriptor> {
        &mut self.submission_queue
    }

    fn submit_sqes(&mut self) -> io::Result<usize> {
        self.ring.submit_sqes()
    }

    fn consume_one_event(&mut self, wakers : &mut Vec<Waker>) -> Option<()> {
        process_one_event(self.ring.peek_for_cqe(), |source| {
            match source.source_type {
                SourceType::LinkRings(true) => {
                    source.source_type = SourceType::LinkRings(false);
                    Some(())
                },
                SourceType::LinkRings(false) => {
                    panic!("Impossible to have an event firing like this");
                },
                _ => None,
            }
        }, wakers)
    }

    fn submit_one_event(&mut self) -> Option<()> {
        if self.submission_queue.is_empty() {
            return None;
        }

        if let Some(mut sqe) = self.ring.next_sqe() {
            let op = self.submission_queue.pop_front().unwrap();
            fill_sqe(&mut sqe, &op);
            return Some(())
        }
        None
    }
}

pub struct Reactor {
    main_ring: RefCell<SleepableRing>,
    latency_ring: RefCell<SleepableRing>,
    poll_ring: RefCell<PollRing>,
    link_rings_src: RefCell<Pin<Box<Source>>>,
    files_registered: Option<RefCell<Vec<libc::c_int>>>,
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

macro_rules! consume_rings {
    (into $output:expr; $( $ring:expr ),+ ) => {{
        let mut consumed = 0;
        $(
            consumed += $ring.consume_completion_queue($output);
        )*
        consumed
    }}
}

macro_rules! flush_rings {
    ($( $ring:expr ),+ ) => {{
        $(
            $ring.consume_submission_queue()?;
        )*
        let ret : io::Result<()> = Ok(());
        ret
    }}
}


macro_rules! queue_request {
    ($ring:expr, $source:ident, $op:expr) => {{
        $ring.borrow_mut()
            .add_to_submission_queue($source, $op)
    }}
}

impl Reactor {
    pub(crate) fn new() -> io::Result<Reactor> {
        let main_ring = SleepableRing::new(128)?;
        let latency_ring = SleepableRing::new(128)?;
        let link_fd = latency_ring.ring_fd();

        Ok(Reactor {
            main_ring        : RefCell::new(main_ring),
            latency_ring     : RefCell::new(latency_ring),
            poll_ring        : RefCell::new(PollRing::new(128)?),
            link_rings_src   : RefCell::new(Source::new(link_fd, SourceType::LinkRings(false))),
        })
    }

    fn register_fd(&self, fd: RawFd) {

    }
    pub(crate) fn interest(&self, source : &Source, read: bool, write: bool) {
        let mut flags = common_flags();
        if read {
            flags |= read_flags();
        }
        if write {
            flags |= write_flags();
        }

        queue_request!(self.main_ring, source, UringOpDescriptor::PollAdd(flags));
    }

    pub(crate) fn write_dma(&self, source : &Source, buf : &[u8], pos : u64) {
        let op = UringOpDescriptor::Write(buf.as_ptr() as *const u8, buf.len(), pos);
        queue_request!(self.poll_ring, source, op);
    }

    pub(crate) fn read_dma(&self, source : &Source, buf : &mut [u8], pos : u64) {
        let op = UringOpDescriptor::Read(buf.as_mut_ptr() as *mut u8, buf.len(), pos);
        queue_request!(self.poll_ring, source, op);
    }

    pub(crate) fn fdatasync(&self, source : &Source) {
        queue_request!(self.main_ring, source, UringOpDescriptor::FDataSync);
    }

    pub(crate) fn fallocate(&self, source : &Source,
                     offset: u64, size: u64,
                     flags: libc::c_int) {

        let op = UringOpDescriptor::Fallocate(offset, size, flags);
        queue_request!(self.main_ring, source, op);
    }

    pub(crate) fn close(&self, source : &Source) {
        let op = UringOpDescriptor::Close;
        queue_request!(self.main_ring, source, op);
    }

    pub(crate) fn open_at(&self,
                   source : &Source,
                   flags  : libc::c_int,
                   mode   : libc::c_int)
    {
        let pathptr = match &source.source_type {
            SourceType::Open(cstring) => cstring.as_c_str().as_ptr(),
            _ => panic!("Wrong source type!"),
        };
        let op = UringOpDescriptor::Open(pathptr as _, flags, mode);
        queue_request!(self.main_ring, source, op);
    }

    pub(crate) fn insert(&self, fd: RawFd) -> io::Result<()> {
        add_flag(fd, libc::O_NONBLOCK)
    }

    /*
    pub(crate) fn cancel_io(&self, source : &Source) {
        let source_ptr = source as *const Source; 
        let op = match source.source_type {
            sys::SourceType::PollableFd => UringOpDescriptor::PollRemove(source_ptr as _),
            _ => UringOpDescriptor::Cancel(source_ptr as _),
        };
        self.add_to_submission_queue(source, op);
    }
    */

    // We want to go to sleep but we can only go to sleep in one of the rings,
    // as we only have one thread. There are more than one sleepable rings, so
    // what we do is we take advantage of the fact that the ring's ring_fd is pollable
    // and register a POLL_ADD event into the ring we will wait on.
    //
    // We may not be able to register an SQE at this point, so we return an Error and
    // will just not sleep.
    fn link_rings_and_sleep(&self,
                            ring: &mut SleepableRing,
                            timeout: Option<Duration>) -> io::Result<()>
    {
        let mut link_rings = self.link_rings_src.borrow_mut();
        ring.sleep(&mut link_rings, timeout)?;
        Ok(())
    }

    pub(crate) fn wait(&self, wakers : &mut Vec<Waker>, timeout: Option<Duration>) -> io::Result<usize> {
        let mut main_ring = self.main_ring.borrow_mut();
        let mut poll_ring = self.poll_ring.borrow_mut();
        let mut lat_ring = self.latency_ring.borrow_mut();

        flush_rings!(main_ring, poll_ring)?;

        let mut should_sleep = poll_ring.can_sleep();
        if let Some(dur) = timeout {
            if dur == Duration::from_millis(0) {
                should_sleep = false;
            }
        };

        let mut completed = 0;
        if should_sleep {
            completed += consume_rings!(into wakers; lat_ring, poll_ring, main_ring);
            if completed == 0 {
                self.link_rings_and_sleep(&mut main_ring, timeout)?;
            }
        }

        completed += consume_rings!(into wakers; lat_ring, poll_ring, main_ring);
        Ok(completed)
    }

    pub(crate) fn notify(&self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
    }
}
