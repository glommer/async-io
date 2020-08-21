use aligned_alloc::{aligned_alloc, aligned_free};
use typenum::*;
use bitmaps::*;
use std::io::IoSlice;
use std::rc::Rc;
use std::cell::RefCell;

pub struct UringSlab {
    data    : *mut u8,
    size    : usize,
    blksize : usize,
    free    : RefCell<Bitmap<U128>>,
}

pub struct UringDmaBuffer<'a> {
//pub struct UringDmaBuffer {
    data      : *mut u8,
    size      : usize,
    slab      : &'a mut UringSlab,
//    slab      : Rc<UringSlab>
}

/*
impl Drop for UringDmaBuffer {
    fn drop(&mut self) {
        self.slab.free_buffer(self.data);
    }
}
*/

/*
impl UringDmaBuffer {
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.data, self.size)
        }
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8{
        self.data
    }
}
*/

impl UringSlab {
    // FIXME: Result
    pub fn new(blksize: usize) -> UringSlab {
        let size = blksize * std::mem::size_of::<u128>();
        let data : *mut u8 = aligned_alloc(size, 4 << 10) as *mut u8;
        // FIXME: if null.... Result
        let mut free = Bitmap::new(); // inits to all false
        free.invert(); // all true now
        UringSlab {
            data,
            size,
            free : RefCell::new(free),
            blksize,
        }
    }

    pub fn as_io_slice(&mut self) -> IoSlice<'_> {
        let slice = unsafe {
            std::slice::from_raw_parts(self.data, self.size)
        };
        IoSlice::new(slice)
    }

    pub fn free_buffer(&self , buffer: *mut u8) {
        let buffer = buffer as u64;
        let data = self.data as u64;
        let diff = (buffer - data) as usize;
        assert_eq!(diff % self.blksize, 0);
        let bit = diff / self.blksize;
        println!("Freeing buffer at {:x} from {:x}, calculated bit {}", buffer, data, bit);
        let mut free = self.free.borrow_mut();
        let was = free.set(bit, true);
        assert_eq!(was, false);
    }

//    pub fn alloc_buffer(self: Rc<Self>, size: usize) -> Option<UringDmaBuffer> {
    pub fn alloc_buffer<'a>(&'a mut self, size: usize) -> Option<UringDmaBuffer<'a>> {
        let mut free = self.free.borrow_mut();
        let bit = free.first_index();
        match bit {
            Some(bit) => {
                free.set(bit, false);
                drop(free);

                let ptr = unsafe { self.data.add(self.blksize * bit) };
                Some(UringDmaBuffer {
                    data: ptr,
                    size, // may be different than allocated size, this is sans any padding
//                    slab: self.clone(),
                    slab: self,                    
                })
            },
            None => None,
        }
    }
}

impl Drop for UringSlab {
    fn drop(&mut self) {
        unsafe {
            aligned_free(self.data as _);
        }
    }
}
