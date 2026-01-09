//! SmallVec it's mostly a small (what irony) and scoped version
//! of [smallvec](https://docs.rs/smallvec/latest/smallvec/) to our needs.
//!
//! Most of code it's straight from there, so all hail to them.

#![allow(unused)]

use std::{
    alloc::Layout,
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    ptr::{copy_nonoverlapping, NonNull},
};

pub(crate) struct SmallVec<T, const S: usize> {
    len: TaggedLength,
    raw: RawSmallVec<T, S>,
    _marker: PhantomData<T>,
}

/// This represents either a stack array with `length <= S` or a heap array
/// whose pointer capacity are stored here.
///
/// Reference: [https://github.com/servo/rust-smallvec/blob/4dff9f2b5bbe5499e57cc6bf5b32f730558de45f/src/lib.rs#L133.]
union RawSmallVec<T, const S: usize> {
    inline: ManuallyDrop<MaybeUninit<[T; S]>>,
    heap: (NonNull<T>, usize),
}

#[derive(Clone, Copy)]
#[repr(transparent)]
struct TaggedLength(usize);

struct DropDealloc {
    ptr: NonNull<u8>,
    size: usize,
    align: usize,
}

struct DropGuard<T> {
    ptr: *mut T,
    len: usize,
}

#[derive(Debug)]
pub enum AllocErr {
    CapacityOverflow,
    Allocation { layout: Layout },
}

#[macro_export]
macro_rules! smallvec {
    (@one $x:expr) => {
        1usize
    };
    () => {
        $crate::core::smallvec::SmallVec::new()
    };
    ($element:expr, $size:expr) => ({
        $crate::core::smallvec::from_element($element, $size)
    });
    ($($x:expr),+$(,)?) => ({
        const COUNT: usize = 0usize $(+ $crate::smallvec!(@one $x))+;
        let mut vec = $crate::core::smallvec::SmallVec::new();
        if COUNT <= vec.capacity() {
            $(vec.push($x);)*
            vec
        } else {
            $crate::core::smallvec::SmallVec::from_vec(vec![$($x,)+])
        }
    });
}

impl<T, const S: usize> SmallVec<T, S> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            len: TaggedLength::new(0, false, Self::is_zst()),
            raw: RawSmallVec::new(),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut vec = Self::new();
        if capacity > Self::inline_size() {
            vec.grow(capacity);
        }

        vec
    }

    #[inline]
    pub fn from_vec(vec: Vec<T>) -> Self {
        if vec.capacity() == 0 {
            return Self::new();
        }

        match Self::is_zst() {
            true => {
                let mut vec = vec;
                let len = vec.len();

                unsafe { vec.set_len(0) };

                Self {
                    raw: RawSmallVec::new(),
                    len: TaggedLength::new(len, false, Self::is_zst()),
                    _marker: PhantomData,
                }
            }
            _ => {
                let mut vec = ManuallyDrop::new(vec);
                let len = vec.len();
                let capacity = vec.capacity();

                let ptr = unsafe { NonNull::new_unchecked(vec.as_mut_ptr()) };

                Self {
                    raw: RawSmallVec::new_heap(ptr, capacity),
                    len: TaggedLength::new(len, true, Self::is_zst()),
                    _marker: PhantomData,
                }
            }
        }
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.len.value(Self::is_zst())
    }

    #[inline]
    pub fn push(&mut self, value: T) {
        let len = self.len();

        if len == self.capacity() {
            self.reserve(1);
        }

        let ptr = unsafe { self.as_mut_ptr().add(len) };
        unsafe { ptr.write(value) };
        unsafe { self.set_len(len + 1) };
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        match self.is_empty() {
            true => None,
            _ => {
                let len = self.len() - 1;
                unsafe { self.set_len(len) };

                let value = unsafe { self.as_mut_ptr().add(len).read() };
                Some(value)
            }
        }
    }

    #[inline]
    pub fn append<const OS: usize>(&mut self, other: &mut SmallVec<T, OS>) {
        let len = self.len();
        let other_len = other.len();
        let total_len = len + other_len;

        if total_len > self.capacity() {
            self.reserve(other_len);
        }

        let ptr = unsafe { self.as_mut_ptr().add(len) };
        unsafe { other.set_len(0) };
        unsafe { copy_nonoverlapping(other.as_ptr(), ptr, other_len) };
        unsafe { self.set_len(total_len) }
    }

    #[inline]
    pub fn pop_if(&mut self, predicate: impl FnOnce(&mut T) -> bool) -> Option<T> {
        let last = self.last_mut()?;

        match predicate(last) {
            true => self.pop(),
            _ => None,
        }
    }

    #[inline]
    pub const fn as_slice(&self) -> &[T] {
        let len = self.len();
        let ptr = self.as_ptr();

        unsafe { core::slice::from_raw_parts(ptr, len) }
    }

    pub const fn as_mut_slice(&mut self) -> &mut [T] {
        let len = self.len();
        let ptr = self.as_mut_ptr();

        unsafe { core::slice::from_raw_parts_mut(ptr, len) }
    }

    #[inline]
    pub const fn as_ptr(&self) -> *const T {
        match self.len.on_heap(Self::is_zst()) {
            true => unsafe { self.raw.as_heap_ptr() },
            _ => self.raw.as_inline_ptr(),
        }
    }

    #[inline]
    pub const fn as_mut_ptr(&mut self) -> *mut T {
        match self.len.on_heap(Self::is_zst()) {
            true => unsafe { self.raw.as_mut_heap_ptr() },
            _ => self.raw.as_mut_inline_ptr(),
        }
    }

    pub const fn capacity(&self) -> usize {
        match self.len.on_heap(Self::is_zst()) {
            true => unsafe { self.raw.heap.1 },
            _ => Self::inline_size(),
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        if additional > self.capacity() - self.len() {
            let capacity = infallible(
                self.len()
                    .checked_add(additional)
                    .and_then(usize::checked_next_power_of_two)
                    .ok_or(AllocErr::CapacityOverflow),
            );

            self.grow(capacity)
        }
    }

    #[inline]
    const fn spilled(&self) -> bool {
        self.len.on_heap(Self::is_zst())
    }

    pub fn grow(&mut self, new_capacity: usize) {
        infallible(self.try_grow(new_capacity))
    }

    fn try_grow(&mut self, new_capacity: usize) -> Result<(), AllocErr> {
        if Self::is_zst() {
            return Ok(());
        }

        let len = self.len();
        assert!(new_capacity >= len, "length must not surpass capacity");

        match new_capacity > Self::inline_size() {
            true => {
                let result = unsafe { self.raw.try_grow(self.len, new_capacity) };
                if result.is_ok() {
                    unsafe { self.set_on_heap() };
                }

                result
            }
            _ => {
                if self.spilled() {
                    unsafe {
                        let (ptr, old_capacity) = self.raw.heap;
                        copy_nonoverlapping(ptr.as_ptr(), self.raw.as_mut_inline_ptr(), len);

                        drop(DropDealloc {
                            ptr: ptr.cast(),
                            size: old_capacity * size_of::<T>(),
                            align: align_of::<T>(),
                        });

                        self.set_inline();
                    }
                }

                Ok(())
            }
        }
    }

    /// # Safety
    ///
    /// - `new_len <= self.capacity()` must be true.
    /// - every element in `..self.len` must be initialisated.
    unsafe fn set_len(&mut self, new_len: usize) {
        assert!(new_len <= self.capacity());
        self.len = TaggedLength::new(new_len, self.len.on_heap(Self::is_zst()), Self::is_zst())
    }

    /// # Safety
    ///
    /// - The active union item must be the `self.raw.heap`
    #[inline]
    unsafe fn set_on_heap(&mut self) {
        self.len = TaggedLength::new(self.len(), true, Self::is_zst());
    }

    /// # Safety
    ///
    /// - The active union item must be the `self.raw.inline`
    #[inline]
    unsafe fn set_inline(&mut self) {
        self.len = TaggedLength::new(self.len(), false, Self::is_zst());
    }

    unsafe fn from_element(element: T, size: usize) -> Self
    where
        T: Clone,
    {
        let mut result = Self::new();

        if size > 0 {
            let ptr = result.raw.as_mut_inline_ptr();
            let mut guard = DropGuard { ptr, len: 0 };

            unsafe {
                for idx in 0..(size - 1) {
                    ptr.add(idx).write(element.clone());
                    guard.len += 1;
                }

                core::mem::forget(guard);
                ptr.add(size - 1).write(element);
            }
        }

        unsafe { result.set_len(size) };
        result
    }

    const fn inline_size() -> usize {
        match Self::is_zst() {
            true => usize::MAX,
            _ => S,
        }
    }

    const fn is_zst() -> bool {
        size_of::<T>() == 0
    }
}

impl<T, const S: usize> Default for SmallVec<T, S> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const S: usize> core::ops::Deref for SmallVec<T, S> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T, const S: usize> core::ops::DerefMut for SmallVec<T, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<T, const S: usize> RawSmallVec<T, S> {
    #[inline]
    const fn is_zst() -> bool {
        size_of::<T>() == 0
    }

    #[inline]
    const fn new() -> Self {
        Self::new_inline(MaybeUninit::uninit())
    }

    #[inline]
    const fn new_inline(inline: MaybeUninit<[T; S]>) -> Self {
        Self {
            inline: ManuallyDrop::new(inline),
        }
    }

    #[inline]
    const fn new_heap(ptr: NonNull<T>, capacity: usize) -> Self {
        Self {
            heap: (ptr, capacity),
        }
    }

    /// # Safety
    ///
    /// - `new_capacity` must be non-zero and greater or equal to length.
    /// - T must not be a ZST.
    unsafe fn try_grow(&mut self, len: TaggedLength, new_capacity: usize) -> Result<(), AllocErr> {
        use std::alloc::{alloc, realloc};

        assert!(!Self::is_zst());
        assert!(new_capacity > 0);
        assert!(new_capacity >= len.value(Self::is_zst()));

        let was_on_heap = len.on_heap(Self::is_zst());
        let ptr = match was_on_heap {
            true => self.as_mut_heap_ptr(),
            _ => self.as_mut_inline_ptr(),
        };

        let len = len.value(Self::is_zst());
        let new_layout =
            Layout::array::<T>(new_capacity).map_err(|_| AllocErr::CapacityOverflow)?;

        if new_layout.size() > isize::MAX as usize {
            return Err(AllocErr::CapacityOverflow);
        }

        let new_ptr = match len == 0 || !was_on_heap {
            // fresh allocation
            true => {
                let new_ptr = alloc(new_layout) as *mut T;
                let new_ptr =
                    NonNull::new(new_ptr).ok_or(AllocErr::Allocation { layout: new_layout })?;

                copy_nonoverlapping(ptr, new_ptr.as_ptr(), len);
                new_ptr
            }
            _ => {
                let old_layout = Layout::from_size_align_unchecked(
                    self.heap.1 * size_of::<T>(),
                    align_of::<T>(),
                );

                let new_ptr = realloc(ptr as *mut u8, old_layout, new_layout.size()) as *mut T;
                NonNull::new(new_ptr).ok_or(AllocErr::Allocation { layout: new_layout })?
            }
        };

        *self = Self::new_heap(new_ptr, new_capacity);
        Ok(())
    }

    /// # Safety
    ///
    /// - This vec must be on heap
    const unsafe fn as_mut_heap_ptr(&mut self) -> *mut T {
        self.heap.0.as_ptr()
    }

    /// # Safety
    ///
    /// - This vec must be on heap
    const unsafe fn as_heap_ptr(&self) -> *const T {
        self.heap.0.as_ptr()
    }

    /// # Safety
    ///
    /// - This vec must be on inline
    const fn as_mut_inline_ptr(&mut self) -> *mut T {
        (unsafe { std::ptr::addr_of!(self.inline) }) as *mut T
    }

    const fn as_inline_ptr(&self) -> *const T {
        (unsafe { std::ptr::addr_of!(self.inline) }) as *const T
    }
}

impl TaggedLength {
    #[inline]
    const fn new(len: usize, on_heap: bool, is_zst: bool) -> Self {
        match is_zst {
            true => {
                debug_assert!(!on_heap);
                Self(len)
            }
            _ => {
                debug_assert!(len < isize::MAX as usize);
                Self((len << 1) | on_heap as usize)
            }
        }
    }

    #[inline]
    const fn value(self, is_zst: bool) -> usize {
        match is_zst {
            true => self.0,
            _ => self.0 >> 1,
        }
    }

    #[inline]
    #[must_use]
    const fn on_heap(self, is_zst: bool) -> bool {
        match is_zst {
            true => false,
            _ => (self.0 & 1usize) == 1,
        }
    }
}

impl Drop for DropDealloc {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            if self.size > 0 {
                std::alloc::dealloc(
                    self.ptr.as_ptr(),
                    Layout::from_size_align_unchecked(self.size, self.align),
                );
            }
        }
    }
}

impl<T> Drop for DropGuard<T> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            core::ptr::slice_from_raw_parts_mut(self.ptr, self.len).drop_in_place();
        }
    }
}

impl core::fmt::Display for AllocErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Allocation error: {:?}", self)
    }
}

impl core::error::Error for AllocErr {}

#[inline]
#[track_caller]
fn from_element<T: Clone, const S: usize>(element: T, size: usize) -> SmallVec<T, S> {
    match size > SmallVec::<T, S>::inline_size() {
        true => SmallVec::<T, S>::from_vec(vec![element; size]),
        _ => unsafe { SmallVec::<T, S>::from_element(element, size) },
    }
}

#[inline]
fn infallible<T>(result: Result<T, AllocErr>) -> T {
    match result {
        Ok(x) => x,
        Err(AllocErr::CapacityOverflow) => panic!("capacity overflow"),
        Err(AllocErr::Allocation { layout }) => std::alloc::handle_alloc_error(layout),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline() {
        let mut vec = SmallVec::<_, 16>::new();

        vec.push("hello");
        vec.push("world");

        assert_eq!(&*vec, &["hello", "world"]);
    }

    #[test]
    fn spill() {
        let mut vec = SmallVec::<_, 2>::new();

        vec.push("hello");
        assert_eq!(vec[0], "hello");
        vec.push("there");
        vec.push("burma");
        assert_eq!(vec[0], "hello");
        vec.push("shave");
        assert_eq!(&*vec, &["hello", "there", "burma", "shave"]);
    }

    #[test]
    fn with_capacity() {
        let vec: SmallVec<u8, 3> = SmallVec::with_capacity(1);
        assert!(vec.is_empty());
        assert!(!vec.spilled());
        assert_eq!(vec.capacity(), 3);

        let vec: SmallVec<u8, 3> = SmallVec::with_capacity(10);
        assert!(vec.is_empty());
        assert!(vec.spilled());
        assert_eq!(vec.capacity(), 10);
    }
}
