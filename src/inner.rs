use core::hint::spin_loop;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::{drop_in_place, NonNull};
use std::sync::atomic::{AtomicU32, Ordering};
use anyhow::{Result,anyhow};

/// 最大读取锁的数量
const SPIN_MAX_COUNT: usize = 5;
const READ_MAX_COUNT: u32 = u16::MAX as u32;
const MAX_REFERENCE_COUNT: u32 = ((2 as u32).pow(16)) - 1;

pub trait LockTrait {}
impl<T> LockTrait for T {}

pub struct RWLock<T: LockTrait> {
    ptr: NonNull<Inner<T>>,
    _marker: PhantomData<Inner<T>>,
}

impl<T: LockTrait> RWLock<T> {
    pub fn new(data: T) -> RWLock<T> {
        let x = Box::new(Inner::new(data));
        unsafe {
            RWLock {
                ptr: NonNull::new_unchecked(Box::into_raw(x)),
                _marker: Default::default(),
            }
        }
    }

    pub fn read(&self) -> Result<RWLockReader<'_, T>> {
        let _ = self.inner().get_read_right()?;
        Ok(RWLockReader { reader: self })
    }

    pub async fn read_async(&self) -> Result<RWLockReader<'_, T>> {
        let _ = self.inner().get_async_read_right().await?;
        Ok(RWLockReader { reader: self })
    }

    pub fn write(&mut self) -> Result<RWLockWriter<'_, T>> {
        let _ = self.inner().get_write_right()?;

        Ok(RWLockWriter { writer: self })
    }

    pub async fn write_async(&mut self) -> Result<RWLockWriter<'_, T>> {
        let _ = self.inner().get_async_write_right().await?;

        Ok(RWLockWriter { writer: self })
    }

    pub fn write_object(self)->Result<RWLockWriteObject<T>>{
        self.inner().get_write_right()?;
        Ok(RWLockWriteObject{
            rw_lock:self
        })
    }

    pub async fn async_write_object(self)->Result<RWLockWriteObject<T>>{
        self.inner().get_async_write_right().await?;
        Ok(RWLockWriteObject{
            rw_lock:self
        })
    }

    pub fn read_object(self)->Result<RWLockReadObject<T>>{
        self.inner().get_read_right()?;
        Ok(RWLockReadObject{
            rw_lock:self
        })
    }

    pub async fn async_read_object(self)->Result<RWLockReadObject<T>>{
        self.inner().get_async_read_right().await?;
        Ok(RWLockReadObject{
            rw_lock:self
        })
    }

    fn inner(&self) -> &Inner<T> {
        unsafe { self.ptr.as_ref() }
    }

    fn inner_mut(&mut self) -> &mut Inner<T> {
        unsafe { self.ptr.as_mut() }
    }
}
unsafe impl<T: Send> Send for RWLock<T> {}
unsafe impl<T: Sync> Sync for RWLock<T> {}

impl<T: LockTrait> Clone for RWLock<T> {
    fn clone(&self) -> Self {
        // 增加引用
        let _ = self.inner().add_ref();

        RWLock {
            ptr: self.ptr,
            _marker: Default::default(),
        }
    }
}
impl<T: LockTrait> Drop for RWLock<T> {
    fn drop(&mut self) {
        let _ = self.inner().ref_reduce();
        // 无引用，则可以释放内存
        if self.inner().check_can_drop() == true {
            unsafe {
                // 释放内存
                drop_in_place(self.ptr.as_ptr());
            }
        }
    }
}

pub struct RWLockReader<'a, T: LockTrait> {
    reader: &'a RWLock<T>,
}
impl<'a, T: LockTrait> Drop for RWLockReader<'a, T> {
    fn drop(&mut self) {
        let _ = self.reader.inner().release_read_lock();
    }
}
impl<'a, T: LockTrait> Deref for RWLockReader<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.reader.inner().data
    }
}

pub struct RWLockWriter<'a, T: LockTrait> {
    writer: &'a mut RWLock<T>,
}
impl<'a, T: LockTrait> Drop for RWLockWriter<'a, T> {
    fn drop(&mut self) {
        let _ = self.writer.inner().release_write_lock();
    }
}
impl<'a, T: LockTrait> Deref for RWLockWriter<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.writer.inner().data
    }
}
impl<'a, T: LockTrait> DerefMut for RWLockWriter<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.writer.inner_mut().data
    }
}

/// 读写锁写对象
pub struct RWLockWriteObject<T>{
    rw_lock:RWLock<T>
}
impl<T> Drop for RWLockWriteObject<T> {
    fn drop(&mut self) {
        let _ = self.rw_lock.inner().release_write_lock();
    }
}
impl<T> Deref for RWLockWriteObject<T>{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.rw_lock.inner().data
    }
}
impl<T> DerefMut for RWLockWriteObject<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rw_lock.inner_mut().data
    }
}

/// 读写对象读锁
pub struct RWLockReadObject<T>{
    rw_lock:RWLock<T>
}
impl<T> Drop for RWLockReadObject<T> {
    fn drop(&mut self) {
        let _ = self.rw_lock.inner().release_read_lock();
    }
}
impl<T> Deref for RWLockReadObject<T>{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.rw_lock.inner().data
    }
}

struct Inner<T: LockTrait> {
    /*
        1bit:写标记,
        2-16bit:对象引用计数，
        17-32bit:读标记
    */
    lock: AtomicU32,
    data: T,
}
impl<T: LockTrait> Inner<T> {
    fn new(data: T) -> Inner<T> {
        let reference = 1 << 16;
        Inner {
            lock: AtomicU32::new(reference),
            data,
        }
    }

    /// 增加引用计数
    pub(super) fn add_ref(&self) -> Result<()> {
        let handle = || -> Result<bool> {
            let current = self.lock.load(Ordering::Acquire);
            let lock = &self.lock;
            let head = current >> 16;

            let reference_count = head & MAX_REFERENCE_COUNT;
            if reference_count + 1 > MAX_REFERENCE_COUNT {
                return Err(anyhow!("too many reference"));
            }

            let new = ((head + 1) << 16) | (current & READ_MAX_COUNT);
            if let Ok(_) = lock.compare_exchange(current, new, Ordering::Release, Ordering::Relaxed)
            {
                return Ok(true);
            }

            Ok(false)
        };

        get_thread_time(handle)
    }

    /// 减少引用
    fn ref_reduce(&self) -> Result<()> {
        let handler = || -> Result<bool> {
            let current = self.lock.load(Ordering::Acquire);
            let lock = &self.lock;
            let header = current >> 16;
            let reference = header & MAX_REFERENCE_COUNT;
            if reference == 0 {
                return Err(anyhow!( "reference error"));
            }

            let now = ((header - 1) << 16) | (current & READ_MAX_COUNT);
            if let Ok(_) = lock.compare_exchange(current, now, Ordering::Release, Ordering::Relaxed)
            {
                return Ok(true);
            }

            Ok(false)
        };

        get_thread_time(handler)
    }

    /// 是否能够被释放
    fn check_can_drop(&self) -> bool {
        let current = self.lock.load(Ordering::Acquire);
        let reference = current >> 16 & MAX_REFERENCE_COUNT;

        reference == 0
    }

    /// 获取读的权限
    fn get_read_right(&self) -> Result<()> {
        let handle = || -> Result<bool> {
            let current = self.lock.load(Ordering::Acquire);
            let lock = &self.lock;

            // 是否被写占用
            let if_write = current >> 31;
            if if_write > 0 {
                return Ok(false);
            }

            let read_count = current & READ_MAX_COUNT;

            if read_count + 1 > READ_MAX_COUNT {
                return Err(anyhow!("too many readers"));
            }

            // 读次数加1
            if let Ok(_) =
                lock.compare_exchange(current, current + 1, Ordering::Release, Ordering::Relaxed)
            {
                return Ok(true);
            }

            return Ok(false);
        };

        get_thread_time(handle)
    }

    async fn get_async_read_right(&self) -> Result<()> {
        let handle = || -> Result<bool> {
            let current = self.lock.load(Ordering::Acquire);
            let lock = &self.lock;

            // 是否被写占用
            let if_write = current >> 31;
            if if_write > 0 {
                return Ok(false);
            }

            let read_count = current & READ_MAX_COUNT;

            if read_count + 1 > READ_MAX_COUNT {
                return Err(anyhow!("too many readers"));
            }

            // 读次数加1
            if let Ok(_) =
                lock.compare_exchange(current, current + 1, Ordering::Release, Ordering::Relaxed)
            {
                return Ok(true);
            }

            return Ok(false);
        };

        get_async_thread_time(handle).await
    }

    /// 释放读锁
    fn release_read_lock(&self) -> Result<()> {
        let handle = || -> Result<bool> {
            let current = self.lock.load(Ordering::Acquire);
            let lock = &self.lock;

            let read = current & READ_MAX_COUNT;
            if read == 0 {
                return Err(anyhow!("release read lock Error"));
            }

            if let Ok(_) =
                lock.compare_exchange(current, current - 1, Ordering::Release, Ordering::Relaxed)
            {
                return Ok(true);
            }

            Ok(false)
        };

        get_thread_time(handle)
    }

    /// 获取写的权限
    fn get_write_right(&self) -> Result<()> {
        let handle = || -> Result<bool> {
            let current = self.lock.load(Ordering::Acquire);
            let lock = &self.lock;

            // 是否被读占用
            let read_count = current & READ_MAX_COUNT;
            if read_count > 0 {
                return Ok(false);
            }

            // 是否被写占用
            let if_write = current >> 31;
            if if_write > 0 {
                return Ok(false);
            }

            let new_data = 1 << 31 | current;
            if let Ok(_) =
                lock.compare_exchange(current, new_data, Ordering::Release, Ordering::Relaxed)
            {
                return Ok(true);
            }

            return Ok(false);
        };

        get_thread_time(handle)
    }

    async fn get_async_write_right(&self) -> Result<()> {
        let handle = || -> Result<bool> {
            let current = self.lock.load(Ordering::Acquire);
            let lock = &self.lock;

            // 是否被读占用
            let read_count = current & READ_MAX_COUNT;
            if read_count > 0 {
                return Ok(false);
            }

            // 是否被写占用
            let if_write = current >> 31;
            if if_write > 0 {
                return Ok(false);
            }

            let new_data = 1 << 31 | current;
            if let Ok(_) =
                lock.compare_exchange(current, new_data, Ordering::Release, Ordering::Relaxed)
            {
                return Ok(true);
            }

            return Ok(false);
        };

        get_async_thread_time(handle).await
    }

    /// 释放写锁
    fn release_write_lock(&self) -> Result<()> {
        let handle = || -> Result<bool> {
            let current = self.lock.load(Ordering::Acquire);
            let lock = &self.lock;

            let writer = current >> 31;
            if writer == 0 {
                return Err(anyhow!("release write lock Error"));
            }

            let now = current ^ (1 << 31);
            if let Ok(_) = lock.compare_exchange(current, now, Ordering::Release, Ordering::Relaxed)
            {
                return Ok(true);
            }

            Ok(false)
        };

        get_thread_time(handle)
    }
}

/// 如果是异步可能导致运行时线程被阻塞
pub(crate) fn get_thread_time<F>(handle: F) -> Result<()>
where
    F: Fn() -> Result<bool>,
{
    let mut spin_execute = 0;
    loop {
        if spin_execute < SPIN_MAX_COUNT - 1 {
            for _ in 0..1 << spin_execute {
                // 自旋
                spin_loop();
            }

            spin_execute = (spin_execute + 1) % SPIN_MAX_COUNT;
        } else {
            for _ in 0..1 << spin_execute {
                // 自旋
                spin_loop();
            }

            // 让出时间片
            std::thread::yield_now();
        }
        let success = handle()?;
        if success == true {
            return Ok(());
        }
    }
}

/// 异步获取线程时间
pub(crate) async fn get_async_thread_time<F>(handle: F) -> Result<()>
where
    F: Fn() -> Result<bool>,
{
    let mut spin_execute = 0;
    loop {
        if spin_execute < SPIN_MAX_COUNT - 1 {
            for _ in 0..1 << spin_execute {
                // 自旋
                spin_loop();
            }

            spin_execute = (spin_execute + 1) % SPIN_MAX_COUNT;
        } else {
            for _ in 0..1 << spin_execute {
                // 自旋
                spin_loop();
            }

            // 让出时间片
            tokio::task::yield_now().await;
        }
        let success = handle()?;
        if success == true {
            return Ok(());
        }
    }
}
