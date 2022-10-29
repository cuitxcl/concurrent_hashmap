use super::inner::{RWLock, RWLockReader, RWLockWriter};
use crate::inner::get_thread_time;
use core::hash::Hasher;
use std::collections::hash_map::DefaultHasher;
use std::collections::hash_map::RandomState;
use std::collections::LinkedList;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::mem::forget;
use std::option::Option::Some;
use std::ptr::{drop_in_place, null_mut, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;
use anyhow::{Result};

/// 负载因子
const LOAD_FACTOR: f64 = 0.75;

const MAX_ENTRY_COUNT: u64 = u32::MAX as u64;

const MAX_OPERATE_COUNT: u64 = i32::MAX as u64;

pub trait HashKey: Hash + Eq + PartialOrd + Clone + Debug {}
impl<T> HashKey for T where T: Hash + Eq + PartialOrd + Clone + Debug {}

pub struct ConcurrentHashMap<K, V>
where
    K: HashKey,
{
    operate_controller: OperateController,
    bucket: NonNull<Arc<BucketContainer<K, V>>>,
}

unsafe impl<K: HashKey, V: Send> Send for ConcurrentHashMap<K, V> {}
unsafe impl<K: HashKey, V: Sync> Sync for ConcurrentHashMap<K, V> {}

impl<K, V> ConcurrentHashMap<K, V>
where
    K: HashKey,
{
    pub fn new_init_capacity(capacity: usize) -> ConcurrentHashMap<K, V> {
        let new_capacity = get_appropriate_size(capacity as u32).unwrap();
        let bucket = Arc::new(BucketContainer::<K, V>::new(new_capacity as usize));
        unsafe {
            ConcurrentHashMap {
                operate_controller: OperateController::new(),
                bucket: NonNull::new_unchecked(Box::into_raw(Box::new(bucket))),
            }
        }
    }

    pub fn new() -> ConcurrentHashMap<K, V> {
        let default_capacity = 256;
        ConcurrentHashMap::new_init_capacity(default_capacity)
    }

    pub fn insert(&self, key: K, value: V) -> Option<RWLock<V>> {
        // 获得操作权限
        let handler = |bucket: &Arc<BucketContainer<K, V>>| -> Option<RWLock<V>> {
            bucket.insert(key, value)
        };

        self.insert_try_resize(handler)
    }

    pub fn insert_rw(&self, key: K, value: RWLock<V>) -> Option<RWLock<V>> {
        // 获得操作权限
        let handler = |bucket: &Arc<BucketContainer<K, V>>| -> Option<RWLock<V>> {
            bucket.insert_rw(key, value)
        };

        self.insert_try_resize(handler)
    }

    /// 不存在则插入，否则返回存在的Value
    pub fn insert_not_exist(&self, key: K, value: V) -> Option<RWLock<V>> {
        // 获得操作权限
        let handler = |bucket: &Arc<BucketContainer<K, V>>| -> Option<RWLock<V>> {
            bucket.insert_not_exist(key, value)
        };

        self.insert_try_resize(handler)
    }

    pub fn remove(&self, key: &K) -> Option<RWLock<V>> {
        self.operate_controller.get_operate_right();

        let bucket = self.get_bucket();
        let result = bucket.remove(key);
        if result.is_some() {
            self.operate_controller.remove_node();
        }

        self.operate_controller.release_operate_right();

        result
    }

    pub fn get(&self, key: &K) -> Option<RWLock<V>> {
        // self.operate_controller.get_operate_right();
        let bucket = self.get_bucket();
        // self.operate_controller.release_operate_right();
        bucket.get(key)
    }

    pub fn exist(&self, key: &K) -> bool {
        // self.operate_controller.get_operate_right();
        let bucket = self.get_bucket();
        // self.operate_controller.release_operate_right();

        bucket.exist(key)
    }

    pub fn len(&self) -> u64 {
        self.operate_controller.len()
    }

    pub fn iter(&self) -> BucketIter<'_, '_, K, V> {
        let bucket = self.get_bucket();
        BucketIter {
            bucket_container: bucket,
            index: 0,
            start_flag: false,
            entry_lock: None,
            entry_reader: None,
            entry_iter: None,
            _lock_market: Default::default(),
            _reader_market: Default::default(),
            _iter_market: Default::default(),
        }
    }

    fn get_bucket(&self) -> Arc<BucketContainer<K, V>> {
        unsafe {
            let bucket = std::ptr::read_volatile(self.bucket.as_ptr());
            let clone = bucket.clone();
            forget(bucket);
            return clone;
        }
    }

    fn insert_try_resize<F>(&self, insert_fn: F) -> Option<RWLock<V>>
    where
        F: FnOnce(&Arc<BucketContainer<K, V>>) -> Option<RWLock<V>>,
    {
        self.operate_controller.get_operate_right();
        let bucket = self.get_bucket();
        let insert_operate = insert_fn(&bucket);

        // 代表写入成功
        if insert_operate.is_none() {
            let max_entry_count = (bucket.capacity as f64 * LOAD_FACTOR) as u64;

            // 代表需要扩容(并且获得扩容资格)
            if self.operate_controller.add_node(max_entry_count) == true {
                // 已经操作完成,释放操作权限
                self.operate_controller.release_operate_right();

                // 获得扩容权限(等待其他操作完成,保证bucket 不会变动)
                self.operate_controller.get_resize_right();

                // 能够重新分配
                if let Ok(new_capacity) = get_appropriate_size(bucket.capacity as u32) {
                    let resize_bucket =
                        BucketContainer::resize_with_bucket(new_capacity as usize, bucket.clone());
                    unsafe {
                        let old_bucket =
                            std::mem::replace(&mut *self.bucket.as_ptr(), *resize_bucket);
                        drop(old_bucket);
                    }
                }

                // 重置操作标记
                self.operate_controller.reset_operate_tag();
            } else {
                // 释放操作权限
                self.operate_controller.release_operate_right();
            }
        } else {
            // 释放操作权限
            self.operate_controller.release_operate_right();
        }

        insert_operate
    }
}

/// 最大容积为:1<<31
fn get_appropriate_size(capacity: u32) -> Result<u32, String> {
    if capacity >= (1 << 31) as u32 {
        return Err("capacity too large".to_owned());
    }

    let mut i = 4;
    loop {
        let current_size = (2 as u32).pow(i);
        if current_size >= (1 << 31) as u32 {
            return Ok(1 << 31);
        }

        if current_size > capacity {
            return Ok(current_size);
        }

        i += 1;
    }
}

struct BucketContainer<K, V>
where
    K: HashKey,
{
    capacity: usize,
    buckets: Vec<AtomicPtr<RWLock<Entry<K, V>>>>,
    hasher: DefaultHasher,
}
impl<K, V> BucketContainer<K, V>
where
    K: HashKey,
{
    fn new(capacity: usize) -> BucketContainer<K, V> {
        let mut buckets = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buckets.push(AtomicPtr::default());
        }

        let random_state = RandomState::new();
        BucketContainer {
            capacity,
            buckets,
            hasher: random_state.build_hasher(),
        }
    }

    /// 扩容
    fn resize_with_bucket(
        capacity: usize,
        other: Arc<BucketContainer<K, V>>,
    ) -> Box<Arc<BucketContainer<K, V>>> {
        let mut buckets = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buckets.push(AtomicPtr::default());
        }

        let random_state = RandomState::new();
        let buckets = Box::new(Arc::new(BucketContainer {
            capacity,
            buckets,
            hasher: random_state.build_hasher(),
        }));

        for i in 0..other.buckets.len() {
            let ptr = other.buckets[i].load(Ordering::Acquire);
            if ptr != null_mut() {
                unsafe {
                    let entry = Box::from_raw(ptr);
                    let entry_clone = entry.clone();
                    forget(entry);
                    let reader = entry_clone.read().unwrap();
                    for node in reader.nodes.iter() {
                        let item = node.as_ref();
                        buckets.push_node(&item.key, node.clone());
                    }
                }
            }
        }

        buckets
    }

    fn insert(&self, key: K, value: V) -> Option<RWLock<V>> {
        let clone_key = key.clone();
        let handler = |_: Option<RWLockReader<Entry<K, V>>>,
                       writer_opt: Option<RWLockWriter<Entry<K, V>>>|
         -> Option<RWLock<V>> {
            let mut writer = writer_opt.unwrap();
            writer.insert(clone_key, value)
        };

        self.get_entry_default_new(&key, true, handler)
    }

    fn insert_rw(&self, key: K, value: RWLock<V>) -> Option<RWLock<V>> {
        let clone_key = key.clone();
        let handler = |_: Option<RWLockReader<Entry<K, V>>>,
                       writer_opt: Option<RWLockWriter<Entry<K, V>>>|
         -> Option<RWLock<V>> {
            let mut writer = writer_opt.unwrap();
            writer.insert_rw(clone_key, value)
        };

        self.get_entry_default_new(&key, true, handler)
    }

    fn insert_not_exist(&self, key: K, value: V) -> Option<RWLock<V>> {
        let clone_key = key.clone();
        let handler = |_: Option<RWLockReader<Entry<K, V>>>,
                       writer_opt: Option<RWLockWriter<Entry<K, V>>>|
         -> Option<RWLock<V>> {
            let mut writer = writer_opt.unwrap();
            writer.insert_not_exist(clone_key, value)
        };

        self.get_entry_default_new(&key, true, handler)
    }

    fn remove(&self, key: &K) -> Option<RWLock<V>> {
        let handler = |key: &K,
                       _: Option<RWLockReader<Entry<K, V>>>,
                       writer_opt: Option<RWLockWriter<Entry<K, V>>>|
         -> Option<RWLock<V>> {
            let mut writer = writer_opt.unwrap();
            writer.remove(key)
        };

        self.get_entry(key, true, handler)
    }

    fn get(&self, key: &K) -> Option<RWLock<V>> {
        let handler = |key: &K,
                       reader_opt: Option<RWLockReader<Entry<K, V>>>,
                       _: Option<RWLockWriter<Entry<K, V>>>|
         -> Option<RWLock<V>> {
            let reader = reader_opt.unwrap();
            reader.get(key)
        };

        self.get_entry(key, false, handler)
    }

    fn push_node(&self, key: &K, node: NonNull<Node<K, V>>) {
        let handler = |_: Option<RWLockReader<Entry<K, V>>>,
                       writer_opt: Option<RWLockWriter<Entry<K, V>>>|
         -> Option<RWLock<V>> {
            let mut writer = writer_opt.unwrap();
            writer.push_node(node);

            None
        };

        let _ = self.get_entry_default_new(key, true, handler);
    }

    fn exist(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    fn get_entry_default_new<F>(&self, key: &K, if_write: bool, handler: F) -> Option<RWLock<V>>
    where
        F: FnOnce(
            Option<RWLockReader<Entry<K, V>>>,
            Option<RWLockWriter<Entry<K, V>>>,
        ) -> Option<RWLock<V>>,
    {
        let mut hash = self.hasher.clone();
        key.hash(&mut hash);
        let hash = hash.finish();
        let cal_index = ((hash >> 32) & (self.capacity as u64 - 1)) as usize;
        let item = &self.buckets[cal_index];
        loop {
            let ptr = item.load(Ordering::Acquire);
            if ptr == null_mut() {
                let new_data = Box::new(RWLock::new(Entry::<K, V>::new()));
                let _ = item.compare_exchange(
                    null_mut(),
                    Box::into_raw(new_data),
                    Ordering::Release,
                    Ordering::Relaxed,
                );
            } else {
                unsafe {
                    let mut entry = Box::from_raw(ptr);
                    let result;
                    if if_write == true {
                        let writer = entry.write().unwrap();
                        result = handler(None, Some(writer));
                    } else {
                        let reader = entry.read().unwrap();
                        result = handler(Some(reader), None);
                    }

                    forget(entry);

                    return result;
                }
            }
        }
    }

    fn get_entry<F>(&self, key: &K, if_write: bool, handler: F) -> Option<RWLock<V>>
    where
        F: FnOnce(
            &K,
            Option<RWLockReader<Entry<K, V>>>,
            Option<RWLockWriter<Entry<K, V>>>,
        ) -> Option<RWLock<V>>,
    {
        let mut hash = self.hasher.clone();
        key.hash(&mut hash);
        let hash = hash.finish();
        let cal_index = ((hash >> 32) & (self.capacity as u64 - 1)) as usize;

        let ptr = self.buckets[cal_index].load(Ordering::Relaxed);

        return if ptr == null_mut() {
            None
        } else {
            unsafe {
                let mut entry = Box::from_raw(ptr);
                let result;
                if if_write == true {
                    let writer = entry.write().unwrap();
                    result = handler(key, None, Some(writer));
                } else {
                    let reader = entry.read().unwrap();
                    result = handler(key, Some(reader), None);
                }

                forget(entry);
                result
            }
        };
    }
}
impl<K: HashKey, V> Drop for BucketContainer<K, V> {
    fn drop(&mut self) {
        for atomic_ptr in self.buckets.iter() {
            let ptr = atomic_ptr.swap(null_mut(), Ordering::Acquire);
            if ptr != null_mut() {
                unsafe {
                    drop_in_place(ptr);
                }
            }
        }
    }
}

pub struct BucketIter<'b, 'c: 'b, K: HashKey, V> {
    bucket_container: Arc<BucketContainer<K, V>>,
    index: usize,
    start_flag: bool,
    entry_lock: Option<NonNull<RWLock<Entry<K, V>>>>,
    entry_reader: Option<NonNull<RWLockReader<'b, Entry<K, V>>>>,
    entry_iter: Option<NonNull<EntryIter<'c, K, V>>>,

    _lock_market: PhantomData<RWLock<Entry<K, V>>>,
    _reader_market: PhantomData<RWLockReader<'b, Entry<K, V>>>,
    _iter_market: PhantomData<EntryIter<'c, K, V>>,
}
impl<'b, 'c: 'b, K: HashKey, V> Iterator for BucketIter<'b, 'c, K, V> {
    type Item =(&'c K, RWLock<V>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.start_flag == true {
                if let Some(entry_iter_origin_ptr) = self.entry_iter.as_mut() {
                    unsafe {
                        let entry_iter = entry_iter_origin_ptr.as_mut();
                        let rw_node = entry_iter.next();
                        if let Some(node) = rw_node {
                            return Some(node);
                        } else {
                            // 释放entry的相关锁
                            let entry_lock = self.entry_lock.take().unwrap();
                            drop_in_place(entry_lock.as_ptr());
                            let entry_reader = self.entry_reader.take().unwrap();
                            drop_in_place(entry_reader.as_ptr());
                            let entry_iter = self.entry_iter.take().unwrap();
                            drop_in_place(entry_iter.as_ptr());
                        }
                    }
                }
            }

            if self.index == self.bucket_container.buckets.len() - 1 {
                return None;
            }

            if self.start_flag == true {
                self.index += 1;
            } else {
                self.start_flag = true;
            }

            let ptr = self.bucket_container.buckets[self.index].load(Ordering::Acquire);
            if ptr != null_mut() {
                // 复制entry锁
                unsafe {
                    let entry_lock = Box::from_raw(ptr);
                    let entry_clone = Box::new((*entry_lock).clone());
                    forget(entry_lock);

                    let entry_lock_ptr = NonNull::new_unchecked(Box::into_raw(entry_clone));
                    let entry_lock_ptr_clone = entry_lock_ptr.clone();
                    self.entry_lock = Some(entry_lock_ptr_clone);

                    let entry_reader = entry_lock_ptr.as_ref().read().unwrap();
                    let read = Box::new(entry_reader);
                    let reader_ptr = NonNull::new_unchecked(Box::into_raw(read));
                    let reader_ptr_clone = reader_ptr.clone();
                    self.entry_reader = Some(reader_ptr);

                    let entry_iter = reader_ptr_clone.as_ref().iter();
                    let entry_iter_box = Box::new(entry_iter);
                    let entry_iter_ptr = NonNull::new(Box::into_raw(entry_iter_box));
                    self.entry_iter = entry_iter_ptr;
                }
            }
        }
    }
}
impl<'b, 'c: 'b, K: HashKey, V> Drop for BucketIter<'b, 'c, K, V> {
    fn drop(&mut self) {
        if let Some(lock) = self.entry_lock.take() {
            unsafe {
                drop_in_place(lock.as_ptr());
            }
        }

        if let Some(reader) = self.entry_reader.take() {
            unsafe {
                drop_in_place(reader.as_ptr());
            }
        }

        if let Some(iter) = self.entry_iter.take() {
            unsafe {
                drop_in_place(iter.as_ptr());
            }
        }
    }
}
unsafe impl<'b, 'c, K: HashKey, V: Send> Send for BucketIter<'_, '_, K, V> {}
unsafe impl<'b, 'c, K: HashKey, V: Sync> Sync for BucketIter<'_, '_, K, V> {}

struct Entry<K, V>
where
    K: HashKey,
{
    nodes: LinkedList<NonNull<Node<K, V>>>,
    _maker: PhantomData<Node<K, V>>,
}
impl<K, V> Entry<K, V>
where
    K: HashKey,
{
    fn new() -> Self {
        Entry {
            nodes: LinkedList::default(),
            _maker: PhantomData::default(),
        }
    }

    fn insert(&mut self, key: K, value: V) -> Option<RWLock<V>> {
        for ptr in self.nodes.iter_mut() {
            unsafe {
                let node = ptr.as_mut();
                if node.compare_key(&key) == true {
                    let result = node.update(value);

                    return Some(result);
                }
            }
        }

        let nodes = Node::new(key, value);
        unsafe {
            let node_ptr = NonNull::new_unchecked(Box::into_raw(Box::new(nodes)));
            self.nodes.push_back(node_ptr);
        }

        None
    }

    fn insert_rw(&mut self, key: K, value: RWLock<V>) -> Option<RWLock<V>> {
        for ptr in self.nodes.iter_mut() {
            unsafe {
                let node = ptr.as_mut();
                if node.compare_key(&key) == true {
                    let result = node.update_rw(value);

                    return Some(result);
                }
            }
        }

        let nodes = Node::new_rw(key, value);
        unsafe {
            let node_ptr = NonNull::new_unchecked(Box::into_raw(Box::new(nodes)));
            self.nodes.push_back(node_ptr);
        }

        None
    }

    fn insert_not_exist(&mut self, key: K, value: V) -> Option<RWLock<V>> {
        for ptr in self.nodes.iter_mut() {
            unsafe {
                let node = ptr.as_mut();
                if node.compare_key(&key) == true {
                    return Some(node.get_value());
                }
            }
        }

        let nodes = Node::new(key, value);
        unsafe {
            let node_ptr = NonNull::new_unchecked(Box::into_raw(Box::new(nodes)));
            self.nodes.push_back(node_ptr);
        }

        None
    }

    fn push_node(&mut self, node: NonNull<Node<K, V>>) {
        self.nodes.push_front(node)
    }

    fn get(&self, key: &K) -> Option<RWLock<V>> {
        for ptr in self.nodes.iter() {
            unsafe {
                let node = ptr.as_ref();
                if node.compare_key(key) == true {
                    return Some(node.get_value());
                }
            }
        }

        return None;
    }

    fn remove(&mut self, key: &K) -> Option<RWLock<V>> {
        let length = self.nodes.len();
        for _ in 0..length {
            if let Some(ptr) = self.nodes.pop_back() {
                let mut exist = false;
                unsafe {
                    let node = ptr.as_ref();
                    if node.compare_key(key) == true {
                        exist = true;
                    }
                }

                if exist == true {
                    unsafe {
                        let data = Box::from_raw(ptr.as_ptr());
                        let result = data.get_value();
                        drop(data);

                        return Some(result);
                    }
                } else {
                    self.nodes.push_front(ptr);
                }
            }
        }

        None
    }

    fn iter(&self) -> EntryIter<'_, K, V> {
        EntryIter {
            node: self.nodes.iter(),
        }
    }
}
impl<K: HashKey, V> Drop for Entry<K, V> {
    fn drop(&mut self) {
        // println!("entry 释放");
    }
}

pub struct EntryIter<'a, K: HashKey, V> {
    node: std::collections::linked_list::Iter<'a, NonNull<Node<K, V>>>,
}
impl<'a, K: HashKey, V> Iterator for EntryIter<'a, K, V> {
    type Item =(&'a K, RWLock<V>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(node_ptr) = self.node.next() {
            unsafe {
                let node = node_ptr.as_ref();
                return Some((&node.key, node.get_value()));
            }
        }
        None
    }
}
impl<'a, K: HashKey, V> Drop for EntryIter<'_, K, V> {
    fn drop(&mut self) {
        // println!("释放 EntryIter");
    }
}

struct Node<K, V>
where
    K: HashKey,
{
    key: K,
    current: RWLock<V>,
}
impl<K, V> Node<K, V>
where
    K: HashKey,
{
    fn new(key: K, value: V) -> Node<K, V> {
        let rw = RWLock::new(value);
        Node { key, current: rw }
    }

    fn new_rw(key: K, value: RWLock<V>) -> Node<K, V> {
        Node {
            key,
            current: value,
        }
    }

    fn compare_key(&self, k: &K) -> bool {
        &self.key == k
    }

    fn update(&mut self, value: V) -> RWLock<V> {
        let value_rw = RWLock::new(value);
        std::mem::replace(&mut self.current, value_rw)
    }

    fn update_rw(&mut self, value: RWLock<V>) -> RWLock<V> {
        std::mem::replace(&mut self.current, value)
    }

    fn get_value(&self) -> RWLock<V> {
        self.current.clone()
    }
}
impl<K, V> Drop for Node<K, V>
where
    K: HashKey,
{
    fn drop(&mut self) {
        // println!("drop node")
    }
}

///操作控制器
struct OperateController {
    // 第1位为禁止操作为,2-32为当前的插入和删除操作,33-64为元素计数器
    controller: AtomicU64,
}
impl OperateController {
    fn new() -> OperateController {
        OperateController {
            controller: AtomicU64::new(0),
        }
    }

    /// 获得操作权限
    fn get_operate_right(&self) {
        let handler = || -> Result<bool> {
            let controller = self.controller.load(Ordering::Acquire);
            let flag = controller >> 63 > 0;
            if flag == true {
                return Ok(false);
            }

            let new_controller = ((controller >> 32) + 1) << 32 | (controller & MAX_ENTRY_COUNT);
            if let Ok(_) = self.controller.compare_exchange(
                controller,
                new_controller,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                return Ok(true);
            }

            Ok(false)
        };

        // 这里不用异步，不存在阻塞得情况
        let _ = get_thread_time(handler);
    }

    /// 释放操作权限
    fn release_operate_right(&self) {
        let handler = || -> Result<bool> {
            let controller = self.controller.load(Ordering::Acquire);
            let operate_count = (controller >> 32) & MAX_OPERATE_COUNT;
            assert_ne!(operate_count, 0);

            let new_controller = ((controller >> 32) - 1) << 32 | (controller & MAX_ENTRY_COUNT);
            if let Ok(_) = self.controller.compare_exchange(
                controller,
                new_controller,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                return Ok(true);
            }

            Ok(false)
        };

        // 这里不用异步，不存在阻塞得情况
        let _ = get_thread_time(handler);
    }

    /// 获得扩容权限
    fn get_resize_right(&self) {
        let handler = || -> Result<bool> {
            let controller = self.controller.load(Ordering::Acquire);
            let flag = controller >> 63 > 0;
            assert_eq!(flag, true);

            let operate_count = (controller >> 32) ^ (1 << 32);
            Ok(operate_count > 0)
        };

        let _ = get_thread_time(handler);
    }

    /// 重置操作权限
    fn reset_operate_tag(&self) {
        let handler = || -> Result<bool> {
            let controller = self.controller.load(Ordering::Acquire);
            let flag = controller >> 63 > 0;
            assert_eq!(flag, true);
            let new_controller = controller ^ (1 << 63);
            if let Ok(_) = self.controller.compare_exchange(
                controller,
                new_controller,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                return Ok(true);
            }

            Ok(false)
        };

        // 这里不用异步，不存在阻塞得情况
        let _ = get_thread_time(handler);
    }

    fn add_node(&self, max_count: u64) -> bool {
        let controller = self.controller.fetch_add(1, Ordering::Release);
        let node_count = (controller + 1) & MAX_ENTRY_COUNT;
        if node_count > max_count {
            loop {
                let controller = self.controller.load(Ordering::Acquire);
                let flag = controller >> 63 > 0;
                if flag == true {
                    return false;
                }

                // 禁止操作
                let new_controller = controller ^ (1 << 63);
                if let Ok(_) = self.controller.compare_exchange(
                    controller,
                    new_controller,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    return true;
                }
            }
        }

        return false;
    }

    fn remove_node(&self) {
        self.controller.fetch_sub(1, Ordering::Release);
    }

    fn len(&self) -> u64 {
        self.controller.load(Ordering::Acquire) & MAX_ENTRY_COUNT
    }
}
