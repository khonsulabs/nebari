use std::{any::Any, sync::Arc};

use lru::LruCache;
use parking_lot::Mutex;

use crate::ArcBytes;

/// A configurable cache that operates at the "chunk" level.
///
/// While writing databases, individual portions of data are often written as a
/// single chunk. These chunks may be stored encrypted on-disk, but the
/// in-memory cache will be after decryption.
///
/// To keep memory usage low, the maximum size for a cached value can be set. It
/// is important that this value be large enough to fit most B-Tree nodes, and
/// that size will depend on how big the tree grows.
#[derive(Clone, Debug)]
#[must_use]
pub struct ChunkCache {
    max_block_length: usize,
    cache: Arc<Mutex<LruCache<ChunkKey, CacheEntry>>>,
}

pub trait AnySendSync: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T> AnySendSync for T
where
    T: Any + Send + Sync,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct ChunkKey {
    position: u64,
    file_id: u64,
}

impl ChunkCache {
    /// Create a new cache with a maximum number of entries (`capacity`) and
    /// `max_chunk_length`. Any chunks longer than `max_chunk_length` will not
    /// be cached. The maximum memory usage of this cache can be calculated as
    /// `capacity * max_chunk_length`, although the actual memory usage will
    /// likely be much smaller as many chunks are small.
    pub fn new(capacity: usize, max_chunk_length: usize) -> Self {
        Self {
            max_block_length: max_chunk_length,
            cache: Arc::new(Mutex::new(LruCache::new(capacity))),
        }
    }

    /// Returns the maximum size of data that can be cached.
    #[must_use]
    pub const fn max_chunk_size(&self) -> usize {
        self.max_block_length
    }

    /// Adds a new cached chunk for `file_path` at `position`.
    pub fn insert(&self, file_id: u64, position: u64, buffer: ArcBytes<'static>) {
        if buffer.len() <= self.max_block_length {
            let mut cache = self.cache.lock();
            cache.put(ChunkKey { position, file_id }, CacheEntry::ArcBytes(buffer));
        }
    }

    /// Adds a new cached chunk for `file_path` at `position`.
    pub fn replace_with_decoded<T: AnySendSync + 'static>(
        &self,
        file_id: u64,
        position: u64,
        value: T,
    ) {
        let mut cache = self.cache.lock();
        cache.put(
            ChunkKey { position, file_id },
            CacheEntry::Decoded(Arc::new(value)),
        );
    }

    /// Looks up a previously read chunk for `file_path` at `position`,
    #[must_use]
    pub fn get(&self, file_id: u64, position: u64) -> Option<CacheEntry> {
        let mut cache = self.cache.lock();
        cache.get(&ChunkKey { position, file_id }).cloned()
    }
}

/// A cached chunk of data that has possibly been decoded already.
#[derive(Clone)]
pub enum CacheEntry {
    /// A buffer of bytes that has been cached.
    ArcBytes(ArcBytes<'static>),
    /// A previously decoded value that was stored using
    /// [`ChunkCache::replace_with_decoded()`].
    Decoded(Arc<dyn AnySendSync>),
}
