use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{
    btree_entry::{BTreeEntry, Reducer},
    read_chunk, BinarySerialization, PagedWriter,
};
use crate::{
    chunk_cache::CacheEntry,
    error::Error,
    io::File,
    tree::{btree_entry::NodeInclusion, key_entry::ValueIndex},
    vault::AnyVault,
    AbortError, ArcBytes, ChunkCache, ErrorKind,
};

/// An interior B-Tree node. Does not contain values directly, and instead
/// points to a node located on-disk elsewhere.
#[derive(Clone, Debug)]
pub struct Interior<Index, ReducedIndex> {
    /// The key with the highest sort value within.
    pub key: ArcBytes<'static>,
    /// The location of the node.
    pub position: Pointer<Index, ReducedIndex>,
    /// The reduced statistics.
    pub stats: ReducedIndex,
}

impl<Index, ReducedIndex> From<BTreeEntry<Index, ReducedIndex>> for Interior<Index, ReducedIndex>
where
    Index: Clone + Debug + ValueIndex + BinarySerialization + 'static,
    ReducedIndex: Reducer<Index> + Clone + Debug + BinarySerialization + 'static,
{
    fn from(entry: BTreeEntry<Index, ReducedIndex>) -> Self {
        let key = entry.max_key().clone();
        let stats = entry.stats();

        Self {
            key,
            stats,
            position: Pointer::Loaded {
                previous_location: None,
                entry: Box::new(entry),
            },
        }
    }
}

/// A pointer to a location on-disk. May also contain the node already loaded.
#[derive(Clone, Debug)]
pub enum Pointer<Index, ReducedIndex> {
    /// The position on-disk of the node.
    OnDisk(u64),
    /// An in-memory node that may have previously been saved on-disk.
    Loaded {
        /// The position on-disk of the node, if it was previously saved.
        previous_location: Option<u64>,
        /// The loaded B-Tree entry.
        entry: Box<BTreeEntry<Index, ReducedIndex>>,
    },
}

impl<
        Index: BinarySerialization + Debug + Clone + 'static,
        ReducedIndex: Reducer<Index> + BinarySerialization + Debug + Clone + 'static,
    > Pointer<Index, ReducedIndex>
{
    /// Attempts to load the node from disk. If the node is already loaded, this
    /// function does nothing.
    #[allow(clippy::missing_panics_doc)] // Currently the only panic is if the types don't match, which shouldn't happen due to these nodes always being accessed through a root.
    pub fn load(
        &mut self,
        file: &mut dyn File,
        validate_crc: bool,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
        current_order: Option<usize>,
    ) -> Result<(), Error> {
        match self {
            Pointer::OnDisk(position) => {
                let entry = match read_chunk(*position, validate_crc, file, vault, cache)? {
                    CacheEntry::ArcBytes(mut buffer) => {
                        // It's worthless to store this node in the cache
                        // because if we mutate, we'll be rewritten.
                        Box::new(BTreeEntry::deserialize_from(&mut buffer, current_order)?)
                    }
                    CacheEntry::Decoded(node) => node
                        .as_ref()
                        .as_any()
                        .downcast_ref::<Box<BTreeEntry<Index, ReducedIndex>>>()
                        .unwrap()
                        .clone(),
                };
                *self = Self::Loaded {
                    entry,
                    previous_location: Some(*position),
                };
            }
            Pointer::Loaded { .. } => {}
        }
        Ok(())
    }

    /// Returns the previously-[`load()`ed](Self::load) entry.
    pub fn get(&mut self) -> Option<&BTreeEntry<Index, ReducedIndex>> {
        match self {
            Pointer::OnDisk(_) => None,
            Pointer::Loaded { entry, .. } => Some(entry),
        }
    }

    /// Returns the previously-[`load()`ed](Self::load) entry as a mutable reference.
    pub fn get_mut(&mut self) -> Option<&mut BTreeEntry<Index, ReducedIndex>> {
        match self {
            Pointer::OnDisk(_) => None,
            Pointer::Loaded { entry, .. } => Some(entry.as_mut()),
        }
    }

    /// Returns the position on-disk of the node being pointed at, if the node
    /// has been saved before.
    #[must_use]
    pub fn position(&self) -> Option<u64> {
        match self {
            Pointer::OnDisk(location) => Some(*location),
            Pointer::Loaded {
                previous_location, ..
            } => *previous_location,
        }
    }

    /// Loads the pointed at node, if necessary, and invokes `callback` with the
    /// loaded node. This is useful in situations where the node isn't needed to
    /// be accessed mutably.
    #[allow(clippy::missing_panics_doc)]
    pub fn map_loaded_entry<
        Output,
        CallerError: Display + Debug,
        Cb: FnOnce(
            &BTreeEntry<Index, ReducedIndex>,
            &mut dyn File,
        ) -> Result<Output, AbortError<CallerError>>,
    >(
        &self,
        file: &mut dyn File,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
        current_order: Option<usize>,
        callback: Cb,
    ) -> Result<Output, AbortError<CallerError>> {
        match self {
            Pointer::OnDisk(position) => match read_chunk(*position, false, file, vault, cache)? {
                CacheEntry::ArcBytes(mut buffer) => {
                    let decoded = BTreeEntry::deserialize_from(&mut buffer, current_order)?;

                    let result = callback(&decoded, file);
                    if let (Some(cache), Some(file_id)) = (cache, file.id()) {
                        cache.replace_with_decoded(file_id, *position, Box::new(decoded));
                    }
                    result
                }
                CacheEntry::Decoded(value) => {
                    let entry = value
                        .as_ref()
                        .as_any()
                        .downcast_ref::<Box<BTreeEntry<Index, ReducedIndex>>>()
                        .unwrap();
                    callback(entry, file)
                }
            },
            Pointer::Loaded { entry, .. } => callback(entry, file),
        }
    }
}

impl<
        Index: Clone + ValueIndex + BinarySerialization + Debug + 'static,
        ReducedIndex: Reducer<Index> + Clone + BinarySerialization + Debug + 'static,
    > Interior<Index, ReducedIndex>
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn copy_data_to<Callback>(
        &mut self,
        include_nodes: NodeInclusion,
        file: &mut dyn File,
        copied_chunks: &mut HashMap<u64, u64>,
        writer: &mut PagedWriter<'_>,
        vault: Option<&dyn AnyVault>,
        scratch: &mut Vec<u8>,
        index_callback: &mut Callback,
    ) -> Result<bool, Error>
    where
        Callback: FnMut(
            &ArcBytes<'static>,
            &mut Index,
            &mut dyn File,
            &mut HashMap<u64, u64>,
            &mut PagedWriter<'_>,
            Option<&dyn AnyVault>,
        ) -> Result<bool, Error>,
    {
        self.position.load(file, true, vault, None, None)?;
        let node = self.position.get_mut().unwrap();
        let mut any_data_copied = node.copy_data_to(
            include_nodes,
            file,
            copied_chunks,
            writer,
            vault,
            scratch,
            index_callback,
        )?;

        // Serialize if we are supposed to
        let position = if include_nodes.should_include() {
            any_data_copied = true;
            scratch.clear();
            node.serialize_to(scratch, writer)?;
            Some(writer.write_chunk(scratch)?)
        } else {
            self.position.position()
        };

        // Remove the node from memory to save RAM during the compaction process.
        if let Some(position) = position {
            self.position = Pointer::OnDisk(position);
        }

        Ok(any_data_copied)
    }
}

impl<
        Index: Clone + BinarySerialization + Debug + 'static,
        ReducedIndex: Reducer<Index> + Clone + BinarySerialization + Debug + 'static,
    > BinarySerialization for Interior<Index, ReducedIndex>
{
    fn serialize_to(
        &mut self,
        writer: &mut Vec<u8>,
        paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error> {
        let mut pointer = Pointer::OnDisk(0);
        std::mem::swap(&mut pointer, &mut self.position);
        let location_on_disk = match pointer {
            Pointer::OnDisk(position) => position,
            Pointer::Loaded {
                mut entry,
                previous_location,
            } => match (entry.dirty, previous_location) {
                // Serialize if dirty, or if this node hasn't been on-disk before.
                (true, _) | (_, None) => {
                    entry.dirty = false;
                    let old_writer_length = writer.len();
                    entry.serialize_to(writer, paged_writer)?;
                    let position =
                        paged_writer.write_chunk(&writer[old_writer_length..writer.len()])?;
                    writer.truncate(old_writer_length);
                    if let (Some(cache), Some(file_id)) = (paged_writer.cache, paged_writer.id()) {
                        cache.replace_with_decoded(file_id, position, entry);
                    }
                    position
                }
                (false, Some(position)) => position,
            },
        };
        self.position = Pointer::OnDisk(location_on_disk);
        let mut bytes_written = 0;
        // Write the key
        let key_len = u16::try_from(self.key.len()).map_err(|_| ErrorKind::KeyTooLarge)?;
        writer.write_u16::<BigEndian>(key_len)?;
        writer.extend_from_slice(&self.key);
        bytes_written += 2 + key_len as usize;

        writer.write_u64::<BigEndian>(location_on_disk)?;
        bytes_written += 8;

        bytes_written += self.stats.serialize_to(writer, paged_writer)?;

        Ok(bytes_written)
    }

    fn deserialize_from(
        reader: &mut ArcBytes<'_>,
        current_order: Option<usize>,
    ) -> Result<Self, Error> {
        let key_len = reader.read_u16::<BigEndian>()? as usize;
        if key_len > reader.len() {
            return Err(Error::data_integrity(format!(
                "key length {} found but only {} bytes remaining",
                key_len,
                reader.len()
            )));
        }
        let key = reader.read_bytes(key_len)?.into_owned();

        let position = reader.read_u64::<BigEndian>()?;
        let stats = ReducedIndex::deserialize_from(reader, current_order)?;

        Ok(Self {
            key,
            position: Pointer::OnDisk(position),
            stats,
        })
    }
}
