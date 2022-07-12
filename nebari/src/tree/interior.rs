use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use sediment::format::GrainId;

use super::{btree::BTreeEntry, read_chunk, BinarySerialization, PagedWriter};
use crate::{
    chunk_cache::CacheEntry,
    error::Error,
    storage::BlobStorage,
    tree::{btree::NodeInclusion, key_entry::PositionIndex},
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

/// A pointer to a location on-disk. May also contain the node already loaded.
#[derive(Clone, Debug)]
pub enum Pointer<Index, ReducedIndex> {
    /// The position on-disk of the node.
    OnDisk(Option<GrainId>),
    /// An in-memory node that may have previously been saved on-disk.
    Loaded {
        /// The position on-disk of the node, if it was previously saved.
        previous_location: Option<GrainId>,
        /// The loaded B-Tree entry.
        entry: Box<BTreeEntry<Index, ReducedIndex>>,
    },
}

impl<
        Index: BinarySerialization + Debug + Clone + 'static,
        ReducedIndex: BinarySerialization + Debug + Clone + 'static,
    > Pointer<Index, ReducedIndex>
{
    /// Attempts to load the node from disk. If the node is already loaded, this
    /// function does nothing.
    #[allow(clippy::missing_panics_doc)] // Currently the only panic is if the types don't match, which shouldn't happen due to these nodes always being accessed through a root.
    pub fn load(
        &mut self,
        file: &mut dyn BlobStorage,
        validate_crc: bool,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
        current_order: Option<usize>,
    ) -> Result<(), Error> {
        match self {
            Pointer::OnDisk(Some(position)) => {
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
            Pointer::OnDisk(None) | Pointer::Loaded { .. } => {}
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
    pub fn position(&self) -> Option<GrainId> {
        match self {
            Pointer::OnDisk(location) => *location,
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
            &mut dyn BlobStorage,
        ) -> Result<Output, AbortError<CallerError>>,
    >(
        &self,
        file: &mut dyn BlobStorage,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
        current_order: Option<usize>,
        callback: Cb,
    ) -> Result<Output, AbortError<CallerError>> {
        match self {
            Pointer::OnDisk(Some(position)) => {
                match read_chunk(*position, false, file, vault, cache)? {
                    CacheEntry::ArcBytes(mut buffer) => {
                        let decoded = BTreeEntry::deserialize_from(&mut buffer, current_order)?;

                        let result = callback(&decoded, file);
                        if let Some(cache) = cache {
                            cache.replace_with_decoded(
                                file.unique_id().id,
                                *position,
                                Box::new(decoded),
                            );
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
                }
            }
            Pointer::OnDisk(None) => Err(AbortError::Nebari(Error::from(
                std::io::ErrorKind::NotFound,
            ))),
            Pointer::Loaded { entry, .. } => callback(entry, file),
        }
    }
}

impl<
        Index: Clone + PositionIndex + BinarySerialization + Debug + 'static,
        ReducedIndex: Clone + BinarySerialization + Debug + 'static,
    > Interior<Index, ReducedIndex>
{
    /// Returns a new instance
    pub fn new<Reducer: super::Reducer<Index, ReducedIndex>>(
        entry: BTreeEntry<Index, ReducedIndex>,
        reducer: &Reducer,
    ) -> Self {
        let key = entry.max_key().clone();

        Self {
            key,
            stats: entry.stats(reducer),
            position: Pointer::Loaded {
                previous_location: None,
                entry: Box::new(entry),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn copy_data_to<Callback>(
        &mut self,
        include_nodes: NodeInclusion,
        file: &mut dyn BlobStorage,
        copied_chunks: &mut HashMap<GrainId, GrainId>,
        writer: &mut PagedWriter<'_, '_>,
        vault: Option<&dyn AnyVault>,
        scratch: &mut Vec<u8>,
        index_callback: &mut Callback,
    ) -> Result<bool, Error>
    where
        Callback: FnMut(
            &ArcBytes<'static>,
            &mut Index,
            &mut dyn BlobStorage,
            &mut HashMap<GrainId, GrainId>,
            &mut PagedWriter<'_, '_>,
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
            self.position = Pointer::OnDisk(Some(position));
        }

        Ok(any_data_copied)
    }
}

impl<
        Index: Clone + BinarySerialization + Debug + 'static,
        ReducedIndex: Clone + BinarySerialization + Debug + 'static,
    > BinarySerialization for Interior<Index, ReducedIndex>
{
    fn serialize_to(
        &mut self,
        writer: &mut Vec<u8>,
        paged_writer: &mut PagedWriter<'_, '_>,
    ) -> Result<usize, Error> {
        let mut pointer = Pointer::OnDisk(None);
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
                    if let Some(cache) = paged_writer.cache {
                        cache.replace_with_decoded(paged_writer.unique_id().id, position, entry);
                    }

                    if let Some(previous_location) = previous_location {
                        paged_writer.free(previous_location)?;
                    }

                    Some(position)
                }
                (false, Some(position)) => Some(position),
            },
        };
        self.position = Pointer::OnDisk(location_on_disk);
        let mut bytes_written = 0;
        // Write the key
        let key_len = u16::try_from(self.key.len()).map_err(|_| ErrorKind::KeyTooLarge)?;
        writer.write_u16::<BigEndian>(key_len)?;
        writer.extend_from_slice(&self.key);
        bytes_written += 2 + key_len as usize;

        writer.write_u64::<BigEndian>(location_on_disk.map_or(0, |location| location.as_u64()))?;
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

        let position = match reader.read_u64::<BigEndian>()? {
            0 => None,
            grain => Some(GrainId::from(grain)),
        };
        let stats = ReducedIndex::deserialize_from(reader, current_order)?;

        Ok(Self {
            key,
            position: Pointer::OnDisk(position),
            stats,
        })
    }
}
