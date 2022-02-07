use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::RangeBounds,
};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};

use super::{
    btree_entry::BTreeEntry,
    by_id::{ByIdStats, UnversionedByIdIndex},
    modify::Modification,
    read_chunk,
    serialization::BinarySerialization,
    KeyEvaluation, KeyRange, PagedWriter,
};
use crate::{
    chunk_cache::CacheEntry,
    error::{Error, InternalError},
    io::File,
    roots::AbortError,
    tree::{
        btree_entry::{KeyOperation, ModificationContext, NodeInclusion, ScanArgs},
        copy_chunk, dynamic_order,
        versioned::ChangeResult,
        BTreeNode, PageHeader, Reducer, Root,
    },
    vault::AnyVault,
    ArcBytes, ChunkCache, ErrorKind,
};

/// An unversioned tree with no additional indexed data.
pub type Unversioned = UnversionedTreeRoot<()>;

/// A versioned B-Tree root. This tree root internally uses two btrees, one to
/// keep track of all writes using a unique "sequence" ID, and one that keeps
/// track of all key-value pairs.
#[derive(Clone, Debug)]
pub struct UnversionedTreeRoot<Index>
where
    Index: Clone + super::EmbeddedIndex + Reducer<Index> + Debug + 'static,
{
    /// The transaction ID of the tree root. If this transaction ID isn't
    /// present in the transaction log, this root should not be trusted.
    pub transaction_id: Option<u64>,
    /// The by-id B-Tree.
    pub by_id_root: BTreeEntry<UnversionedByIdIndex<Index>, ByIdStats<Index>>,
}

impl<Index> Default for UnversionedTreeRoot<Index>
where
    Index: Clone + Reducer<Index> + super::EmbeddedIndex + Debug + 'static,
{
    fn default() -> Self {
        Self {
            transaction_id: None,
            by_id_root: BTreeEntry::default(),
        }
    }
}

impl<Index> UnversionedTreeRoot<Index>
where
    Index: Clone + Reducer<Index> + super::EmbeddedIndex + Debug + 'static,
{
    fn modify_id_root<'a, 'w>(
        &'a mut self,
        mut modification: Modification<'_, ArcBytes<'static>>,
        writer: &'a mut PagedWriter<'w>,
        max_order: Option<usize>,
    ) -> Result<(), Error> {
        modification.reverse()?;

        let total_keys = self.by_id_root.stats().total_keys() + modification.keys.len() as u64;
        let by_id_order = dynamic_order(total_keys, max_order);
        let minimum_children = by_id_order / 2 - 1;
        let minimum_children =
            minimum_children.min(usize::try_from(total_keys).unwrap_or(usize::MAX));

        while !modification.keys.is_empty() {
            match self.by_id_root.modify(
                &mut modification,
                &ModificationContext {
                    current_order: by_id_order,
                    minimum_children,
                    indexer: |key: &ArcBytes<'_>,
                              value: Option<&ArcBytes<'static>>,
                              _existing_index,
                              _changes,
                              writer: &mut PagedWriter<'_>| {
                        if let Some(value) = value {
                            let position = writer.write_chunk(value, false)?;
                            // write_chunk errors if it can't fit within a u32
                            #[allow(clippy::cast_possible_truncation)]
                            let value_length = value.len() as u32;
                            Ok(KeyOperation::Set(UnversionedByIdIndex {
                                value_length,
                                position,
                                embedded: Index::index(key, Some(value)),
                            }))
                        } else {
                            Ok(KeyOperation::Remove)
                        }
                    },
                    loader: |index, writer| match writer.read_chunk(index.position)? {
                        CacheEntry::ArcBytes(buffer) => Ok(Some(buffer.clone())),
                        CacheEntry::Decoded(_) => unreachable!(),
                    },
                    _phantom: PhantomData,
                },
                None,
                &mut (),
                writer,
            )? {
                ChangeResult::Absorb | ChangeResult::Changed | ChangeResult::Unchanged => {}
                ChangeResult::Remove => {
                    self.by_id_root.node = BTreeNode::Leaf(vec![]);
                    self.by_id_root.dirty = true;
                }
                ChangeResult::Split => {
                    self.by_id_root.split_root();
                }
            }
        }

        Ok(())
    }
}

impl<EmbeddedIndex> Root for UnversionedTreeRoot<EmbeddedIndex>
where
    EmbeddedIndex: Clone + super::EmbeddedIndex + Reducer<EmbeddedIndex> + Debug + 'static,
{
    const HEADER: PageHeader = PageHeader::UnversionedHeader;
    type Index = UnversionedByIdIndex<EmbeddedIndex>;
    type ReducedIndex = ByIdStats<EmbeddedIndex>;

    fn count(&self) -> u64 {
        self.by_id_root.stats().alive_keys
    }

    fn initialized(&self) -> bool {
        self.transaction_id.is_some()
    }

    fn dirty(&self) -> bool {
        self.by_id_root.dirty
    }

    fn initialize_default(&mut self) {
        self.transaction_id = Some(0);
    }

    fn deserialize(mut bytes: ArcBytes<'_>) -> Result<Self, Error> {
        let transaction_id = Some(bytes.read_u64::<BigEndian>()?);
        let by_id_size = bytes.read_u32::<BigEndian>()? as usize;
        if by_id_size != bytes.len() {
            return Err(Error::data_integrity(format!(
                "Header reported index size {}, but data has {} remaining",
                by_id_size,
                bytes.len()
            )));
        };

        let mut by_id_bytes = bytes.read_bytes(by_id_size)?.to_owned();

        let by_id_root = BTreeEntry::deserialize_from(&mut by_id_bytes, None)?;

        Ok(Self {
            transaction_id,
            by_id_root,
        })
    }

    fn serialize(
        &mut self,
        paged_writer: &mut PagedWriter<'_>,
        output: &mut Vec<u8>,
    ) -> Result<(), Error> {
        output.write_u64::<BigEndian>(
            self.transaction_id
                .expect("serializing an uninitialized root"),
        )?;
        // Reserve space for by_id size.
        output.write_u32::<BigEndian>(0)?;

        let by_id_size = self.by_id_root.serialize_to(output, paged_writer)?;
        let by_id_size = u32::try_from(by_id_size)
            .ok()
            .ok_or(ErrorKind::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[8..12], by_id_size);

        Ok(())
    }

    fn transaction_id(&self) -> u64 {
        self.transaction_id.unwrap_or_default()
    }

    fn modify(
        &mut self,
        modification: Modification<'_, ArcBytes<'static>>,
        writer: &mut PagedWriter<'_>,
        max_order: Option<usize>,
    ) -> Result<(), Error> {
        let transaction_id = modification.transaction_id;

        self.modify_id_root(modification, writer, max_order)?;

        // Only update the transaction id if a new one was specified.
        if let Some(transaction_id) = transaction_id {
            self.transaction_id = Some(transaction_id);
        }

        Ok(())
    }

    fn get_multiple<'keys, KeyEvaluator, KeyReader, Keys>(
        &self,
        keys: &mut Keys,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut dyn File,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
    ) -> Result<(), Error>
    where
        KeyEvaluator: FnMut(&ArcBytes<'static>) -> KeyEvaluation,
        KeyReader: FnMut(ArcBytes<'static>, ArcBytes<'static>) -> Result<(), Error>,
        Keys: Iterator<Item = &'keys [u8]>,
    {
        let mut positions_to_read = Vec::new();
        self.by_id_root.get(
            &mut KeyRange::new(keys),
            &mut |key, _index| key_evaluator(key),
            &mut |key, index| {
                // Deleted keys are stored with a 0 position.
                if index.position > 0 {
                    positions_to_read.push((key, index.position));
                }
                Ok(())
            },
            file,
            vault,
            cache,
        )?;

        // Sort by position on disk
        positions_to_read.sort_by(|a, b| a.1.cmp(&b.1));

        for (key, position) in positions_to_read {
            if position > 0 {
                match read_chunk(position, false, file, vault, cache)? {
                    CacheEntry::ArcBytes(contents) => {
                        key_reader(key, contents)?;
                    }
                    CacheEntry::Decoded(_) => unreachable!(),
                };
            }
        }
        Ok(())
    }

    fn scan<
        'keys,
        CallerError: Display + Debug,
        NodeEvaluator,
        KeyRangeBounds,
        KeyEvaluator,
        DataCallback,
    >(
        &self,
        range: &'keys KeyRangeBounds,
        args: &mut ScanArgs<
            Self::Index,
            Self::ReducedIndex,
            CallerError,
            NodeEvaluator,
            KeyEvaluator,
            DataCallback,
        >,
        file: &mut dyn File,
        vault: Option<&dyn AnyVault>,
        cache: Option<&ChunkCache>,
    ) -> Result<bool, AbortError<CallerError>>
    where
        NodeEvaluator: FnMut(&ArcBytes<'static>, &Self::ReducedIndex, usize) -> bool,
        KeyEvaluator: FnMut(&ArcBytes<'static>, &Self::Index) -> KeyEvaluation,
        KeyRangeBounds: RangeBounds<&'keys [u8]> + Debug + ?Sized,
        DataCallback: FnMut(
            ArcBytes<'static>,
            &Self::Index,
            ArcBytes<'static>,
        ) -> Result<(), AbortError<CallerError>>,
    {
        self.by_id_root.scan(range, args, file, vault, cache, 0)
    }

    fn copy_data_to(
        &mut self,
        include_nodes: bool,
        file: &mut dyn File,
        copied_chunks: &mut HashMap<u64, u64>,
        writer: &mut PagedWriter<'_>,
        vault: Option<&dyn AnyVault>,
    ) -> Result<(), Error> {
        let mut scratch = Vec::new();
        self.by_id_root.copy_data_to(
            if include_nodes {
                NodeInclusion::IncludeNext
            } else {
                NodeInclusion::Exclude
            },
            file,
            copied_chunks,
            writer,
            vault,
            &mut scratch,
            &mut |_key,
                  index: &mut UnversionedByIdIndex<EmbeddedIndex>,
                  from_file,
                  copied_chunks,
                  to_file,
                  vault| {
                let new_position =
                    copy_chunk(index.position, from_file, copied_chunks, to_file, vault)?;

                if new_position == index.position {
                    // Data is already in the new file
                    Ok(false)
                } else {
                    index.position = new_position;
                    Ok(true)
                }
            },
        )?;

        Ok(())
    }
}
