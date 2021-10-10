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
    by_id::{ByIdStats, VersionedByIdIndex},
    by_sequence::{BySequenceIndex, BySequenceStats},
    modify::Modification,
    read_chunk,
    serialization::BinarySerialization,
    KeyEvaluation, KeyRange, PagedWriter, PAGE_SIZE,
};
use crate::{
    chunk_cache::CacheEntry,
    error::{Error, InternalError},
    io::ManagedFile,
    roots::AbortError,
    tree::{
        btree_entry::{KeyOperation, ModificationContext, NodeInclusion, ScanArgs},
        copy_chunk, dynamic_order,
        key_entry::KeyEntry,
        modify::Operation,
        PageHeader, Root, DEFAULT_MAX_ORDER,
    },
    Buffer, ChunkCache, ErrorKind, Vault,
};

const UNINITIALIZED_SEQUENCE: u64 = 0;

/// A versioned B-Tree root. This tree root internally uses two btrees, one to
/// keep track of all writes using a unique "sequence" ID, and one that keeps
/// track of all key-value pairs.
#[derive(Clone, Default, Debug)]
pub struct VersionedTreeRoot {
    /// The transaction ID of the tree root. If this transaction ID isn't
    /// present in the transaction log, this root should not be trusted.
    pub transaction_id: u64,
    /// The last sequence ID inside of this root.
    pub sequence: u64,
    /// The by-sequence B-Tree.
    pub by_sequence_root: BTreeEntry<BySequenceIndex, BySequenceStats>,
    /// The by-id B-Tree.
    pub by_id_root: BTreeEntry<VersionedByIdIndex, ByIdStats>,
}

pub enum ChangeResult<Index: BinarySerialization, ReducedIndex: BinarySerialization> {
    Unchanged,
    Remove,
    Absorb(Vec<KeyEntry<Index>>),
    Changed,
    Split(BTreeEntry<Index, ReducedIndex>),
}

impl VersionedTreeRoot {
    fn modify_sequence_root<File: ManagedFile>(
        &mut self,
        mut modification: Modification<'_, BySequenceIndex>,
        writer: &mut PagedWriter<'_, File>,
        max_order: Option<usize>,
    ) -> Result<(), Error> {
        // Reverse so that pop is efficient.
        modification.reverse()?;

        let total_sequence_records =
            self.by_sequence_root.stats().total_sequences + modification.keys.len() as u64;
        let by_sequence_order = dynamic_order(total_sequence_records, max_order);

        let by_sequence_minimum_children = by_sequence_order / 2 - 1;
        let by_sequence_minimum_children = by_sequence_minimum_children
            .min(usize::try_from(total_sequence_records).unwrap_or(usize::MAX));

        while !modification.keys.is_empty() {
            match self.by_sequence_root.modify(
                &mut modification,
                &ModificationContext {
                    current_order: by_sequence_order,
                    minimum_children: by_sequence_minimum_children,
                    indexer: |_key: &Buffer<'_>,
                              value: Option<&BySequenceIndex>,
                              _existing_index: Option<&BySequenceIndex>,
                              _changes: &mut EntryChanges,
                              _writer: &mut PagedWriter<'_, File>| {
                        Ok(KeyOperation::Set(value.unwrap().clone()))
                    },
                    loader: |_index: &BySequenceIndex, _writer: &mut PagedWriter<'_, File>| {
                        Ok(None)
                    },
                    _phantom: PhantomData,
                },
                None,
                &mut EntryChanges::default(),
                writer,
            )? {
                ChangeResult::Absorb(_) => unreachable!(),
                ChangeResult::Remove | ChangeResult::Unchanged | ChangeResult::Changed => {}
                ChangeResult::Split(upper) => {
                    self.by_sequence_root.split_root(upper);
                }
            }
        }
        Ok(())
    }

    fn modify_id_root<File: ManagedFile>(
        &mut self,
        mut modification: Modification<'_, Buffer<'static>>,
        changes: &mut EntryChanges,
        writer: &mut PagedWriter<'_, File>,
        max_order: Option<usize>,
    ) -> Result<(), Error> {
        modification.reverse()?;

        let total_id_records =
            self.by_id_root.stats().total_keys() + modification.keys.len() as u64;
        let by_id_order = dynamic_order(total_id_records, max_order);

        let by_id_minimum_children = by_id_order / 2 - 1;
        let by_id_minimum_children =
            by_id_minimum_children.min(usize::try_from(total_id_records).unwrap_or(usize::MAX));

        while !modification.keys.is_empty() {
            match self.by_id_root.modify(
                &mut modification,
                &ModificationContext {
                    current_order: by_id_order,
                    minimum_children: by_id_minimum_children,
                    indexer: |key: &Buffer<'_>,
                              value: Option<&Buffer<'static>>,
                              _existing_index: Option<&VersionedByIdIndex>,
                              changes: &mut EntryChanges,
                              writer: &mut PagedWriter<'_, File>| {
                        let (position, value_size) = if let Some(value) = value {
                            let new_position = writer.write_chunk(value, false)?;
                            // write_chunk errors if it can't fit within a u32
                            #[allow(clippy::cast_possible_truncation)]
                            let value_length = value.len() as u32;
                            (new_position, value_length)
                        } else {
                            (0, 0)
                        };
                        changes.current_sequence = changes
                            .current_sequence
                            .checked_add(1)
                            .expect("sequence rollover prevented");
                        let key = key.to_owned();
                        changes.changes.push(EntryChange {
                            key: key.clone(),
                            sequence: changes.current_sequence,
                            value_position: position,
                            value_size,
                        });
                        Ok(KeyOperation::Set(VersionedByIdIndex {
                            sequence_id: changes.current_sequence,
                            position,
                            value_length: value_size,
                        }))
                    },
                    loader: |index, writer| {
                        if index.position > 0 {
                            match writer.read_chunk(index.position) {
                                Ok(CacheEntry::Buffer(buffer)) => Ok(Some(buffer)),
                                Ok(CacheEntry::Decoded(_)) => unreachable!(),
                                Err(err) => Err(err),
                            }
                        } else {
                            Ok(None)
                        }
                    },
                    _phantom: PhantomData,
                },
                None,
                changes,
                writer,
            )? {
                ChangeResult::Remove | ChangeResult::Changed | ChangeResult::Unchanged => {}
                ChangeResult::Split(upper) => {
                    self.by_id_root.split_root(upper);
                }
                ChangeResult::Absorb(_) => unreachable!(),
            }
        }

        self.sequence = changes.current_sequence;

        Ok(())
    }
}

impl Root for VersionedTreeRoot {
    const HEADER: PageHeader = PageHeader::VersionedHeader;

    fn initialized(&self) -> bool {
        self.sequence != UNINITIALIZED_SEQUENCE
    }

    fn dirty(&self) -> bool {
        self.by_id_root.dirty || self.by_sequence_root.dirty
    }

    fn initialize_default(&mut self) {
        self.sequence = 1;
    }

    fn count(&self) -> u64 {
        self.by_id_root.stats().alive_keys
    }

    fn deserialize(mut bytes: Buffer<'_>) -> Result<Self, Error> {
        let transaction_id = bytes.read_u64::<BigEndian>()?;
        let sequence = bytes.read_u64::<BigEndian>()?;
        let by_sequence_size = bytes.read_u32::<BigEndian>()? as usize;
        let by_id_size = bytes.read_u32::<BigEndian>()? as usize;
        if by_sequence_size + by_id_size != bytes.len() {
            return Err(Error::data_integrity(format!(
                "Header reported index sizes {} and {}, but data has {} remaining",
                by_sequence_size,
                by_id_size,
                bytes.len()
            )));
        };

        let mut by_sequence_bytes = bytes.read_bytes(by_sequence_size)?.to_owned();
        let mut by_id_bytes = bytes.read_bytes(by_id_size)?.to_owned();

        let by_sequence_root =
            BTreeEntry::deserialize_from(&mut by_sequence_bytes, DEFAULT_MAX_ORDER)?;
        let by_id_root = BTreeEntry::deserialize_from(&mut by_id_bytes, DEFAULT_MAX_ORDER)?;

        Ok(Self {
            transaction_id,
            sequence,
            by_sequence_root,
            by_id_root,
        })
    }

    fn serialize<File: ManagedFile>(
        &mut self,
        paged_writer: &mut PagedWriter<'_, File>,
        mut output: &mut Vec<u8>,
    ) -> Result<(), Error> {
        output.reserve(PAGE_SIZE);
        output.write_u64::<BigEndian>(self.transaction_id)?;
        output.write_u64::<BigEndian>(self.sequence)?;
        // Reserve space for by_sequence and by_id sizes (2xu16).
        output.write_u64::<BigEndian>(0)?;

        let by_sequence_size = self
            .by_sequence_root
            .serialize_to(&mut output, paged_writer)?;

        let by_id_size = self.by_id_root.serialize_to(&mut output, paged_writer)?;

        let by_sequence_size = u32::try_from(by_sequence_size)
            .ok()
            .ok_or(ErrorKind::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[16..20], by_sequence_size);
        let by_id_size = u32::try_from(by_id_size)
            .ok()
            .ok_or(ErrorKind::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[20..24], by_id_size);

        Ok(())
    }

    fn transaction_id(&self) -> u64 {
        self.transaction_id
    }

    fn modify<File: ManagedFile>(
        &mut self,
        modification: Modification<'_, Buffer<'static>>,
        writer: &mut PagedWriter<'_, File>,
        max_order: Option<usize>,
    ) -> Result<(), Error> {
        let transaction_id = modification.transaction_id;

        // Insert into both trees
        let mut changes = EntryChanges {
            current_sequence: self.sequence,
            changes: Vec::with_capacity(modification.keys.len()),
        };
        self.modify_id_root(modification, &mut changes, writer, max_order)?;

        // Convert the changes into a modification request for the id root.
        let mut values = Vec::with_capacity(changes.changes.len());
        let keys = changes
            .changes
            .into_iter()
            .map(|change| {
                values.push(BySequenceIndex {
                    key: change.key,
                    value_length: change.value_size,
                    position: change.value_position,
                });
                Buffer::from(change.sequence.to_be_bytes())
            })
            .collect();
        let sequence_modifications = Modification {
            transaction_id,
            keys,
            operation: Operation::SetEach(values),
        };

        self.modify_sequence_root(sequence_modifications, writer, max_order)?;

        if transaction_id != 0 {
            self.transaction_id = transaction_id;
        }

        Ok(())
    }

    fn get_multiple<File: ManagedFile, KeyEvaluator, KeyReader>(
        &self,
        keys: &mut KeyRange<'_>,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut File,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<(), Error>
    where
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), Error>,
    {
        let mut positions_to_read = Vec::new();
        self.by_id_root.get(
            keys,
            key_evaluator,
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
                    CacheEntry::Buffer(contents) => {
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
        File: ManagedFile,
        KeyRangeBounds,
        KeyEvaluator,
        KeyReader,
    >(
        &self,
        range: &KeyRangeBounds,
        args: &mut ScanArgs<Buffer<'static>, CallerError, KeyEvaluator, KeyReader>,
        file: &mut File,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<(), AbortError<CallerError>>
    where
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<CallerError>>,
        KeyRangeBounds: RangeBounds<Buffer<'keys>> + Debug,
    {
        let mut positions_to_read = Vec::new();
        self.by_id_root.scan(
            range,
            &mut ScanArgs::new(
                args.forwards,
                &mut args.key_evaluator,
                &mut |key, index: &VersionedByIdIndex| {
                    positions_to_read.push((key, index.position));
                    Ok(())
                },
            ),
            file,
            vault,
            cache,
        )?;

        // Sort by position on disk
        positions_to_read.sort_by(|a, b| a.1.cmp(&b.1));

        for (key, position) in positions_to_read {
            if position > 0 {
                match read_chunk(position, false, file, vault, cache)? {
                    CacheEntry::Buffer(contents) => {
                        (args.key_reader)(key, contents)?;
                    }
                    CacheEntry::Decoded(_) => unreachable!(),
                };
            }
        }
        Ok(())
    }

    // TODO can we make compaction smarter to not get rid of *all* old data in a versioned file?
    fn copy_data_to<File: ManagedFile>(
        &mut self,
        include_nodes: bool,
        file: &mut File,
        copied_chunks: &mut HashMap<u64, u64>,
        writer: &mut PagedWriter<'_, File>,
        vault: Option<&dyn Vault>,
    ) -> Result<(), Error> {
        // Copy all of the data using the ID root.
        let mut sequence_indexes = Vec::with_capacity(
            usize::try_from(self.by_id_root.stats().alive_keys).unwrap_or(usize::MAX),
        );
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
            &mut |key, index: &mut VersionedByIdIndex, from_file, copied_chunks, to_file, vault| {
                let new_position =
                    copy_chunk(index.position, from_file, copied_chunks, to_file, vault)?;

                sequence_indexes.push((
                    key.clone(),
                    BySequenceIndex {
                        key: key.clone(),
                        value_length: index.value_length,
                        position: new_position,
                    },
                ));

                index.position = new_position;
                Ok(true)
            },
        )?;

        // Replace our by_sequence index with a new truncated one.
        self.by_sequence_root = BTreeEntry::default();

        sequence_indexes.sort_by(|a, b| a.0.cmp(&b.0));
        let by_sequence_order = dynamic_order(sequence_indexes.len() as u64, None);
        let mut keys = Vec::with_capacity(sequence_indexes.len());
        let mut indexes = Vec::with_capacity(sequence_indexes.len());
        for (id, index) in sequence_indexes {
            keys.push(id);
            indexes.push(index);
        }

        let mut modification = Modification {
            transaction_id: self.transaction_id,
            keys,
            operation: Operation::SetEach(indexes),
        };

        let minimum_children = by_sequence_order / 2 - 1;
        let minimum_children = minimum_children.min(modification.keys.len());

        // This modification copies the `sequence_indexes` into the sequence root.
        self.by_sequence_root.modify(
            &mut modification,
            &ModificationContext {
                current_order: by_sequence_order,
                minimum_children,
                indexer: |_key: &Buffer<'_>,
                          value: Option<&BySequenceIndex>,
                          _existing_index: Option<&BySequenceIndex>,
                          _changes: &mut EntryChanges,
                          _writer: &mut PagedWriter<'_, File>| {
                    Ok(KeyOperation::Set(value.unwrap().clone()))
                },
                loader: |_index: &BySequenceIndex, _writer: &mut PagedWriter<'_, File>| unreachable!(),
                _phantom: PhantomData,
            },
            None,
            &mut EntryChanges::default(),
            writer,
        )?;

        Ok(())
    }
}

#[derive(Default)]
pub struct EntryChanges {
    pub current_sequence: u64,
    pub changes: Vec<EntryChange>,
}
pub struct EntryChange {
    pub sequence: u64,
    pub key: Buffer<'static>,
    pub value_position: u64,
    pub value_size: u32,
}
