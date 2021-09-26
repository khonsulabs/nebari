use std::{
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
    KeyEvaluation, KeyRange, PagedWriter, PAGE_SIZE,
};
use crate::{
    chunk_cache::CacheEntry,
    error::InternalError,
    roots::AbortError,
    tree::{
        btree_entry::{KeyOperation, ModificationContext, ScanArgs},
        versioned::ChangeResult,
        Root,
    },
    Buffer, ChunkCache, Error, ManagedFile, Vault,
};

const MAX_ORDER: usize = 1000;

/// A versioned B-Tree root. This tree root internally uses two btrees, one to
/// keep track of all writes using a unique "sequence" ID, and one that keeps
/// track of all key-value pairs.
#[derive(Clone, Default, Debug)]
pub struct UnversionedTreeRoot {
    /// The transaction ID of the tree root. If this transaction ID isn't
    /// present in the transaction log, this root should not be trusted.
    pub(crate) transaction_id: Option<u64>,
    /// The by-id B-Tree.
    pub(crate) by_id_root: BTreeEntry<UnversionedByIdIndex, ByIdStats>,
}

impl UnversionedTreeRoot {
    fn modify_id_root<'a, 'w, F: ManagedFile>(
        &'a mut self,
        mut modification: Modification<'_, Buffer<'static>>,
        writer: &'a mut PagedWriter<'w, F>,
    ) -> Result<(), Error> {
        modification.reverse()?;

        let by_id_order = dynamic_order::<MAX_ORDER>(
            self.by_id_root.stats().total_documents() + modification.keys.len() as u64,
        );

        while !modification.keys.is_empty() {
            match self.by_id_root.modify(
                &mut modification,
                &ModificationContext {
                    current_order: by_id_order,
                    indexer: |_key: &Buffer<'_>,
                              value: Option<&Buffer<'static>>,
                              _existing_index,
                              _changes,
                              writer: &mut PagedWriter<'_, F>| {
                        if let Some(value) = value {
                            let new_position = writer.write_chunk(value)?;
                            // write_chunk errors if it can't fit within a u32
                            #[allow(clippy::cast_possible_truncation)]
                            let document_size = value.len() as u32;
                            Ok(KeyOperation::Set(UnversionedByIdIndex {
                                document_size,
                                position: new_position,
                            }))
                        } else {
                            Ok(KeyOperation::Remove)
                        }
                    },
                    loader: |index, writer| match writer.read_chunk(index.position)? {
                        CacheEntry::Buffer(buffer) => Ok(Some(buffer.clone())),
                        CacheEntry::Decoded(_) => unreachable!(),
                    },
                    _phantom: PhantomData,
                },
                None,
                &mut (),
                writer,
            )? {
                ChangeResult::Changed | ChangeResult::Unchanged | ChangeResult::Remove => {}
                ChangeResult::Split(upper) => {
                    self.by_id_root.split_root(upper);
                }
            }
        }

        Ok(())
    }
}

impl Root for UnversionedTreeRoot {
    fn initialized(&self) -> bool {
        self.transaction_id.is_some()
    }

    fn initialize_default(&mut self) {
        self.transaction_id = Some(0);
    }

    fn deserialize(mut bytes: Buffer<'_>) -> Result<Self, Error> {
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

        let by_id_root = BTreeEntry::deserialize_from(&mut by_id_bytes, MAX_ORDER)?;

        Ok(Self {
            transaction_id,
            by_id_root,
        })
    }

    fn serialize<F: ManagedFile>(
        &mut self,
        paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<Vec<u8>, Error> {
        let mut output = Vec::new();
        output.reserve(PAGE_SIZE);
        output.write_u64::<BigEndian>(
            self.transaction_id
                .expect("serializing an uninitialized root"),
        )?;
        // Reserve space for by_id size.
        output.write_u32::<BigEndian>(0)?;

        let by_id_size = self.by_id_root.serialize_to(&mut output, paged_writer)?;
        let by_id_size = u32::try_from(by_id_size)
            .ok()
            .ok_or(Error::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[8..12], by_id_size);

        Ok(output)
    }

    fn transaction_id(&self) -> u64 {
        self.transaction_id.unwrap_or_default()
    }

    fn modify<'a, 'w, F: ManagedFile>(
        &'a mut self,
        modification: Modification<'_, Buffer<'static>>,
        writer: &'a mut PagedWriter<'w, F>,
    ) -> Result<(), Error> {
        let transaction_id = modification.transaction_id;

        self.modify_id_root(modification, writer)?;

        if transaction_id != 0 {
            self.transaction_id = Some(transaction_id);
        }

        Ok(())
    }

    fn get_multiple<F: ManagedFile, KeyEvaluator, KeyReader>(
        &self,
        keys: &mut KeyRange<'_>,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut F,
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
                match read_chunk(position, file, vault, cache)? {
                    CacheEntry::Buffer(contents) => {
                        key_reader(key, contents)?;
                    }
                    CacheEntry::Decoded(_) => unreachable!(),
                };
            }
        }
        Ok(())
    }

    fn scan<'k, E: Display + Debug, F: ManagedFile, KeyRangeBounds, KeyEvaluator, KeyReader>(
        &self,
        range: &KeyRangeBounds,
        args: &mut ScanArgs<Buffer<'static>, E, KeyEvaluator, KeyReader>,
        file: &mut F,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<(), AbortError<E>>
    where
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<E>>,
        KeyRangeBounds: RangeBounds<Buffer<'k>> + Debug,
    {
        let mut positions_to_read = Vec::new();
        self.by_id_root.scan(
            range,
            &mut ScanArgs::new(
                args.forwards,
                &mut args.key_evaluator,
                &mut |key, index: &UnversionedByIdIndex| {
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
                match read_chunk(position, file, vault, cache)? {
                    CacheEntry::Buffer(contents) => {
                        (args.key_reader)(key, contents)?;
                    }
                    CacheEntry::Decoded(_) => unreachable!(),
                };
            }
        }
        Ok(())
    }
}

/// Returns a value for the "order" (maximum children per node) value for the
/// database. This function is meant to keep the tree shallow while still
/// keeping the nodes smaller along the way. This is an approximation that
/// always returns an order larger than what is needed, but will never return a
/// value larger than `MAX_ORDER`.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn dynamic_order<const MAX_ORDER: usize>(number_of_records: u64) -> usize {
    // Current approximation is the 4th root
    if number_of_records > MAX_ORDER.pow(4) as u64 {
        MAX_ORDER
    } else {
        let estimated_order = 2.max((number_of_records as f64).sqrt().sqrt().ceil() as usize);
        // Add some padding so that we don't have a 100% fill rate.
        let estimated_order = estimated_order + (estimated_order / 3).max(1);
        MAX_ORDER.min(estimated_order)
    }
}
