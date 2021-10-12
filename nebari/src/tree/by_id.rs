use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{btree_entry::Reducer, BinarySerialization, PagedWriter};
use crate::{error::Error, io::ManagedFile, Buffer};

/// The index stored within [`VersionedTreeRoot::by_id_root`](crate::tree::VersionedTreeRoot::by_id_root).
#[derive(Clone, Debug)]
pub struct VersionedByIdIndex {
    /// The unique sequence id generated when writing the value to the file.
    pub sequence_id: u64,
    /// The size of the value stored on disk.
    pub value_length: u32,
    /// The position of the value on disk.
    pub position: u64,
}

impl BinarySerialization for VersionedByIdIndex {
    fn serialize_to<File: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_, File>,
    ) -> Result<usize, Error> {
        writer.write_u64::<BigEndian>(self.sequence_id)?;
        writer.write_u32::<BigEndian>(self.value_length)?;
        writer.write_u64::<BigEndian>(self.position)?;
        Ok(20)
    }

    fn deserialize_from(
        reader: &mut Buffer<'_>,
        _current_order: Option<usize>,
    ) -> Result<Self, Error> {
        let sequence_id = reader.read_u64::<BigEndian>()?;
        let value_length = reader.read_u32::<BigEndian>()?;
        let position = reader.read_u64::<BigEndian>()?;
        Ok(Self {
            sequence_id,
            value_length,
            position,
        })
    }
}

/// The index stored within [`UnversionedTreeRoot::by_id_root`](crate::tree::UnversionedTreeRoot::by_id_root).
#[derive(Clone, Debug)]
pub struct UnversionedByIdIndex {
    /// The size of the value stored on disk.
    pub value_length: u32,
    /// The position of the value on disk.
    pub position: u64,
}

impl BinarySerialization for UnversionedByIdIndex {
    fn serialize_to<File: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_, File>,
    ) -> Result<usize, Error> {
        writer.write_u32::<BigEndian>(self.value_length)?;
        writer.write_u64::<BigEndian>(self.position)?;
        Ok(12)
    }

    fn deserialize_from(
        reader: &mut Buffer<'_>,
        _current_order: Option<usize>,
    ) -> Result<Self, Error> {
        let value_length = reader.read_u32::<BigEndian>()?;
        let position = reader.read_u64::<BigEndian>()?;
        Ok(Self {
            value_length,
            position,
        })
    }
}

/// The reduced index of both [`VersionedByIdIndex`] and [`UnversionedByIdIndex`]
#[derive(Clone, Debug)]
pub struct ByIdStats {
    /// The number of keys that have values stored within them.
    pub alive_keys: u64,
    /// The number of keys that no longer have values stored within them.
    pub deleted_keys: u64,
    /// The total number of bytes stored on disk associated with currently-alive values.
    pub total_indexed_bytes: u64,
}

impl ByIdStats {
    /// Returns the total number of keys regardless of whether data is stored within them.
    #[must_use]
    pub const fn total_keys(&self) -> u64 {
        self.alive_keys + self.deleted_keys
    }
}

impl BinarySerialization for ByIdStats {
    fn serialize_to<File: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_, File>,
    ) -> Result<usize, Error> {
        writer.write_u64::<BigEndian>(self.alive_keys)?;
        writer.write_u64::<BigEndian>(self.deleted_keys)?;
        writer.write_u64::<BigEndian>(self.total_indexed_bytes)?;
        Ok(24)
    }

    fn deserialize_from(
        reader: &mut Buffer<'_>,
        _current_order: Option<usize>,
    ) -> Result<Self, Error> {
        let alive_keys = reader.read_u64::<BigEndian>()?;
        let deleted_keys = reader.read_u64::<BigEndian>()?;
        let total_indexed_bytes = reader.read_u64::<BigEndian>()?;
        Ok(Self {
            alive_keys,
            deleted_keys,
            total_indexed_bytes,
        })
    }
}

impl Reducer<VersionedByIdIndex> for ByIdStats {
    fn key_count(&self) -> u64 {
        self.alive_keys + self.deleted_keys
    }

    fn reduce(values: &[&VersionedByIdIndex]) -> Self {
        reduce(values)
    }

    fn rereduce(values: &[&Self]) -> Self {
        rereduce(values)
    }
}

impl Reducer<UnversionedByIdIndex> for ByIdStats {
    fn key_count(&self) -> u64 {
        self.alive_keys
    }

    fn reduce(values: &[&UnversionedByIdIndex]) -> Self {
        reduce(values)
    }

    fn rereduce(values: &[&Self]) -> Self {
        rereduce(values)
    }
}

fn reduce(values: &[&impl IdIndex]) -> ByIdStats {
    let (alive_keys, deleted_keys, total_indexed_bytes) = values
        .iter()
        .map(|index| {
            if index.position() > 0 {
                // Alive key
                (1, 0, u64::from(index.value_size()))
            } else {
                // Deleted
                (0, 1, 0)
            }
        })
        .reduce(
            |(total_alive, total_deleted, total_size), (alive, deleted, size)| {
                (
                    total_alive + alive,
                    total_deleted + deleted,
                    total_size + size,
                )
            },
        )
        .unwrap_or_default();
    ByIdStats {
        alive_keys,
        deleted_keys,
        total_indexed_bytes,
    }
}

trait IdIndex {
    fn value_size(&self) -> u32;
    fn position(&self) -> u64;
}

impl IdIndex for UnversionedByIdIndex {
    fn value_size(&self) -> u32 {
        self.value_length
    }

    fn position(&self) -> u64 {
        self.position
    }
}

impl IdIndex for VersionedByIdIndex {
    fn value_size(&self) -> u32 {
        self.value_length
    }

    fn position(&self) -> u64 {
        self.position
    }
}

fn rereduce(values: &[&ByIdStats]) -> ByIdStats {
    ByIdStats {
        alive_keys: values.iter().map(|v| v.alive_keys).sum(),
        deleted_keys: values.iter().map(|v| v.deleted_keys).sum(),
        total_indexed_bytes: values.iter().map(|v| v.total_indexed_bytes).sum(),
    }
}
