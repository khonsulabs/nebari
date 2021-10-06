use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{btree_entry::Reducer, BinarySerialization, PagedWriter};
use crate::{error::Error, io::ManagedFile, Buffer};

#[derive(Clone, Debug)]
pub struct VersionedByIdIndex {
    pub sequence_id: u64,
    pub document_size: u32,
    pub position: u64,
}

impl BinarySerialization for VersionedByIdIndex {
    fn serialize_to<F: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error> {
        writer.write_u64::<BigEndian>(self.sequence_id)?;
        writer.write_u32::<BigEndian>(self.document_size)?;
        writer.write_u64::<BigEndian>(self.position)?;
        Ok(20)
    }

    fn deserialize_from(reader: &mut Buffer<'_>, _current_order: usize) -> Result<Self, Error> {
        let sequence_id = reader.read_u64::<BigEndian>()?;
        let document_size = reader.read_u32::<BigEndian>()?;
        let position = reader.read_u64::<BigEndian>()?;
        Ok(Self {
            sequence_id,
            document_size,
            position,
        })
    }
}

#[derive(Clone, Debug)]
pub struct UnversionedByIdIndex {
    pub document_size: u32,
    pub position: u64,
}

impl BinarySerialization for UnversionedByIdIndex {
    fn serialize_to<F: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error> {
        writer.write_u32::<BigEndian>(self.document_size)?;
        writer.write_u64::<BigEndian>(self.position)?;
        Ok(12)
    }

    fn deserialize_from(reader: &mut Buffer<'_>, _current_order: usize) -> Result<Self, Error> {
        let document_size = reader.read_u32::<BigEndian>()?;
        let position = reader.read_u64::<BigEndian>()?;
        Ok(Self {
            document_size,
            position,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ByIdStats {
    pub alive_documents: u64,
    pub deleted_documents: u64,
    pub total_size: u64,
}

impl ByIdStats {
    pub const fn total_documents(&self) -> u64 {
        self.alive_documents + self.deleted_documents
    }
}

impl BinarySerialization for ByIdStats {
    fn serialize_to<F: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error> {
        writer.write_u64::<BigEndian>(self.alive_documents)?;
        writer.write_u64::<BigEndian>(self.deleted_documents)?;
        writer.write_u64::<BigEndian>(self.total_size)?;
        Ok(24)
    }

    fn deserialize_from(reader: &mut Buffer<'_>, _current_order: usize) -> Result<Self, Error> {
        let alive_documents = reader.read_u64::<BigEndian>()?;
        let deleted_documents = reader.read_u64::<BigEndian>()?;
        let total_size = reader.read_u64::<BigEndian>()?;
        Ok(Self {
            alive_documents,
            deleted_documents,
            total_size,
        })
    }
}

impl Reducer<VersionedByIdIndex> for ByIdStats {
    fn reduce(values: &[&VersionedByIdIndex]) -> Self {
        reduce(values)
    }

    fn rereduce(values: &[&Self]) -> Self {
        rereduce(values)
    }
}

impl Reducer<UnversionedByIdIndex> for ByIdStats {
    fn reduce(values: &[&UnversionedByIdIndex]) -> Self {
        reduce(values)
    }

    fn rereduce(values: &[&Self]) -> Self {
        rereduce(values)
    }
}

fn reduce(values: &[&impl IdIndex]) -> ByIdStats {
    let (alive_documents, deleted_documents, total_size) = values
        .iter()
        .map(|index| {
            if index.position() > 0 {
                // Alive document
                (1, 0, u64::from(index.document_size()))
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
        alive_documents,
        deleted_documents,
        total_size,
    }
}

trait IdIndex {
    fn document_size(&self) -> u32;
    fn position(&self) -> u64;
}

impl IdIndex for UnversionedByIdIndex {
    fn document_size(&self) -> u32 {
        self.document_size
    }

    fn position(&self) -> u64 {
        self.position
    }
}

impl IdIndex for VersionedByIdIndex {
    fn document_size(&self) -> u32 {
        self.document_size
    }

    fn position(&self) -> u64 {
        self.position
    }
}

fn rereduce(values: &[&ByIdStats]) -> ByIdStats {
    ByIdStats {
        alive_documents: values.iter().map(|v| v.alive_documents).sum(),
        deleted_documents: values.iter().map(|v| v.deleted_documents).sum(),
        total_size: values.iter().map(|v| v.total_size).sum(),
    }
}
