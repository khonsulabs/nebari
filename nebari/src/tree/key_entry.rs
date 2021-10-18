use std::{collections::HashMap, convert::TryFrom};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{serialization::BinarySerialization, PagedWriter};
use crate::{error::Error, io::ManagedFile, vault::AnyVault, Buffer, ErrorKind};

/// An entry for a key. Stores a single index value for a single key.
#[derive(Debug, Clone)]
pub struct KeyEntry<Index> {
    /// The key of this entry.
    pub key: Buffer<'static>,
    /// The index value of this entry.
    pub index: Index,
}

/// An index that serializes a value to the file.
pub trait ValueIndex {
    /// The position on-disk of the stored value.
    fn position(&self) -> u64;
}

impl<Index: ValueIndex + BinarySerialization> KeyEntry<Index> {
    pub(crate) fn copy_data_to<File, Callback>(
        &mut self,
        file: &mut File,
        copied_chunks: &mut HashMap<u64, u64>,
        writer: &mut PagedWriter<'_, File>,
        vault: Option<&dyn AnyVault>,
        index_callback: &mut Callback,
    ) -> Result<bool, Error>
    where
        File: ManagedFile,
        Callback: FnMut(
            &Buffer<'static>,
            &mut Index,
            &mut File,
            &mut HashMap<u64, u64>,
            &mut PagedWriter<'_, File>,
            Option<&dyn AnyVault>,
        ) -> Result<bool, Error>,
    {
        index_callback(
            &self.key,
            &mut self.index,
            file,
            copied_chunks,
            writer,
            vault,
        )
    }
}

impl<Index: BinarySerialization> BinarySerialization for KeyEntry<Index> {
    fn serialize_to<File: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        paged_writer: &mut PagedWriter<'_, File>,
    ) -> Result<usize, Error> {
        let mut bytes_written = 0;
        // Write the key
        let key_len = u16::try_from(self.key.len()).map_err(|_| ErrorKind::KeyTooLarge)?;
        writer.write_u16::<BigEndian>(key_len)?;
        writer.extend_from_slice(&self.key);
        bytes_written += 2 + key_len as usize;

        // Write the value
        bytes_written += self.index.serialize_to(writer, paged_writer)?;
        Ok(bytes_written)
    }

    fn deserialize_from(
        reader: &mut Buffer<'_>,
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
        let key = reader.read_bytes(key_len)?.to_owned();

        let value = Index::deserialize_from(reader, current_order)?;

        Ok(Self { key, index: value })
    }
}
