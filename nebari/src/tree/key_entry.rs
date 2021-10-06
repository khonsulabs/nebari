use std::{collections::HashMap, convert::TryFrom};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{serialization::BinarySerialization, PagedWriter};
use crate::{error::Error, io::ManagedFile, Buffer, ErrorKind, Vault};

#[derive(Debug, Clone)]
pub struct KeyEntry<I> {
    pub key: Buffer<'static>,
    pub index: I,
}

impl<I: BinarySerialization> KeyEntry<I> {
    pub fn copy_data_to<F, Callback>(
        &mut self,
        file: &mut F,
        copied_chunks: &mut HashMap<u64, u64>,
        writer: &mut PagedWriter<'_, F>,
        vault: Option<&dyn Vault>,
        index_callback: &mut Callback,
    ) -> Result<bool, Error>
    where
        F: ManagedFile,
        Callback: FnMut(
            &Buffer<'static>,
            &mut I,
            &mut F,
            &mut HashMap<u64, u64>,
            &mut PagedWriter<'_, F>,
            Option<&dyn Vault>,
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

impl<I: BinarySerialization> BinarySerialization for KeyEntry<I> {
    fn serialize_to<F: ManagedFile>(
        &mut self,
        writer: &mut Vec<u8>,
        paged_writer: &mut PagedWriter<'_, F>,
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

    fn deserialize_from(reader: &mut Buffer<'_>, current_order: usize) -> Result<Self, Error> {
        let key_len = reader.read_u16::<BigEndian>()? as usize;
        if key_len > reader.len() {
            return Err(Error::data_integrity(format!(
                "key length {} found but only {} bytes remaining",
                key_len,
                reader.len()
            )));
        }
        let key = reader.read_bytes(key_len)?.to_owned();

        let value = I::deserialize_from(reader, current_order)?;

        Ok(Self { key, index: value })
    }
}
