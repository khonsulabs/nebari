use super::PagedWriter;
use crate::{error::Error, ArcBytes};

/// A trait for serializing and deserializing from a tree.
pub trait BinarySerialization: Send + Sync + Sized {
    /// Serializes `self` into `writer`, using `paged_writer` to store any
    /// additional data as needed.
    fn serialize_to(
        &mut self,
        writer: &mut Vec<u8>,
        paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error>;

    /// Deserialize an instance from `reader`. If an allocation of nodes is
    /// needed, assume `current_order` is the maximum number of nodes a node can
    /// contain.
    fn deserialize_from(
        reader: &mut ArcBytes<'_>,
        current_order: Option<usize>,
    ) -> Result<Self, Error>;
}

impl BinarySerialization for () {
    fn serialize_to(
        &mut self,
        _writer: &mut Vec<u8>,
        _paged_writer: &mut PagedWriter<'_>,
    ) -> Result<usize, Error> {
        Ok(0)
    }

    fn deserialize_from(
        _reader: &mut ArcBytes<'_>,
        _current_order: Option<usize>,
    ) -> Result<Self, Error> {
        Ok(())
    }
}
