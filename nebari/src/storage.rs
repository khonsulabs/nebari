pub mod sediment;

use std::path::Path;

use arc_bytes::ArcBytes;

use crate::{
    tree::{self, State},
    Error,
};

pub trait BlobStorage {
    fn unique_id(&self) -> u64;
    fn read_header(&mut self) -> Result<ArcBytes<'static>, Error>;
    fn write_header(&mut self, data: &[u8]) -> Result<(), Error>;

    fn read(&mut self, id: u64) -> Result<ArcBytes<'static>, Error>;
    fn free(&mut self, id: u64) -> Result<(), Error>;
    fn write(&mut self, data: Vec<u8>) -> Result<u64, Error>;
    fn sync(&mut self) -> Result<(), Error>;
}

pub trait StorageManager {
    type Storage: BlobStorage;

    fn open<Path: AsRef<std::path::Path> + ?Sized>(
        &self,
        path: &Path,
    ) -> Result<Self::Storage, Error>;
}
