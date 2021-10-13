use std::fmt::Display;

use ::nebari::Buffer;
use criterion::{Criterion, Throughput};
use nanorand::{Pcg64, Rng};
use ubyte::ToByteUnit;

use crate::{BenchConfig, SimpleBench, UnversionedBenchmark, VersionedBenchmark};
mod nebari;
// #[cfg(feature = "persy")]
// mod persy;
// #[cfg(feature = "sled")]
// mod sled;
// #[cfg(feature = "sqlite")]
// mod sqlite;

pub fn benches(c: &mut Criterion) {
    inserts(c);
}

pub fn inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("blobs-insert");

    for blob_size in [1024, 1024 * 1024, 1024 * 1024 * 64] {
        let config = InsertConfig { blob_size };

        nebari::InsertBlobs::<VersionedBenchmark>::run(&mut group, &config);
        nebari::InsertBlobs::<UnversionedBenchmark>::run(&mut group, &config);
        // #[cfg(feature = "sled")]
        // sled::InsertLogs::run(&mut group, &config);
        // #[cfg(feature = "sqlite")]
        // sqlite::InsertLogs::run(&mut group, &config);
        // #[cfg(feature = "persy")]
        // persy::InsertLogs::run(&mut group, &config);
        // #[cfg(feature = "couchdb")]
        // couchdb::InsertLogs::run(&mut group, &config);
    }
}

#[derive(Clone)]
pub struct InsertConfig {
    pub blob_size: usize,
}

#[derive(Clone)]
pub struct Blob(u64, Buffer<'static>);

impl Iterator for Blob {
    type Item = Blob;

    fn next(&mut self) -> Option<Self::Item> {
        self.0 += 1;
        Some(self.clone())
    }
}

impl BenchConfig for InsertConfig {
    type GroupState = ();
    type State = Blob;
    type Batch = Blob;

    fn initialize_group(&self) -> Self::GroupState {}

    fn initialize(&self, _group_state: &Self::GroupState) -> Self::State {
        let mut bytes = Vec::with_capacity(self.blob_size);
        let mut rng = Pcg64::new_seed(1);
        for _ in 0..self.blob_size {
            bytes.push(rng.generate());
        }
        Blob(0, Buffer::from(bytes))
    }

    fn throughput(&self) -> Throughput {
        Throughput::Bytes(self.blob_size as u64)
    }
}

impl Display for InsertConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.blob_size.bytes())
    }
}
