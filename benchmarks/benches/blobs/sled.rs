use std::time::{Duration, Instant};

use _sled::{Db, IVec};
use tempfile::TempDir;

use super::InsertConfig;
use crate::{blobs::BlobGenerator, BenchConfig, SimpleBench};

pub struct InsertBlobs {
    _tempfile: TempDir,
    db: Db,
    blob: BlobGenerator,
}

impl SimpleBench for InsertBlobs {
    type GroupState = ();
    type Config = InsertConfig;
    const BACKEND: &'static str = "sled";

    fn initialize_group(
        _config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
    }

    fn initialize(
        _group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let tempfile = TempDir::new()?;
        let db = _sled::open(tempfile.path())?;

        Ok(Self {
            _tempfile: tempfile,
            db,
            blob: config.initialize(config_group_state),
        })
    }

    fn execute_measured(
        &mut self,
        _config: &Self::Config,
        iters: u64,
    ) -> Result<Duration, anyhow::Error> {
        let mut total_duration = Duration::default();
        for _ in 0..iters {
            let batch = self.blob.next().unwrap();
            let start = Instant::now();
            self.db
                .insert(&batch.0.to_be_bytes(), IVec::from(batch.1.to_vec()))?;
            self.db.flush()?;
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}
