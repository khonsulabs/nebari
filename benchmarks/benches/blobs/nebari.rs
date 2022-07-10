use std::{
    marker::PhantomData,
    time::{Duration, Instant},
};

use nebari::{
    sediment::io::fs::StdFileManager,
    tree::{State, TreeFile},
    ArcBytes, ChunkCache, Context,
};
use tempfile::TempDir;

use super::InsertConfig;
use crate::{blobs::BlobGenerator, BenchConfig, NebariBenchmark, SimpleBench};

pub struct InsertBlobs<B: NebariBenchmark> {
    _tempfile: TempDir,
    tree: TreeFile<B::Root, StdFileManager>,
    blob: BlobGenerator,
    _bench: PhantomData<B>,
}

impl<B: NebariBenchmark> SimpleBench for InsertBlobs<B> {
    type GroupState = ();
    type Config = InsertConfig;
    const BACKEND: &'static str = B::BACKEND;

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
        let tempfile = TempDir::new_in(".")?;
        let tree = TreeFile::open(
            &tempfile.path().join("tree"),
            State::default(),
            &Context::default().with_cache(ChunkCache::new(2000, 4096)),
            None,
        )?;

        Ok(Self {
            _tempfile: tempfile,
            tree,
            blob: config.initialize(config_group_state),
            _bench: PhantomData,
        })
    }

    fn execute_measured(
        &mut self,
        _config: &Self::Config,
        iters: u64,
    ) -> Result<Duration, anyhow::Error> {
        // While it might be tempting to move serialization out of the measured
        // function, that isn't fair to sql databases which necessarily require
        // encoding the data at least once before saving. While we could pick a
        // faster serialization framework, the goal of our benchmarks aren't to
        // reach maximum speed at all costs: it's to have realistic scenarios
        // measured, and in BonsaiDb, the storage format is going to be `pot`.
        let mut total_duration = Duration::default();
        for _ in 0..iters {
            let blob = self.blob.next().unwrap();
            let start = Instant::now();
            self.tree
                .set(None, ArcBytes::from(blob.0.to_be_bytes()), blob.1.clone())?;
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}
