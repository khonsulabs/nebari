use std::marker::PhantomData;

use nebari::{
    io::{fs::StdFile, FileManager, ManagedFile, OpenableFile},
    tree::{State, TreeFile},
    Buffer, ChunkCache,
};
use tempfile::TempDir;

use super::InsertConfig;
use crate::{blobs::Blob, BenchConfig, NebariBenchmark, SimpleBench};

pub struct InsertBlobs<B: NebariBenchmark> {
    _tempfile: TempDir,
    tree: TreeFile<B::Root, StdFile>,
    blob: Blob,
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
        let tempfile = TempDir::new()?;
        let manager = <<StdFile as ManagedFile>::Manager as Default>::default();
        let file = manager.append(tempfile.path().join("tree"))?;
        let state = State::initialized(file.id(), None);
        let tree = TreeFile::<B::Root, StdFile>::new(
            file,
            state,
            None,
            Some(ChunkCache::new(100, 160_384)),
        )?;

        Ok(Self {
            _tempfile: tempfile,
            tree,
            blob: config.initialize(config_group_state),
            _bench: PhantomData,
        })
    }

    fn execute_measured(&mut self, _config: &Self::Config) -> Result<(), anyhow::Error> {
        // While it might be tempting to move serialization out of the measured
        // function, that isn't fair to sql databases which necessarily require
        // encoding the data at least once before saving. While we could pick a
        // faster serialization framework, the goal of our benchmarks aren't to
        // reach maximum speed at all costs: it's to have realistic scenarios
        // measured, and in BonsaiDb, the storage format is going to be `pot`.
        let blob = self.blob.next().unwrap();
        self.tree
            .push(None, Buffer::from(blob.0.to_be_bytes()), blob.1.clone())?;
        Ok(())
    }
}
