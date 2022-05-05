use std::{
    marker::PhantomData,
    time::{Duration, Instant},
};

use nebari::{
    io::fs::StdFile,
    tree::{Modification, Operation, PersistenceMode, State, TreeFile},
    ArcBytes, ChunkCache, Context,
};
use tempfile::TempDir;

use super::{InsertConfig, LogEntry, LogEntryBatchGenerator, ReadConfig, ReadState};
use crate::{
    logs::{ScanConfig, ScanState},
    BenchConfig, NebariBenchmark, SimpleBench,
};

pub struct InsertLogs<B: NebariBenchmark> {
    _tempfile: TempDir,
    tree: TreeFile<B::Root, StdFile>,
    state: LogEntryBatchGenerator,
    _bench: PhantomData<B>,
}
impl<B: NebariBenchmark> SimpleBench for InsertLogs<B> {
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
        let tree = TreeFile::<B::Root, StdFile>::write(
            tempfile.path().join("tree"),
            State::default(),
            &Context::default(),
            None,
        )?;

        Ok(Self {
            _tempfile: tempfile,
            tree,
            state: config.initialize(config_group_state),
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
            let batch = self.state.next().unwrap();
            let start = Instant::now();
            self.tree.modify(Modification {
                persistence_mode: PersistenceMode::Sync,
                keys: batch
                    .iter()
                    .map(|e| ArcBytes::from(e.id.to_be_bytes()))
                    .collect(),
                operation: Operation::SetEach(
                    batch
                        .iter()
                        .map(|e| ArcBytes::from(pot::to_vec(e).unwrap()))
                        .collect(),
                ),
            })?;
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}

pub struct ReadLogs<B: NebariBenchmark> {
    tree: TreeFile<B::Root, StdFile>,
    state: ReadState,
}

impl<B: NebariBenchmark> SimpleBench for ReadLogs<B> {
    type GroupState = TempDir;
    type Config = ReadConfig;
    const BACKEND: &'static str = B::BACKEND;

    fn initialize_group(
        config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
        let tempfile = TempDir::new().unwrap();
        let mut tree = TreeFile::<B::Root, StdFile>::write(
            tempfile.path().join("tree"),
            State::default(),
            &Context::default(),
            None,
        )
        .unwrap();

        config.for_each_database_chunk(1_000_000, |chunk| {
            tree.modify(Modification {
                persistence_mode: PersistenceMode::Sync,
                keys: chunk
                    .iter()
                    .map(|e| ArcBytes::from(e.id.to_be_bytes()))
                    .collect(),
                operation: Operation::SetEach(
                    chunk
                        .iter()
                        .map(|e| ArcBytes::from(pot::to_vec(e).unwrap()))
                        .collect(),
                ),
            })
            .unwrap();
        });
        tempfile
    }

    fn initialize(
        group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let context = Context::default().with_cache(ChunkCache::new(2000, 160_384));
        let file_path = group_state.path().join("tree");
        let tree = TreeFile::<B::Root, StdFile>::read(&file_path, State::default(), &context, None)
            .unwrap();
        let state = config.initialize(config_group_state);
        Ok(Self { tree, state })
    }

    fn execute_measured(
        &mut self,
        config: &Self::Config,
        iters: u64,
    ) -> Result<Duration, anyhow::Error> {
        let mut total_duration = Duration::default();
        for _ in 0..iters {
            if config.get_count == 1 {
                let entry = self.state.next().unwrap();
                let start = Instant::now();
                let bytes = self
                    .tree
                    .get(&entry.id.to_be_bytes(), false)?
                    .expect("value not found");
                let decoded = pot::from_slice::<LogEntry>(&bytes)?;
                assert_eq!(&decoded, &entry);
                total_duration += Instant::now() - start;
            } else {
                let mut entry_key_bytes = (0..config.get_count)
                    .map(|_| self.state.next().unwrap().id.to_be_bytes())
                    .collect::<Vec<_>>();
                let start = Instant::now();
                entry_key_bytes.sort_unstable();
                let buffers = self
                    .tree
                    .get_multiple(entry_key_bytes.iter().map(|k| &k[..]), false)?;
                assert_eq!(buffers.len(), config.get_count);
                total_duration += Instant::now() - start;
            }
        }
        Ok(total_duration)
    }
}

pub struct ScanLogs<B: NebariBenchmark> {
    tree: TreeFile<B::Root, StdFile>,
    state: ScanState,
}

impl<B: NebariBenchmark> SimpleBench for ScanLogs<B> {
    type GroupState = TempDir;
    type Config = ScanConfig;
    const BACKEND: &'static str = B::BACKEND;

    fn initialize_group(
        config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
        let tempfile = TempDir::new().unwrap();
        let mut tree = TreeFile::<B::Root, StdFile>::write(
            tempfile.path().join("tree"),
            State::default(),
            &Context::default(),
            None,
        )
        .unwrap();
        config.for_each_database_chunk(1_000_000, |chunk| {
            tree.modify(Modification {
                persistence_mode: PersistenceMode::Sync,
                keys: chunk
                    .iter()
                    .map(|e| ArcBytes::from(e.id.to_be_bytes()))
                    .collect(),
                operation: Operation::SetEach(
                    chunk
                        .iter()
                        .map(|e| ArcBytes::from(pot::to_vec(e).unwrap()))
                        .collect(),
                ),
            })
            .unwrap();
        });
        tempfile
    }

    fn initialize(
        group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let context = Context::default().with_cache(ChunkCache::new(2000, 160_384));
        let file_path = group_state.path().join("tree");
        let tree = TreeFile::<B::Root, StdFile>::read(&file_path, State::default(), &context, None)
            .unwrap();
        let state = config.initialize(config_group_state);
        Ok(Self { tree, state })
    }

    fn execute_measured(
        &mut self,
        config: &Self::Config,
        iters: u64,
    ) -> Result<Duration, anyhow::Error> {
        let mut total_duration = Duration::default();
        for _ in 0..iters {
            let range = self.state.next().unwrap();
            let start = Instant::now();
            let start_bytes = range.start().to_be_bytes();
            let end_bytes = range.end().to_be_bytes();
            let range = &start_bytes[..]..=&end_bytes[..];
            let entries = self.tree.get_range(&range, false)?;
            assert_eq!(entries.len(), config.element_count);
            total_duration += Instant::now() - start;
        }
        Ok(total_duration)
    }
}
