use std::marker::PhantomData;

use nebari::{
    io::{fs::StdFile, FileManager, ManagedFile, OpenableFile},
    tree::{Modification, Operation, State, TreeFile},
    Buffer, ChunkCache, Context,
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
            state: config.initialize(config_group_state),
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
        let batch = self.state.next().unwrap();
        self.tree.modify(Modification {
            transaction_id: None,
            keys: batch
                .iter()
                .map(|e| Buffer::from(e.id.to_be_bytes()))
                .collect(),
            operation: Operation::SetEach(
                batch
                    .iter()
                    .map(|e| Buffer::from(pot::to_vec(e).unwrap()))
                    .collect(),
            ),
        })?;
        Ok(())
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
        let manager = <<StdFile as ManagedFile>::Manager as Default>::default();
        let file = manager.append(tempfile.path().join("tree")).unwrap();
        let state = State::initialized(file.id(), None);
        let mut tree = TreeFile::<B::Root, StdFile>::new(
            file,
            state,
            None,
            Some(ChunkCache::new(2000, 160_384)),
        )
        .unwrap();

        config.for_each_database_chunk(1_000_000, |chunk| {
            tree.modify(Modification {
                transaction_id: None,
                keys: chunk
                    .iter()
                    .map(|e| Buffer::from(e.id.to_be_bytes()))
                    .collect(),
                operation: Operation::SetEach(
                    chunk
                        .iter()
                        .map(|e| Buffer::from(pot::to_vec(e).unwrap()))
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
        let manager = <<StdFile as ManagedFile>::Manager as Default>::default();
        let context = Context {
            file_manager: manager,
            vault: None,
            cache: Some(ChunkCache::new(2000, 160_384)),
        };
        let file_path = group_state.path().join("tree");
        let file = context.file_manager.append(&file_path).unwrap();
        let state = State::default();
        TreeFile::<B::Root, StdFile>::initialize_state(
            &state,
            &file_path,
            file.id(),
            &context,
            None,
        )
        .unwrap();
        let tree = TreeFile::<B::Root, StdFile>::new(
            file,
            state,
            context.vault.clone(),
            context.cache.clone(),
        )
        .unwrap();
        let state = config.initialize(config_group_state);
        Ok(Self { tree, state })
    }

    fn execute_measured(&mut self, config: &Self::Config) -> Result<(), anyhow::Error> {
        if config.get_count == 1 {
            let entry = self.state.next().unwrap();
            let bytes = self
                .tree
                .get(&entry.id.to_be_bytes(), false)?
                .expect("value not found");
            let decoded = pot::from_slice::<LogEntry>(&bytes)?;
            assert_eq!(&decoded, &entry);
        } else {
            let mut entry_key_bytes = (0..config.get_count)
                .map(|_| self.state.next().unwrap().id.to_be_bytes())
                .collect::<Vec<_>>();
            entry_key_bytes.sort_unstable();
            let entry_keys = entry_key_bytes.iter().map(|k| &k[..]).collect::<Vec<_>>();
            let buffers = self.tree.get_multiple(&entry_keys, false)?;
            assert_eq!(buffers.len(), config.get_count);
        }
        Ok(())
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
        let manager = <<StdFile as ManagedFile>::Manager as Default>::default();
        let file = manager.append(tempfile.path().join("tree")).unwrap();
        let state = State::initialized(file.id(), None);
        let mut tree = TreeFile::<B::Root, StdFile>::new(
            file,
            state,
            None,
            Some(ChunkCache::new(2000, 160_384)),
        )
        .unwrap();

        config.for_each_database_chunk(1_000_000, |chunk| {
            tree.modify(Modification {
                transaction_id: None,
                keys: chunk
                    .iter()
                    .map(|e| Buffer::from(e.id.to_be_bytes()))
                    .collect(),
                operation: Operation::SetEach(
                    chunk
                        .iter()
                        .map(|e| Buffer::from(pot::to_vec(e).unwrap()))
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
        let manager = <<StdFile as ManagedFile>::Manager as Default>::default();
        let context = Context {
            file_manager: manager,
            vault: None,
            cache: Some(ChunkCache::new(2000, 160_384)),
        };
        let file_path = group_state.path().join("tree");
        let file = context.file_manager.append(&file_path).unwrap();
        let state = State::default();
        TreeFile::<B::Root, StdFile>::initialize_state(
            &state,
            &file_path,
            file.id(),
            &context,
            None,
        )
        .unwrap();
        let tree = TreeFile::<B::Root, StdFile>::new(
            file,
            state,
            context.vault.clone(),
            context.cache.clone(),
        )
        .unwrap();
        let state = config.initialize(config_group_state);
        Ok(Self { tree, state })
    }

    fn execute_measured(&mut self, config: &Self::Config) -> Result<(), anyhow::Error> {
        let range = self.state.next().unwrap();
        let range =
            Buffer::from(range.start().to_be_bytes())..=Buffer::from(range.end().to_be_bytes());
        let entries = self.tree.get_range(range, false)?;
        assert_eq!(entries.len(), config.element_count);
        Ok(())
    }
}
