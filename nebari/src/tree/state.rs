use std::{fmt::Debug, sync::Arc};

use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::chunk_cache::AnySendSync;

/// The current state of a tree file. Must be initialized before passing to
/// `TreeFile::new` if the file already exists.
#[derive(Clone, Debug)]
#[must_use]
pub struct State<Root: super::Root> {
    reader: Arc<RwLock<Arc<ActiveState<Root>>>>,
    writer: Arc<Mutex<ActiveState<Root>>>,
}

impl<Root> State<Root>
where
    Root: super::Root,
{
    /// Returns an uninitialized state.
    pub fn new(file_id: Option<u64>, max_order: Option<usize>, root: Root) -> Self {
        let state = ActiveState {
            file_id,
            max_order,
            current_position: 0,
            root,
        };

        Self {
            reader: Arc::new(RwLock::new(Arc::new(state.clone()))),
            writer: Arc::new(Mutex::new(state)),
        }
    }
    /// Returns an initialized state. This should only be used if you're
    /// creating a file from scratch.
    pub fn initialized(file_id: Option<u64>, max_order: Option<usize>, mut root: Root) -> Self {
        root.initialize_default();
        let state = ActiveState {
            file_id,
            max_order,
            current_position: 0,
            root,
        };

        Self {
            reader: Arc::new(RwLock::new(Arc::new(state.clone()))),
            writer: Arc::new(Mutex::new(state)),
        }
    }

    /// Locks the state for writing.
    pub(crate) fn lock(&self) -> MutexGuard<'_, ActiveState<Root>> {
        self.writer.lock()
    }

    /// Reads the current state.
    #[must_use]
    pub fn read(&self) -> Arc<ActiveState<Root>> {
        let reader = self.reader.read();
        reader.clone()
    }
}

impl<Root> Default for State<Root>
where
    Root: super::Root + Default,
{
    fn default() -> Self {
        Self::new(None, None, Root::default())
    }
}

pub trait AnyTreeState: AnySendSync + Debug {
    fn cloned(&self) -> Box<dyn AnyTreeState>;
    fn publish(&self);
}

impl<Root: super::Root> AnyTreeState for State<Root> {
    fn cloned(&self) -> Box<dyn AnyTreeState> {
        Box::new(self.clone())
    }

    fn publish(&self) {
        let state = self.lock();
        state.publish(self);
    }
}

/// An active state for a tree file.
#[derive(Clone, Debug, Default)]
pub struct ActiveState<Root: super::Root> {
    /// The current file id associated with this tree file. Database compaction
    /// will cause the file_id to be changed once the operation succeeds.
    pub file_id: Option<u64>,
    /// The current location within the file for data to be written.
    pub current_position: u64,
    /// The root of the B-Tree.
    pub root: Root,
    /// The maximum "order" of the B-Tree. This controls the maximum number of
    /// children any node in the tree may contain. Nebari will automatically
    /// scale up to this number as the database grows.
    pub max_order: Option<usize>,
}

impl<Root> ActiveState<Root>
where
    Root: super::Root,
{
    /// Returns true if the state has been initialized.
    pub fn initialized(&self) -> bool {
        self.root.initialized()
    }

    pub(crate) fn publish(&self, state: &State<Root>) {
        let mut reader = state.reader.write();
        *reader = Arc::new(self.clone());
    }

    pub(crate) fn rollback(&mut self, state: &State<Root>) {
        let reader = state.reader.read();
        self.root = reader.root.clone();
    }
}
