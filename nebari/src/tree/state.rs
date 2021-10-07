use std::{fmt::Debug, sync::Arc};

use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard};

use crate::chunk_cache::AnySendSync;

/// The current state of a tree file. Must be initialized before passing to
/// `TreeFile::new` if the file already exists.
#[derive(Default, Clone, Debug)]
#[must_use]
pub struct State<Root: super::Root> {
    reader: Arc<RwLock<ActiveState<Root>>>,
    writer: Arc<Mutex<ActiveState<Root>>>,
}

impl<Root> State<Root>
where
    Root: super::Root,
{
    /// Returns an initialized state. This should only be used if you're
    /// creating a file from scratch.
    pub fn initialized(file_id: Option<u64>) -> Self {
        let mut header = Root::default();
        header.initialize_default();
        let state = ActiveState {
            file_id,
            current_position: 0,
            root: header,
        };

        Self {
            reader: Arc::new(RwLock::new(state.clone())),
            writer: Arc::new(Mutex::new(state)),
        }
    }

    /// Locks the state for writing.
    pub(crate) fn lock(&self) -> MutexGuard<'_, ActiveState<Root>> {
        self.writer.lock()
    }

    /// Reads the current state. Holding onto the returned value will block
    /// writers from publishing new changes. If you plan on needing access to
    /// the data for an extended period, clone the state and drop the guard.
    ///
    /// Be aware that by not holding the lock, database compaction could execute
    /// in the background and cause the `file_id` to no longer match the file
    /// handle your thread is working with. This will result in
    /// [`ErrorKind::TreeCompacted`](crate::ErrorKind::TreeCompacted) being
    /// raised from any operation on the file.
    pub fn read(&self) -> RwLockReadGuard<'_, ActiveState<Root>> {
        self.reader.read()
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
        *reader = self.clone();
    }

    pub(crate) fn rollback(&mut self, state: &State<Root>) {
        let reader = state.reader.read();
        self.root = reader.root.clone();
    }
}
