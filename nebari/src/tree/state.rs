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
    pub fn initialized() -> Self {
        let state = Self::default();
        {
            let mut state = state.lock();
            state.header.initialize_default();
        }
        state
    }

    /// Locks the state.
    pub(crate) fn lock(&self) -> MutexGuard<'_, ActiveState<Root>> {
        self.writer.lock()
    }

    /// Locks the state.
    pub(crate) fn read(&self) -> RwLockReadGuard<'_, ActiveState<Root>> {
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

#[derive(Clone, Debug, Default)]
pub struct ActiveState<Root: super::Root> {
    pub current_position: u64,
    pub header: Root,
}

impl<Root> ActiveState<Root>
where
    Root: super::Root,
{
    pub fn initialized(&self) -> bool {
        self.header.initialized()
    }

    pub(crate) fn publish(&self, state: &State<Root>) {
        let mut reader = state.reader.write();
        *reader = self.clone();
    }

    pub(crate) fn rollback(&mut self, state: &State<Root>) {
        let reader = state.reader.read();
        self.header = reader.header.clone();
    }
}
