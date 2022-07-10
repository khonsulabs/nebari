use std::sync::Arc;

use sediment::io;

use crate::{vault::AnyVault, ChunkCache, Vault};

/// A shared environment for database operations.
#[derive(Default, Debug, Clone)]
#[must_use]
pub struct Context<M: io::FileManager> {
    /// The file manager for the [`ManagedFile`](crate::io::ManagedFile) implementor.
    pub file_manager: M,
    /// The optional vault in use.
    pub(crate) vault: Option<Arc<dyn AnyVault>>,
    /// The optional chunk cache to use.
    pub(crate) cache: Option<ChunkCache>,
}

impl<M: io::FileManager> Context<M> {
    /// Returns the vault as a dynamic reference.
    pub fn vault(&self) -> Option<&dyn AnyVault> {
        self.vault.as_deref()
    }

    /// Returns the context's chunk cache.
    pub const fn cache(&self) -> Option<&ChunkCache> {
        self.cache.as_ref()
    }

    /// Replaces the cache currently set with `cache`.
    #[allow(clippy::missing_const_for_fn)] // destructors
    pub fn with_cache(mut self, cache: ChunkCache) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Replaces the vault currently set with `vault`.
    pub fn with_vault<V: Vault>(self, vault: V) -> Self {
        self.with_any_vault(Arc::new(vault))
    }

    /// Replaces the vault currently set with `vault`.
    pub fn with_any_vault(mut self, vault: Arc<dyn AnyVault>) -> Self {
        self.vault = Some(vault);
        self
    }
}
