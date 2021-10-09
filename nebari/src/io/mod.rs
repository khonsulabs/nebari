//! IO abstractions for Nebari.
//!
//! Nebari was written to have the flexibility of multiple backend options. This
//! may allow Nebari to target `no_std` in the future or allow for other IO
//! strategies to be implemented in addition to the ones seen here today.

use std::{
    collections::HashMap,
    fmt::Debug,
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};

use crate::error::Error;

/// Filesystem IO provided by `std::fs`.
pub mod fs;
/// A virtual memory-based filesystem.
pub mod memory;

/// A file that can be interacted with using async operations.
///
/// This trait is an abstraction that mimics `tokio-uring`'s File type, allowing
/// for a non-uring implementation to be provided as well. This is why the
/// read/write APIs take ownership of the buffer -- to satisfy the requirements
/// of tokio-uring.
pub trait ManagedFile: Debug + Send + Sync + Seek + Read + Write + Sized + 'static {
    /// The file manager that synchronizes file access across threads.
    type Manager: FileManager<File = Self>;

    /// Returns the unique ID of this file. Only unique within [`Self::Manager`].
    fn id(&self) -> Option<u64>;

    /// Returns the path to the file.
    fn path(&self) -> &Path;

    /// Opens a file at `path` with read-only permission.
    fn open_for_read(path: impl AsRef<Path> + Send, id: Option<u64>) -> Result<Self, Error>;
    /// Opens or creates a file at `path`, positioning the cursor at the end of the file.
    fn open_for_append(path: impl AsRef<Path> + Send, id: Option<u64>) -> Result<Self, Error>;

    /// Returns the length of the file.
    fn length(&self) -> Result<u64, Error>;

    /// Safely closes the file after flushing any pending operations to disk.
    fn close(self) -> Result<(), Error>;
}

/// A manager that is responsible for controlling write access to a file.
pub trait FileManager: Send + Sync + Clone + Default + Debug + 'static {
    /// The [`ManagedFile`] that this manager is for.
    type File: ManagedFile<Manager = Self>;
    /// A file handle type, which can have operations executed against it.
    type FileHandle: OpenableFile<Self::File>;

    /// Returns a file handle that can be used for reading operations.
    fn read(&self, path: impl AsRef<Path>) -> Result<Self::FileHandle, Error>;

    /// Returns a file handle that can be used to read and write.
    fn append(&self, path: impl AsRef<Path>) -> Result<Self::FileHandle, Error>;

    /// Returns the length of the file.
    fn file_length(&self, path: impl AsRef<Path>) -> Result<u64, Error> {
        path.as_ref()
            .metadata()
            .map_err(Error::from)
            .map(|metadata| metadata.len())
    }

    /// Check if the file exists.
    fn exists(&self, path: impl AsRef<Path>) -> Result<bool, Error> {
        Ok(path.as_ref().exists())
    }

    /// Closes all open handles for `path`, and calls `publish_callback` before
    /// unlocking any locks aquired during the operation.
    fn close_handles<F: FnOnce(u64)>(&self, path: impl AsRef<Path>, publish_callback: F);

    /// Check if the file exists.
    fn delete(&self, path: impl AsRef<Path>) -> Result<bool, Error>;

    /// Removes a directory and all of its contents.
    fn delete_directory(&self, path: impl AsRef<Path>) -> Result<(), Error>;
}

/// A file that can have operations performed on it.
pub trait OpenableFile<F: ManagedFile>: Debug + Sized + Send + Sync {
    /// Returns the id of the file assigned from the file manager.
    fn id(&self) -> Option<u64>;

    /// Executes an operation.
    fn execute<W: FileOp<F>>(&mut self, operator: W) -> W::Output;

    /// Replaces the current file with the file located at `path` atomically.
    fn replace_with<C: FnOnce(u64)>(
        self,
        path: &Path,
        manager: &F::Manager,
        publish_callback: C,
    ) -> Result<Self, Error>;

    /// Closes the file. This may not actually close the underlying file,
    /// depending on what other tasks have access to the underlying file as
    /// well.
    fn close(self) -> Result<(), Error>;
}

/// An operation to perform on a file.
pub trait FileOp<F: ManagedFile> {
    /// The output type of the operation.
    type Output;

    /// Executes the operation and returns the result.
    fn execute(&mut self, file: &mut F) -> Self::Output;
}

/// Converts between paths and unique IDs.
#[derive(Default, Clone, Debug)]
pub struct PathIds {
    file_id_counter: Arc<AtomicU64>,
    file_ids: Arc<RwLock<HashMap<PathBuf, u64>>>,
}

impl PathIds {
    fn file_id_for_path(&self, path: &Path) -> u64 {
        let file_ids = self.file_ids.upgradable_read();
        if let Some(id) = file_ids.get(path) {
            *id
        } else {
            let mut file_ids = RwLockUpgradableReadGuard::upgrade(file_ids);
            *file_ids
                .entry(path.to_path_buf())
                .or_insert_with(|| self.file_id_counter.fetch_add(1, Ordering::SeqCst))
        }
    }

    fn remove_file_id_for_path(&self, path: &Path) -> Option<u64> {
        let mut file_ids = self.file_ids.write();
        file_ids.remove(path)
    }

    fn recreate_file_id_for_path(&self, path: &Path) -> Option<RecreatedFile<'_>> {
        let mut file_ids = self.file_ids.write();
        let new_id = self.file_id_counter.fetch_add(1, Ordering::SeqCst);
        file_ids
            .insert(path.to_path_buf(), new_id)
            .map(|old_id| RecreatedFile {
                previous_id: old_id,
                new_id,
                _guard: file_ids,
            })
    }

    fn remove_file_ids_for_path_prefix(&self, path: &Path) -> Vec<u64> {
        let mut file_ids = self.file_ids.write();
        let mut ids_to_remove = Vec::new();
        let mut paths_to_remove = Vec::new();
        for (file, id) in file_ids.iter() {
            if file.starts_with(path) {
                paths_to_remove.push(file.clone());
                ids_to_remove.push(*id);
            }
        }

        for path in paths_to_remove {
            file_ids.remove(&path);
        }

        ids_to_remove
    }
}

/// A file that has had its contents replaced. While this value exists, all
/// other threads will be blocked from interacting with the [`PathIds`]
/// structure. Only hold onto this value for short periods of time.
pub struct RecreatedFile<'a> {
    /// The file's previous id.
    pub previous_id: u64,
    /// The file's new id.
    pub new_id: u64,
    _guard: RwLockWriteGuard<'a, HashMap<PathBuf, u64>>,
}
