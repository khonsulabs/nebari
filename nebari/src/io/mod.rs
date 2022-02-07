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

/// A wrapper type for any file type.
pub mod any;
/// Filesystem IO provided by `std::fs`.
pub mod fs;
/// A virtual memory-based filesystem.
pub mod memory;

/// A file that is managed by a [`FileManager`].
pub trait ManagedFile: File {
    /// The file manager that synchronizes file access across threads.
    type Manager: FileManager<File = Self>;
}

/// A generic file trait.
pub trait File: Debug + Send + Sync + Seek + Read + Write + 'static {
    /// Returns the unique ID of this file. Only unique within the manager it
    /// was opened from.
    fn id(&self) -> Option<u64>;

    /// Returns the path to the file.
    fn path(&self) -> &Path;

    /// Returns the length of the file.
    fn length(&self) -> Result<u64, Error>;

    /// Safely closes the file after flushing any pending operations to disk.
    fn close(self) -> Result<(), Error>;
}

/// A type that can open managed files.
pub trait ManagedFileOpener<File>
where
    File: ManagedFile,
{
    /// Opens a file at `path` with read-only permission.
    fn open_for_read(&self, path: impl AsRef<Path> + Send, id: Option<u64>) -> Result<File, Error>;

    /// Opens or creates a file at `path`, positioning the cursor at the end of the file.
    fn open_for_append(
        &self,
        path: impl AsRef<Path> + Send,
        id: Option<u64>,
    ) -> Result<File, Error>;
}

/// A manager that is responsible for controlling write access to a file.
pub trait FileManager:
    ManagedFileOpener<Self::File> + Default + Clone + Debug + Send + Sync + 'static
{
    /// The type of file managed by this manager.
    type File: ManagedFile<Manager = Self>;
    /// A file handle type, which can have operations executed against it.
    type FileHandle: OpenableFile<Self::File> + OperableFile<Self::File>;

    /// Returns a file handle that can be used for reading operations.
    fn read(&self, path: impl AsRef<Path>) -> Result<Self::FileHandle, Error>;

    /// Returns a file handle that can be used to read and write.
    fn append(&self, path: impl AsRef<Path>) -> Result<Self::FileHandle, Error>;

    /// Returns the length of the file.
    fn file_length(&self, path: impl AsRef<Path>) -> Result<u64, Error>;

    /// Check if the file exists.
    fn exists(&self, path: impl AsRef<Path>) -> Result<bool, Error>;

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

    /// Replaces the current file with the file located at `path` atomically.
    fn replace_with<C: FnOnce(u64)>(
        self,
        replacement: F,
        manager: &F::Manager,
        publish_callback: C,
    ) -> Result<Self, Error>;

    /// Closes the file. This may not actually close the underlying file,
    /// depending on what other tasks have access to the underlying file as
    /// well.
    fn close(self) -> Result<(), Error>;
}

/// A file that can have an operation performed against it.
pub trait OperableFile<File>
where
    File: ManagedFile,
{
    /// Executes an operation and returns the results.
    fn execute<Output, Op: FileOp<Output>>(&mut self, operator: Op) -> Output;
}

/// An operation to perform on a file.
pub trait FileOp<Output> {
    /// Executes the operation and returns the result.
    fn execute(self, file: &mut dyn File) -> Output;
}

/// Converts between paths and unique IDs.
#[derive(Default, Clone, Debug)]
pub struct PathIds {
    file_id_counter: Arc<AtomicU64>,
    file_ids: Arc<RwLock<HashMap<PathBuf, u64>>>,
}

impl PathIds {
    fn file_id_for_path(&self, path: &Path, insert_if_not_found: bool) -> Option<u64> {
        let file_ids = self.file_ids.upgradable_read();
        if let Some(id) = file_ids.get(path) {
            Some(*id)
        } else if insert_if_not_found {
            let mut file_ids = RwLockUpgradableReadGuard::upgrade(file_ids);
            Some(
                *file_ids
                    .entry(path.to_path_buf())
                    .or_insert_with(|| self.file_id_counter.fetch_add(1, Ordering::SeqCst)),
            )
        } else {
            None
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
