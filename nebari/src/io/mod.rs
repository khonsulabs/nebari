//! IO abstractions for Nebari.
//!
//! Nebari was written to have the flexibility of multiple backend options. This
//! may allow Nebari to target `no_std` in the future or allow for other IO
//! strategies to be implemented in addition to the ones seen here today.

use std::{
    borrow::Borrow,
    collections::HashSet,
    fmt::Debug,
    hash::Hash,
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
    fn id(&self) -> &PathId;

    /// Returns the length of the file.
    fn length(&self) -> Result<u64, Error>;

    /// Synchronizes data and metadata to the final destination. This calls
    /// [`std::fs::File::sync_all()`] on files, which ensures all filesystem
    /// metadata (such as newly allocated blocks) and data is synchronized to
    /// the destination device.
    fn synchronize(&mut self) -> Result<(), Error>;

    /// Safely closes the file after flushing any pending operations to disk.
    fn close(self) -> Result<(), Error>;
}

/// A type that can open managed files.
pub trait ManagedFileOpener<File>
where
    File: ManagedFile,
{
    /// Opens a file at `path` with read-only permission.
    fn open_for_read(&self, path: impl IntoPathId + Send) -> Result<File, Error>;

    /// Opens or creates a file at `path`, positioning the cursor at the end of the file.
    fn open_for_append(&self, path: impl IntoPathId + Send) -> Result<File, Error>;
}

/// A manager that is responsible for controlling write access to a file.
pub trait FileManager:
    ManagedFileOpener<Self::File> + Default + Clone + Debug + Send + Sync + 'static
{
    /// The type of file managed by this manager.
    type File: ManagedFile<Manager = Self>;
    /// A file handle type, which can have operations executed against it.
    type FileHandle: OpenableFile<Self::File> + OperableFile<Self::File>;

    /// Returns the `PathId` for the given path. If the file manager does not
    /// know of this path and `create_if_not_found` is false, None will be
    /// returned. Otherwise, a value will always be returned.
    ///
    /// Until a path is deleted, the same `PathId` will be returned for the same
    /// `Path`.
    fn resolve_path(&self, path: impl AsRef<Path>, create_if_not_found: bool) -> Option<PathId>;

    /// Returns a file handle that can be used for reading operations.
    fn read(&self, path: impl IntoPathId) -> Result<Self::FileHandle, Error>;

    /// Returns a file handle that can be used to read and write.
    fn append(&self, path: impl IntoPathId) -> Result<Self::FileHandle, Error>;

    /// Returns the length of the file.
    fn file_length(&self, path: impl IntoPathId) -> Result<u64, Error>;

    /// Check if the file exists.
    fn exists(&self, path: impl IntoPathId) -> Result<bool, Error>;

    /// Closes all open handles for `path`, and calls `publish_callback` before
    /// unlocking any locks aquired during the operation.
    fn close_handles<F: FnOnce(PathId)>(&self, path: impl IntoPathId, publish_callback: F);

    /// Check if the file exists.
    fn delete(&self, path: impl IntoPathId) -> Result<bool, Error>;

    /// Removes a directory and all of its contents.
    fn delete_directory(&self, path: impl AsRef<Path>) -> Result<(), Error>;
}

/// A file that can have operations performed on it.
pub trait OpenableFile<F: ManagedFile>: Debug + Sized + Send + Sync {
    /// Returns the id of the file assigned from the file manager.
    fn id(&self) -> &PathId;

    /// Replaces the current file with the file located at `path` atomically.
    fn replace_with<C: FnOnce(PathId)>(
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

/// A unique ID for a path.
#[derive(Clone, Debug)]
pub struct PathId {
    id: Option<u64>,
    path: Arc<PathBuf>,
}

impl PathId {
    /// Returns the id of the path, if present.
    #[must_use]
    pub const fn id(&self) -> Option<u64> {
        self.id
    }

    /// Returns the original path of this ID.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Hash for PathId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl Eq for PathId {}

impl PartialEq for PathId {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id || self.path == other.path
    }
}

impl PartialEq<Path> for PathId {
    fn eq(&self, other: &Path) -> bool {
        &**self.path == other
    }
}

impl Borrow<Path> for PathId {
    fn borrow(&self) -> &Path {
        &self.path
    }
}

/// Resolves path-like types into the parts used by [`PathId`].
pub trait IntoPathId {
    /// Returns `self` as a `PathId`.
    fn into_path_id(self) -> PathId;
    /// Returns this type as a path.
    fn path(&self) -> &Path;
    /// Returns the id of this path, or None if not available.
    fn id(&self) -> Option<u64>;
}

impl<'a> IntoPathId for &'a Path {
    fn into_path_id(self) -> PathId {
        PathId {
            id: None,
            path: Arc::new(self.to_path_buf()),
        }
    }

    fn id(&self) -> Option<u64> {
        None
    }

    fn path(&self) -> &Path {
        self
    }
}

impl<'a> IntoPathId for &'a PathBuf {
    fn into_path_id(self) -> PathId {
        PathId {
            id: None,
            path: Arc::new(self.clone()),
        }
    }

    fn id(&self) -> Option<u64> {
        None
    }

    fn path(&self) -> &Path {
        self
    }
}

impl IntoPathId for PathBuf {
    fn into_path_id(self) -> PathId {
        PathId {
            id: None,
            path: Arc::new(self),
        }
    }

    fn id(&self) -> Option<u64> {
        None
    }

    fn path(&self) -> &Path {
        self
    }
}

impl IntoPathId for Arc<PathBuf> {
    fn into_path_id(self) -> PathId {
        PathId {
            id: None,
            path: self,
        }
    }

    fn id(&self) -> Option<u64> {
        None
    }

    fn path(&self) -> &Path {
        self
    }
}

impl<'a> IntoPathId for &'a Arc<PathBuf> {
    fn into_path_id(self) -> PathId {
        PathId {
            id: None,
            path: self.clone(),
        }
    }

    fn id(&self) -> Option<u64> {
        None
    }

    fn path(&self) -> &Path {
        self
    }
}

impl IntoPathId for PathId {
    fn into_path_id(self) -> PathId {
        self
    }

    fn id(&self) -> Option<u64> {
        self.id
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl<'a> IntoPathId for &'a PathId {
    fn into_path_id(self) -> PathId {
        self.clone()
    }

    fn id(&self) -> Option<u64> {
        self.id
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl IntoPathId for String {
    fn into_path_id(self) -> PathId {
        PathId {
            id: None,
            path: Arc::new(PathBuf::from(self)),
        }
    }

    fn id(&self) -> Option<u64> {
        None
    }

    fn path(&self) -> &Path {
        Path::new(self)
    }
}

impl<'a> IntoPathId for &'a str {
    fn into_path_id(self) -> PathId {
        PathId {
            id: None,
            path: Arc::new(PathBuf::from(self)),
        }
    }

    fn id(&self) -> Option<u64> {
        None
    }

    fn path(&self) -> &Path {
        Path::new(self)
    }
}

/// Converts between paths and unique IDs.
#[derive(Default, Clone, Debug)]
pub struct PathIds {
    file_id_counter: Arc<AtomicU64>,
    file_ids: Arc<RwLock<HashSet<PathId>>>,
}

impl PathIds {
    fn file_id_for_path(&self, path: impl IntoPathId, insert_if_not_found: bool) -> Option<PathId> {
        let file_ids = self.file_ids.upgradable_read();
        let path = path.into_path_id();
        if let Some(id) = file_ids.get(&path) {
            Some(id.clone())
        } else if insert_if_not_found {
            let mut file_ids = RwLockUpgradableReadGuard::upgrade(file_ids);
            // Assume that in the optimal flow, multiple threads aren't asking
            // to open the same path for the first time.
            let new_id = PathId {
                path: path.path.clone(),
                id: Some(self.file_id_counter.fetch_add(1, Ordering::SeqCst)),
            };
            if file_ids.insert(new_id.clone()) {
                Some(new_id)
            } else {
                file_ids.get(&path).cloned()
            }
        } else {
            None
        }
    }

    fn remove_file_id_for_path(&self, path: impl IntoPathId) -> Option<PathId> {
        let mut file_ids = self.file_ids.write();
        if path.id().is_some() {
            file_ids.take(&path.into_path_id())
        } else {
            file_ids.take(path.path())
        }
    }

    fn recreate_file_id_for_path(&self, path: impl IntoPathId) -> Option<RecreatedFile<'_>> {
        let existing = path.into_path_id();
        let mut file_ids = self.file_ids.write();
        let new_id = self.file_id_counter.fetch_add(1, Ordering::SeqCst);
        let new_id = PathId {
            path: existing.path,
            id: Some(new_id),
        };
        file_ids
            .replace(new_id.clone())
            .map(|old_id| RecreatedFile {
                previous_id: old_id,
                new_id,
                _guard: file_ids,
            })
    }

    fn remove_file_ids_for_path_prefix(&self, path: &Path) -> Vec<PathId> {
        let mut file_ids = self.file_ids.write();
        let mut ids_to_remove = Vec::new();
        let mut paths_to_remove = Vec::new();
        for id in file_ids.iter() {
            if id.path().starts_with(path) {
                paths_to_remove.push(id.clone());
                ids_to_remove.push(id.clone());
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
#[derive(Debug)]
pub struct RecreatedFile<'a> {
    /// The file's previous id.
    pub previous_id: PathId,
    /// The file's new id.
    pub new_id: PathId,
    _guard: RwLockWriteGuard<'a, HashSet<PathId>>,
}
