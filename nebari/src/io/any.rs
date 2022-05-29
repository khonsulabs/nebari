use std::{
    io::{Read, Seek, Write},
    path::Path,
};

use crate::io::{
    fs::{OpenStdFile, StdFile, StdFileManager},
    memory::{MemoryFile, MemoryFileManager, OpenMemoryFile},
    FileManager, FileOp, IntoPathId, ManagedFile, ManagedFileOpener, OpenableFile, OperableFile,
    PathId,
};

/// A file that can be either a [`StdFile`] or [`MemoryFile`].
#[derive(Debug)]
pub enum AnyFile {
    /// A file backed by a filesystem.
    Std(StdFile),
    /// A simulated file backed by memory.
    Memory(MemoryFile),
}

impl ManagedFile for AnyFile {
    type Manager = AnyFileManager;
}

impl super::File for AnyFile {
    fn id(&self) -> &PathId {
        match self {
            Self::Std(file) => file.id(),
            Self::Memory(file) => file.id(),
        }
    }

    fn length(&self) -> Result<u64, crate::Error> {
        match self {
            Self::Std(file) => file.length(),
            Self::Memory(file) => file.length(),
        }
    }

    fn close(self) -> Result<(), crate::Error> {
        match self {
            Self::Std(file) => file.close(),
            Self::Memory(file) => file.close(),
        }
    }

    fn synchronize(&mut self) -> Result<(), crate::Error> {
        match self {
            Self::Std(file) => file.synchronize(),
            Self::Memory(file) => file.synchronize(),
        }
    }
}

impl Write for AnyFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Std(file) => file.write(buf),
            Self::Memory(file) => file.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Std(file) => file.flush(),
            Self::Memory(file) => file.flush(),
        }
    }
}

impl Read for AnyFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Std(file) => file.read(buf),
            Self::Memory(file) => file.read(buf),
        }
    }
}

impl Seek for AnyFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::Std(file) => file.seek(pos),
            Self::Memory(file) => file.seek(pos),
        }
    }
}

/// A file manager that can either be a [`StdFileManager`] or a [`MemoryFileManager`].
#[derive(Debug, Clone)]
pub enum AnyFileManager {
    /// A file manager that uses the filesystem.
    Std(StdFileManager),
    /// A simulated file system backed by memory.
    Memory(MemoryFileManager),
}

impl AnyFileManager {
    /// Returns a new filesystem-backed manager.
    #[must_use]
    pub fn std() -> Self {
        Self::Std(StdFileManager::default())
    }

    /// Returns a new memory-backed manager.
    #[must_use]
    pub fn memory() -> Self {
        Self::Memory(MemoryFileManager::default())
    }
}

impl FileManager for AnyFileManager {
    type File = AnyFile;
    type FileHandle = AnyFileHandle;

    fn resolve_path(&self, path: impl AsRef<Path>, create_if_not_found: bool) -> Option<PathId> {
        match self {
            Self::Std(manager) => manager.resolve_path(path, create_if_not_found),
            Self::Memory(manager) => manager.resolve_path(path, create_if_not_found),
        }
    }

    fn read(&self, path: impl IntoPathId) -> Result<Self::FileHandle, crate::Error> {
        match self {
            Self::Std(manager) => manager.read(path).map(AnyFileHandle::Std),
            Self::Memory(manager) => manager.read(path).map(AnyFileHandle::Memory),
        }
    }

    fn append(&self, path: impl IntoPathId) -> Result<Self::FileHandle, crate::Error> {
        match self {
            Self::Std(manager) => manager.append(path).map(AnyFileHandle::Std),
            Self::Memory(manager) => manager.append(path).map(AnyFileHandle::Memory),
        }
    }

    fn close_handles<F: FnOnce(PathId)>(&self, path: impl IntoPathId, publish_callback: F) {
        match self {
            Self::Std(manager) => manager.close_handles(path, publish_callback),
            Self::Memory(manager) => manager.close_handles(path, publish_callback),
        }
    }

    fn delete(&self, path: impl IntoPathId) -> Result<bool, crate::Error> {
        match self {
            Self::Std(manager) => manager.delete(path),
            Self::Memory(manager) => manager.delete(path),
        }
    }

    fn delete_directory(&self, path: impl AsRef<Path>) -> Result<(), crate::Error> {
        match self {
            Self::Std(manager) => manager.delete_directory(path),
            Self::Memory(manager) => manager.delete_directory(path),
        }
    }

    fn exists(&self, path: impl IntoPathId) -> Result<bool, crate::Error> {
        match self {
            Self::Std(manager) => manager.exists(path),
            Self::Memory(manager) => manager.exists(path),
        }
    }

    fn file_length(&self, path: impl IntoPathId) -> Result<u64, crate::Error> {
        match self {
            Self::Std(manager) => manager.file_length(path),
            Self::Memory(manager) => manager.file_length(path),
        }
    }
}

impl ManagedFileOpener<AnyFile> for AnyFileManager {
    fn open_for_read(&self, path: impl IntoPathId + Send) -> Result<AnyFile, crate::Error> {
        match self {
            AnyFileManager::Std(manager) => manager.open_for_read(path).map(AnyFile::Std),
            AnyFileManager::Memory(manager) => manager.open_for_read(path).map(AnyFile::Memory),
        }
    }

    fn open_for_append(&self, path: impl IntoPathId + Send) -> Result<AnyFile, crate::Error> {
        match self {
            AnyFileManager::Std(manager) => manager.open_for_append(path).map(AnyFile::Std),
            AnyFileManager::Memory(manager) => manager.open_for_append(path).map(AnyFile::Memory),
        }
    }
}

impl Default for AnyFileManager {
    fn default() -> Self {
        Self::Std(StdFileManager::default())
    }
}

/// A handle to an open file that could be either an [`OpenStdFile`] or an [`OpenMemoryFile`].
#[derive(Debug)]
pub enum AnyFileHandle {
    /// An open file on the filesystem.
    Std(OpenStdFile),
    /// An open file in memory.
    Memory(OpenMemoryFile),
}

impl OpenableFile<AnyFile> for AnyFileHandle {
    fn id(&self) -> &PathId {
        match self {
            AnyFileHandle::Std(file) => file.id(),
            AnyFileHandle::Memory(file) => file.id(),
        }
    }

    fn replace_with<C: FnOnce(PathId)>(
        self,
        replacement: AnyFile,
        manager: &<AnyFile as ManagedFile>::Manager,
        publish_callback: C,
    ) -> Result<Self, crate::Error> {
        match (self, replacement, manager) {
            (AnyFileHandle::Std(file), AnyFile::Std(replacement), AnyFileManager::Std(manager)) => {
                file.replace_with(replacement, manager, publish_callback)
                    .map(AnyFileHandle::Std)
            }
            (
                AnyFileHandle::Memory(file),
                AnyFile::Memory(replacement),
                AnyFileManager::Memory(manager),
            ) => file
                .replace_with(replacement, manager, publish_callback)
                .map(AnyFileHandle::Memory),
            _ => Err(crate::Error::from("incompatible file and manager")),
        }
    }

    fn close(self) -> Result<(), crate::Error> {
        match self {
            AnyFileHandle::Std(file) => file.close(),
            AnyFileHandle::Memory(file) => file.close(),
        }
    }
}

impl OperableFile<AnyFile> for AnyFileHandle {
    fn execute<Output, Op: FileOp<Output>>(&mut self, operator: Op) -> Output {
        match self {
            AnyFileHandle::Std(file) => file.execute(operator),
            AnyFileHandle::Memory(file) => file.execute(operator),
        }
    }
}
