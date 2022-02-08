use std::{
    collections::HashMap,
    convert::TryFrom,
    io::{self, SeekFrom},
    ops::Neg,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};

use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};

use super::{FileManager, FileOp, ManagedFile, OpenableFile};
use crate::{
    error::Error,
    io::{File, ManagedFileOpener, OperableFile, PathIds},
    ErrorKind,
};

type FileBuffer = Arc<RwLock<Vec<u8>>>;

/// A fake "file" represented by an in-memory buffer. This should only be used
/// in testing, as this database format is not optimized for memory efficiency.
#[derive(Clone)]
pub struct MemoryFile {
    id: Option<u64>,
    path: PathBuf,
    buffer: FileBuffer,
    position: usize,
}

impl std::fmt::Debug for MemoryFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let buffer = self.buffer.read();
        f.debug_struct("MemoryFile")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("buffer", &buffer.len())
            .field("position", &self.position)
            .finish()
    }
}

type OpenBuffers = Arc<Mutex<HashMap<PathBuf, Weak<RwLock<Vec<u8>>>>>>;
static OPEN_BUFFERS: Lazy<OpenBuffers> = Lazy::new(Arc::default);

#[allow(clippy::needless_pass_by_value)]
fn lookup_buffer(
    path: impl AsRef<std::path::Path> + Send,
    create_if_not_found: bool,
) -> Option<Arc<RwLock<Vec<u8>>>> {
    let mut open_buffers = OPEN_BUFFERS.lock();
    if let Some(existing_buffer) = open_buffers.get(path.as_ref()).and_then(Weak::upgrade) {
        Some(existing_buffer)
    } else if create_if_not_found {
        let new_buffer = Arc::default();
        open_buffers.insert(path.as_ref().to_path_buf(), Arc::downgrade(&new_buffer));
        Some(new_buffer)
    } else {
        None
    }
}

impl ManagedFile for MemoryFile {
    type Manager = MemoryFileManager;
}

#[allow(clippy::cast_possible_truncation)]
impl super::File for MemoryFile {
    fn id(&self) -> Option<u64> {
        self.id
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn length(&self) -> Result<u64, Error> {
        let file_buffer = self.buffer.read();
        Ok(file_buffer.len() as u64)
    }

    fn close(self) -> Result<(), Error> {
        Ok(())
    }
}

/// A [`ManagedFileOpener`] implementation that produces [`MemoryFile`]s.
pub struct MemoryFileOpener;

impl ManagedFileOpener<MemoryFile> for MemoryFileOpener {
    fn open_for_read(
        &self,
        path: impl AsRef<std::path::Path> + Send,
        id: Option<u64>,
    ) -> Result<MemoryFile, Error> {
        let path = path.as_ref();
        Ok(MemoryFile {
            id,
            path: path.to_path_buf(),
            buffer: lookup_buffer(path, true).unwrap(),
            position: 0,
        })
    }

    fn open_for_append(
        &self,
        path: impl AsRef<std::path::Path> + Send,
        id: Option<u64>,
    ) -> Result<MemoryFile, Error> {
        let path = path.as_ref();
        let buffer = lookup_buffer(path, true).unwrap();
        let position = {
            let buffer = buffer.read();
            buffer.len()
        };
        Ok(MemoryFile {
            id,
            buffer,
            position,
            path: path.to_path_buf(),
        })
    }
}

impl std::io::Seek for MemoryFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(position) => self.position = usize::try_from(position).unwrap(),
            SeekFrom::End(from_end) => {
                let buffer = self.buffer.read();
                self.position = if from_end.is_positive() {
                    buffer.len()
                } else {
                    buffer.len() - usize::try_from(from_end.neg()).unwrap()
                };
            }
            SeekFrom::Current(relative) => {
                self.position = if relative.is_positive() {
                    self.position + usize::try_from(relative).unwrap()
                } else {
                    self.position - usize::try_from(relative.neg()).unwrap()
                }
            }
        }
        Ok(self.position as u64)
    }
}

impl std::io::Read for MemoryFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let file_buffer = self.buffer.read();

        let read_end = self.position as usize + buffer.len();
        if read_end > file_buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                ErrorKind::message("read requested more bytes than available"),
            ));
        }

        buffer.copy_from_slice(&file_buffer[self.position..read_end]);
        self.position = read_end;

        Ok(buffer.len())
    }
}

impl std::io::Write for MemoryFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut file_buffer = self.buffer.write();

        file_buffer.extend_from_slice(buf);
        self.position += buf.len();

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The [`FileManager`] implementation for [`MemoryFile`]. Simulates a
/// persistent in-memory filesystem.
#[derive(Debug, Default, Clone)]
pub struct MemoryFileManager {
    file_ids: PathIds,
    open_files: Arc<Mutex<HashMap<u64, FileBuffer>>>,
}

impl MemoryFileManager {
    fn lookup_file(
        &self,
        path: impl AsRef<Path>,
        create_if_needed: bool,
        id: Option<u64>,
    ) -> Result<Option<MemoryFile>, Error> {
        let path = path.as_ref();
        let id = match id {
            Some(id) => id,
            None => match self.file_ids.file_id_for_path(path, create_if_needed) {
                Some(id) => id,
                None => return Ok(None),
            },
        };
        let mut open_files = self.open_files.lock();
        if let Some(open_file) = open_files.get(&id) {
            Ok(Some(MemoryFile {
                id: Some(id),
                buffer: open_file.clone(),
                path: path.to_path_buf(),
                position: 0,
            }))
        } else if create_if_needed {
            let file = MemoryFileOpener.open_for_append(path, Some(id))?;
            open_files.insert(id, file.buffer.clone());
            Ok(Some(file))
        } else {
            Ok(None)
        }
    }

    fn forget_file(&self, path: &Path) -> bool {
        if let Some(id) = self.file_ids.remove_file_id_for_path(path) {
            let mut open_files = self.open_files.lock();
            open_files.remove(&id).is_some()
        } else {
            false
        }
    }
}

impl FileManager for MemoryFileManager {
    type File = MemoryFile;
    type FileHandle = OpenMemoryFile;

    fn append(&self, path: impl AsRef<Path>) -> Result<Self::FileHandle, Error> {
        let path = path.as_ref();
        let id = self.file_ids.file_id_for_path(path, true);
        self.lookup_file(path, true, id).map(|file| OpenMemoryFile {
            file: file.unwrap(),
            manager: self.clone(),
        })
    }

    fn read(&self, path: impl AsRef<Path>) -> Result<Self::FileHandle, Error> {
        self.append(path)
    }

    fn file_length(&self, path: impl AsRef<Path>) -> Result<u64, Error> {
        let file = self.lookup_file(path, false, None)?.ok_or_else(|| {
            ErrorKind::Io(io::Error::new(
                io::ErrorKind::NotFound,
                ErrorKind::message("not found"),
            ))
        })?;
        file.length()
    }

    fn exists(&self, path: impl AsRef<Path>) -> Result<bool, Error> {
        Ok(self.lookup_file(path, false, None)?.is_some())
    }

    fn delete(&self, path: impl AsRef<Path>) -> Result<bool, Error> {
        let path = path.as_ref();
        {
            let mut open_buffers = OPEN_BUFFERS.lock();
            open_buffers.remove(path);
        }
        Ok(self.forget_file(path))
    }

    fn delete_directory(&self, path: impl AsRef<Path>) -> Result<(), Error> {
        let path = path.as_ref();
        let removed_ids = self.file_ids.remove_file_ids_for_path_prefix(path);
        let mut open_files = self.open_files.lock();
        for id in removed_ids {
            open_files.remove(&id);
        }

        Ok(())
    }

    fn close_handles<F: FnOnce(u64)>(&self, path: impl AsRef<Path>, publish_callback: F) {
        let path = path.as_ref();
        self.forget_file(path);
        let new_id = self.file_ids.file_id_for_path(path, true).unwrap();
        publish_callback(new_id);
    }
}

impl ManagedFileOpener<MemoryFile> for MemoryFileManager {
    fn open_for_read(
        &self,
        path: impl AsRef<Path> + Send,
        id: Option<u64>,
    ) -> Result<MemoryFile, Error> {
        MemoryFileOpener.open_for_read(path, id)
    }

    fn open_for_append(
        &self,
        path: impl AsRef<Path> + Send,
        id: Option<u64>,
    ) -> Result<MemoryFile, Error> {
        MemoryFileOpener.open_for_append(path, id)
    }
}

/// An open [`MemoryFile`] that is owned by a [`MemoryFileManager`].
#[derive(Debug)]
pub struct OpenMemoryFile {
    file: MemoryFile,
    manager: MemoryFileManager,
}

impl OpenableFile<MemoryFile> for OpenMemoryFile {
    fn id(&self) -> Option<u64> {
        self.file.id
    }

    fn replace_with<C: FnOnce(u64)>(
        self,
        replacement: MemoryFile,
        manager: &MemoryFileManager,
        publish_callback: C,
    ) -> Result<Self, Error> {
        let weak_buffer = Arc::downgrade(&replacement.buffer);
        drop(self.manager.delete(replacement.path()));
        {
            let mut open_buffers = OPEN_BUFFERS.lock();
            open_buffers.insert(self.file.path.clone(), weak_buffer);
        }

        manager.close_handles(&self.file.path, publish_callback);

        let new_file = manager.append(&self.file.path)?;
        {
            assert!(Arc::ptr_eq(&new_file.file.buffer, &replacement.buffer));
        }
        Ok(new_file)
    }

    fn close(self) -> Result<(), Error> {
        drop(self);
        Ok(())
    }
}

impl OperableFile<MemoryFile> for OpenMemoryFile {
    fn execute<Output, Op: FileOp<Output>>(&mut self, operator: Op) -> Output {
        operator.execute(&mut self.file)
    }
}
