use std::{
    collections::HashMap,
    convert::TryFrom,
    io::{self, SeekFrom},
    ops::Neg,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
};

use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};

use super::{FileManager, FileOp, ManagedFile, OpenableFile};
use crate::Error;

/// A fake "file" represented by an in-memory buffer. This should only be used
/// in testing, as this database format is not optimized for memory efficiency.
#[derive(Debug)]
pub struct MemoryFile {
    id: Option<u64>,
    buffer: Arc<RwLock<Vec<u8>>>,
    position: usize,
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

#[allow(clippy::cast_possible_truncation)]
impl ManagedFile for MemoryFile {
    type Manager = MemoryFileManager;
    fn id(&self) -> Option<u64> {
        self.id
    }

    fn open_for_read(
        path: impl AsRef<std::path::Path> + Send,
        id: Option<u64>,
    ) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            id,
            buffer: lookup_buffer(path, true).unwrap(),
            position: 0,
        })
    }

    fn open_for_append(
        path: impl AsRef<std::path::Path> + Send,
        id: Option<u64>,
    ) -> Result<Self, Error> {
        let path = path.as_ref();
        let buffer = lookup_buffer(path, true).unwrap();
        let position = {
            let buffer = buffer.read();
            buffer.len()
        };
        Ok(Self {
            id,
            buffer,
            position,
        })
    }

    fn length(&self) -> Result<u64, Error> {
        let file_buffer = self.buffer.read();
        Ok(file_buffer.len() as u64)
    }

    fn close(self) -> Result<(), Error> {
        Ok(())
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
                Error::message("read requested more bytes than available"),
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
    file_id_counter: Arc<AtomicU64>,
    file_ids: Arc<RwLock<HashMap<PathBuf, u64>>>,
    open_files: Arc<Mutex<HashMap<u64, Arc<Mutex<MemoryFile>>>>>,
}

impl MemoryFileManager {
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

    fn lookup_file(
        &self,
        path: impl AsRef<Path>,
        create_if_needed: bool,
        id: u64,
    ) -> Result<Option<Arc<Mutex<MemoryFile>>>, Error> {
        let path = path.as_ref();
        let mut open_files = self.open_files.lock();
        if let Some(open_file) = open_files.get(&id) {
            Ok(Some(open_file.clone()))
        } else if create_if_needed {
            let file = Arc::new(Mutex::new(MemoryFile::open_for_append(path, Some(id))?));
            open_files.insert(id, file.clone());
            Ok(Some(file))
        } else {
            Ok(None)
        }
    }
}

impl FileManager for MemoryFileManager {
    type File = MemoryFile;
    type FileHandle = OpenMemoryFile;

    fn append(&self, path: impl AsRef<Path> + Send) -> Result<Self::FileHandle, Error> {
        let path = path.as_ref();
        let id = self.file_id_for_path(path);
        self.lookup_file(path, true, id)
            .map(|file| OpenMemoryFile(file.unwrap()))
    }

    fn read(&self, path: impl AsRef<Path> + Send) -> Result<Self::FileHandle, Error> {
        self.append(path)
    }

    fn file_length(&self, path: impl AsRef<Path> + Send) -> Result<u64, Error> {
        let file = self.lookup_file(path, false, 0)?.ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                Error::message("not found"),
            ))
        })?;
        let file = file.lock();
        let buffer = file.buffer.read();
        Ok(buffer.len() as u64)
    }

    fn exists(&self, path: impl AsRef<Path> + Send) -> Result<bool, Error> {
        Ok(self.lookup_file(path, false, 0)?.is_some())
    }

    fn delete(&self, path: impl AsRef<Path> + Send) -> Result<bool, Error> {
        let path = path.as_ref();
        let id = self.file_id_for_path(path);
        let mut open_files = self.open_files.lock();
        Ok(open_files.remove(&id).is_some())
    }

    fn delete_directory(&self, path: impl AsRef<Path> + Send) -> Result<(), Error> {
        let path = path.as_ref();
        let removed_ids = self.remove_file_ids_for_path_prefix(path);
        let mut open_files = self.open_files.lock();
        for id in removed_ids {
            open_files.remove(&id);
        }

        Ok(())
    }
}

/// An open [`MemoryFile`] that is owned by a [`MemoryFileManager`].
pub struct OpenMemoryFile(Arc<Mutex<MemoryFile>>);

impl OpenableFile<MemoryFile> for OpenMemoryFile {
    fn execute<W: FileOp<MemoryFile>>(&mut self, mut writer: W) -> W::Output {
        let mut file = self.0.lock();
        writer.execute(&mut file)
    }

    fn close(self) -> Result<(), Error> {
        drop(self);
        Ok(())
    }
}
