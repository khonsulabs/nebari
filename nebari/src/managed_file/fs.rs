use std::{
    collections::{HashMap, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};

use super::{FileManager, FileOp, ManagedFile, OpenableFile};
use crate::Error;

/// An open file that uses [`std::fs`].
#[derive(Debug)]
pub struct StdFile {
    file: File,
    id: Option<u64>,
}

impl ManagedFile for StdFile {
    type Manager = StdFileManager;
    fn id(&self) -> Option<u64> {
        self.id
    }

    fn open_for_read(
        path: impl AsRef<std::path::Path> + Send,
        id: Option<u64>,
    ) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            file: File::open(path).unwrap(),
            id,
        })
    }

    fn open_for_append(
        path: impl AsRef<std::path::Path> + Send,
        id: Option<u64>,
    ) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            file: OpenOptions::new()
                .write(true)
                .append(true)
                .read(true)
                .create(true)
                .open(path)?,
            id,
        })
    }

    fn length(&self) -> Result<u64, Error> {
        let metadata = self.file.metadata()?;
        Ok(metadata.len())
    }

    fn close(mut self) -> Result<(), Error> {
        // Closing is done by just dropping it
        self.flush().map_err(Error::from)
    }
}

impl Seek for StdFile {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self,)))]
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

impl Write for StdFile {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, buf)))]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl Read for StdFile {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, buf)))]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

#[derive(Debug, Default, Clone)]
pub struct StdFileManager {
    file_id_counter: Arc<AtomicU64>,
    file_ids: Arc<RwLock<HashMap<PathBuf, u64>>>,
    open_files: Arc<Mutex<HashMap<u64, Option<StdFile>>>>,
    reader_files: Arc<Mutex<HashMap<u64, VecDeque<StdFile>>>>,
}

impl StdFileManager {
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
}

impl FileManager for StdFileManager {
    type File = StdFile;
    type FileHandle = OpenStdFile;
    fn append(&self, path: impl AsRef<Path> + Send) -> Result<Self::FileHandle, Error> {
        let path = path.as_ref();
        let file_id = self.file_id_for_path(path);
        let mut open_files = self.open_files.lock();
        if let Some(open_file) = open_files.get_mut(&file_id) {
            Ok(OpenStdFile {
                file: Some(open_file.take().expect("file already owned")),
                reader: false,
                manager: Some(self.clone()),
            })
        } else {
            let file = StdFile::open_for_append(path, Some(file_id))?;
            open_files.insert(file_id, None);
            Ok(OpenStdFile {
                file: Some(file),
                reader: false,
                manager: Some(self.clone()),
            })
        }
    }

    fn read(&self, path: impl AsRef<Path> + Send) -> Result<Self::FileHandle, Error> {
        // TODO we should come up with a way to cache open files. We want to
        // support more than one open reader for a file at any given time, but
        // we should be able to limit the number of open files. A time+capacity
        // based Lru cache might work.
        let path = path.as_ref();
        let file_id = self.file_id_for_path(path);
        {
            let mut reader_files = self.reader_files.lock();
            if let Some(file) = reader_files.get_mut(&file_id).and_then(VecDeque::pop_front) {
                return Ok(OpenStdFile {
                    file: Some(file),
                    manager: Some(self.clone()),
                    reader: true,
                });
            }
        }

        let file = StdFile::open_for_read(path, Some(file_id))?;
        Ok(OpenStdFile {
            file: Some(file),
            manager: Some(self.clone()),
            reader: true,
        })
    }

    fn delete(&self, path: impl AsRef<Path> + Send) -> Result<bool, Error> {
        let path = path.as_ref();
        let file_id = self.remove_file_id_for_path(path);
        if let Some(file_id) = file_id {
            let mut open_files = self.open_files.lock();
            let mut reader_files = self.reader_files.lock();
            open_files.remove(&file_id);
            reader_files.remove(&file_id);
        }

        if path.exists() {
            std::fs::remove_file(path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

pub struct OpenStdFile {
    file: Option<StdFile>,
    manager: Option<StdFileManager>,
    reader: bool,
}

impl OpenableFile<StdFile> for OpenStdFile {
    fn execute<W: FileOp<StdFile>>(&mut self, mut writer: W) -> W::Output {
        writer.execute(self.file.as_mut().unwrap())
    }

    fn close(self) -> Result<(), Error> {
        drop(self);
        Ok(())
    }
}

impl Drop for OpenStdFile {
    fn drop(&mut self) {
        if let Some(manager) = &self.manager {
            let file = self.file.take().unwrap();
            if let Some(file_id) = file.id {
                if self.reader {
                    let mut reader_files = manager.reader_files.lock();
                    let path_files = reader_files.entry(file_id).or_default();
                    path_files.push_front(file);
                } else {
                    let mut writer_files = manager.open_files.lock();
                    *writer_files.get_mut(&file_id).unwrap() = Some(file);
                }
            }
        }
    }
}
