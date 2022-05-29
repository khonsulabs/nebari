use std::{
    collections::{HashMap, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

use parking_lot::Mutex;

use super::{FileManager, FileOp, ManagedFile, OpenableFile};
use crate::{
    error::Error,
    io::{File as _, IntoPathId, ManagedFileOpener, OperableFile, PathId, PathIds},
};

/// An open file that uses [`std::fs`].
#[derive(Debug)]
pub struct StdFile {
    file: File,
    id: PathId,
}

impl ManagedFile for StdFile {
    type Manager = StdFileManager;
}

impl super::File for StdFile {
    fn id(&self) -> &PathId {
        &self.id
    }

    fn length(&self) -> Result<u64, Error> {
        let metadata = self.file.metadata()?;
        Ok(metadata.len())
    }

    fn close(mut self) -> Result<(), Error> {
        self.synchronize()
    }

    fn synchronize(&mut self) -> Result<(), crate::Error> {
        self.file.sync_data().map_err(Error::from)
    }
}

/// A [`ManagedFileOpener`] implementation that produces [`StdFile`]s.
pub struct StdFileOpener;

impl ManagedFileOpener<StdFile> for StdFileOpener {
    fn open_for_read(&self, path: impl IntoPathId + Send) -> Result<StdFile, Error> {
        let path = path.into_path_id();
        Ok(StdFile {
            file: File::open(path.path())?,
            id: path,
        })
    }

    fn open_for_append(&self, path: impl IntoPathId + Send) -> Result<StdFile, Error> {
        let path = path.into_path_id();
        Ok(StdFile {
            file: OpenOptions::new()
                .write(true)
                .append(true)
                .read(true)
                .create(true)
                .open(path.path())?,
            id: path,
        })
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

/// The [`FileManager`] for [`StdFile`].
#[derive(Debug, Default, Clone)]
pub struct StdFileManager {
    file_ids: PathIds,
    open_files: Arc<Mutex<HashMap<u64, FileSlot>>>,
    reader_files: Arc<Mutex<HashMap<u64, VecDeque<StdFile>>>>,
}

#[derive(Debug)]
enum FileSlot {
    Available(StdFile),
    Taken,
    Waiting(flume::Sender<StdFile>),
}

impl FileManager for StdFileManager {
    type File = StdFile;
    type FileHandle = OpenStdFile;

    fn resolve_path(&self, path: impl AsRef<Path>, create_if_not_found: bool) -> Option<PathId> {
        self.file_ids
            .file_id_for_path(path.as_ref(), create_if_not_found)
    }

    fn read(&self, path: impl IntoPathId) -> Result<Self::FileHandle, Error> {
        let path = path.into_path_id();
        let file_id = self.file_ids.file_id_for_path(path, true).unwrap();

        let mut reader_files = self.reader_files.lock();
        let files = reader_files.entry(file_id.id().unwrap()).or_default();

        if let Some(file) = files.pop_front() {
            return Ok(OpenStdFile {
                file: Some(file),
                manager: Some(self.clone()),
                reader: true,
            });
        }

        let file = StdFileOpener.open_for_read(file_id)?;
        Ok(OpenStdFile {
            file: Some(file),
            manager: Some(self.clone()),
            reader: true,
        })
    }

    fn append(&self, path: impl IntoPathId) -> Result<Self::FileHandle, Error> {
        let path = path.into_path_id();
        let file_id = self.file_ids.file_id_for_path(path, true).unwrap();
        let mut open_files = self.open_files.lock();
        if let Some(open_file) = open_files.get_mut(&file_id.id().unwrap()) {
            let mut file = FileSlot::Taken;
            std::mem::swap(&mut file, open_file);
            let file = match file {
                FileSlot::Available(file) => file,
                other => {
                    let (file_sender, file_receiver) = flume::bounded(1);
                    *open_file = FileSlot::Waiting(file_sender);
                    drop(open_files);

                    match file_receiver.recv() {
                        Ok(file) => {
                            // If we stole the slot from another waiter (shouldn't
                            // happen in real usage), we need to reinstall it.
                            if let FileSlot::Waiting(other_sender) = other {
                                let mut open_files = self.open_files.lock();
                                if let Some(open_file) = open_files.get_mut(&file_id.id().unwrap())
                                {
                                    *open_file = FileSlot::Waiting(other_sender);
                                }
                            }
                            file
                        }
                        Err(flume::RecvError::Disconnected) => {
                            // If we are disconnected, we should recurse to try
                            // to acquire the file again.
                            return self.append(file_id);
                        }
                    }
                }
            };
            Ok(OpenStdFile {
                file: Some(file),
                reader: false,
                manager: Some(self.clone()),
            })
        } else {
            let file = self.open_for_append(file_id.clone())?;
            open_files.insert(file_id.id().unwrap(), FileSlot::Taken);
            Ok(OpenStdFile {
                file: Some(file),
                reader: false,
                manager: Some(self.clone()),
            })
        }
    }

    fn file_length(&self, path: impl IntoPathId) -> Result<u64, Error> {
        path.path()
            .metadata()
            .map_err(Error::from)
            .map(|metadata| metadata.len())
    }

    fn exists(&self, path: impl IntoPathId) -> Result<bool, crate::Error> {
        if let Some(path_id) = self.resolve_path(path.path(), false) {
            {
                let open_files = self.open_files.lock();
                if open_files.contains_key(&path_id.id().unwrap()) {
                    return Ok(true);
                }
            }
            {
                let reader_files = self.reader_files.lock();
                if reader_files.contains_key(&path_id.id().unwrap()) {
                    return Ok(true);
                }
            }
        }

        // Not already open, just ask the filesystem
        Ok(path.path().exists())
    }

    fn close_handles<F: FnOnce(PathId)>(&self, path: impl IntoPathId, publish_callback: F) {
        if let Some(result) = self.file_ids.recreate_file_id_for_path(path) {
            let mut open_files = self.open_files.lock();
            let mut reader_files = self.reader_files.lock();
            open_files.remove(&result.previous_id.id().unwrap());
            reader_files.remove(&result.previous_id.id().unwrap());
            publish_callback(result.new_id);
        }
    }

    fn delete(&self, path: impl IntoPathId) -> Result<bool, Error> {
        let in_path = path.into_path_id();
        let file_id = self.file_ids.remove_file_id_for_path(&in_path);
        if let Some(file_id) = file_id {
            let mut open_files = self.open_files.lock();
            let mut reader_files = self.reader_files.lock();
            open_files.remove(&file_id.id().unwrap());
            reader_files.remove(&file_id.id().unwrap());
        }

        if in_path.path().exists() {
            std::fs::remove_file(in_path.path())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn delete_directory(&self, path: impl AsRef<Path>) -> Result<(), Error> {
        let path = path.as_ref();
        let removed_ids = self.file_ids.remove_file_ids_for_path_prefix(path);
        let mut open_files = self.open_files.lock();
        let mut reader_files = self.reader_files.lock();
        for path_id in removed_ids {
            open_files.remove(&path_id.id().unwrap());
            reader_files.remove(&path_id.id().unwrap());
        }

        if path.exists() {
            std::fs::remove_dir_all(path)?;
        }

        Ok(())
    }
}

impl ManagedFileOpener<StdFile> for StdFileManager {
    fn open_for_read(&self, path: impl IntoPathId + Send) -> Result<StdFile, Error> {
        StdFileOpener.open_for_read(path)
    }

    fn open_for_append(&self, path: impl IntoPathId + Send) -> Result<StdFile, Error> {
        StdFileOpener.open_for_append(path)
    }
}

/// An open [`StdFile`] that belongs to a [`StdFileManager`].
#[derive(Debug)]
pub struct OpenStdFile {
    file: Option<StdFile>,
    manager: Option<StdFileManager>,
    reader: bool,
}

impl OpenableFile<StdFile> for OpenStdFile {
    fn id(&self) -> &PathId {
        self.file.as_ref().unwrap().id()
    }

    fn replace_with<C: FnOnce(PathId)>(
        self,
        replacement: StdFile,
        manager: &StdFileManager,
        publish_callback: C,
    ) -> Result<Self, Error> {
        let current_path = self.file.as_ref().unwrap().id.path.clone();
        self.close()?;
        let path = replacement.id.clone();
        replacement.close()?;

        std::fs::rename(path.path(), current_path.path())?;
        manager.close_handles(&current_path, publish_callback);
        manager.append(current_path)
    }

    fn close(self) -> Result<(), Error> {
        drop(self);
        Ok(())
    }
}

impl OperableFile<StdFile> for OpenStdFile {
    fn execute<Output, Op: FileOp<Output>>(&mut self, operator: Op) -> Output {
        operator.execute(self.file.as_mut().unwrap())
    }
}

impl Drop for OpenStdFile {
    fn drop(&mut self) {
        if let Some(manager) = &self.manager {
            let file = self.file.take().unwrap();
            if let Some(file_id) = file.id.id() {
                if self.reader {
                    let mut reader_files = manager.reader_files.lock();
                    if let Some(path_files) = reader_files.get_mut(&file_id) {
                        path_files.push_front(file);
                    }
                } else {
                    let mut writer_files = manager.open_files.lock();
                    if let Some(writer_file) = writer_files.get_mut(&file_id) {
                        match writer_file {
                            FileSlot::Available(_) => unreachable!(),
                            FileSlot::Taken => {
                                *writer_file = FileSlot::Available(file);
                            }
                            FileSlot::Waiting(sender) => {
                                if let Err(flume::SendError(file)) = sender.send(file) {
                                    *writer_file = FileSlot::Available(file);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
