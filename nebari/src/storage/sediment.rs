use std::io::ErrorKind;

use arc_bytes::ArcBytes;
use sediment::{
    database::{Database, GrainData, HeaderUpdateSession, PendingCommit, WriteSession},
    format::{BatchId, GrainId},
    io::{self},
};

use crate::storage::BlobStorage;

#[derive(Debug)]
pub struct SedimentFile<FileManager: io::FileManager> {
    pub db: Database<FileManager>,
    automatic_checkpointing: bool,
    checkpoint_to: BatchId,
    session: Option<Session<FileManager>>,
}

impl<FileManager: io::FileManager> Clone for SedimentFile<FileManager> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            automatic_checkpointing: self.automatic_checkpointing,
            checkpoint_to: self.checkpoint_to,
            session: None,
        }
    }
}

impl<FileManager> BlobStorage for SedimentFile<FileManager>
where
    FileManager: io::FileManager,
{
    fn unique_id(&self) -> io::paths::PathId {
        self.db.path_id().clone()
    }
    fn read_header(&mut self) -> Result<ArcBytes<'static>, crate::Error> {
        let data = self
            .db
            .embedded_header()
            .map(|header| self.read(header))
            .transpose()?;
        Ok(data.unwrap_or_default())
    }

    fn write_header(&mut self, data: &[u8]) -> Result<(), crate::Error> {
        let mut session = match self.session.take() {
            Some(Session::Exclusive(session)) => session,
            Some(Session::Parallel(session)) => session.updating_embedded_header(),
            None => self.db.new_session().updating_embedded_header(),
        };

        let grain_id = session.push(data)?;
        session.set_embedded_header(Some(grain_id))?;
        self.session = Some(Session::Exclusive(session));
        Ok(())
    }

    fn write_header_async(&mut self, data: Vec<u8>) -> Result<(), crate::Error> {
        let mut session = match self.session.take() {
            Some(Session::Exclusive(session)) => session,
            Some(Session::Parallel(session)) => session.updating_embedded_header(),
            None => self.db.new_session().updating_embedded_header(),
        };
        let grain_id = session.push_async(data)?;
        session.set_embedded_header(Some(grain_id))?;
        self.session = Some(Session::Exclusive(session));
        Ok(())
    }

    fn read(&mut self, grain_id: GrainId) -> Result<ArcBytes<'static>, crate::Error> {
        let grain_data = match self.session.as_mut() {
            Some(Session::Exclusive(session)) => session.get(grain_id),
            Some(Session::Parallel(session)) => session.get(grain_id),
            None => self.db.get(grain_id),
        }?;

        if let Some(data) = grain_data {
            let len = data.len();
            let data = ArcBytes::from(data.into_raw_bytes())
                .slice(GrainData::HEADER_BYTES..GrainData::HEADER_BYTES + len);
            Ok(data)
        } else {
            Err(crate::Error::from(ErrorKind::NotFound))
        }
    }

    fn free(&mut self, id: GrainId) -> Result<(), crate::Error> {
        if self.session.is_none() {
            self.session = Some(Session::Parallel(self.db.new_session()));
        }
        match self.session.as_mut().unwrap() {
            Session::Exclusive(session) => session.archive(id)?,
            Session::Parallel(session) => session.archive(id)?,
        }
        Ok(())
    }

    fn write(&mut self, data: &[u8]) -> Result<GrainId, crate::Error> {
        if self.session.is_none() {
            self.session = Some(Session::Parallel(self.db.new_session()));
        }
        let grain_id = match self.session.as_mut().unwrap() {
            Session::Exclusive(session) => session.push(data)?,
            Session::Parallel(session) => session.push(data)?,
        };
        Ok(grain_id)
    }

    fn write_async(&mut self, data: Vec<u8>) -> Result<GrainId, crate::Error> {
        if self.session.is_none() {
            self.session = Some(Session::Parallel(self.db.new_session()));
        }
        let grain_id = match self.session.as_mut().unwrap() {
            Session::Exclusive(session) => session.push_async(data)?,
            Session::Parallel(session) => session.push_async(data)?,
        };
        Ok(grain_id)
    }

    fn sync(&mut self) -> Result<BatchId, crate::Error> {
        self.checkpoint_to = match self.session.take() {
            Some(Session::Exclusive(session)) => {
                if self.automatic_checkpointing {
                    session.commit_and_checkpoint(self.checkpoint_to)?
                } else {
                    session.commit()?
                }
            }
            Some(Session::Parallel(session)) => {
                if self.automatic_checkpointing {
                    session.commit_and_checkpoint(self.checkpoint_to)?
                } else {
                    session.commit()?
                }
            }
            None => self.checkpoint_to,
        };

        Ok(self.checkpoint_to)
    }
}

impl<FileManager: io::FileManager> SedimentFile<FileManager> {
    pub fn open<Path: AsRef<std::path::Path> + ?Sized>(
        path: &Path,
        automatic_checkpointing: bool,
        manager: FileManager,
    ) -> Result<Self, crate::Error> {
        let path = manager.resolve_path(path);
        let db = Database::open_with_manager(path, manager)?;

        Ok(SedimentFile {
            db,
            automatic_checkpointing: false,
            checkpoint_to: BatchId::default(),
            session: None,
        })
    }

    pub fn enqueue_commit(&mut self) -> io::Result<Option<PendingCommit<FileManager>>> {
        let pending_commit = match self.session.take() {
            Some(Session::Exclusive(session)) => Some(session.enqueue_commit()?),
            Some(Session::Parallel(session)) => Some(session.enqueue_commit()),
            None => None,
        };
        Ok(pending_commit)
    }
}

#[derive(Debug)]
enum Session<Manager: io::FileManager> {
    Parallel(WriteSession<Manager>),
    Exclusive(HeaderUpdateSession<Manager>),
}
