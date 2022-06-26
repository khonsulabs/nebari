use arc_bytes::ArcBytes;
use sediment::{
    database::{Database, GrainData, HeaderUpdateSession, WriteSession},
    format::{BatchId, GrainId},
    io::{self, fs::StdFileManager, FileManager},
};

use crate::{
    storage::{BlobStorage, StorageManager},
    ErrorKind,
};

#[derive(Debug)]
pub struct SedimentFile<File: io::FileManager> {
    db: Database<File>,
    checkpoint_to: BatchId,
    session: Option<Session<File>>,
}

impl<File> BlobStorage for SedimentFile<File>
where
    File: io::FileManager,
{
    fn unique_id(&self) -> u64 {
        self.db.path_id()
    }

    fn read_header(&mut self) -> Result<ArcBytes<'static>, crate::Error> {
        let data = self
            .db
            .embedded_header()
            .map(|header| self.read(header.as_u64()))
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

    fn read(&mut self, id: u64) -> Result<ArcBytes<'static>, crate::Error> {
        let grain_id = GrainId::from(id);
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
            Ok(ArcBytes::default())
        }
    }

    fn write(&mut self, data: Vec<u8>) -> Result<u64, crate::Error> {
        if self.session.is_none() {
            self.session = Some(Session::Parallel(self.db.new_session()));
        }
        let grain_id = match self.session.as_mut().unwrap() {
            Session::Exclusive(session) => session.push_async(data)?,
            Session::Parallel(session) => session.push_async(data)?,
        };
        Ok(grain_id.into())
    }

    fn sync(&mut self) -> Result<(), crate::Error> {
        self.checkpoint_to = match self.session.take() {
            Some(Session::Exclusive(session)) => {
                session.commit_and_checkpoint(self.checkpoint_to)?
            }
            Some(Session::Parallel(session)) => {
                session.commit_and_checkpoint(self.checkpoint_to)?
            }
            None => self.checkpoint_to,
        };

        Ok(())
    }

    fn free(&mut self, id: u64) -> Result<(), crate::Error> {
        if self.session.is_none() {
            self.session = Some(Session::Parallel(self.db.new_session()));
        }
        match self.session.as_mut().unwrap() {
            Session::Exclusive(session) => session.archive(GrainId::from(id))?,
            Session::Parallel(session) => session.archive(GrainId::from(id))?,
        }
        Ok(())
    }
}

impl StorageManager for StdFileManager {
    type Storage = SedimentFile<Self>;

    fn open<Path: AsRef<std::path::Path> + ?Sized>(
        &self,
        path: &Path,
    ) -> Result<Self::Storage, crate::Error> {
        let path = self.resolve_path(path);
        let db = Database::open_with_manager(path, self.clone())?;

        Ok(SedimentFile {
            db,
            session: None,
            checkpoint_to: BatchId::default(),
        })
    }
}

#[derive(Debug)]
enum Session<Manager: io::FileManager> {
    Parallel(WriteSession<Manager>),
    Exclusive(HeaderUpdateSession<Manager>),
}
