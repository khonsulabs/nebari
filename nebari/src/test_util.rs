use std::{
    io::ErrorKind,
    ops::Deref,
    path::{Path, PathBuf},
};

use crate::Vault;

// TODO this should be shared between nebari and bonsaidb-core.

pub struct TestDirectory(pub PathBuf);

impl TestDirectory {
    pub fn new<S: AsRef<Path>>(name: S) -> Self {
        let path = std::env::temp_dir().join(name);
        if path.exists() {
            std::fs::remove_dir_all(&path).expect("error clearing temporary directory");
        }
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_dir_all(&self.0) {
            if err.kind() != ErrorKind::NotFound {
                eprintln!("Failed to clean up temporary folder: {:?}", err);
            }
        }
    }
}

impl AsRef<Path> for TestDirectory {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Deref for TestDirectory {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct RotatorVault {
    rotation_amount: u8,
}

impl RotatorVault {
    pub const fn new(rotation_amount: u8) -> Self {
        Self { rotation_amount }
    }
}

impl Vault for RotatorVault {
    type Error = NotEncrypted;
    fn encrypt(&self, payload: &[u8]) -> Result<Vec<u8>, NotEncrypted> {
        let mut output = Vec::with_capacity(payload.len() + 4);
        output.extend(b"rotv");
        output.extend(payload.iter().map(|c| c.wrapping_add(self.rotation_amount)));
        Ok(output)
    }

    fn decrypt(&self, payload: &[u8]) -> Result<Vec<u8>, NotEncrypted> {
        if payload.len() < 4 {
            return Err(NotEncrypted);
        }
        let (header, payload) = payload.split_at(4);
        if header != b"rotv" {
            return Err(NotEncrypted);
        }

        Ok(payload
            .iter()
            .map(|c| c.wrapping_sub(self.rotation_amount))
            .collect())
    }
}

#[derive(thiserror::Error, Debug)]
#[error("not an encrypted payload")]
pub struct NotEncrypted;
