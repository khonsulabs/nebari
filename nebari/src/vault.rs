use crate::{error::SendSyncError, ErrorKind};

/// A provider of encryption for blocks of data.
pub trait Vault: std::fmt::Debug + Send + Sync + 'static {
    /// The error type that the vault can produce.
    type Error: SendSyncError;

    /// Encrypts `payload`, returning a new buffer that contains all information
    /// necessary to decrypt it in the future.
    fn encrypt(&self, payload: &[u8]) -> Result<Vec<u8>, Self::Error>;

    /// Decrypts a previously encrypted `payload`, returning the decrypted
    /// information.
    fn decrypt(&self, payload: &[u8]) -> Result<Vec<u8>, Self::Error>;
}

/// A [`Vault`] that can be boxed.
pub trait AnyVault: std::fmt::Debug + Send + Sync + 'static {
    /// Encrypts `payload`, returning a new buffer that contains all information
    /// necessary to decrypt it in the future.
    fn encrypt(&self, payload: &[u8]) -> Result<Vec<u8>, crate::Error>;

    /// Decrypts a previously encrypted `payload`, returning the decrypted
    /// information.
    fn decrypt(&self, payload: &[u8]) -> Result<Vec<u8>, crate::Error>;
}

impl<T, E> AnyVault for T
where
    T: Vault<Error = E> + std::fmt::Debug,
    E: SendSyncError,
{
    fn encrypt(&self, payload: &[u8]) -> Result<Vec<u8>, crate::Error> {
        self.encrypt(payload)
            .map_err(|err| crate::Error::from(ErrorKind::Vault(Box::new(err))))
    }

    fn decrypt(&self, payload: &[u8]) -> Result<Vec<u8>, crate::Error> {
        Vault::decrypt(self, payload)
            .map_err(|err| crate::Error::from(ErrorKind::Vault(Box::new(err))))
    }
}
