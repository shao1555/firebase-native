use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::Result;

/// Persistence for the Auth session blob (the refresh-token session).
///
/// The library owns no storage. The application shell implements this
/// trait over the backend it trusts:
///
/// - a file under the app's data directory for development builds;
/// - the OS secure store (Keychain / Credential Manager / Secret
///   Service) for production builds;
/// - or nothing at all — pass no store to Auth and the user signs in on
///   every launch. This fail-closed behaviour is the intended fallback
///   when no secure store is available; silently falling back to a
///   plaintext file in production is not.
///
/// The blob is opaque to implementations: store the bytes verbatim.
#[async_trait]
pub trait CredentialStore: Send + Sync {
    /// Loads the persisted blob, or `None` when nothing is stored.
    async fn load(&self) -> Result<Option<Vec<u8>>>;
    /// Persists the blob, replacing any previous one.
    async fn save(&self, blob: &[u8]) -> Result<()>;
    /// Removes any persisted blob (sign-out).
    async fn clear(&self) -> Result<()>;
}

/// In-memory [`CredentialStore`], for tests and ephemeral sessions.
#[derive(Debug, Default)]
pub struct MemoryCredentialStore {
    blob: Mutex<Option<Vec<u8>>>,
}

impl MemoryCredentialStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CredentialStore for MemoryCredentialStore {
    async fn load(&self) -> Result<Option<Vec<u8>>> {
        Ok(self.blob.lock().await.clone())
    }

    async fn save(&self, blob: &[u8]) -> Result<()> {
        *self.blob.lock().await = Some(blob.to_vec());
        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        *self.blob.lock().await = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_store_round_trip() {
        let store = MemoryCredentialStore::new();
        assert_eq!(store.load().await.unwrap(), None);
        store.save(b"session").await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(b"session".to_vec()));
        store.clear().await.unwrap();
        assert_eq!(store.load().await.unwrap(), None);
    }
}
