use async_trait::async_trait;

use crate::Result;

/// Supplies Firebase ID tokens to service clients.
///
/// Implementations (in practice, the Auth client) refresh the token
/// proactively before expiry. Service clients call this **per request**;
/// they must never cache the returned string beyond a single call.
///
/// `force_refresh` is set after the server answers `UNAUTHENTICATED`:
/// the implementation must discard any cached token, refresh it, and
/// return the fresh one. Callers retry the failed request exactly once.
#[async_trait]
pub trait TokenProvider: Send + Sync {
    /// Returns a currently valid ID token for the signed-in user.
    async fn id_token(&self, force_refresh: bool) -> Result<String>;

    /// The signed-in user's local ID (`uid`), when known without a
    /// network call.
    async fn uid(&self) -> Option<String> {
        None
    }
}
