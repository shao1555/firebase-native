use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

/// Refresh the ID token when it expires within this window.
pub(crate) const REFRESH_WINDOW: Duration = Duration::from_secs(300);

/// The signed-in user, as reported by the Identity Toolkit API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    /// The user's local ID (`uid`).
    pub uid: String,
    /// The user's email address, when the account has one.
    pub email: Option<String>,
    /// The display name, when set.
    pub display_name: Option<String>,
}

/// Live session state held in memory.
#[derive(Debug, Clone)]
pub(crate) struct Session {
    pub user: User,
    pub id_token: String,
    pub refresh_token: String,
    pub expires_at: SystemTime,
}

impl Session {
    /// Whether the ID token is expired or inside the refresh window.
    pub(crate) fn needs_refresh(&self, now: SystemTime) -> bool {
        match self.expires_at.duration_since(now) {
            Ok(remaining) => remaining <= REFRESH_WINDOW,
            Err(_) => true,
        }
    }

    pub(crate) fn stored(&self) -> StoredSession {
        StoredSession {
            uid: self.user.uid.clone(),
            email: self.user.email.clone(),
            display_name: self.user.display_name.clone(),
            refresh_token: self.refresh_token.clone(),
        }
    }
}

/// What is persisted through `CredentialStore`: the refresh-token
/// session. The ID token is deliberately not persisted; restoring a
/// session always exchanges the refresh token for a fresh ID token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredSession {
    pub uid: String,
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub refresh_token: String,
}

impl StoredSession {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoredSession serialization cannot fail")
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }

    pub(crate) fn user(&self) -> User {
        User {
            uid: self.uid.clone(),
            email: self.email.clone(),
            display_name: self.display_name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_session_round_trip() {
        let stored = StoredSession {
            uid: "uid-1".to_string(),
            email: Some("s@example.jp".to_string()),
            display_name: None,
            refresh_token: "refresh".to_string(),
        };
        let bytes = stored.to_bytes();
        assert_eq!(StoredSession::from_bytes(&bytes), Some(stored));
        assert_eq!(StoredSession::from_bytes(b"not json"), None);
    }

    #[test]
    fn needs_refresh_inside_window() {
        let now = SystemTime::now();
        let session = Session {
            user: User {
                uid: "u".to_string(),
                email: None,
                display_name: None,
            },
            id_token: "token".to_string(),
            refresh_token: "refresh".to_string(),
            expires_at: now + Duration::from_secs(60),
        };
        assert!(session.needs_refresh(now));
        let fresh = Session {
            expires_at: now + Duration::from_secs(3600),
            ..session
        };
        assert!(!fresh.needs_refresh(now));
    }
}
