use std::collections::BTreeMap;
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

/// The result of a successful sign-in: the user, plus the
/// offline-activation material issued for the new session.
///
/// The `offline_token` is a random secret shown to the caller
/// only here. The caller keeps it (typically in the OS secure
/// store, next to its own offline policy) and presents it to
/// `Auth::restore_session` to activate the session on a later
/// launch, including offline. The `session_key` is the token's
/// hash: not a secret, safe to keep in a plain account
/// registry, and the handle for targeted operations such as
/// `Auth::sign_out_session`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedIn {
    /// The signed-in user.
    pub user: User,
    /// The offline token for this session (preimage; secret).
    pub offline_token: String,
    /// The session key: SHA-256 hash of the offline token.
    pub session_key: SessionKey,
}

/// A session's public handle: the SHA-256 hash (hex) of its
/// offline token. The hash is the crate's internal storage key
/// for the session, so it is not a secret and reveals nothing
/// about the token; possession of the hash authorizes nothing
/// but destruction (sign-out), which is safe to allow because
/// the data it removes is only the device's copy.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionKey(String);

impl SessionKey {
    /// Derives the session key for an offline token.
    pub fn from_offline_token(offline_token: &str) -> Self {
        let digest =
            aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, offline_token.as_bytes());
        let mut hex = String::with_capacity(64);
        for byte in digest.as_ref() {
            hex.push_str(&format!("{byte:02x}"));
        }
        SessionKey(hex)
    }

    /// The key as a string (hex SHA-256 of the offline token).
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Rebuilds a key from its stored string form.
    pub(crate) fn from_key_string(key: String) -> Self {
        SessionKey(key)
    }
}

impl std::fmt::Display for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Generates a fresh offline token: 32 random bytes, base64url.
pub(crate) fn generate_offline_token() -> String {
    use base64::Engine;
    let rng = aws_lc_rs::rand::SystemRandom::new();
    let mut bytes = [0u8; 32];
    aws_lc_rs::rand::SecureRandom::fill(&rng, &mut bytes).expect("system RNG must be available");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// The sync-relevant facts of a session, exposed so the caller
/// can implement its own offline policy (the crate holds no
/// offline-duration policy of its own).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionFacts {
    /// The session's user ID.
    pub uid: String,
    /// When the cached ID token expires, when known.
    pub id_token_expires_at: Option<SystemTime>,
    /// When an ID token was last successfully obtained from the
    /// server (sign-in or refresh), when known.
    pub last_refresh_at: Option<SystemTime>,
}

/// Live session state held in memory.
#[derive(Debug, Clone)]
pub(crate) struct Session {
    pub user: User,
    pub id_token: String,
    pub refresh_token: String,
    pub expires_at: SystemTime,
    /// When the ID token was last obtained from the server.
    pub last_refresh_at: SystemTime,
    /// The collection key this session persists under, when it
    /// is backed by a credential store.
    pub key: Option<SessionKey>,
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
            id_token: Some(self.id_token.clone()),
            id_token_expires_at: Some(unix_seconds(self.expires_at)),
            last_refresh_at: Some(unix_seconds(self.last_refresh_at)),
        }
    }

    pub(crate) fn facts(&self) -> SessionFacts {
        SessionFacts {
            uid: self.user.uid.clone(),
            id_token_expires_at: Some(self.expires_at),
            last_refresh_at: Some(self.last_refresh_at),
        }
    }
}

/// One user's persisted session: the refresh token, plus the
/// ID-token cache (the last ID token, its expiry, and when it
/// was obtained). The cache is what an offline restore
/// activates from when the refresh exchange cannot run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredSession {
    pub uid: String,
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub refresh_token: String,
    /// The last ID token, cached for offline restore.
    #[serde(default)]
    pub id_token: Option<String>,
    /// Unix seconds at which the cached ID token expires.
    #[serde(default)]
    pub id_token_expires_at: Option<i64>,
    /// Unix seconds of the last successful token acquisition.
    #[serde(default)]
    pub last_refresh_at: Option<i64>,
}

impl StoredSession {
    pub(crate) fn user(&self) -> User {
        User {
            uid: self.uid.clone(),
            email: self.email.clone(),
            display_name: self.display_name.clone(),
        }
    }

    pub(crate) fn facts(&self) -> SessionFacts {
        SessionFacts {
            uid: self.uid.clone(),
            id_token_expires_at: self.id_token_expires_at.map(from_unix_seconds),
            last_refresh_at: self.last_refresh_at.map(from_unix_seconds),
        }
    }

    /// Builds the in-memory session for an offline restore from
    /// the cached ID token, if one was persisted.
    pub(crate) fn cached_session(&self, key: &SessionKey) -> Option<Session> {
        Some(Session {
            user: self.user(),
            id_token: self.id_token.clone()?,
            refresh_token: self.refresh_token.clone(),
            expires_at: self
                .id_token_expires_at
                .map(from_unix_seconds)
                .unwrap_or(SystemTime::UNIX_EPOCH),
            last_refresh_at: self
                .last_refresh_at
                .map(from_unix_seconds)
                .unwrap_or(SystemTime::UNIX_EPOCH),
            key: Some(key.clone()),
        })
    }
}

/// The persisted form of the credential blob (version 2): a
/// collection of per-user sessions keyed by session key
/// (SHA-256 of each session's offline token). Blobs written by
/// earlier versions (a single bare session) are not migrated:
/// they predate offline tokens, so no entry in them could be
/// activated under this model; they read as an empty
/// collection and the user signs in once more.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredSessions {
    pub version: u32,
    #[serde(default)]
    pub sessions: BTreeMap<String, StoredSession>,
}

impl StoredSessions {
    pub(crate) const VERSION: u32 = 2;

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoredSessions serialization cannot fail")
    }

    /// Parses a blob; anything that is not a version-2
    /// collection (including legacy single-session blobs and
    /// corruption) reads as an empty collection.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        match serde_json::from_slice::<StoredSessions>(bytes) {
            Ok(parsed) if parsed.version == Self::VERSION => parsed,
            _ => StoredSessions {
                version: Self::VERSION,
                sessions: BTreeMap::new(),
            },
        }
    }
}

pub(crate) fn unix_seconds(time: SystemTime) -> i64 {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(_) => 0,
    }
}

pub(crate) fn from_unix_seconds(seconds: i64) -> SystemTime {
    if seconds <= 0 {
        SystemTime::UNIX_EPOCH
    } else {
        SystemTime::UNIX_EPOCH + Duration::from_secs(seconds as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(uid: &str) -> StoredSession {
        StoredSession {
            uid: uid.to_string(),
            email: Some("s@example.jp".to_string()),
            display_name: None,
            refresh_token: "refresh-1".to_string(),
            id_token: Some("id-1".to_string()),
            id_token_expires_at: Some(1_800_000_000),
            last_refresh_at: Some(1_799_996_400),
        }
    }

    #[test]
    fn collection_round_trip() {
        let key_a = SessionKey::from_offline_token("token-a");
        let key_b = SessionKey::from_offline_token("token-b");
        let mut sessions = StoredSessions {
            version: StoredSessions::VERSION,
            sessions: BTreeMap::new(),
        };
        sessions
            .sessions
            .insert(key_a.as_str().to_string(), entry("uid-a"));
        sessions
            .sessions
            .insert(key_b.as_str().to_string(), entry("uid-b"));
        let bytes = sessions.to_bytes();
        assert_eq!(StoredSessions::from_bytes(&bytes), sessions);
        // Not JSON, and a legacy bare-session blob, both read empty.
        assert!(StoredSessions::from_bytes(b"not json").sessions.is_empty());
        let legacy = br#"{"uid":"u","email":null,"display_name":null,"refresh_token":"r"}"#;
        assert!(StoredSessions::from_bytes(legacy).sessions.is_empty());
    }

    #[test]
    fn session_key_is_the_token_hash() {
        let key = SessionKey::from_offline_token("some-token");
        assert_eq!(key.as_str().len(), 64);
        assert_eq!(key, SessionKey::from_offline_token("some-token"));
        assert_ne!(key, SessionKey::from_offline_token("other-token"));
        // The key must not contain the token itself.
        assert!(!key.as_str().contains("some-token"));
    }

    #[test]
    fn generated_tokens_are_unique() {
        assert_ne!(generate_offline_token(), generate_offline_token());
    }

    #[test]
    fn cached_session_requires_a_cached_token() {
        let key = SessionKey::from_offline_token("token-a");
        let with_cache = entry("uid-a").cached_session(&key).unwrap();
        assert_eq!(with_cache.id_token, "id-1");
        assert_eq!(with_cache.user.uid, "uid-a");
        let mut without = entry("uid-a");
        without.id_token = None;
        assert!(without.cached_session(&key).is_none());
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
            id_token: "id".to_string(),
            refresh_token: "refresh".to_string(),
            expires_at: now + Duration::from_secs(60),
            last_refresh_at: now,
            key: None,
        };
        assert!(session.needs_refresh(now));
        let fresh = Session {
            expires_at: now + Duration::from_secs(3600),
            ..session
        };
        assert!(!fresh.needs_refresh(now));
    }
}
