use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{Mutex, RwLock};

use firebase_native_core::{App, CredentialStore, Error, FirebaseOptions, Result, TokenProvider};

use crate::session::{
    generate_offline_token, Session, SessionFacts, SessionKey, SignedIn, StoredSessions, User,
};
use crate::wire::{parse_api_error, RefreshResponse, SignInResponse};

const IDENTITY_TOOLKIT_BASE: &str = "https://identitytoolkit.googleapis.com/v1";
const SECURE_TOKEN_URL: &str = "https://securetoken.googleapis.com/v1/token";

/// Firebase Authentication client.
///
/// Cheap to share behind an `Arc`: Firestore and Storage take
/// `Arc<dyn TokenProvider>`, and `Auth` implements that trait, so the
/// usual wiring is `Arc::new(auth)` handed to both this client (for
/// sign-in calls) and the service clients (for per-call tokens).
/// Session state plus a generation counter. The generation is bumped
/// on every session replacement (sign-in, restore, sign-out, clearing
/// a dead session), so an in-flight refresh can tell whether the
/// session it started from is still current before applying its
/// result — a refresh for user A must never overwrite or clear a
/// session that user B signed into meanwhile.
#[derive(Default)]
struct AuthState {
    session: Option<Session>,
    generation: u64,
}

pub struct Auth {
    options: FirebaseOptions,
    http: reqwest::Client,
    session: RwLock<AuthState>,
    /// Serializes session transitions together with their
    /// credential-store mutations, so a save/clear can never be
    /// reordered against another transition's save/clear. Lock
    /// ordering is always lifecycle -> session state.
    lifecycle: Mutex<()>,
    store: Option<Arc<dyn CredentialStore>>,
    identity_base: String,
    secure_token_url: String,
}

impl Auth {
    /// Creates an Auth client for the app, without session persistence.
    pub fn new(app: &App) -> Self {
        Self::with_store(app, None)
    }

    /// Creates an Auth client that persists sessions through the
    /// given store (a versioned collection of per-user sessions,
    /// each with its ID-token cache). Pass `None` for the
    /// fail-closed mode: nothing is persisted and the user signs
    /// in on every launch.
    pub fn with_store(app: &App, store: Option<Arc<dyn CredentialStore>>) -> Self {
        Self {
            options: app.options().clone(),
            http: reqwest::Client::new(),
            session: RwLock::new(AuthState::default()),
            lifecycle: Mutex::new(()),
            store,
            identity_base: IDENTITY_TOOLKIT_BASE.to_string(),
            secure_token_url: SECURE_TOKEN_URL.to_string(),
        }
    }

    /// Overrides the Identity Toolkit base URL (emulator / tests).
    pub fn with_identity_base(mut self, base: impl Into<String>) -> Self {
        self.identity_base = base.into();
        self
    }

    /// Overrides the Secure Token URL (emulator / tests).
    pub fn with_secure_token_url(mut self, url: impl Into<String>) -> Self {
        self.secure_token_url = url.into();
        self
    }

    /// The currently signed-in user, if any.
    pub async fn current_user(&self) -> Option<User> {
        self.session
            .read()
            .await
            .session
            .as_ref()
            .map(|s| s.user.clone())
    }

    // -- Sign-in methods -------------------------------------------------

    /// Signs in with a Firebase custom token, as minted by a trusted
    /// server (e.g. as issued after an OAuth 2.0 device flow).
    pub async fn sign_in_with_custom_token(&self, custom_token: &str) -> Result<SignedIn> {
        let body = json!({
            "token": custom_token,
            "returnSecureToken": true,
        });
        let response: SignInResponse = self.accounts_call("signInWithCustomToken", body).await?;
        self.adopt_sign_in(response).await
    }

    /// Signs in with email and password.
    pub async fn sign_in_with_email_password(
        &self,
        email: &str,
        password: &str,
    ) -> Result<SignedIn> {
        let body = json!({
            "email": email,
            "password": password,
            "returnSecureToken": true,
        });
        let response: SignInResponse = self.accounts_call("signInWithPassword", body).await?;
        self.adopt_sign_in(response).await
    }

    /// Creates a new email/password account and signs in.
    pub async fn sign_up_with_email_password(
        &self,
        email: &str,
        password: &str,
    ) -> Result<SignedIn> {
        let body = json!({
            "email": email,
            "password": password,
            "returnSecureToken": true,
        });
        let response: SignInResponse = self.accounts_call("signUp", body).await?;
        self.adopt_sign_in(response).await
    }

    /// Signs in anonymously.
    pub async fn sign_in_anonymously(&self) -> Result<SignedIn> {
        let body = json!({ "returnSecureToken": true });
        let response: SignInResponse = self.accounts_call("signUp", body).await?;
        self.adopt_sign_in(response).await
    }

    /// Signs in with an OAuth ID token from an external provider
    /// (e.g. Google), via `accounts:signInWithIdp`.
    pub async fn sign_in_with_idp_id_token(
        &self,
        provider_id: &str,
        oauth_id_token: &str,
    ) -> Result<SignedIn> {
        let post_body = format!(
            "id_token={}&providerId={}",
            url_encode(oauth_id_token),
            url_encode(provider_id),
        );
        let body = json!({
            "postBody": post_body,
            "requestUri": "http://localhost",
            "returnIdpCredential": true,
            "returnSecureToken": true,
        });
        let response: SignInResponse = self.accounts_call("signInWithIdp", body).await?;
        self.adopt_sign_in(response).await
    }

    // -- Session lifecycle ----------------------------------------------

    /// Activates a persisted session by presenting its offline
    /// token (issued at sign-in; see [`SignedIn`]). Returns
    /// `true` when a session is now active.
    ///
    /// The stored refresh token is exchanged for a fresh ID
    /// token when the server is reachable, and the stored entry
    /// is updated (rotation, ID cache, last-refresh time). When
    /// the exchange cannot run — the device is offline — the
    /// session is instead activated from the persisted ID-token
    /// cache, and the refresh catches up on later calls. When
    /// the server *rejects* the refresh token (revoked, user
    /// deleted or disabled), the stored entry is removed and
    /// `false` is returned. How long an offline activation may
    /// be used is the caller's policy: the crate only exposes
    /// the facts (see [`Auth::session_facts`]).
    pub async fn restore_session(&self, offline_token: &str) -> Result<bool> {
        if self.store.is_none() {
            return Ok(false);
        }
        let key = SessionKey::from_offline_token(offline_token);
        // Snapshot the generation before the (unlocked) load and
        // exchange. If any session transition happens meanwhile -
        // a sign-in, or a sign-out - this restore is stale and
        // must neither persist nor install its session: a delayed
        // restore must never resurrect a signed-out user or
        // replace a newer sign-in.
        let generation = self.session.read().await.generation;
        let sessions = self.load_sessions().await?;
        let Some(entry) = sessions.sessions.get(key.as_str()).cloned() else {
            return Ok(false);
        };
        let (session, refreshed_online) =
            match self.exchange_refresh_token(&entry.refresh_token).await {
                Ok(refreshed) => {
                    let now = SystemTime::now();
                    let expires_in = refreshed.expires_in_secs();
                    (
                        Session {
                            user: User {
                                uid: refreshed.user_id.clone(),
                                ..entry.user()
                            },
                            id_token: refreshed.id_token,
                            refresh_token: refreshed.refresh_token,
                            expires_at: now + Duration::from_secs(expires_in),
                            last_refresh_at: now,
                            key: Some(key.clone()),
                        },
                        true,
                    )
                }
                Err(Error::Unauthenticated(_)) => {
                    // The refresh token is dead. Removing the entry
                    // needs no generation check: it deletes stored
                    // state for this key only, and a newer sign-in
                    // always holds a different key.
                    let _lifecycle = self.lifecycle.lock().await;
                    self.remove_entry(&key).await?;
                    return Ok(false);
                }
                Err(_) => {
                    // Transport or transient failure: activate from
                    // the ID-token cache instead of failing.
                    let Some(session) = entry.cached_session(&key) else {
                        return Ok(false);
                    };
                    (session, false)
                }
            };
        // Apply phase: re-check under the lifecycle lock, then
        // persist + install atomically with respect to other
        // transitions. The exchange may have rotated the refresh
        // token, so the replacement is what gets persisted;
        // without this save, the next launch would restore with
        // the old (possibly already rotated-out) token.
        let _lifecycle = self.lifecycle.lock().await;
        let mut state = self.session.write().await;
        if state.generation != generation || state.session.is_some() {
            return Ok(false);
        }
        // The entry may have been deleted while the exchange
        // was in flight (a targeted sign-out of a not-yet-active
        // session does not touch the in-memory state, so the
        // generation check alone cannot see it): a restore must
        // never resurrect a deleted session, so re-read the
        // collection and require the entry to still exist.
        let sessions = self.load_sessions().await?;
        if !sessions.sessions.contains_key(key.as_str()) {
            return Ok(false);
        }
        if refreshed_online {
            self.upsert_entry(&session).await?;
        }
        state.session = Some(session);
        state.generation += 1;
        Ok(true)
    }

    /// Signs out the active session: drops it from memory and
    /// removes its stored entry (other users' entries are kept).
    /// Fully local — it works offline, which callers' offline
    /// guards rely on when they force a re-sign-in.
    pub async fn sign_out(&self) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let key = {
            let mut state = self.session.write().await;
            let key = state
                .session
                .as_ref()
                .and_then(|session| session.key.clone());
            state.session = None;
            state.generation += 1;
            key
        };
        if let Some(key) = &key {
            self.remove_entry(key).await?;
        }
        Ok(())
    }

    /// Signs out a stored session by its key, without
    /// activating it: removes the stored entry, and invalidates
    /// the in-memory session when the key names the active one.
    /// Destruction needs no token preimage — the key (the
    /// token's hash) is enough. Returns `true` when anything
    /// was removed. Fully local, like [`Auth::sign_out`].
    pub async fn sign_out_session(&self, key: &SessionKey) -> Result<bool> {
        let _lifecycle = self.lifecycle.lock().await;
        let mut removed = false;
        {
            let mut state = self.session.write().await;
            if state
                .session
                .as_ref()
                .and_then(|session| session.key.as_ref())
                == Some(key)
            {
                state.session = None;
                removed = true;
            }
            // Any stored-session deletion invalidates pending
            // restores - including a restore of this very key,
            // which is not active yet and so invisible to the
            // comparison above. Bump the generation
            // unconditionally; the restore path additionally
            // re-checks that its entry still exists.
            state.generation += 1;
        }
        if self.remove_entry(key).await? {
            removed = true;
        }
        Ok(removed)
    }

    /// The kill switch: wipes every stored session and the
    /// in-memory one, with no authentication — for a lost
    /// offline token (and even a lost key), or for handing over
    /// or disposing of a device. Deletion is fail-closed (only
    /// the device's copies go away; server-side data is
    /// untouched), so no capability is required. Fully local.
    pub async fn wipe_all_sessions(&self) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        {
            let mut state = self.session.write().await;
            state.session = None;
            state.generation += 1;
        }
        if let Some(store) = &self.store {
            store.clear().await?;
        }
        Ok(())
    }

    /// The active session's facts (user, ID-token expiry, last
    /// successful refresh), for the caller's offline policy.
    pub async fn session_facts(&self) -> Option<SessionFacts> {
        self.session
            .read()
            .await
            .session
            .as_ref()
            .map(|session| session.facts())
    }

    /// A stored session's facts by key, without activating it.
    pub async fn stored_session_facts(&self, key: &SessionKey) -> Result<Option<SessionFacts>> {
        Ok(self
            .load_sessions()
            .await?
            .sessions
            .get(key.as_str())
            .map(|entry| entry.facts()))
    }

    /// The keys of all stored sessions (for an account
    /// registry; keys are hashes, not secrets).
    pub async fn stored_session_keys(&self) -> Result<Vec<SessionKey>> {
        Ok(self
            .load_sessions()
            .await?
            .sessions
            .keys()
            .map(|key| SessionKey::from_key_string(key.clone()))
            .collect())
    }

    // -- Credential collection --------------------------------------------

    /// Loads the stored session collection (empty without a
    /// store, without a blob, or for a pre-collection blob).
    async fn load_sessions(&self) -> Result<StoredSessions> {
        let Some(store) = &self.store else {
            return Ok(StoredSessions::from_bytes(&[]));
        };
        match store.load().await? {
            Some(bytes) => Ok(StoredSessions::from_bytes(&bytes)),
            None => Ok(StoredSessions::from_bytes(&[])),
        }
    }

    /// Inserts or replaces the store entry for `session` under
    /// its key. The caller holds the lifecycle lock.
    async fn upsert_entry(&self, session: &Session) -> Result<()> {
        let (Some(store), Some(key)) = (&self.store, &session.key) else {
            return Ok(());
        };
        let mut sessions = self.load_sessions().await?;
        sessions
            .sessions
            .insert(key.as_str().to_string(), session.stored());
        store.save(&sessions.to_bytes()).await
    }

    /// Removes the store entry for `key`, clearing the blob
    /// when the collection becomes empty. The caller holds the
    /// lifecycle lock. Returns whether an entry was removed.
    async fn remove_entry(&self, key: &SessionKey) -> Result<bool> {
        let Some(store) = &self.store else {
            return Ok(false);
        };
        let mut sessions = self.load_sessions().await?;
        if sessions.sessions.remove(key.as_str()).is_none() {
            return Ok(false);
        }
        if sessions.sessions.is_empty() {
            store.clear().await?;
        } else {
            store.save(&sessions.to_bytes()).await?;
        }
        Ok(true)
    }

    /// Forces a refresh of the current session's ID token.
    pub async fn force_refresh(&self) -> Result<String> {
        self.refresh_locked().await
    }

    // -- Internals --------------------------------------------------------

    async fn accounts_call<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        body: Value,
    ) -> Result<T> {
        let url = format!(
            "{}/accounts:{}?key={}",
            self.identity_base, method, self.options.api_key
        );
        let response = self
            .http
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(http_error)?;
        let status = response.status();
        let text = response.text().await.map_err(http_error)?;
        if !status.is_success() {
            return Err(parse_api_error(status.as_u16(), &text));
        }
        serde_json::from_str(&text).map_err(Error::from)
    }

    async fn exchange_refresh_token(&self, refresh_token: &str) -> Result<RefreshResponse> {
        let url = format!("{}?key={}", self.secure_token_url, self.options.api_key);
        let response = self
            .http
            .post(url)
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token),
            ])
            .send()
            .await
            .map_err(http_error)?;
        let status = response.status();
        let text = response.text().await.map_err(http_error)?;
        if !status.is_success() {
            return Err(parse_api_error(status.as_u16(), &text));
        }
        serde_json::from_str(&text).map_err(Error::from)
    }

    async fn adopt_sign_in(&self, response: SignInResponse) -> Result<SignedIn> {
        let expires_at = SystemTime::now() + Duration::from_secs(response.expires_in_secs());
        // Some sign-in responses (custom token, observed in
        // production) omit localId/email; the ID token claims are
        // the fallback and are authoritative anyway.
        let (uid, email) = match response.local_id {
            Some(uid) => (uid, response.email),
            None => {
                let (uid, claims_email) = crate::wire::claims_from_id_token(&response.id_token)
                    .ok_or_else(|| {
                        Error::Serde(
                            "sign-in response has no localId and the ID token \
                             carries no uid claim"
                                .to_string(),
                        )
                    })?;
                (uid, response.email.or(claims_email))
            }
        };
        let user = User {
            uid,
            email,
            display_name: response.display_name,
        };
        let offline_token = generate_offline_token();
        let session_key = SessionKey::from_offline_token(&offline_token);
        let session = Session {
            user: user.clone(),
            id_token: response.id_token,
            refresh_token: response.refresh_token,
            expires_at,
            last_refresh_at: SystemTime::now(),
            key: Some(session_key.clone()),
        };
        let _lifecycle = self.lifecycle.lock().await;
        self.upsert_entry(&session).await?;
        let mut state = self.session.write().await;
        state.session = Some(session);
        state.generation += 1;
        Ok(SignedIn {
            user,
            offline_token,
            session_key,
        })
    }

    async fn refresh_locked(&self) -> Result<String> {
        // Snapshot the session this refresh belongs to. The lock is
        // released for the HTTP call, and the session may be replaced
        // meanwhile (another sign-in, or sign-out) - the generation
        // tells us whether applying the result is still legitimate.
        let (refresh_token, generation, key) = {
            let guard = self.session.read().await;
            let session = guard
                .session
                .as_ref()
                .ok_or_else(|| Error::Unauthenticated("no signed-in user".to_string()))?;
            (
                session.refresh_token.clone(),
                guard.generation,
                session.key.clone(),
            )
        };
        let refreshed = match self.exchange_refresh_token(&refresh_token).await {
            Ok(refreshed) => refreshed,
            Err(Error::Unauthenticated(_)) => {
                // The refresh token is dead; drop the dead session so
                // callers see a clean signed-out state - but only if
                // it is still the session this refresh started from,
                // and clear the store while still holding the
                // lifecycle lock, so the clear cannot land after a
                // newer session has saved its credentials.
                let _lifecycle = self.lifecycle.lock().await;
                let still_current = {
                    let mut guard = self.session.write().await;
                    if guard.generation == generation {
                        guard.session = None;
                        guard.generation += 1;
                        true
                    } else {
                        false
                    }
                };
                if still_current {
                    if let Some(key) = &key {
                        let _ = self.remove_entry(key).await;
                    }
                }
                return Err(Error::Unauthenticated(
                    "refresh token rejected; sign in again".to_string(),
                ));
            }
            Err(err) => return Err(err),
        };
        // Apply phase: check + mutate + persist under the lifecycle
        // lock, so the save is ordered with every other transition's
        // save/clear (a stale refresh must never persist its tokens
        // over a newer sign-in's).
        let _lifecycle = self.lifecycle.lock().await;
        let mut guard = self.session.write().await;
        if guard.generation != generation {
            // The session changed while the refresh was in flight.
            // The refreshed tokens belong to the previous session and
            // must not overwrite (or be persisted over) the current
            // one; hand back the current session's token instead.
            return match guard.session.as_ref() {
                Some(session) => Ok(session.id_token.clone()),
                None => Err(Error::Unauthenticated("no signed-in user".to_string())),
            };
        }
        let session = guard
            .session
            .as_mut()
            .ok_or_else(|| Error::Unauthenticated("no signed-in user".to_string()))?;
        let expires_at = SystemTime::now() + Duration::from_secs(refreshed.expires_in_secs());
        session.id_token = refreshed.id_token.clone();
        session.refresh_token = refreshed.refresh_token.clone();
        session.expires_at = expires_at;
        session.last_refresh_at = SystemTime::now();
        let entry = session.stored();
        let key = session.key.clone();
        if let (Some(store), Some(key)) = (&self.store, &key) {
            let mut sessions = self.load_sessions().await?;
            sessions.sessions.insert(key.as_str().to_string(), entry);
            store.save(&sessions.to_bytes()).await?;
        }
        Ok(refreshed.id_token)
    }
}

#[async_trait]
impl TokenProvider for Auth {
    async fn id_token(&self, force_refresh: bool) -> Result<String> {
        if !force_refresh {
            let guard = self.session.read().await;
            if let Some(session) = guard.session.as_ref() {
                if !session.needs_refresh(SystemTime::now()) {
                    return Ok(session.id_token.clone());
                }
            }
        }
        self.refresh_locked().await
    }

    async fn uid(&self) -> Option<String> {
        self.session
            .read()
            .await
            .session
            .as_ref()
            .map(|session| session.user.uid.clone())
    }
}

/// Formats a reqwest failure with its full source chain, so transport
/// problems (proxy, TLS, DNS) stay diagnosable from the message alone.
fn http_error(err: reqwest::Error) -> Error {
    let mut message = err.to_string();
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&err);
    while let Some(err) = source {
        message.push_str(": ");
        message.push_str(&err.to_string());
        source = err.source();
    }
    Error::Http(message)
}

/// Minimal percent-encoding for form-style post bodies.
fn url_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_encode_escapes_reserved() {
        assert_eq!(url_encode("a b+c/d="), "a%20b%2Bc%2Fd%3D");
        assert_eq!(url_encode("google.com"), "google.com");
    }

    #[tokio::test]
    async fn id_token_without_session_is_unauthenticated() {
        let app = App::new(firebase_native_core::FirebaseOptions::new("key", "demo"));
        let auth = Auth::new(&app);
        match auth.id_token(false).await {
            Err(Error::Unauthenticated(_)) => {}
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(auth.current_user().await, None);
    }
}

#[cfg(test)]
mod race_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_app() -> App {
        App::new(FirebaseOptions::new("test-key", "demo-project"))
    }

    /// One-shot HTTP stub for the Secure Token endpoint: reads the
    /// request, waits, then returns a canned refresh response.
    async fn stub_secure_token(delay_ms: u64, body: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
        format!("http://{addr}/token")
    }

    fn expired_session(uid: &str, refresh_token: &str) -> Session {
        Session {
            user: User {
                uid: uid.to_string(),
                email: None,
                display_name: None,
            },
            id_token: format!("token-{uid}-old"),
            refresh_token: refresh_token.to_string(),
            expires_at: SystemTime::now() - Duration::from_secs(10),
            last_refresh_at: SystemTime::now() - Duration::from_secs(3600),
            key: None,
        }
    }

    #[tokio::test]
    async fn refresh_result_does_not_overwrite_a_newer_session() {
        let url = stub_secure_token(
            300,
            r#"{"id_token":"token-a-new","refresh_token":"refresh-a-new","expires_in":"3600","user_id":"uid-a"}"#,
        )
        .await;
        let auth = Arc::new(Auth::new(&test_app()).with_secure_token_url(url));
        {
            let mut state = auth.session.write().await;
            state.session = Some(expired_session("uid-a", "refresh-a-old"));
        }
        // Start user A's refresh; it stays in flight for ~300 ms.
        let refresher = {
            let auth = auth.clone();
            tokio::spawn(async move { auth.force_refresh().await })
        };
        // Meanwhile user B signs in, replacing the session.
        tokio::time::sleep(Duration::from_millis(50)).await;
        auth.adopt_sign_in(SignInResponse {
            local_id: Some("uid-b".to_string()),
            email: None,
            display_name: None,
            id_token: "token-b".to_string(),
            refresh_token: "refresh-b".to_string(),
            expires_in: "3600".to_string(),
        })
        .await
        .unwrap();
        let _ = refresher.await.unwrap();

        // B's session must be fully intact: user, tokens, everything.
        let user = auth.current_user().await.unwrap();
        assert_eq!(user.uid, "uid-b");
        let state = auth.session.read().await;
        let session = state.session.as_ref().unwrap();
        assert_eq!(session.id_token, "token-b");
        assert_eq!(session.refresh_token, "refresh-b");
    }

    #[tokio::test]
    async fn delayed_restore_does_not_resurrect_after_sign_out() {
        use firebase_native_core::MemoryCredentialStore;

        let url = stub_secure_token(
            300,
            r#"{"id_token":"token-a-new","refresh_token":"refresh-a-new","expires_in":"3600","user_id":"uid-a"}"#,
        )
        .await;
        let store = Arc::new(MemoryCredentialStore::new());
        let key = SessionKey::from_offline_token("offline-token-a");
        let mut sessions = StoredSessions::from_bytes(&[]);
        sessions.sessions.insert(
            key.as_str().to_string(),
            expired_session("uid-a", "refresh-a-old").stored(),
        );
        store.save(&sessions.to_bytes()).await.unwrap();
        let credential_store: Arc<dyn CredentialStore> = store.clone();
        let auth = Arc::new(
            Auth::with_store(&test_app(), Some(credential_store)).with_secure_token_url(url),
        );
        // Start the restore; its token exchange stays in flight.
        let restorer = {
            let auth = auth.clone();
            tokio::spawn(async move { auth.restore_session("offline-token-a").await })
        };
        // Sign-out completes while the restore is still pending.
        tokio::time::sleep(Duration::from_millis(50)).await;
        auth.sign_out().await.unwrap();
        let restored = restorer.await.unwrap().unwrap();

        // The stale restore must neither install its session nor
        // re-save the exchanged tokens over the stored entry.
        assert!(!restored);
        assert!(auth.current_user().await.is_none());
        let blob = store.load().await.unwrap().expect("entry must remain");
        let sessions = StoredSessions::from_bytes(&blob);
        let entry = sessions.sessions.get(key.as_str()).unwrap();
        assert_eq!(entry.refresh_token, "refresh-a-old");
    }

    #[tokio::test]
    async fn refresh_does_not_overwrite_newer_sign_in_in_store() {
        use firebase_native_core::MemoryCredentialStore;

        let url = stub_secure_token(
            300,
            r#"{"id_token":"token-a-new","refresh_token":"refresh-a-new","expires_in":"3600","user_id":"uid-a"}"#,
        )
        .await;
        let store = Arc::new(MemoryCredentialStore::new());
        let credential_store: Arc<dyn CredentialStore> = store.clone();
        let auth = Arc::new(
            Auth::with_store(&test_app(), Some(credential_store)).with_secure_token_url(url),
        );
        {
            let mut state = auth.session.write().await;
            state.session = Some(expired_session("uid-a", "refresh-a-old"));
        }
        let refresher = {
            let auth = auth.clone();
            tokio::spawn(async move { auth.force_refresh().await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        auth.adopt_sign_in(SignInResponse {
            local_id: Some("uid-b".to_string()),
            email: None,
            display_name: None,
            id_token: "token-b".to_string(),
            refresh_token: "refresh-b".to_string(),
            expires_in: "3600".to_string(),
        })
        .await
        .unwrap();
        let _ = refresher.await.unwrap();

        // Memory says B, and the persisted collection must also
        // hold B - the stale refresh must not save A's tokens
        // last (A's session has no key, so it persists nowhere).
        assert_eq!(auth.current_user().await.unwrap().uid, "uid-b");
        let blob = store.load().await.unwrap().expect("B must be persisted");
        let sessions = StoredSessions::from_bytes(&blob);
        assert_eq!(sessions.sessions.len(), 1);
        let entry = sessions.sessions.values().next().unwrap();
        assert_eq!(entry.user().uid, "uid-b");
        assert_eq!(entry.refresh_token, "refresh-b");
    }
}

#[cfg(test)]
mod p5_tests {
    use super::*;
    use crate::session::StoredSession;
    use firebase_native_core::MemoryCredentialStore;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_app() -> App {
        App::new(FirebaseOptions::new("test-key", "demo-project"))
    }

    /// One-shot HTTP stub answering with the given status/body.
    async fn stub_token_endpoint(status: u16, body: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let reason = if status == 200 { "OK" } else { "Bad Request" };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
        format!("http://{addr}/token")
    }

    fn entry(uid: &str, refresh: &str, cached_id: Option<&str>, expires_in: i64) -> StoredSession {
        let now = crate::session::unix_seconds(SystemTime::now());
        StoredSession {
            uid: uid.to_string(),
            email: None,
            display_name: None,
            refresh_token: refresh.to_string(),
            id_token: cached_id.map(str::to_string),
            id_token_expires_at: cached_id.map(|_| now + expires_in),
            last_refresh_at: Some(now - 1800),
        }
    }

    /// A token endpoint that answers after a delay, so a test
    /// can interleave a sign-out with an in-flight restore.
    async fn stub_token_endpoint_delayed(status: u16, body: &'static str, delay_ms: u64) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            let reason = if status == 200 { "OK" } else { "Bad Request" };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
        format!("http://{addr}/token")
    }

    async fn seeded_auth(
        secure_token_url: String,
        entries: Vec<(&str, StoredSession)>,
    ) -> (Auth, Arc<MemoryCredentialStore>) {
        let store = Arc::new(MemoryCredentialStore::new());
        let mut sessions = StoredSessions::from_bytes(&[]);
        for (token, entry) in entries {
            let key = SessionKey::from_offline_token(token);
            sessions.sessions.insert(key.as_str().to_string(), entry);
        }
        store.save(&sessions.to_bytes()).await.unwrap();
        let credential_store: Arc<dyn CredentialStore> = store.clone();
        let auth = Auth::with_store(&test_app(), Some(credential_store))
            .with_secure_token_url(secure_token_url);
        (auth, store)
    }

    #[tokio::test]
    async fn restore_online_updates_the_stored_entry() {
        let url = stub_token_endpoint(
            200,
            r#"{"id_token":"id-new","refresh_token":"refresh-new","expires_in":"3600","user_id":"uid-a"}"#,
        )
        .await;
        let (auth, _store) = seeded_auth(
            url,
            vec![("tok-a", entry("uid-a", "refresh-old", Some("id-old"), 60))],
        )
        .await;
        let before = crate::session::unix_seconds(SystemTime::now());
        assert!(auth.restore_session("tok-a").await.unwrap());
        assert_eq!(auth.current_user().await.unwrap().uid, "uid-a");
        // The entry now holds the rotated refresh token, the
        // fresh ID cache, and a current last-refresh time.
        let key = SessionKey::from_offline_token("tok-a");
        let facts = auth.stored_session_facts(&key).await.unwrap().unwrap();
        assert_eq!(facts.uid, "uid-a");
        assert!(facts.last_refresh_at.unwrap() >= crate::session::from_unix_seconds(before));
        let sessions = auth.load_sessions().await.unwrap();
        let stored = sessions.sessions.get(key.as_str()).unwrap();
        assert_eq!(stored.refresh_token, "refresh-new");
        assert_eq!(stored.id_token.as_deref(), Some("id-new"));
        // The active session reports the same facts.
        assert_eq!(auth.session_facts().await.unwrap().uid, "uid-a");
    }

    #[tokio::test]
    async fn restore_offline_falls_back_to_the_id_cache() {
        // Nothing listens on port 1: the exchange fails at the
        // transport level, so the cached ID token activates.
        let (auth, _store) = seeded_auth(
            "http://127.0.0.1:1/token".to_string(),
            vec![(
                "tok-a",
                entry("uid-a", "refresh-old", Some("id-cached"), 3600),
            )],
        )
        .await;
        assert!(auth.restore_session("tok-a").await.unwrap());
        assert_eq!(auth.current_user().await.unwrap().uid, "uid-a");
        assert_eq!(auth.id_token(false).await.unwrap(), "id-cached");
        // Facts still show the old last-refresh time: no server
        // contact happened.
        let key = SessionKey::from_offline_token("tok-a");
        let facts = auth.stored_session_facts(&key).await.unwrap().unwrap();
        let now = crate::session::unix_seconds(SystemTime::now());
        let last = crate::session::unix_seconds(facts.last_refresh_at.unwrap());
        assert!(
            now - last >= 1700,
            "last refresh must stay old: {last} vs {now}"
        );
    }

    #[tokio::test]
    async fn restore_rejected_removes_the_entry() {
        let url = stub_token_endpoint(400, r#"{"error":{"message":"TOKEN_EXPIRED"}}"#).await;
        let (auth, store) = seeded_auth(
            url,
            vec![("tok-a", entry("uid-a", "refresh-dead", Some("id-old"), 60))],
        )
        .await;
        assert!(!auth.restore_session("tok-a").await.unwrap());
        assert!(auth.current_user().await.is_none());
        assert!(auth.stored_session_keys().await.unwrap().is_empty());
        assert!(store.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn targeted_sign_out_during_a_pending_restore_prevents_resurrection() {
        let url = stub_token_endpoint_delayed(
            200,
            r#"{"id_token":"id-new","refresh_token":"refresh-new","expires_in":"3600","user_id":"uid-a"}"#,
            400,
        )
        .await;
        let (auth, store) = seeded_auth(
            url,
            vec![("tok-a", entry("uid-a", "refresh-old", Some("id-old"), 3600))],
        )
        .await;
        let auth = std::sync::Arc::new(auth);
        let restore = {
            let auth = auth.clone();
            tokio::spawn(async move { auth.restore_session("tok-a").await })
        };
        // The exchange is in flight (the stub answers after
        // 400 ms); the targeted sign-out lands meanwhile. The
        // session is not active yet, so only the stored-entry
        // deletion and the restore-side re-check can stop it.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let key = SessionKey::from_offline_token("tok-a");
        assert!(auth.sign_out_session(&key).await.unwrap());
        assert!(auth.stored_session_keys().await.unwrap().is_empty());
        // The restore must not resurrect the deleted session:
        // no activation, and no re-persisted entry.
        assert!(!restore.await.unwrap().unwrap());
        assert!(auth.current_user().await.is_none());
        assert!(store.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn restore_with_unknown_token_is_false() {
        let (auth, _store) = seeded_auth(
            "http://127.0.0.1:1/token".to_string(),
            vec![(
                "tok-a",
                entry("uid-a", "refresh-old", Some("id-cached"), 3600),
            )],
        )
        .await;
        assert!(!auth.restore_session("tok-other").await.unwrap());
        assert!(auth.current_user().await.is_none());
    }

    #[tokio::test]
    async fn sign_out_session_removes_only_the_target() {
        let (auth, _store) = seeded_auth(
            "http://127.0.0.1:1/token".to_string(),
            vec![
                ("tok-a", entry("uid-a", "refresh-a", None, 0)),
                ("tok-b", entry("uid-b", "refresh-b", None, 0)),
            ],
        )
        .await;
        let key_a = SessionKey::from_offline_token("tok-a");
        let key_b = SessionKey::from_offline_token("tok-b");
        assert!(auth.sign_out_session(&key_a).await.unwrap());
        assert_eq!(auth.stored_session_keys().await.unwrap(), vec![key_b]);
        // A second call removes nothing.
        assert!(!auth.sign_out_session(&key_a).await.unwrap());
    }

    #[tokio::test]
    async fn sign_out_removes_the_active_entry_and_keeps_others() {
        let (auth, _store) = seeded_auth(
            "http://127.0.0.1:1/token".to_string(),
            vec![("tok-b", entry("uid-b", "refresh-b", None, 0))],
        )
        .await;
        let signed_in = auth
            .adopt_sign_in(crate::wire::SignInResponse {
                local_id: Some("uid-a".to_string()),
                email: None,
                display_name: None,
                id_token: "id-a".to_string(),
                refresh_token: "refresh-a".to_string(),
                expires_in: "3600".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(auth.stored_session_keys().await.unwrap().len(), 2);
        auth.sign_out().await.unwrap();
        assert!(auth.current_user().await.is_none());
        let keys = auth.stored_session_keys().await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_ne!(keys[0], signed_in.session_key);
    }

    #[tokio::test]
    async fn sign_out_session_invalidates_the_active_session() {
        let (auth, _store) = seeded_auth("http://127.0.0.1:1/token".to_string(), vec![]).await;
        let signed_in = auth
            .adopt_sign_in(crate::wire::SignInResponse {
                local_id: Some("uid-a".to_string()),
                email: None,
                display_name: None,
                id_token: "id-a".to_string(),
                refresh_token: "refresh-a".to_string(),
                expires_in: "3600".to_string(),
            })
            .await
            .unwrap();
        assert!(auth.current_user().await.is_some());
        assert!(auth.sign_out_session(&signed_in.session_key).await.unwrap());
        assert!(auth.current_user().await.is_none());
        assert!(auth.stored_session_keys().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn wipe_all_sessions_clears_store_and_memory() {
        let (auth, store) = seeded_auth(
            "http://127.0.0.1:1/token".to_string(),
            vec![("tok-b", entry("uid-b", "refresh-b", None, 0))],
        )
        .await;
        auth.adopt_sign_in(crate::wire::SignInResponse {
            local_id: Some("uid-a".to_string()),
            email: None,
            display_name: None,
            id_token: "id-a".to_string(),
            refresh_token: "refresh-a".to_string(),
            expires_in: "3600".to_string(),
        })
        .await
        .unwrap();
        auth.wipe_all_sessions().await.unwrap();
        assert!(auth.current_user().await.is_none());
        assert!(auth.stored_session_keys().await.unwrap().is_empty());
        assert!(store.load().await.unwrap().is_none());
    }
}
