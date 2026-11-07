use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{Mutex, RwLock};

use firebase_native_core::{App, CredentialStore, Error, FirebaseOptions, Result, TokenProvider};

use crate::session::{Session, StoredSession, User};
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

    /// Creates an Auth client that persists the refresh-token session
    /// through the given store. Pass `None` for the fail-closed mode:
    /// nothing is persisted and the user signs in on every launch.
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
    pub async fn sign_in_with_custom_token(&self, custom_token: &str) -> Result<User> {
        let body = json!({
            "token": custom_token,
            "returnSecureToken": true,
        });
        let response: SignInResponse = self.accounts_call("signInWithCustomToken", body).await?;
        self.adopt_sign_in(response).await
    }

    /// Signs in with email and password.
    pub async fn sign_in_with_email_password(&self, email: &str, password: &str) -> Result<User> {
        let body = json!({
            "email": email,
            "password": password,
            "returnSecureToken": true,
        });
        let response: SignInResponse = self.accounts_call("signInWithPassword", body).await?;
        self.adopt_sign_in(response).await
    }

    /// Creates a new email/password account and signs in.
    pub async fn sign_up_with_email_password(&self, email: &str, password: &str) -> Result<User> {
        let body = json!({
            "email": email,
            "password": password,
            "returnSecureToken": true,
        });
        let response: SignInResponse = self.accounts_call("signUp", body).await?;
        self.adopt_sign_in(response).await
    }

    /// Signs in anonymously.
    pub async fn sign_in_anonymously(&self) -> Result<User> {
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
    ) -> Result<User> {
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

    /// Restores a persisted session from the `CredentialStore`, if one
    /// was configured and holds a session. Exchanges the stored refresh
    /// token for a fresh ID token. Returns `true` when a session is now
    /// active.
    pub async fn restore_session(&self) -> Result<bool> {
        let Some(store) = &self.store else {
            return Ok(false);
        };
        // Snapshot the generation before the (unlocked) load and
        // exchange. If any session transition happens meanwhile - a
        // sign-in, or a sign-out - this restore is stale and must
        // neither persist nor install its session: a delayed restore
        // must never resurrect a signed-out user or replace a newer
        // sign-in.
        let generation = self.session.read().await.generation;
        let Some(bytes) = store.load().await? else {
            return Ok(false);
        };
        let Some(stored) = StoredSession::from_bytes(&bytes) else {
            return Ok(false);
        };
        let user = stored.user();
        let refreshed = self.exchange_refresh_token(&stored.refresh_token).await?;
        let expires_at = SystemTime::now() + Duration::from_secs(refreshed.expires_in_secs());
        let session = Session {
            user: User {
                uid: refreshed.user_id.clone(),
                ..user
            },
            id_token: refreshed.id_token.clone(),
            refresh_token: refreshed.refresh_token.clone(),
            expires_at,
        };
        // Apply phase: re-check under the lifecycle lock, then
        // persist + install atomically with respect to other
        // transitions. The exchange may have rotated the refresh
        // token, so the replacement is what gets persisted; without
        // this save, the next launch would restore with the old
        // (possibly already rotated-out) token.
        let _lifecycle = self.lifecycle.lock().await;
        let mut state = self.session.write().await;
        if state.generation != generation || state.session.is_some() {
            return Ok(false);
        }
        store.save(&session.stored().to_bytes()).await?;
        state.session = Some(session);
        state.generation += 1;
        Ok(true)
    }

    /// Signs out: drops the in-memory session and clears the store.
    pub async fn sign_out(&self) -> Result<()> {
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

    async fn adopt_sign_in(&self, response: SignInResponse) -> Result<User> {
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
        let session = Session {
            user: user.clone(),
            id_token: response.id_token,
            refresh_token: response.refresh_token,
            expires_at,
        };
        let _lifecycle = self.lifecycle.lock().await;
        if let Some(store) = &self.store {
            store.save(&session.stored().to_bytes()).await?;
        }
        let mut state = self.session.write().await;
        state.session = Some(session);
        state.generation += 1;
        Ok(user)
    }

    async fn refresh_locked(&self) -> Result<String> {
        // Snapshot the session this refresh belongs to. The lock is
        // released for the HTTP call, and the session may be replaced
        // meanwhile (another sign-in, or sign-out) - the generation
        // tells us whether applying the result is still legitimate.
        let (refresh_token, generation) = {
            let guard = self.session.read().await;
            let session = guard
                .session
                .as_ref()
                .ok_or_else(|| Error::Unauthenticated("no signed-in user".to_string()))?;
            (session.refresh_token.clone(), guard.generation)
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
                    if let Some(store) = &self.store {
                        let _ = store.clear().await;
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
        if let Some(store) = &self.store {
            store.save(&session.stored().to_bytes()).await?;
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
        store
            .save(
                &expired_session("uid-a", "refresh-a-old")
                    .stored()
                    .to_bytes(),
            )
            .await
            .unwrap();
        let credential_store: Arc<dyn CredentialStore> = store.clone();
        let auth = Arc::new(
            Auth::with_store(&test_app(), Some(credential_store)).with_secure_token_url(url),
        );
        // Start the restore; its token exchange stays in flight.
        let restorer = {
            let auth = auth.clone();
            tokio::spawn(async move { auth.restore_session().await })
        };
        // Sign-out completes while the restore is still pending.
        tokio::time::sleep(Duration::from_millis(50)).await;
        auth.sign_out().await.unwrap();
        let restored = restorer.await.unwrap().unwrap();

        // The stale restore must neither install nor re-save A.
        assert!(!restored);
        assert!(auth.current_user().await.is_none());
        assert!(store.load().await.unwrap().is_none());
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

        // Memory says B, and the persisted blob must also be B -
        // the stale refresh must not save A's tokens last.
        assert_eq!(auth.current_user().await.unwrap().uid, "uid-b");
        let blob = store.load().await.unwrap().expect("B must be persisted");
        let stored = StoredSession::from_bytes(&blob).unwrap();
        assert_eq!(stored.user().uid, "uid-b");
        assert_eq!(stored.refresh_token, "refresh-b");
    }
}
