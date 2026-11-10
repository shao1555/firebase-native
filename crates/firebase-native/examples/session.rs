//! Session lifecycle live check (P5): sign-in issuing an
//! offline token, cross-process session activation (online and
//! simulated-offline), hash-addressed sign-out, and the wipe.
//!
//! A file stands in for the OS secure store (the example's own
//! `CredentialStore`); the blob path is `FIREBASE_SESSION_BLOB`.
//!
//! Modes (`FIREBASE_SESSION_MODE`):
//! - `signin` (default): signs in with `FIREBASE_CUSTOM_TOKEN`
//!   and prints the offline token and session key.
//! - `restore`: activates the session named by
//!   `FIREBASE_OFFLINE_TOKEN`. With `FIREBASE_SIMULATE_OFFLINE=1`
//!   the Secure Token endpoint is pointed at a dead port, so
//!   activation must come from the persisted ID-token cache.
//! - `signout`: removes the session named by
//!   `FIREBASE_SESSION_KEY` (the hash) without activating it.
//! - `wipe`: wipes all stored sessions.
//!
//! Required env: FIREBASE_API_KEY, FIREBASE_PROJECT_ID,
//! FIREBASE_SESSION_BLOB; plus FIREBASE_CUSTOM_TOKEN (signin),
//! FIREBASE_OFFLINE_TOKEN (restore) or FIREBASE_SESSION_KEY
//! (signout).

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use firebase_native::auth::Auth;
use firebase_native::core::{App, CredentialStore, Error, FirebaseOptions, Result};

/// A file-backed CredentialStore for the live check (a real
/// shell would use the OS keychain instead).
struct FileStore {
    path: PathBuf,
}

#[async_trait]
impl CredentialStore for FileStore {
    async fn load(&self) -> Result<Option<Vec<u8>>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(Error::Io(error.to_string())),
        }
    }

    async fn save(&self, blob: &[u8]) -> Result<()> {
        std::fs::write(&self.path, blob).map_err(|error| Error::Io(error.to_string()))
    }

    async fn clear(&self) -> Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::Io(error.to_string())),
        }
    }
}

fn required(name: &str) -> Result<String> {
    env::var(name)
        .map_err(|_| Error::InvalidArgument(format!("environment variable {name} is required")))
}

#[tokio::main]
async fn main() -> Result<()> {
    let run = run_inner().await;
    match run {
        Ok(()) => Ok(()),
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(1);
        }
    }
}

async fn run_inner() -> Result<()> {
    let options = FirebaseOptions::new(
        required("FIREBASE_API_KEY")?,
        required("FIREBASE_PROJECT_ID")?,
    )
    .with_auth_domain(env::var("FIREBASE_AUTH_DOMAIN").unwrap_or_default());
    let app = App::new(options);
    let blob = required("FIREBASE_SESSION_BLOB")?;
    let store: Arc<dyn CredentialStore> = Arc::new(FileStore {
        path: PathBuf::from(blob),
    });
    let mut auth = Auth::with_store(&app, Some(store));
    if env::var("FIREBASE_SIMULATE_OFFLINE").is_ok() {
        auth = auth.with_secure_token_url("http://127.0.0.1:1/token");
    }
    let auth = Arc::new(auth);
    let mode = env::var("FIREBASE_SESSION_MODE").unwrap_or_else(|_| "signin".to_string());
    match mode.as_str() {
        "signin" => {
            let signed_in = auth
                .sign_in_with_custom_token(&required("FIREBASE_CUSTOM_TOKEN")?)
                .await?;
            println!("signed in: uid={}", signed_in.user.uid);
            println!("offline_token={}", signed_in.offline_token);
            println!("session_key={}", signed_in.session_key);
        }
        "restore" => {
            let restored = auth
                .restore_session(&required("FIREBASE_OFFLINE_TOKEN")?)
                .await?;
            println!("restored: {restored}");
            if restored {
                let user = auth.current_user().await.expect("restored user");
                println!("user: uid={}", user.uid);
                let facts = auth.session_facts().await.expect("session facts");
                println!(
                    "facts: expires={:?} last_refresh={:?}",
                    facts.id_token_expires_at, facts.last_refresh_at
                );
            }
        }
        "signout" => {
            // Keys come from the crate (sign-in results and the
            // stored-key listing); the caller matches the one
            // its registry recorded.
            let keys = auth.stored_session_keys().await?;
            println!("stored keys before: {}", keys.len());
            let target = required("FIREBASE_SESSION_KEY")?;
            let key = keys
                .iter()
                .find(|key| key.as_str() == target)
                .cloned()
                .ok_or_else(|| Error::NotFound("no such stored session".to_string()))?;
            let removed = auth.sign_out_session(&key).await?;
            println!("removed: {removed}");
            println!(
                "stored keys after: {}",
                auth.stored_session_keys().await?.len()
            );
        }
        "wipe" => {
            auth.wipe_all_sessions().await?;
            println!(
                "stored keys after wipe: {}",
                auth.stored_session_keys().await?.len()
            );
        }
        other => {
            return Err(Error::InvalidArgument(format!(
                "unknown FIREBASE_SESSION_MODE: {other}"
            )));
        }
    }
    Ok(())
}
