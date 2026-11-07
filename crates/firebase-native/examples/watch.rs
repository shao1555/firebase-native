//! Live Listen smoke test against a real Firebase project.
//!
//! Signs in with a custom token, opens a document watch, and waits
//! for the initial snapshot (the server sends it once the target is
//! `CURRENT`). Read-only: nothing is written.
//!
//! Environment: the same variables as the `smoke` example, plus
//! `FIREBASE_WATCH_PATH` — the relative document path to watch
//! (required).
//!
//! Run with: `cargo run --example watch`

use std::env;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use firebase_native::auth::Auth;
use firebase_native::firestore::Firestore;
use firebase_native::{App, FirebaseOptions};

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("environment variable {name} is required"))
}

#[tokio::main]
async fn main() -> ExitCode {
    let run = async {
        let options = FirebaseOptions {
            api_key: required("FIREBASE_API_KEY")?,
            project_id: required("FIREBASE_PROJECT_ID")?,
            auth_domain: None,
            storage_bucket: None,
            app_id: None,
            database_id: env::var("FIREBASE_DATABASE_ID")
                .unwrap_or_else(|_| "(default)".to_string()),
        };
        let path = required("FIREBASE_WATCH_PATH")?;
        let app = App::new(options);
        let auth = Arc::new(Auth::new(&app));
        let user = auth
            .sign_in_with_custom_token(&required("FIREBASE_CUSTOM_TOKEN")?)
            .await
            .map_err(|e| e.to_string())?;
        println!(
            "signed in: uid={} email={}",
            user.uid,
            user.email.as_deref().unwrap_or("-")
        );

        let firestore = Firestore::new(&app, auth);
        let mut listener = firestore
            .watch_document(&path)
            .await
            .map_err(|e| e.to_string())?;
        let snapshot = tokio::time::timeout(Duration::from_secs(30), listener.next())
            .await
            .map_err(|_| "timed out waiting for the initial snapshot".to_string())?
            .ok_or_else(|| "watch ended before the initial snapshot".to_string())?
            .map_err(|e| e.to_string())?;
        println!(
            "initial snapshot: {} document(s), {} change(s), read_time={:?}",
            snapshot.documents.len(),
            snapshot.changes.len(),
            snapshot.read_time
        );
        for change in &snapshot.changes {
            println!("  {:?} {}", change.kind, change.document.name);
        }
        if let Some(document) = snapshot.documents.first() {
            println!("document: {}", document.name);
            if let Some(title) = document.fields.get("title") {
                println!("  title: {title:?}");
            }
        }
        Ok::<(), String>(())
    };
    match run.await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("watch: {error}");
            ExitCode::FAILURE
        }
    }
}
