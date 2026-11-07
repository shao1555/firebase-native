//! Smoke test against a live Firebase project.
//!
//! Signs in with a custom token and reads one document. Configuration
//! comes from the environment (the Firebase web config values are
//! public identifiers; the custom token is a short-lived secret and
//! is read from the environment only):
//!
//! - `FIREBASE_API_KEY` — project Web API key (required)
//! - `FIREBASE_PROJECT_ID` — project ID (required)
//! - `FIREBASE_DATABASE_ID` — Firestore database ID (default `(default)`)
//! - `FIREBASE_CUSTOM_TOKEN` — a custom token minted by a trusted
//!   server, as issued after an OAuth 2.0 device flow (required)
//! - `FIREBASE_DOCUMENT_PATH` — relative document path to read,
//!   e.g. `notes/<id>` under the given database (required)
//!
//! Run with: `cargo run --example smoke`

use std::env;
use std::process::ExitCode;
use std::sync::Arc;

use firebase_native::auth::Auth;
use firebase_native::firestore::Firestore;
use firebase_native::{App, FirebaseOptions};

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("environment variable {name} is required"))
}

#[tokio::main]
async fn main() -> ExitCode {
    let run = async {
        let options = FirebaseOptions::new(
            required("FIREBASE_API_KEY")?,
            required("FIREBASE_PROJECT_ID")?,
        )
        .with_database_id(
            env::var("FIREBASE_DATABASE_ID").unwrap_or_else(|_| "(default)".to_string()),
        );
        let app = App::new(options);

        let auth = Arc::new(Auth::new(&app));
        let custom_token = required("FIREBASE_CUSTOM_TOKEN")?;
        let user = auth
            .sign_in_with_custom_token(&custom_token)
            .await
            .map_err(|err| format!("sign-in failed: {err}"))?;
        println!(
            "signed in: uid={} email={}",
            user.uid,
            user.email.as_deref().unwrap_or("-")
        );

        let firestore = Firestore::new(&app, auth.clone());
        let path = required("FIREBASE_DOCUMENT_PATH")?;
        let document = firestore
            .get_document(&path)
            .await
            .map_err(|err| format!("get_document failed: {err}"))?;
        match document {
            Some(document) => {
                println!("document: {}", document.name);
                for (key, value) in &document.fields {
                    println!("  {key}: {value:?}");
                }
            }
            None => println!("document not found: {path}"),
        }
        Ok::<(), String>(())
    };

    match run.await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("smoke: {message}");
            ExitCode::FAILURE
        }
    }
}
