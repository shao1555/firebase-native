//! Live check of the P3 Storage client against a real project.
//!
//! Signs in with a custom token, then drives [`Storage`]
//! through media and multipart uploads, a resumable upload
//! large enough to span two chunks, download (authenticated
//! and via the token URL), metadata read/patch, listing, and
//! delete — under `users/{uid}/attachments/`, the layout the
//! target project's rules allow. Configuration comes from the
//! environment:
//!
//! - `FIREBASE_API_KEY` — project Web API key (required)
//! - `FIREBASE_PROJECT_ID` — project ID (required)
//! - `FIREBASE_CUSTOM_TOKEN` — a custom token from a trusted
//!   server (required)
//! - `FIREBASE_STORAGE_BUCKET` — bucket name (required)
//!
//! Run with: `cargo run --example storage`

use std::env;
use std::process::ExitCode;
use std::sync::Arc;

use firebase_native::auth::Auth;
use firebase_native::storage::PutMetadata;
use firebase_native::{App, FirebaseOptions, Storage};

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
        .with_storage_bucket(required("FIREBASE_STORAGE_BUCKET")?);
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

        let storage = Storage::new(&app, auth.clone());
        let run_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        // The project's rules allow exactly
        // users/{uid}/attachments/{attachmentId}/{fileName}, so
        // the run id rides in the attachmentId segment.
        let dir = storage.reference(&format!("users/{}/attachments/p3check-{run_id}", user.uid));

        // 1. Media upload + authenticated download.
        let hello = dir.child("hello.txt");
        let uploaded = hello
            .put_bytes(b"hello storage".to_vec(), None)
            .await
            .map_err(|err| format!("media upload failed: {err}"))?;
        println!(
            "media upload: name={} size={} generation={:?}",
            uploaded.name, uploaded.size, uploaded.generation
        );
        if uploaded.size != 13 {
            return Err("media upload reported the wrong size".to_string());
        }
        let downloaded = hello
            .get_bytes()
            .await
            .map_err(|err| format!("download failed: {err}"))?;
        if downloaded != b"hello storage" {
            return Err("downloaded bytes differ from uploaded bytes".to_string());
        }
        println!("download matches");

        // 2. Multipart upload with metadata, then a patch.
        let noted = dir.child("noted.txt");
        let mut metadata = PutMetadata {
            content_type: Some("text/plain".to_string()),
            ..Default::default()
        };
        metadata
            .custom_metadata
            .insert("origin".to_string(), "p3-live-check".to_string());
        let uploaded = noted
            .put_bytes(b"with metadata".to_vec(), Some(metadata))
            .await
            .map_err(|err| format!("multipart upload failed: {err}"))?;
        if uploaded.content_type.as_deref() != Some("text/plain")
            || uploaded.custom_metadata.get("origin").map(String::as_str) != Some("p3-live-check")
        {
            return Err(format!("multipart metadata not stored: {uploaded:?}"));
        }
        println!("multipart upload with metadata: ok");
        let patched = noted
            .update_metadata(PutMetadata {
                cache_control: Some("max-age=60".to_string()),
                ..Default::default()
            })
            .await
            .map_err(|err| format!("metadata patch failed: {err}"))?;
        let fetched = noted
            .get_metadata()
            .await
            .map_err(|err| format!("metadata read failed: {err}"))?;
        if fetched != patched {
            return Err("patched metadata does not read back".to_string());
        }
        println!("metadata patch + read: ok");

        // 3. Resumable upload across two chunks (8 MiB + tail).
        let big = dir.child("big.bin");
        let payload: Vec<u8> = (0..(8 * 1024 * 1024 + 12345))
            .map(|i| (i % 251) as u8)
            .collect();
        let uploaded = big
            .put_bytes_resumable(payload.clone(), None)
            .await
            .map_err(|err| format!("resumable upload failed: {err}"))?;
        if uploaded.size as usize != payload.len() {
            return Err("resumable upload reported the wrong size".to_string());
        }
        let downloaded = big
            .get_bytes()
            .await
            .map_err(|err| format!("resumable download failed: {err}"))?;
        if downloaded != payload {
            return Err("resumable round trip bytes differ".to_string());
        }
        println!("resumable upload ({} bytes, two chunks): ok", payload.len());

        // 4. Listing: the three objects are under the check
        // dir. This project's rules grant per-file access only
        // (attachments are tracked in Firestore and addressed
        // by ID; nothing lists), so a PermissionDenied here is
        // the rules speaking, not a client failure — either
        // outcome verifies the path (a success is checked for
        // the three names).
        match dir.list_all(None).await {
            Ok(listing) => {
                let names: Vec<&str> = listing.items.iter().map(|item| item.name()).collect();
                println!("list: {names:?}");
                for expected in ["hello.txt", "noted.txt", "big.bin"] {
                    if !names.contains(&expected) {
                        return Err(format!("list is missing {expected}"));
                    }
                }
            }
            Err(firebase_native::Error::PermissionDenied(_)) => {
                println!("list: denied by this project's rules (expected)");
            }
            Err(err) => return Err(format!("list failed: {err}")),
        }

        // 5. The token download URL works without credentials.
        let url = hello
            .download_url()
            .await
            .map_err(|err| format!("download url failed: {err}"))?;
        let body = reqwest::get(&url)
            .await
            .map_err(|err| format!("download url fetch failed: {err}"))?
            .bytes()
            .await
            .map_err(|err| format!("download url body failed: {err}"))?;
        if body.as_ref() != b"hello storage" {
            return Err("download url returned different bytes".to_string());
        }
        println!("download url: ok");

        // 6. Delete everything; reads then report NotFound.
        for reference in [&hello, &noted, &big] {
            reference
                .delete()
                .await
                .map_err(|err| format!("delete failed: {err}"))?;
        }
        match hello.get_metadata().await {
            Err(firebase_native::Error::NotFound(_)) => {}
            other => return Err(format!("expected NotFound after delete, got {other:?}")),
        }
        println!("delete + NotFound: ok");
        println!("STORAGE LIVE CHECK PASSED");
        Ok::<(), String>(())
    };
    match run.await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}
