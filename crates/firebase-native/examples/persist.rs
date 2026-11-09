//! Live check of the P2 persistence path against a real project.
//!
//! Signs in with a custom token, then drives a persistent
//! [`Firestore`] through create → commit → verify → update →
//! delete, cross-checking the server state with a plain online
//! client at each step. Configuration comes from the environment:
//!
//! - `FIREBASE_API_KEY` — project Web API key (required)
//! - `FIREBASE_PROJECT_ID` — project ID (required)
//! - `FIREBASE_DATABASE_ID` — Firestore database ID (default `(default)`)
//! - `FIREBASE_CUSTOM_TOKEN` — a custom token from a trusted server
//!   (required)
//! - `FIREBASE_STORE_PATH` — where the local SQLite store lives
//!   (required)
//! - `FIREBASE_TEST_DOC_ID` — document id under `notes/` to use
//!   (default: generated)
//! - `FIREBASE_ENDPOINT` — gRPC endpoint override (e.g. an
//!   unreachable address for `seed` mode)
//! - `FIREBASE_PERSIST_MODE` — `full` (default), `seed` (queue a
//!   create and exit without committing) or `drain` (commit a
//!   previously seeded queue in a new process, verifying restart
//!   recovery)
//! - `FIREBASE_SPACE_ID` — value for the document's `spaceId` field
//!   (default `personal`); the target project's rules may require it
//!
//! Run with: `cargo run --example persist`

use std::env;
use std::process::ExitCode;
use std::sync::Arc;

use firebase_native::auth::Auth;
use firebase_native::firestore::{FieldOp, Fields, Filter, Firestore, Query, Value};
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

        let store_path = required("FIREBASE_STORE_PATH")?;
        let doc_id = env::var("FIREBASE_TEST_DOC_ID").unwrap_or_else(|_| {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            format!("p2-persist-check-{nanos}")
        });
        let path = format!("notes/{doc_id}");
        let space_id = env::var("FIREBASE_SPACE_ID").unwrap_or_else(|_| "personal".to_string());

        let mut persistent = Firestore::new(&app, auth.clone());
        if let Ok(endpoint) = env::var("FIREBASE_ENDPOINT") {
            persistent = persistent.with_endpoint(endpoint);
        }
        let persistent = persistent
            .enable_local_store(store_path.as_ref())
            .map_err(|err| format!("enable_local_store failed: {err}"))?;
        let online = Firestore::new(&app, auth.clone());
        let mode = env::var("FIREBASE_PERSIST_MODE").unwrap_or_else(|_| "full".to_string());
        if mode == "drain" {
            // A previous `seed` run queued a create against an
            // unreachable endpoint and exited. This process must
            // commit the surviving batch, verify it server-side,
            // and clean up.
            persistent
                .wait_for_pending_writes()
                .await
                .map_err(|err| format!("drain failed: {err}"))?;
            let server = online
                .get_document(&path)
                .await
                .map_err(|err| format!("drain online get failed: {err}"))?
                .ok_or("seeded document never reached the server")?;
            println!(
                "drained after restart: content={:?}",
                server.fields.get("content")
            );
            persistent
                .delete_document(&path)
                .await
                .map_err(|err| format!("drain delete failed: {err}"))?;
            persistent
                .wait_for_pending_writes()
                .await
                .map_err(|err| format!("drain delete wait failed: {err}"))?;
            println!("RESTART RECOVERY PASSED");
            return Ok(());
        }

        // 1. Create locally; the local view answers before any ack.
        let mut fields = Fields::new();
        fields.insert("spaceId".to_string(), Value::String(space_id.clone()));
        fields.insert(
            "title".to_string(),
            Value::String("firebase-native P2 persistence check".to_string()),
        );
        fields.insert(
            "content".to_string(),
            Value::String("created via the Write stream".to_string()),
        );
        let created = persistent
            .create_document("notes", Some(&doc_id), fields)
            .await
            .map_err(|err| format!("create failed: {err}"))?;
        println!("local view after create: {}", created.name);
        let local = persistent
            .get_document(&path)
            .await
            .map_err(|err| format!("local get failed: {err}"))?;
        println!("local get before ack: present={}", local.is_some());
        if mode == "seed" {
            // Exit without waiting: the batch stays queued in the
            // store (the endpoint points nowhere), to be committed
            // by a later process in `drain` mode.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            println!("SEEDED (batch left queued)");
            return Ok(());
        }

        // 2. Wait for the Write stream commit, then verify online.
        persistent
            .wait_for_pending_writes()
            .await
            .map_err(|err| format!("pending writes failed: {err}"))?;
        let server = online
            .get_document(&path)
            .await
            .map_err(|err| format!("online get failed: {err}"))?
            .ok_or("server does not have the created document")?;
        println!(
            "server after commit: update_time={:?} content={:?}",
            server.update_time,
            server.fields.get("content")
        );

        // 3. Update through the persistent client and re-verify.
        let mut update = Fields::new();
        update.insert(
            "content".to_string(),
            Value::String("updated via persistent set".to_string()),
        );
        persistent
            .set_document(&path, update)
            .await
            .map_err(|err| format!("set failed: {err}"))?;
        persistent
            .wait_for_pending_writes()
            .await
            .map_err(|err| format!("pending writes (set) failed: {err}"))?;
        let server = online
            .get_document(&path)
            .await
            .map_err(|err| format!("online get (set) failed: {err}"))?
            .ok_or("server lost the document after set")?;
        println!(
            "server after set: content={:?}",
            server.fields.get("content")
        );

        // 4. Delete through the persistent client and verify.
        persistent
            .delete_document(&path)
            .await
            .map_err(|err| format!("delete failed: {err}"))?;
        persistent
            .wait_for_pending_writes()
            .await
            .map_err(|err| format!("pending writes (delete) failed: {err}"))?;
        // Note: with rules that gate reads on the document's own
        // fields, a get for a just-deleted document is answered
        // PERMISSION_DENIED rather than NOT_FOUND (the read cannot
        // be authorized against a document that no longer exists).
        // Both outcomes prove the document is gone.
        let server = match online.get_document(&path).await {
            Ok(document) => document,
            Err(firebase_native::core::Error::PermissionDenied(_)) => None,
            Err(err) => return Err(format!("online get (delete) failed: {err}")),
        };
        println!("server after delete: present={}", server.is_some());
        if server.is_some() {
            return Err("document still present after delete".to_string());
        }

        // 5. Store-backed watch: recreate the document, watch it
        // through the persistent client, and confirm a local
        // update shows up in a snapshot (latency compensation
        // delivers it from the queued write; the stream echo
        // would too). The first snapshot is the cache snapshot.
        let mut fields = Fields::new();
        fields.insert("spaceId".to_string(), Value::String(space_id.clone()));
        fields.insert(
            "content".to_string(),
            Value::String("watched document".to_string()),
        );
        persistent
            .create_document("notes", Some(&doc_id), fields)
            .await
            .map_err(|err| format!("watch setup create failed: {err}"))?;
        persistent
            .wait_for_pending_writes()
            .await
            .map_err(|err| format!("watch setup wait failed: {err}"))?;
        let mut listener = persistent
            .watch_document(&path)
            .await
            .map_err(|err| format!("watch failed: {err}"))?;
        let first = tokio::time::timeout(std::time::Duration::from_secs(30), listener.next())
            .await
            .map_err(|_| "timed out waiting for the cache snapshot".to_string())?
            .ok_or("watch ended before the cache snapshot")?
            .map_err(|err| format!("cache snapshot failed: {err}"))?;
        println!("cache snapshot: {} document(s)", first.documents.len());
        if !first.metadata.is_from_cache {
            return Err("cache snapshot is not marked from-cache".to_string());
        }
        if first.metadata.has_pending_writes {
            return Err("cache snapshot unexpectedly reports pending writes".to_string());
        }
        println!("cache snapshot metadata: from-cache, no pending writes");
        if first.documents.len() != 1 {
            return Err("cache snapshot does not hold the watched document".to_string());
        }
        let mut update = Fields::new();
        update.insert("spaceId".to_string(), Value::String(space_id.clone()));
        update.insert(
            "content".to_string(),
            Value::String("updated while watched".to_string()),
        );
        persistent
            .set_document(&path, update)
            .await
            .map_err(|err| format!("watched set failed: {err}"))?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut saw_update = false;
        let mut saw_server = false;
        while std::time::Instant::now() < deadline && !saw_update {
            match tokio::time::timeout(std::time::Duration::from_secs(5), listener.next()).await {
                Ok(Some(Ok(snapshot))) => {
                    if !snapshot.metadata.is_from_cache && !saw_server {
                        saw_server = true;
                        println!("server snapshot received (initial sync complete)");
                    }
                    saw_update = snapshot.documents.iter().any(|document| {
                        document.fields.get("content")
                            == Some(&Value::String("updated while watched".to_string()))
                    });
                }
                Ok(Some(Err(err))) => return Err(format!("watch snapshot failed: {err}")),
                Ok(None) => return Err("watch ended before the update snapshot".to_string()),
                Err(_) => {}
            }
        }
        if !saw_update {
            return Err("no snapshot showed the local update".to_string());
        }
        println!("watch snapshot with the local update: received");
        // The initial server snapshot (and the write
        // acknowledgement, which clears the pending flag) are
        // metadata flips: they are delivered even when the view
        // itself does not move. Drain until one is observed.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !saw_server && std::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_secs(5), listener.next()).await {
                Ok(Some(Ok(snapshot))) => {
                    if !snapshot.metadata.is_from_cache {
                        saw_server = true;
                    }
                }
                Ok(Some(Err(err))) => return Err(format!("watch snapshot failed: {err}")),
                Ok(None) => break,
                Err(_) => {}
            }
        }
        if !saw_server {
            return Err("no server snapshot arrived during the watch phase".to_string());
        }
        println!("server snapshot received (initial sync complete)");
        drop(listener);
        persistent
            .delete_document(&path)
            .await
            .map_err(|err| format!("watch cleanup delete failed: {err}"))?;
        persistent
            .wait_for_pending_writes()
            .await
            .map_err(|err| format!("watch cleanup wait failed: {err}"))?;
        println!("WATCH LIVE CHECK PASSED");

        // 6. Limbo resolution: cache a document through the
        // persistent client, delete it on the server behind its
        // back, then watch its collection. The tokenless watch
        // seeds the document from the store and delivers it in
        // the cache snapshot; the server never re-confirms it,
        // so it must leave the delivered snapshots once its
        // document-target resolution settles as absent — it
        // must not linger as a stale cache entry.
        let limbo_id = format!("{doc_id}-limbo");
        let limbo_path = format!("notes/{limbo_id}");
        let mut fields = Fields::new();
        fields.insert("spaceId".to_string(), Value::String(space_id.clone()));
        fields.insert(
            "content".to_string(),
            Value::String("limbo candidate".to_string()),
        );
        online
            .create_document("notes", Some(&limbo_id), fields)
            .await
            .map_err(|err| format!("limbo setup create failed: {err}"))?;
        let cached = persistent
            .get_document(&limbo_path)
            .await
            .map_err(|err| format!("limbo cache get failed: {err}"))?;
        if cached.is_none() {
            return Err("limbo document was not cached".to_string());
        }
        online
            .delete_document(&limbo_path)
            .await
            .map_err(|err| format!("limbo server delete failed: {err}"))?;
        // The query is scoped to the space, as the project's
        // rules require (an unscoped collection query is denied:
        // rules are not filters).
        let mut listener = persistent
            .watch_query(
                "",
                Query::new("notes").filter(Filter::Field {
                    field: "spaceId".to_string(),
                    op: FieldOp::Equal,
                    value: Value::String(space_id.clone()),
                }),
            )
            .await
            .map_err(|err| format!("limbo watch failed: {err}"))?;
        let first = tokio::time::timeout(std::time::Duration::from_secs(30), listener.next())
            .await
            .map_err(|_| "timed out waiting for the limbo cache snapshot".to_string())?
            .ok_or("limbo watch ended before the cache snapshot")?
            .map_err(|err| format!("limbo cache snapshot failed: {err}"))?;
        if !first
            .documents
            .iter()
            .any(|document| document.name.ends_with(&limbo_id))
        {
            return Err("cache snapshot does not hold the limbo document".to_string());
        }
        println!("limbo: cache snapshot holds the server-deleted document");
        // Poke the collection so the stream produces barriers:
        // the resolution settles at one.
        let poke_id = format!("{doc_id}-poke");
        let mut fields = Fields::new();
        fields.insert("spaceId".to_string(), Value::String(space_id.clone()));
        fields.insert("content".to_string(), Value::String("poke".to_string()));
        online
            .create_document("notes", Some(&poke_id), fields)
            .await
            .map_err(|err| format!("limbo poke create failed: {err}"))?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut resolved = false;
        while std::time::Instant::now() < deadline && !resolved {
            match tokio::time::timeout(std::time::Duration::from_secs(5), listener.next()).await {
                Ok(Some(Ok(snapshot))) => {
                    resolved = !snapshot
                        .documents
                        .iter()
                        .any(|document| document.name.ends_with(&limbo_id));
                }
                Ok(Some(Err(err))) => {
                    return Err(format!("limbo watch snapshot failed: {err}"));
                }
                Ok(None) => return Err("limbo watch ended before the resolution".to_string()),
                Err(_) => {}
            }
        }
        if !resolved {
            return Err("limbo document never left the snapshots".to_string());
        }
        println!("limbo: resolution removed the server-deleted document");
        drop(listener);
        online
            .delete_document(&format!("notes/{poke_id}"))
            .await
            .map_err(|err| format!("limbo poke cleanup failed: {err}"))?;
        println!("LIMBO LIVE CHECK PASSED");
        println!("PERSIST LIVE CHECK PASSED");
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
