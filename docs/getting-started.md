# Getting started

## Add the crates

```toml
[dependencies]
firebase-native = "0.1"   # umbrella crate: core, auth, firestore, storage
tokio = { version = "1", features = ["full"] }
```

The umbrella crate re-exports the four service crates
(`firebase_native::core`, `::auth`, `::firestore`,
`::storage`). Depend on the individual crates instead if you
only need one service. The minimum supported Rust version is
**1.75**.

## Configure the app

Every client is built from an `App`, which carries the
project configuration (`FirebaseOptions`). The API key and
project ID are required; the auth domain, the Firestore
database ID (for named databases) and the default Storage
bucket are set with builder methods:

```rust
use firebase_native::core::{App, FirebaseOptions};

let options = FirebaseOptions::new("api-key", "my-project")
    .with_auth_domain("my-project.firebaseapp.com")
    .with_database_id("mydb")            // omit for `(default)`
    .with_storage_bucket("my-project.firebasestorage.app");
let app = App::new(options);
```

## Sign in and call Firestore

`Auth` implements `TokenProvider`, so the same `Arc<Auth>` is
handed to the service clients as their token source. Tokens
are fetched **per call** through that trait — never captured
as a string at construction time — refreshed proactively
before expiry, and force-refreshed once if a call comes back
`UNAUTHENTICATED`.

```rust
use std::sync::Arc;
use firebase_native::auth::Auth;
use firebase_native::firestore::{Firestore, Value};

let auth = Arc::new(Auth::new(&app));
let signed_in = auth
    .sign_in_with_email_password("user@example.com", "password")
    .await?;
println!("uid: {}", signed_in.user.uid);

let firestore = Firestore::new(&app, auth.clone());
let document = firestore.get_document("cities/SF").await?;
if let Some(document) = document {
    if let Some(Value::String(name)) = document.fields.get("name") {
        println!("name: {name}");
    }
}
```

Without a credential store (`Auth::new`), the session lives
in memory only. To persist sessions across launches — and to
use the offline/multi-user session model — construct the
client with `Auth::with_store` and your own
`CredentialStore`; see [Auth and sessions](auth-and-sessions.md).

## Enable offline persistence (optional)

```rust
let firestore = Firestore::new(&app, auth.clone())
    .enable_local_store(std::path::Path::new("firestore.db"))?;
```

With a local store, writes queue on disk, reads and watches
are served from the local view with latency compensation,
and the queue drains in the background when the network is
available. For multi-user devices, place each user's store
under its own directory with
`firebase_native::firestore::user_store_path`. Details in
[Firestore](firestore.md).

## Storage

```rust
use firebase_native::storage::{PutMetadata, Storage};

let storage = Storage::new(&app, auth.clone());
let reference = storage.reference("users/uid/avatar.png");
let metadata = reference
    .put_bytes(png_bytes, Some(PutMetadata {
        content_type: Some("image/png".to_string()),
        ..Default::default()
    }))
    .await?;
let url = reference.download_url().await?;
```

See [Storage](storage.md) for resumable uploads, listing and
metadata updates.

## Errors

All operations return `firebase_native::core::Result<T>`,
whose error type is `firebase_native::core::Error`. Notable
variants: `Unauthenticated` (the credential itself was
rejected — as opposed to a transient failure), `NotFound`,
`PermissionDenied`, `InvalidArgument`, and `Io` for local
store problems. Match on the variant rather than the message
when deciding whether to retry, re-authenticate or surface
the failure.

## Runnable examples

`crates/firebase-native/examples/` contains complete
programs against a live project; each file's header lists
the environment variables it expects:

| Example | Shows |
|---|---|
| `smoke` | Sign-in + one document read |
| `watch` | A document watch with snapshot metadata |
| `persist` | Offline persistence across process restarts |
| `storage` | Upload / download / metadata / delete round-trip |
| `session` | The session lifecycle: issue, activate (online and offline), targeted sign-out |
