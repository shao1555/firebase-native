#![warn(missing_docs)]
//! Cloud Firestore for `firebase-native`, over gRPC (`tonic`).
//!
//! The public API is a hand-written model - [`Value`], [`Document`],
//! [`Query`] - and never exposes the generated `prost` types; an
//! internal conversion layer translates between the two. Tokens are
//! obtained per call through
//! [`firebase_native_core::TokenProvider`]; on an `UNAUTHENTICATED`
//! status the client forces one token refresh and retries the call
//! exactly once.
//!
//! # What the client covers
//!
//! - Unary document operations ([`Firestore::get_document`],
//!   [`Firestore::create_document`], [`Firestore::set_document`],
//!   [`Firestore::update_document`], [`Firestore::delete_document`],
//!   [`Firestore::list_documents`]) and structured queries
//!   ([`Firestore::run_query`]).
//! - Server-computed field values through the [`FieldValue`]
//!   sentinel API ([`Firestore::set_document_with`] /
//!   [`Firestore::update_document_with`]).
//! - Realtime watches ([`Firestore::watch_query`] /
//!   [`Firestore::watch_document`]) over the Listen stream, with
//!   resume tokens, production limbo resolution, and
//!   [`SnapshotMetadata`] (`is_from_cache` / `has_pending_writes`)
//!   on every [`Snapshot`].
//! - Offline persistence via [`Firestore::enable_local_store`]:
//!   writes queue in a local SQLite store, reads and watches are
//!   served from the composed local view with latency
//!   compensation, and the queue drains over the Write stream in
//!   the background, surviving restarts. Query evaluation is
//!   local-first against that view.
//! - Per-user store conventions ([`user_store_path`],
//!   [`wipe_user_store`], [`wipe_all_user_stores`]) so several
//!   users of one device keep separate caches.
//!
//! # Example
//!
//! ```text
//! use firebase_native_firestore::{Filter, Order, Query, Value};
//!
//! let query = Query::new("cities")
//!     .filter(Filter::eq("capital", Value::Bool(true)))
//!     .order_by(Order::asc("name"))
//!     .limit(10);
//! ```
//!
//! Network use starts from [`Firestore::new`] with an `App` and a
//! token provider; see the guides under `docs/` in the
//! repository for end-to-end examples.

// Generated protobuf modules. Placed at the crate root under `google`
// because the generated code resolves cross-package references by
// walking up to this tree. Hidden: not part of the public API.
#[doc(hidden)]
#[allow(clippy::all, missing_docs, rustdoc::all)]
pub mod google {
    pub mod api {
        tonic::include_proto!("google.api");
    }
    pub mod firestore {
        pub mod v1 {
            tonic::include_proto!("google.firestore.v1");
        }
    }
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
    pub mod r#type {
        // prost escapes the keyword `type` in the generated file name,
        // so `include_proto!` cannot be used for this package.
        include!(concat!(env!("OUT_DIR"), "/google.r#type.rs"));
    }
}

mod bloom;
mod client;
mod engine;
#[cfg(test)]
mod golden;
mod listen;
mod mutation;
mod path;
mod proxy;
mod query;
#[cfg(test)]
mod spec_runner;
mod store;
mod value;
mod write;

pub use client::{
    Firestore, ListDocumentsResult, LocalStoreOptions, DEFAULT_CACHE_SIZE_LIMIT_BYTES,
};
pub use listen::{DocumentChange, DocumentChangeKind, Listener, Snapshot, SnapshotMetadata};
pub use path::{collection_path_of, document_id_of, is_collection_path, is_document_path};
pub use query::{FieldOp, Filter, Order, Query, UnaryOp};
pub use value::{Document, FieldValue, GeoPoint, Timestamp, Value};

/// A document's fields: an ordered map of field name to [`Value`].
pub type Fields = std::collections::BTreeMap<String, Value>;

/// The fields of a `set_document_with` / `update_document_with`
/// call: an ordered map of field *path* to [`FieldValue`], so
/// nested fields are addressed as `"stats.count"`.
pub type FieldValues = std::collections::BTreeMap<String, FieldValue>;

// -- Per-user local stores ------------------------------------------------

/// The conventional local-store path for one user under a
/// shared root: `<root>/<uid name>/firestore.db`, where the
/// uid name is the digest encoding from `uid_dir_name`.
/// Keeping each
/// user's store in its own directory namespaces the cached
/// documents, mutation queue and resume tokens per user, so
/// they never mix between users of the same device. Pass the
/// returned path to [`Firestore::enable_local_store`].
pub fn user_store_path(root: &std::path::Path, uid: &str) -> std::path::PathBuf {
    root.join(uid_dir_name(uid)).join("firestore.db")
}

/// Encodes a uid as a single safe directory name: the
/// lowercase hex of its SHA-256 digest - always exactly
/// 64 characters. A digest (rather than the uid itself, or
/// a length-doubling encoding of it) keeps the name
/// bounded under the 255-byte filesystem component limit
/// for every valid uid (Firebase allows up to 128
/// characters), keeps it to one normal path component on
/// every filesystem (never empty, `.`/`..`, or containing
/// a separator, whatever string the caller passes), and
/// keeps distinct uids distinct on case-insensitive
/// filesystems (default macOS and Windows volumes), where
/// literal `uid-a` and `uid-A` directories would collide.
/// Distinct uids share a name only under a SHA-256
/// collision.
fn uid_dir_name(uid: &str) -> String {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, uid.as_bytes());
    let mut name = String::with_capacity(64);
    for byte in digest.as_ref() {
        name.push_str(&format!("{byte:02x}"));
    }
    name
}

/// Removes one user's local store (the whole per-user
/// directory created via [`user_store_path`]). This is the
/// store side of sign-out: pair it with
/// `firebase_native_auth::Auth::sign_out` /
/// `sign_out_session`. The store must not be open in any
/// process: a persistence engine, once started, runs for
/// the rest of its process's lifetime - there is no
/// shutdown API yet, and dropping the client does not stop
/// it - and this wipe has no cross-process coordination,
/// so in practice call this before the owning application
/// opens the store, or after it has exited. Deletion while
/// an engine is running is not interlocked. A missing
/// store is not an error.
pub fn wipe_user_store(root: &std::path::Path, uid: &str) -> firebase_native_core::Result<()> {
    remove_dir_if_present(&root.join(uid_dir_name(uid)))
}

/// Removes every per-user local store under `root` (the whole
/// root directory): the store side of the kill switch, paired
/// with `firebase_native_auth::Auth::wipe_all_sessions`. The
/// root is expected to be dedicated to these stores. The
/// same not-open-in-any-process requirement as
/// [`wipe_user_store`] applies to every store under the
/// root. A missing root is not an error.
pub fn wipe_all_user_stores(root: &std::path::Path) -> firebase_native_core::Result<()> {
    remove_dir_if_present(root)
}

fn remove_dir_if_present(dir: &std::path::Path) -> firebase_native_core::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(firebase_native_core::Error::Io(format!(
            "removing {}: {error}",
            dir.display()
        ))),
    }
}

#[cfg(test)]
mod store_path_tests {
    #[test]
    fn uids_cannot_escape_the_store_root() {
        let base = std::env::temp_dir().join(format!("fn-store-escape-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let root = base.join("stores");
        let sibling = base.join("sibling");
        std::fs::create_dir_all(&sibling).unwrap();
        std::fs::write(sibling.join("keep.txt"), b"keep").unwrap();
        for uid in ["..", "../sibling", "/abs/path", "", ".", "a/b", "a\\b"] {
            let path = user_store_path(&root, uid);
            assert!(path.starts_with(&root), "uid {uid:?} escapes: {path:?}");
            wipe_user_store(&root, uid).unwrap();
        }
        assert!(sibling.join("keep.txt").exists());
        // A store created for a hostile uid is still removed
        // by the matching wipe (same encoding on both sides).
        let hostile = user_store_path(&root, "../sibling");
        std::fs::create_dir_all(hostile.parent().unwrap()).unwrap();
        std::fs::write(&hostile, b"db").unwrap();
        wipe_user_store(&root, "../sibling").unwrap();
        assert!(!hostile.exists());
        let _ = std::fs::remove_dir_all(&base);
    }

    use super::*;

    fn temp_root(tag: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "firebase-native-stores-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        root
    }

    #[test]
    fn user_store_paths_are_namespaced_per_uid() {
        let root = std::path::Path::new("/data/stores");
        let a = user_store_path(root, "uid-a");
        assert_eq!(a.file_name().unwrap(), "firestore.db");
        assert!(a.starts_with(root));
        assert_ne!(a, user_store_path(root, "uid-b"));
        // Case-distinct uids are distinct users: their names
        // must differ by more than letter case, so they stay
        // distinct on case-insensitive filesystems too.
        assert_ne!(a, user_store_path(root, "uid-A"));
    }

    #[test]
    fn maximum_length_uids_fit_component_limits() {
        let root = temp_root("length");
        // Firebase allows uids up to 128 characters; a
        // 128-byte uid must not overflow the 255-byte
        // component limit (a length-doubling encoding of it
        // would, at 256 bytes).
        for uid in ["a".repeat(128), "é".repeat(128)] {
            let path = user_store_path(&root, &uid);
            let dir_name = path
                .parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            assert_eq!(dir_name.len(), 64);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, b"db").unwrap();
            wipe_user_store(&root, &uid).unwrap();
            assert!(!path.exists());
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn case_distinct_uids_have_isolated_stores() {
        let root = temp_root("case");
        let lower = user_store_path(&root, "uid-a");
        std::fs::create_dir_all(lower.parent().unwrap()).unwrap();
        std::fs::write(&lower, b"db").unwrap();
        // The other case variant must not see or remove it.
        let upper = user_store_path(&root, "uid-A");
        assert!(!upper.exists());
        wipe_user_store(&root, "uid-A").unwrap();
        assert!(lower.exists());
        wipe_user_store(&root, "uid-a").unwrap();
        assert!(!lower.exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn wipes_remove_only_their_scope() {
        let root = temp_root("scope");
        for uid in ["uid-a", "uid-b"] {
            let path = user_store_path(&root, uid);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, b"db").unwrap();
            std::fs::write(path.with_extension("db-wal"), b"wal").unwrap();
        }
        wipe_user_store(&root, "uid-a").unwrap();
        assert!(!user_store_path(&root, "uid-a").exists());
        assert!(user_store_path(&root, "uid-b").exists());
        // Missing stores wipe cleanly.
        wipe_user_store(&root, "uid-a").unwrap();
        wipe_all_user_stores(&root).unwrap();
        assert!(!root.exists());
        wipe_all_user_stores(&root).unwrap();
    }
}
