//! Cloud Firestore for `firebase-native`, over gRPC (`tonic`).
//!
//! The public API is a hand-written model — [`Value`], [`Document`],
//! [`Query`] — and never exposes the generated `prost` types; a
//! conversion layer in [`value`](crate::value) translates between the
//! two. Tokens are obtained per call through
//! [`firebase_native_core::TokenProvider`]; on an `UNAUTHENTICATED`
//! status the client forces one token refresh and retries the call
//! exactly once.
//!
//! P0 scope was online-only unary operations: get / create / set /
//! update / delete / list and a basic structured `run_query`. P1 adds
//! the Listen stream ([`Firestore::watch_query`] /
//! [`Firestore::watch_document`]). P2 (part 1) adds offline
//! persistence: with [`Firestore::enable_local_store`] writes are
//! queued in a local SQLite store, reflected in reads immediately,
//! and committed in the background over the Write stream, surviving
//! restarts. Listen results do not flow through the store yet, and
//! queries still run online in persistent mode; the Write/Listen
//! spec-test suites land in P2 (part 2).

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

pub use client::{Firestore, ListDocumentsResult};
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
