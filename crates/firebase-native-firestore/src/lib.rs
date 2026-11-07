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
//! P0 scope is online-only unary operations: get / create / set /
//! update / delete / list and a basic structured `run_query`. The
//! Listen and Write streams, the local store and offline persistence
//! arrive in P1/P2 (see the project work plan).

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

mod client;
mod path;
mod proxy;
mod query;
mod value;

pub use client::{Firestore, ListDocumentsResult};
pub use path::{collection_path_of, document_id_of, is_collection_path, is_document_path};
pub use query::{FieldOp, Filter, Order, Query, UnaryOp};
pub use value::{Document, GeoPoint, Timestamp, Value};

/// A document's fields: an ordered map of field name to [`Value`].
pub type Fields = std::collections::BTreeMap<String, Value>;
