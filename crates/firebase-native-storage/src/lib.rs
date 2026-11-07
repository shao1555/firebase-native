//! Cloud Storage for Firebase for `firebase-native`.
//!
//! Placeholder crate: the implementation lands in P3, over the JSON
//! API at `firebasestorage.googleapis.com/v0/b/{bucket}/o` (simple and
//! resumable uploads, download via `alt=media`, metadata, delete),
//! obtaining ID tokens per call through
//! [`firebase_native_core::TokenProvider`] like the other services.
//! The crate exists now so the workspace layout and the umbrella
//! crate's re-exports are stable from P0.

use std::sync::Arc;

use firebase_native_core::{App, FirebaseOptions, TokenProvider};

/// Cloud Storage client handle (not yet functional; P3).
pub struct Storage {
    options: FirebaseOptions,
    #[allow(dead_code)]
    tokens: Arc<dyn TokenProvider>,
}

impl Storage {
    /// Creates a Storage handle for the app.
    pub fn new(app: &App, tokens: Arc<dyn TokenProvider>) -> Self {
        Self {
            options: app.options().clone(),
            tokens,
        }
    }

    /// The default bucket from the app options, when configured.
    pub fn bucket(&self) -> Option<&str> {
        self.options.storage_bucket.as_deref()
    }
}
