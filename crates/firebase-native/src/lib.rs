//! `firebase-native`: a pure-Rust Firebase client for native
//! applications.
//!
//! - [`core`] — `App`, `FirebaseOptions`, the shared `Error` type, and
//!   the `TokenProvider` / `CredentialStore` traits.
//! - [`auth`] — Firebase Authentication over the Identity Toolkit REST
//!   API, with automatic refresh and shell-owned session persistence.
//! - [`firestore`] — Cloud Firestore over gRPC, with a hand-written
//!   public value model (generated protobuf types are never exposed).
//! - [`storage`] — Cloud Storage for Firebase (placeholder until P3).
//!
//! Unlike a wrapper around the Firebase C++ SDK, this implementation
//! is pure Rust, so it builds natively for every target Rust supports
//! — including Windows on ARM64, for which the C++ SDK ships no
//! prebuilt libraries.

pub use firebase_native_auth as auth;
pub use firebase_native_core as core;
pub use firebase_native_firestore as firestore;
pub use firebase_native_storage as storage;

pub use firebase_native_auth::Auth;
pub use firebase_native_core::{
    App, CredentialStore, Error, FirebaseOptions, Result, TokenProvider,
};
pub use firebase_native_firestore::Firestore;
pub use firebase_native_storage::Storage;
