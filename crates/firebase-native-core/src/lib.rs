#![warn(missing_docs)]
//! Core types shared by every `firebase-native` service crate.
//!
//! This crate owns no network code. It defines:
//!
//! - [`FirebaseOptions`] and [`App`], the application handle every service
//!   client is built from;
//! - [`Error`], the shared error type;
//! - [`TokenProvider`], through which service clients obtain an ID token
//!   **per call** (never a token string captured at construction time);
//! - [`CredentialStore`], through which the application shell decides where
//!   the refresh-token session blob is persisted. The library itself owns
//!   no storage: the shell supplies a file-backed store for development
//!   builds, an OS secure store (Keychain / Credential Manager / Secret
//!   Service) for production, or no store at all — the fail-closed option,
//!   where the user signs in on every launch.

mod app;
mod credential;
mod error;
mod token;

pub use app::{App, FirebaseOptions, DEFAULT_DATABASE_ID};
pub use credential::{CredentialStore, MemoryCredentialStore};
pub use error::{Error, Result};
pub use token::TokenProvider;
