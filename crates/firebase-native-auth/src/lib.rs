#![warn(missing_docs)]
//! Firebase Authentication for `firebase-native`, implemented directly
//! over the Identity Toolkit REST API
//! (`identitytoolkit.googleapis.com/v1/accounts:*`) and the Secure Token
//! service (`securetoken.googleapis.com/v1/token`).
//!
//! The [`Auth`] client implements
//! [`firebase_native_core::TokenProvider`]: service clients such as
//! Firestore obtain the ID token per call through that trait, the token
//! is refreshed proactively before expiry, and a forced refresh is
//! available for the unauthenticated-retry path. The token is never a
//! fixed string captured at construction time.
//!
//! Session persistence is delegated to the application shell through
//! [`firebase_native_core::CredentialStore`]; see that trait for the
//! storage policy (secure store in production, fail-closed otherwise).

mod client;
mod session;
mod wire;

pub use client::Auth;
pub use session::{SessionFacts, SessionKey, SignedIn, User};
