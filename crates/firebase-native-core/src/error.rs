use std::fmt;

/// Result alias using the shared [`Error`] type.
pub type Result<T> = std::result::Result<T, Error>;

/// The shared error type for all `firebase-native` crates.
///
/// Service-specific details (an Identity Toolkit error code, a gRPC
/// status) are carried as structured variants so callers can react —
/// most importantly, Firestore retries exactly once on
/// [`Error::Unauthenticated`] after a forced token refresh.
#[derive(Debug)]
pub enum Error {
    /// A caller supplied an invalid argument (bad path, empty field, ...).
    InvalidArgument(String),
    /// No signed-in user, or the server rejected the ID token.
    Unauthenticated(String),
    /// The requested document or resource does not exist.
    NotFound(String),
    /// The server refused the operation for permission reasons.
    PermissionDenied(String),
    /// An error reported by a Firebase REST API, with its machine-readable
    /// code (e.g. `EMAIL_EXISTS`, `TOKEN_EXPIRED`).
    Api {
        /// HTTP status code, when the error came over HTTP.
        status: Option<u16>,
        /// Machine-readable error code reported by the API.
        code: String,
        /// Human-readable message reported by the API.
        message: String,
    },
    /// A transport-level HTTP failure.
    Http(String),
    /// A gRPC failure, flattened to code name and message so this crate
    /// does not depend on `tonic`.
    Rpc {
        /// gRPC code name, e.g. `UNAVAILABLE`.
        code: String,
        /// Status message.
        message: String,
    },
    /// JSON (de)serialization failed.
    Serde(String),
    /// Local I/O failed (credential store backends, ...).
    Io(String),
    /// Anything else.
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            Error::Unauthenticated(msg) => write!(f, "unauthenticated: {msg}"),
            Error::NotFound(msg) => write!(f, "not found: {msg}"),
            Error::PermissionDenied(msg) => write!(f, "permission denied: {msg}"),
            Error::Api {
                status,
                code,
                message,
            } => match status {
                Some(status) => write!(f, "api error (http {status}) {code}: {message}"),
                None => write!(f, "api error {code}: {message}"),
            },
            Error::Http(msg) => write!(f, "http error: {msg}"),
            Error::Rpc { code, message } => write!(f, "rpc error {code}: {message}"),
            Error::Serde(msg) => write!(f, "serialization error: {msg}"),
            Error::Io(msg) => write!(f, "io error: {msg}"),
            Error::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serde(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err.to_string())
    }
}
