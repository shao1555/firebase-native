//! Wire types for the Identity Toolkit and Secure Token REST APIs.
//!
//! Field names follow the APIs exactly (`expiresIn` is a string of
//! seconds in sign-in responses). Parsing is kept separate from the
//! client so it can be unit-tested without any network.

use serde::Deserialize;

use firebase_native_core::Error;

/// Successful response of the `accounts:*` sign-in endpoints.
///
/// `localId` is optional: the production `signInWithCustomToken`
/// response observed on 2026-11-07 carries only `kind`, `idToken`,
/// `refreshToken`, `expiresIn` and `isNewUser`. When it is absent the
/// uid (and email) are taken from the ID token's claims instead,
/// which are authoritative anyway.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SignInResponse {
    pub local_id: Option<String>,
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub id_token: String,
    pub refresh_token: String,
    /// Seconds until the ID token expires, encoded as a string.
    pub expires_in: String,
}

impl SignInResponse {
    pub(crate) fn expires_in_secs(&self) -> u64 {
        self.expires_in.parse().unwrap_or(3600)
    }
}

/// Claims read from an ID token (a JWT) without verifying it — the
/// token arrived over TLS directly from Google's endpoint, and the
/// server verifies it on every subsequent call regardless.
#[derive(Debug, Deserialize)]
struct IdTokenClaims {
    user_id: Option<String>,
    sub: Option<String>,
    email: Option<String>,
}

/// Extracts the uid (`user_id`, falling back to `sub`) and email from
/// an ID token's payload.
pub(crate) fn claims_from_id_token(id_token: &str) -> Option<(String, Option<String>)> {
    use base64::Engine;
    let payload = id_token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    let claims: IdTokenClaims = serde_json::from_slice(&bytes).ok()?;
    let uid = claims.user_id.or(claims.sub)?;
    Some((uid, claims.email))
}

/// Successful response of `securetoken.googleapis.com/v1/token`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RefreshResponse {
    pub id_token: String,
    pub refresh_token: String,
    /// Seconds until the ID token expires, encoded as a string.
    pub expires_in: String,
    pub user_id: String,
}

impl RefreshResponse {
    pub(crate) fn expires_in_secs(&self) -> u64 {
        self.expires_in.parse().unwrap_or(3600)
    }
}

/// Error envelope shared by both APIs: `{ "error": { "message": ... } }`.
#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    message: String,
}

/// Maps an HTTP error body to the shared error type. Known auth failures
/// keep their machine-readable code in [`Error::Api`]; token failures
/// that mean "sign in again" map to [`Error::Unauthenticated`].
pub(crate) fn parse_api_error(status: u16, body: &str) -> Error {
    let code = serde_json::from_str::<ErrorEnvelope>(body)
        .map(|envelope| envelope.error.message)
        .unwrap_or_else(|_| body.trim().to_string());
    // The message may carry a suffix after " : ", e.g.
    // "TOKEN_EXPIRED : Token expired." — the code is the first token.
    let code = code
        .split([' ', ':'])
        .next()
        .unwrap_or("UNKNOWN")
        .to_string();
    match code.as_str() {
        "TOKEN_EXPIRED"
        | "TOKEN_INVALID"
        | "USER_NOT_FOUND"
        | "USER_DISABLED"
        | "INVALID_REFRESH_TOKEN"
        | "CREDENTIAL_TOO_OLD_LOGIN_AGAIN" => Error::Unauthenticated(code),
        _ => Error::Api {
            status: Some(status),
            code,
            message: body.trim().to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sign_in_response() {
        let body = r#"{
            "kind": "identitytoolkit#VerifyCustomTokenResponse",
            "localId": "uid-42",
            "email": "s@example.jp",
            "idToken": "id-token",
            "refreshToken": "refresh-token",
            "expiresIn": "3600",
            "isNewUser": false
        }"#;
        let parsed: SignInResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.local_id.as_deref(), Some("uid-42"));
        assert_eq!(parsed.email.as_deref(), Some("s@example.jp"));
        assert_eq!(parsed.id_token, "id-token");
        assert_eq!(parsed.expires_in_secs(), 3600);
    }

    #[test]
    fn parses_sign_in_response_without_local_id() {
        // The shape observed in production for
        // signInWithCustomToken on 2026-11-07.
        let body = r#"{
            "kind": "identitytoolkit#VerifyCustomTokenResponse",
            "idToken": "id-token",
            "refreshToken": "refresh-token",
            "expiresIn": "3600",
            "isNewUser": false
        }"#;
        let parsed: SignInResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.local_id, None);
        assert_eq!(parsed.id_token, "id-token");
    }

    #[test]
    fn extracts_claims_from_id_token() {
        use base64::Engine;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"user_id":"uid-42","email":"s@example.jp","exp":1700003600}"#);
        let token = format!("header.{payload}.signature");
        assert_eq!(
            claims_from_id_token(&token),
            Some(("uid-42".to_string(), Some("s@example.jp".to_string())))
        );
        assert_eq!(claims_from_id_token("not-a-jwt"), None);
    }

    #[test]
    fn parses_refresh_response() {
        let body = r#"{
            "access_token": "id-token",
            "expires_in": "3600",
            "token_type": "Bearer",
            "refresh_token": "new-refresh",
            "id_token": "id-token",
            "user_id": "uid-42",
            "project_id": "605782024688"
        }"#;
        let parsed: RefreshResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.user_id, "uid-42");
        assert_eq!(parsed.refresh_token, "new-refresh");
        assert_eq!(parsed.expires_in_secs(), 3600);
    }

    #[test]
    fn maps_token_expired_to_unauthenticated() {
        let body = r#"{"error":{"code":400,"message":"TOKEN_EXPIRED","errors":[{"message":"TOKEN_EXPIRED"}]}}"#;
        match parse_api_error(400, body) {
            Error::Unauthenticated(code) => assert_eq!(code, "TOKEN_EXPIRED"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn maps_other_errors_to_api() {
        let body = r#"{"error":{"code":400,"message":"EMAIL_EXISTS","errors":[]}}"#;
        match parse_api_error(400, body) {
            Error::Api { code, status, .. } => {
                assert_eq!(code, "EMAIL_EXISTS");
                assert_eq!(status, Some(400));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
