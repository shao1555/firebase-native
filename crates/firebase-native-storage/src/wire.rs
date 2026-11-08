//! JSON wire types and pure helpers for the Storage JSON API
//! (`firebasestorage.googleapis.com/v0`): the object resource,
//! the list response, the error envelope, URL encoding of object
//! names and multipart upload bodies. Everything here is
//! transport-independent and unit tested without a server.

use std::collections::BTreeMap;

use firebase_native_core::{Error, Result};
use serde::{Deserialize, Serialize};

/// Percent-encodes an object name for use as a single URL path
/// segment (the JSON API addresses objects as
/// `/b/{bucket}/o/{object}` with the whole name encoded).
pub(crate) fn encode_object_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// The object resource as the API returns it. Numeric fields
/// arrive as strings; unknown fields are ignored.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObjectResource {
    pub name: String,
    pub bucket: String,
    pub generation: Option<String>,
    pub metageneration: Option<String>,
    pub size: Option<String>,
    pub content_type: Option<String>,
    pub md5_hash: Option<String>,
    pub crc32c: Option<String>,
    pub etag: Option<String>,
    pub time_created: Option<String>,
    pub updated: Option<String>,
    pub download_tokens: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

/// The `GET /b/{bucket}/o` list response.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListResource {
    #[serde(default)]
    pub items: Vec<ObjectResource>,
    #[serde(default)]
    pub prefixes: Vec<String>,
    pub next_page_token: Option<String>,
}

/// The metadata payload sent on upload and PATCH: only set
/// fields are serialized.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MetadataPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_disposition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_language: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

/// The API's error envelope: `{"error": {"code": 404,
/// "message": "...", "status": "NOT_FOUND"}}` (the `status`
/// member is not always present).
#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    // The numeric `code` member duplicates the HTTP status and
    // is not read.
    message: Option<String>,
    status: Option<String>,
}

/// Maps an HTTP failure (status + response body) to the shared
/// error type, preferring the envelope's machine-readable
/// status and message when present.
pub(crate) fn map_http_error(status: reqwest::StatusCode, body: &str) -> Error {
    let envelope: Option<ErrorEnvelope> = serde_json::from_str(body).ok();
    let message = envelope
        .as_ref()
        .and_then(|env| env.error.message.clone())
        .unwrap_or_else(|| format!("storage request failed with HTTP {status}"));
    let code = envelope
        .as_ref()
        .and_then(|env| env.error.status.clone())
        .unwrap_or_else(|| status.canonical_reason().unwrap_or("UNKNOWN").to_string());
    match status.as_u16() {
        401 => Error::Unauthenticated(message),
        403 => Error::PermissionDenied(message),
        404 => Error::NotFound(message),
        _ => Error::Api {
            status: Some(status.as_u16()),
            code,
            message,
        },
    }
}

/// Builds a `multipart/related` simple-upload body: a JSON
/// metadata part followed by the media part.
pub(crate) fn multipart_body(
    boundary: &str,
    metadata_json: &str,
    content_type: &str,
    data: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(b"Content-Type: application/json; charset=UTF-8\r\n\r\n");
    body.extend_from_slice(metadata_json.as_bytes());
    body.extend_from_slice(format!("\r\n--{boundary}\r\n").as_bytes());
    body.extend_from_slice(format!("Content-Type: {content_type}\r\n\r\n").as_bytes());
    body.extend_from_slice(data);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

/// Converts a metadata payload error-free; serialization of
/// this struct cannot fail in practice, but no panic paths.
pub(crate) fn payload_json(payload: &MetadataPayload) -> Result<String> {
    serde_json::to_string(payload).map_err(|err| Error::Serde(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_names_are_fully_encoded() {
        assert_eq!(
            encode_object_name("users/u1/attachments/a b#c.txt"),
            "users%2Fu1%2Fattachments%2Fa%20b%23c.txt"
        );
        assert_eq!(encode_object_name("plain-name_1.2~"), "plain-name_1.2~");
    }

    #[test]
    fn object_resource_parses() {
        let json = r#"{
            "name": "users/u1/attachments/note.md",
            "bucket": "my-project.appspot.com",
            "generation": "1700000000000001",
            "metageneration": "1",
            "size": "1234",
            "contentType": "text/markdown",
            "md5Hash": "abc=",
            "crc32c": "def=",
            "etag": "CKj=",
            "timeCreated": "2026-11-08T00:00:00.000Z",
            "updated": "2026-11-08T00:00:01.000Z",
            "downloadTokens": "tok-1,tok-2",
            "metadata": {"origin": "test"}
        }"#;
        let resource: ObjectResource = serde_json::from_str(json).unwrap();
        assert_eq!(resource.name, "users/u1/attachments/note.md");
        assert_eq!(resource.size.as_deref(), Some("1234"));
        assert_eq!(resource.download_tokens.as_deref(), Some("tok-1,tok-2"));
        assert_eq!(
            resource.metadata.get("origin").map(String::as_str),
            Some("test")
        );
    }

    #[test]
    fn list_resource_parses() {
        let json = r#"{
            "items": [{"name": "a.txt", "bucket": "b"}],
            "prefixes": ["dir/"],
            "nextPageToken": "tok"
        }"#;
        let list: ListResource = serde_json::from_str(json).unwrap();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.prefixes, ["dir/"]);
        assert_eq!(list.next_page_token.as_deref(), Some("tok"));
    }

    #[test]
    fn error_envelope_maps_to_typed_errors() {
        let body = r#"{"error": {"code": 404, "message": "not found", "status": "NOT_FOUND"}}"#;
        match map_http_error(reqwest::StatusCode::NOT_FOUND, body) {
            Error::NotFound(message) => assert_eq!(message, "not found"),
            other => panic!("expected NotFound, got {other:?}"),
        }
        match map_http_error(reqwest::StatusCode::FORBIDDEN, body) {
            Error::PermissionDenied(_) => {}
            other => panic!("expected PermissionDenied, got {other:?}"),
        }
        match map_http_error(reqwest::StatusCode::BAD_GATEWAY, "garbage") {
            Error::Api { status, .. } => assert_eq!(status, Some(502)),
            other => panic!("expected Api, got {other:?}"),
        }
    }

    #[test]
    fn multipart_body_layout() {
        let body = multipart_body("BOUNDARY", "{\"name\":\"x\"}", "text/plain", b"hello");
        let text = String::from_utf8_lossy(&body);
        assert!(text.starts_with("--BOUNDARY\r\nContent-Type: application/json"));
        assert!(text.contains("\r\n--BOUNDARY\r\nContent-Type: text/plain\r\n\r\nhello"));
        assert!(text.ends_with("\r\n--BOUNDARY--\r\n"));
    }
}
