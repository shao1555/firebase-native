//! HTTP plumbing for the Storage JSON API: one request
//! builder per operation, an ID token fetched per call through
//! the app's [`TokenProvider`] (never captured at construction),
//! and exactly one forced-refresh retry on 401 — the same
//! invariants as the other services in this workspace.

use std::sync::Arc;

use firebase_native_core::{Error, Result, TokenProvider};
use serde::de::DeserializeOwned;

use crate::wire::{self, ListResource, MetadataPayload, ObjectResource};

/// The default JSON API base (`/v0` of the Firebase Storage
/// host). An endpoint override replaces it wholesale (tests,
/// the Storage emulator).
pub(crate) const DEFAULT_ENDPOINT: &str = "https://firebasestorage.googleapis.com/v0";

/// Bytes per resumable-upload chunk: a multiple of 256 KiB, as
/// the protocol requires for all but the final chunk.
pub(crate) const RESUMABLE_CHUNK_SIZE: usize = 8 * 1024 * 1024;

pub(crate) struct HttpClient {
    http: reqwest::Client,
    tokens: Arc<dyn TokenProvider>,
    endpoint: String,
}

impl HttpClient {
    pub(crate) fn new(tokens: Arc<dyn TokenProvider>, endpoint: Option<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            tokens,
            endpoint: endpoint.unwrap_or_else(|| DEFAULT_ENDPOINT.to_string()),
        }
    }

    /// The API base in use (for composing download URLs).
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn object_url(&self, bucket: &str, path: &str) -> String {
        format!(
            "{}/b/{}/o/{}",
            self.endpoint,
            bucket,
            wire::encode_object_name(path)
        )
    }

    fn collection_url(&self, bucket: &str) -> String {
        format!("{}/b/{}/o", self.endpoint, bucket)
    }

    /// Sends a request built by `build`, attaching a fresh ID
    /// token (`Authorization: Firebase <token>`, the scheme this
    /// host expects for Firebase ID tokens). On 401 the token is
    /// force-refreshed and the request rebuilt and retried
    /// exactly once.
    async fn send(
        &self,
        build: impl Fn(&reqwest::Client, String) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response> {
        let mut force_refresh = false;
        loop {
            let token = self.tokens.id_token(force_refresh).await?;
            let response = build(&self.http, format!("Firebase {token}"))
                .send()
                .await
                .map_err(http_error)?;
            if response.status() == reqwest::StatusCode::UNAUTHORIZED && !force_refresh {
                force_refresh = true;
                continue;
            }
            return Ok(response);
        }
    }

    /// Returns the response when successful, or the mapped
    /// error (envelope-aware) otherwise.
    async fn ensure_success(&self, response: reqwest::Response) -> Result<reqwest::Response> {
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(wire::map_http_error(status, &body))
    }

    async fn send_json<T: DeserializeOwned>(
        &self,
        build: impl Fn(&reqwest::Client, String) -> reqwest::RequestBuilder,
    ) -> Result<T> {
        let response = self.ensure_success(self.send(build).await?).await?;
        response
            .json::<T>()
            .await
            .map_err(|err| Error::Serde(err.to_string()))
    }

    pub(crate) async fn get_metadata(&self, bucket: &str, path: &str) -> Result<ObjectResource> {
        let url = self.object_url(bucket, path);
        self.send_json(|http, auth| http.get(&url).header("Authorization", auth))
            .await
    }

    pub(crate) async fn download(&self, bucket: &str, path: &str) -> Result<Vec<u8>> {
        let url = self.object_url(bucket, path);
        let response = self
            .ensure_success(
                self.send(|http, auth| {
                    http.get(&url)
                        .query(&[("alt", "media")])
                        .header("Authorization", auth)
                })
                .await?,
            )
            .await?;
        let bytes = response.bytes().await.map_err(http_error)?;
        Ok(bytes.to_vec())
    }

    pub(crate) async fn delete(&self, bucket: &str, path: &str) -> Result<()> {
        let url = self.object_url(bucket, path);
        self.ensure_success(
            self.send(|http, auth| http.delete(&url).header("Authorization", auth))
                .await?,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn patch_metadata(
        &self,
        bucket: &str,
        path: &str,
        payload: &MetadataPayload,
    ) -> Result<ObjectResource> {
        let url = self.object_url(bucket, path);
        let body = wire::payload_json(payload)?;
        self.send_json(|http, auth| {
            http.patch(&url)
                .header("Authorization", auth)
                .header("Content-Type", "application/json")
                .body(body.clone())
        })
        .await
    }

    pub(crate) async fn list(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_results: Option<i32>,
        page_token: Option<&str>,
    ) -> Result<ListResource> {
        let url = self.collection_url(bucket);
        let mut query: Vec<(&str, String)> = Vec::new();
        if let Some(prefix) = prefix {
            query.push(("prefix", prefix.to_string()));
        }
        if let Some(delimiter) = delimiter {
            query.push(("delimiter", delimiter.to_string()));
        }
        if let Some(max_results) = max_results {
            query.push(("maxResults", max_results.to_string()));
        }
        if let Some(page_token) = page_token {
            query.push(("pageToken", page_token.to_string()));
        }
        self.send_json(|http, auth| http.get(&url).query(&query).header("Authorization", auth))
            .await
    }

    /// Simple upload with `uploadType=media` (no metadata
    /// beyond the content type).
    pub(crate) async fn upload_media(
        &self,
        bucket: &str,
        name: &str,
        content_type: &str,
        data: Vec<u8>,
    ) -> Result<ObjectResource> {
        let url = self.collection_url(bucket);
        self.send_json(|http, auth| {
            http.post(&url)
                .query(&[("uploadType", "media"), ("name", name)])
                .header("Authorization", auth)
                .header("Content-Type", content_type)
                .body(data.clone())
        })
        .await
    }

    /// Simple upload with metadata plus media in one request.
    /// The v0 endpoint speaks the `X-Goog-Upload` protocol for
    /// this (verified live: the GCS-style `uploadType=multipart`
    /// query parameter is rejected with "Invalid HTTP
    /// method/URL pair").
    pub(crate) async fn upload_multipart(
        &self,
        bucket: &str,
        name: &str,
        payload: &MetadataPayload,
        content_type: &str,
        data: Vec<u8>,
    ) -> Result<ObjectResource> {
        let url = self.collection_url(bucket);
        let boundary = multipart_boundary();
        let body = wire::multipart_body(
            &boundary,
            &wire::payload_json(payload)?,
            content_type,
            &data,
        );
        self.send_json(|http, auth| {
            http.post(&url)
                .query(&[("name", name)])
                .header("Authorization", auth)
                .header("X-Goog-Upload-Protocol", "multipart")
                .header(
                    "Content-Type",
                    format!("multipart/related; boundary={boundary}"),
                )
                .body(body.clone())
        })
        .await
    }

    /// Starts a resumable upload session and returns its URI
    /// (the `X-Goog-Upload-URL` response header).
    pub(crate) async fn resumable_start(
        &self,
        bucket: &str,
        name: &str,
        payload: &MetadataPayload,
        content_type: &str,
        total: usize,
    ) -> Result<String> {
        let url = self.collection_url(bucket);
        let body = wire::payload_json(payload)?;
        let response = self
            .ensure_success(
                self.send(|http, auth| {
                    http.post(&url)
                        .query(&[("name", name)])
                        .header("Authorization", auth)
                        .header("Content-Type", "application/json")
                        .header("X-Goog-Upload-Protocol", "resumable")
                        .header("X-Goog-Upload-Command", "start")
                        .header("X-Goog-Upload-Header-Content-Type", content_type)
                        .header("X-Goog-Upload-Header-Content-Length", total.to_string())
                        .body(body.clone())
                })
                .await?,
            )
            .await?;
        let session = response
            .headers()
            .get("X-Goog-Upload-URL")
            .or_else(|| response.headers().get(reqwest::header::LOCATION))
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| Error::Api {
                status: Some(200),
                code: "MISSING_UPLOAD_SESSION".to_string(),
                message: "resumable upload start returned no session URI".to_string(),
            })?;
        Ok(session.to_string())
    }

    /// Uploads one chunk of a resumable session (a POST with
    /// `X-Goog-Upload-Command: upload`, plus `finalize` on the
    /// last chunk). An `active` upload status means the session
    /// continues: the committed byte count comes from
    /// `X-Goog-Upload-Size-Received` when present, else from
    /// the chunk just sent. A `final` status completes the
    /// upload and the body carries the object resource.
    /// Non-final chunks must be a multiple of the 256 KiB
    /// chunk granularity the server advertises at session
    /// start.
    pub(crate) async fn resumable_put_chunk(
        &self,
        session_uri: &str,
        chunk: Vec<u8>,
        start: usize,
        is_final: bool,
    ) -> Result<ChunkOutcome> {
        let session = session_uri.to_string();
        let command = if is_final {
            "upload, finalize"
        } else {
            "upload"
        };
        let sent_end = start + chunk.len();
        let response = self
            .ensure_success(
                self.send(|http, auth| {
                    http.post(&session)
                        .header("Authorization", auth)
                        .header("X-Goog-Upload-Command", command)
                        .header("X-Goog-Upload-Offset", start.to_string())
                        .body(chunk.clone())
                })
                .await?,
            )
            .await?;
        let status_header = response
            .headers()
            .get("X-Goog-Upload-Status")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        if status_header.as_deref() == Some("active") {
            let committed = response
                .headers()
                .get("X-Goog-Upload-Size-Received")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(sent_end);
            return Ok(ChunkOutcome::Committed(committed));
        }
        let resource: ObjectResource = response
            .json()
            .await
            .map_err(|err| Error::Serde(err.to_string()))?;
        Ok(ChunkOutcome::Done(Box::new(resource)))
    }
}

/// The result of one resumable chunk PUT.
pub(crate) enum ChunkOutcome {
    /// Bytes the server has committed so far.
    Committed(usize),
    /// The upload completed; the final object resource.
    Done(Box<ObjectResource>),
}

/// A multipart boundary unique enough for upload bodies
/// (process-wide counter; the content is not required to be
/// unpredictable).
fn multipart_boundary() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "firebase-native-{}",
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Formats a reqwest failure with its full source chain, so
/// transport errors stay diagnosable (mirrors the auth crate).
fn http_error(err: reqwest::Error) -> Error {
    let mut message = err.to_string();
    let mut source = std::error::Error::source(&err);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    Error::Http(message)
}
