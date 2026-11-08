//! Cloud Storage for Firebase for `firebase-native`.
//!
//! A client for the Storage JSON API at
//! `firebasestorage.googleapis.com/v0/b/{bucket}/o`: simple
//! (media and multipart) and resumable uploads, download via
//! `alt=media`, metadata read/patch, listing and delete.
//!
//! Like the other services in this workspace, the client takes
//! an ID token per call through
//! [`firebase_native_core::TokenProvider`] (sent as
//! `Authorization: Firebase <token>`, the scheme this host
//! expects) and force-refreshes and retries exactly once on a
//! 401. Object paths follow the bucket's rules as data (e.g.
//! `users/{uid}/attachments/...`); this crate imposes no layout.

mod client;
mod wire;

use std::collections::BTreeMap;
use std::sync::Arc;

use firebase_native_core::{App, Error, FirebaseOptions, Result, TokenProvider};

use client::{HttpClient, RESUMABLE_CHUNK_SIZE};
use wire::ObjectResource;

/// Metadata that can be set at upload time or patched later.
/// Unset fields are left alone (on upload they take the
/// server's defaults; on patch they are not sent).
#[derive(Debug, Clone, Default)]
pub struct PutMetadata {
    /// MIME type of the object.
    pub content_type: Option<String>,
    /// `Cache-Control` header served on download.
    pub cache_control: Option<String>,
    /// `Content-Disposition` header served on download.
    pub content_disposition: Option<String>,
    /// `Content-Encoding` header served on download.
    pub content_encoding: Option<String>,
    /// `Content-Language` header served on download.
    pub content_language: Option<String>,
    /// Custom key/value metadata stored with the object.
    pub custom_metadata: BTreeMap<String, String>,
}

impl PutMetadata {
    fn to_payload(&self, name: Option<String>) -> wire::MetadataPayload {
        wire::MetadataPayload {
            name,
            content_type: self.content_type.clone(),
            cache_control: self.cache_control.clone(),
            content_disposition: self.content_disposition.clone(),
            content_encoding: self.content_encoding.clone(),
            content_language: self.content_language.clone(),
            metadata: self.custom_metadata.clone(),
        }
    }
}

/// An object's metadata as reported by the API. Times are the
/// API's RFC 3339 strings; sizes and generations are parsed
/// from their string wire forms.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectMetadata {
    /// Full object name (path within the bucket).
    pub name: String,
    /// The bucket the object lives in.
    pub bucket: String,
    /// Content generation, when reported.
    pub generation: Option<i64>,
    /// Metadata generation, when reported.
    pub metageneration: Option<i64>,
    /// Object size in bytes.
    pub size: u64,
    /// MIME type, when set.
    pub content_type: Option<String>,
    /// Base64 MD5 hash, when reported.
    pub md5_hash: Option<String>,
    /// Base64 CRC32C, when reported.
    pub crc32c: Option<String>,
    /// The object's ETag, when reported.
    pub etag: Option<String>,
    /// Creation time (RFC 3339), when reported.
    pub time_created: Option<String>,
    /// Last update time (RFC 3339), when reported.
    pub updated: Option<String>,
    /// Download tokens minted for the object (the first is what
    /// `download_url` uses).
    pub download_tokens: Vec<String>,
    /// Custom metadata stored with the object.
    pub custom_metadata: BTreeMap<String, String>,
}

impl From<ObjectResource> for ObjectMetadata {
    fn from(resource: ObjectResource) -> Self {
        Self {
            name: resource.name,
            bucket: resource.bucket,
            generation: resource.generation.and_then(|v| v.parse().ok()),
            metageneration: resource.metageneration.and_then(|v| v.parse().ok()),
            size: resource.size.and_then(|v| v.parse().ok()).unwrap_or(0),
            content_type: resource.content_type,
            md5_hash: resource.md5_hash,
            crc32c: resource.crc32c,
            etag: resource.etag,
            time_created: resource.time_created,
            updated: resource.updated,
            download_tokens: resource
                .download_tokens
                .map(|tokens| {
                    tokens
                        .split(',')
                        .map(|token| token.trim().to_string())
                        .filter(|token| !token.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            custom_metadata: resource.metadata,
        }
    }
}

/// One page of a list operation: object references, prefix
/// references (with a delimiter, the "subdirectories"), and
/// the token for the next page, if any.
#[derive(Debug, Clone)]
pub struct ListResult {
    /// Objects in this page.
    pub items: Vec<Reference>,
    /// Prefixes in this page (only with a delimiter).
    pub prefixes: Vec<Reference>,
    /// Token for the next page, when the listing continues.
    pub next_page_token: Option<String>,
}

struct Inner {
    options: FirebaseOptions,
    tokens: Arc<dyn TokenProvider>,
    client: HttpClient,
}

/// Cloud Storage client handle.
pub struct Storage {
    inner: Arc<Inner>,
}

impl Storage {
    /// Creates a Storage handle for the app.
    pub fn new(app: &App, tokens: Arc<dyn TokenProvider>) -> Self {
        let options = app.options().clone();
        Self {
            inner: Arc::new(Inner {
                options,
                tokens: tokens.clone(),
                client: HttpClient::new(tokens, None),
            }),
        }
    }

    /// Overrides the JSON API base URL (tests, the Storage
    /// emulator). The default is
    /// `https://firebasestorage.googleapis.com/v0`.
    pub fn with_endpoint(self, endpoint: impl Into<String>) -> Self {
        let tokens = self.inner.tokens.clone();
        Self {
            inner: Arc::new(Inner {
                options: self.inner.options.clone(),
                tokens: tokens.clone(),
                client: HttpClient::new(tokens, Some(endpoint.into())),
            }),
        }
    }

    /// The default bucket from the app options, when configured.
    pub fn bucket(&self) -> Option<&str> {
        self.inner.options.storage_bucket.as_deref()
    }

    /// A reference to `path` in the app's default bucket. The
    /// empty path refers to the bucket root (useful for
    /// listing).
    pub fn reference(&self, path: &str) -> Reference {
        Reference {
            inner: self.inner.clone(),
            bucket: self.inner.options.storage_bucket.clone(),
            path: normalize_path(path),
        }
    }

    /// A reference to `path` in an explicit bucket.
    pub fn reference_in_bucket(&self, bucket: &str, path: &str) -> Reference {
        Reference {
            inner: self.inner.clone(),
            bucket: Some(bucket.to_string()),
            path: normalize_path(path),
        }
    }
}

/// Trims leading and trailing slashes, so `"a/b/"`, `"/a/b"`
/// and `"a/b"` address the same object.
fn normalize_path(path: &str) -> String {
    path.trim_matches('/').to_string()
}

/// A reference to an object (or a prefix, for listing) in a
/// bucket. References are cheap handles; every operation goes
/// to the server when called.
#[derive(Clone)]
pub struct Reference {
    inner: Arc<Inner>,
    bucket: Option<String>,
    path: String,
}

impl std::fmt::Debug for Reference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reference")
            .field("bucket", &self.bucket)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl Reference {
    /// The bucket this reference points into, when known.
    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }

    /// The object path within the bucket (empty for the root).
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The last path segment.
    pub fn name(&self) -> &str {
        self.path.rsplit('/').next().unwrap_or(&self.path)
    }

    /// A reference to a descendant of this reference.
    pub fn child(&self, path: &str) -> Reference {
        let child = normalize_path(path);
        let joined = if self.path.is_empty() {
            child
        } else if child.is_empty() {
            self.path.clone()
        } else {
            format!("{}/{}", self.path, child)
        };
        Reference {
            inner: self.inner.clone(),
            bucket: self.bucket.clone(),
            path: joined,
        }
    }

    fn require_bucket(&self) -> Result<&str> {
        self.bucket.as_deref().ok_or_else(|| {
            Error::InvalidArgument(
                "no storage bucket configured (set FirebaseOptions::with_storage_bucket \
                 or use reference_in_bucket)"
                    .to_string(),
            )
        })
    }

    fn require_object_path(&self) -> Result<&str> {
        if self.path.is_empty() {
            return Err(Error::InvalidArgument(
                "an object path is required for this operation".to_string(),
            ));
        }
        Ok(&self.path)
    }

    /// Uploads `data` in a single request: a media upload when
    /// no metadata is given, a multipart upload when it is.
    pub async fn put_bytes(
        &self,
        data: Vec<u8>,
        metadata: Option<PutMetadata>,
    ) -> Result<ObjectMetadata> {
        let bucket = self.require_bucket()?;
        let path = self.require_object_path()?;
        let resource = match &metadata {
            None => {
                self.inner
                    .client
                    .upload_media(bucket, path, "application/octet-stream", data)
                    .await?
            }
            Some(metadata) => {
                let content_type = metadata
                    .content_type
                    .clone()
                    .unwrap_or_else(|| "application/octet-stream".to_string());
                let payload = metadata.to_payload(Some(path.to_string()));
                self.inner
                    .client
                    .upload_multipart(bucket, path, &payload, &content_type, data)
                    .await?
            }
        };
        Ok(resource.into())
    }

    /// Uploads `data` through a resumable session, in chunks
    /// of 8 MiB (the final chunk may be short). If the server
    /// commits fewer bytes than a chunk carried, the upload
    /// resumes from the committed offset.
    pub async fn put_bytes_resumable(
        &self,
        data: Vec<u8>,
        metadata: Option<PutMetadata>,
    ) -> Result<ObjectMetadata> {
        let bucket = self.require_bucket()?;
        let path = self.require_object_path()?;
        let metadata = metadata.unwrap_or_default();
        let content_type = metadata
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let payload = metadata.to_payload(Some(path.to_string()));
        let session = self
            .inner
            .client
            .resumable_start(bucket, path, &payload, &content_type, data.len())
            .await?;
        let total = data.len();
        let mut cursor = 0usize;
        loop {
            let end = (cursor + RESUMABLE_CHUNK_SIZE).min(total);
            let chunk = if total == 0 {
                Vec::new()
            } else {
                data[cursor..end].to_vec()
            };
            let is_final = end >= total;
            match self
                .inner
                .client
                .resumable_put_chunk(&session, chunk, cursor, is_final)
                .await?
            {
                client::ChunkOutcome::Done(resource) => return Ok((*resource).into()),
                client::ChunkOutcome::Committed(committed) => {
                    if committed <= cursor || committed > total {
                        return Err(Error::Api {
                            status: None,
                            code: "UPLOAD_STALLED".to_string(),
                            message: format!(
                                "resumable upload made no progress (committed {committed} of {total})"
                            ),
                        });
                    }
                    cursor = committed;
                }
            }
        }
    }

    /// Downloads the object's bytes (`alt=media`).
    pub async fn get_bytes(&self) -> Result<Vec<u8>> {
        let bucket = self.require_bucket()?;
        let path = self.require_object_path()?;
        self.inner.client.download(bucket, path).await
    }

    /// Reads the object's metadata.
    pub async fn get_metadata(&self) -> Result<ObjectMetadata> {
        let bucket = self.require_bucket()?;
        let path = self.require_object_path()?;
        Ok(self.inner.client.get_metadata(bucket, path).await?.into())
    }

    /// Patches the object's metadata (only the fields set on
    /// `metadata` are sent).
    pub async fn update_metadata(&self, metadata: PutMetadata) -> Result<ObjectMetadata> {
        let bucket = self.require_bucket()?;
        let path = self.require_object_path()?;
        let payload = metadata.to_payload(None);
        Ok(self
            .inner
            .client
            .patch_metadata(bucket, path, &payload)
            .await?
            .into())
    }

    /// Deletes the object.
    pub async fn delete(&self) -> Result<()> {
        let bucket = self.require_bucket()?;
        let path = self.require_object_path()?;
        self.inner.client.delete(bucket, path).await
    }

    /// Lists one page under this reference. The reference's
    /// path is the listing prefix (its children, not itself).
    /// With a delimiter of `"/"`, names are grouped into
    /// prefixes instead of being listed recursively;
    /// `max_results` bounds the page size and `page_token`
    /// continues a previous page.
    pub async fn list(
        &self,
        delimiter: Option<&str>,
        max_results: Option<i32>,
        page_token: Option<&str>,
    ) -> Result<ListResult> {
        let bucket = self.require_bucket()?.to_string();
        let prefix = if self.path.is_empty() {
            None
        } else {
            Some(format!("{}/", self.path))
        };
        let resource = self
            .inner
            .client
            .list(
                &bucket,
                prefix.as_deref(),
                delimiter,
                max_results,
                page_token,
            )
            .await?;
        Ok(self.to_list_result(&bucket, resource))
    }

    /// Lists everything under this reference, following page
    /// tokens. With a delimiter of `"/"`, only the immediate
    /// level is returned (prefixes are not descended into).
    pub async fn list_all(&self, delimiter: Option<&str>) -> Result<ListResult> {
        let mut items = Vec::new();
        let mut prefixes = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let page = self.list(delimiter, None, page_token.as_deref()).await?;
            items.extend(page.items);
            prefixes.extend(page.prefixes);
            match page.next_page_token {
                Some(token) => page_token = Some(token),
                None => {
                    return Ok(ListResult {
                        items,
                        prefixes,
                        next_page_token: None,
                    })
                }
            }
        }
    }

    fn to_list_result(&self, bucket: &str, resource: wire::ListResource) -> ListResult {
        let make_ref = |path: &str| Reference {
            inner: self.inner.clone(),
            bucket: Some(bucket.to_string()),
            path: normalize_path(path),
        };
        ListResult {
            items: resource
                .items
                .iter()
                .map(|item| make_ref(&item.name))
                .collect(),
            prefixes: resource
                .prefixes
                .iter()
                .map(|prefix| make_ref(prefix))
                .collect(),
            next_page_token: resource.next_page_token,
        }
    }

    /// A download URL for the object, composed from its first
    /// download token: `{endpoint}/b/{bucket}/o/{object}` with
    /// `alt=media&token=...`. Anyone holding the URL can read
    /// the object while the token is valid; treat it as a
    /// capability.
    pub async fn download_url(&self) -> Result<String> {
        let bucket = self.require_bucket()?.to_string();
        let metadata = self.get_metadata().await?;
        let token = metadata.download_tokens.first().ok_or_else(|| {
            Error::InvalidArgument(format!("object {} has no download token", self.path))
        })?;
        Ok(format!(
            "{}/b/{}/o/{}?alt=media&token={}",
            self.inner.client.endpoint(),
            bucket,
            wire::encode_object_name(&self.path),
            token
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_normalize() {
        assert_eq!(normalize_path("/a/b/"), "a/b");
        assert_eq!(normalize_path(""), "");
        assert_eq!(normalize_path("a"), "a");
    }

    #[test]
    fn metadata_converts_from_the_wire_resource() {
        let resource: ObjectResource = serde_json::from_str(
            r#"{"name":"x/y.txt","bucket":"b","generation":"42","size":"7",
                "downloadTokens":"t1, t2","metadata":{"k":"v"}}"#,
        )
        .unwrap();
        let metadata = ObjectMetadata::from(resource);
        assert_eq!(metadata.generation, Some(42));
        assert_eq!(metadata.size, 7);
        assert_eq!(metadata.download_tokens, ["t1", "t2"]);
        assert_eq!(
            metadata.custom_metadata.get("k").map(String::as_str),
            Some("v")
        );
    }
}
