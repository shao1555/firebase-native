use std::sync::Arc;

use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request, Status};

use firebase_native_core::{App, Error, FirebaseOptions, Result, TokenProvider};

use crate::google::firestore::v1::firestore_client::FirestoreClient;
use crate::google::firestore::v1::{
    CreateDocumentRequest, DeleteDocumentRequest, DocumentMask, GetDocumentRequest,
    ListDocumentsRequest, RunQueryRequest, UpdateDocumentRequest,
};
use crate::path::{
    full_document_name, split_collection, validate_collection_path, validate_document_path,
};
use crate::query::Query;
use crate::value::{fields_to_proto, Document};
use crate::Fields;

const DEFAULT_ENDPOINT: &str = "https://firestore.googleapis.com";

pub(crate) type AuthService = InterceptedService<Channel, AuthInterceptor>;

#[derive(Clone)]
pub(crate) struct AuthInterceptor {
    header_value: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
    request_params: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.header_value.clone());
        request
            .metadata_mut()
            .insert("x-goog-request-params", self.request_params.clone());
        Ok(request)
    }
}

/// Builds the `x-goog-request-params` routing header value. Named
/// databases require it (the server rejects requests without it);
/// the official clients send it for every database.
fn routing_header(project_id: &str, database_id: &str) -> String {
    format!(
        "project_id={}&database_id={}",
        percent_encode(project_id),
        percent_encode(database_id)
    )
}

/// Percent-encodes a header parameter value (unreserved characters
/// are kept, everything else becomes `%XX`).
fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// The result of a `list_documents` page.
#[derive(Debug, Clone)]
pub struct ListDocumentsResult {
    /// The documents in this page.
    pub documents: Vec<Document>,
    /// Token for the next page, empty when there are no more pages.
    pub next_page_token: String,
}

/// Cloud Firestore client (online unary operations; P0 scope).
///
/// The client holds no token string: every call asks the
/// [`TokenProvider`] for the current ID token, and on an
/// `UNAUTHENTICATED` status it forces exactly one refresh and retries
/// the call exactly once.
pub struct Firestore {
    options: FirebaseOptions,
    tokens: Arc<dyn TokenProvider>,
    endpoint: String,
    channel: tokio::sync::OnceCell<Channel>,
}

impl Firestore {
    /// Creates a client for the app, obtaining tokens from `tokens`
    /// (in practice, an `Arc<Auth>`).
    pub fn new(app: &App, tokens: Arc<dyn TokenProvider>) -> Self {
        Self {
            options: app.options().clone(),
            tokens,
            endpoint: DEFAULT_ENDPOINT.to_string(),
            channel: tokio::sync::OnceCell::new(),
        }
    }

    /// Overrides the gRPC endpoint (Firestore emulator / tests).
    /// Plain `http://` endpoints connect without TLS.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    async fn channel(&self) -> Result<Channel> {
        // The dependency graph enables more than one rustls crypto
        // provider, so rustls cannot pick one automatically; select
        // ring as the process default before any TLS config is built
        // (a no-op when a default was already installed).
        let _ = rustls::crypto::ring::default_provider().install_default();
        self.channel
            .get_or_try_init(|| async {
                let mut endpoint = Endpoint::from_shared(self.endpoint.clone())
                    .map_err(|err| Error::Other(format!("invalid endpoint: {err}")))?;
                if self.endpoint.starts_with("https://") {
                    // Behind a proxy (grpc_proxy / https_proxy / all_proxy
                    // in the environment), tonic's own transport cannot
                    // reach the endpoint: tunnel with CONNECT instead.
                    if let Some(proxy) = endpoint
                        .uri()
                        .host()
                        .and_then(crate::proxy::ProxyConfig::from_env_for)
                    {
                        let proxy = std::sync::Arc::new(proxy);
                        // The connector returns the CONNECT tunnel;
                        // this TLS config runs over it (and is also
                        // what makes tonic accept an https URI).
                        let endpoint = endpoint
                            .tls_config(ClientTlsConfig::new().with_native_roots())
                            .map_err(|err| Error::Other(format!("tls config: {err}")))?;
                        return Ok(endpoint.connect_with_connector_lazy(tower::service_fn(
                            move |uri: tonic::transport::Uri| {
                                let proxy = proxy.clone();
                                async move {
                                    let host = uri.host().unwrap_or_default().to_string();
                                    let port = uri.port_u16().unwrap_or(443);
                                    crate::proxy::connect_via_proxy(proxy, host, port).await
                                }
                            },
                        )));
                    }
                    endpoint = endpoint
                        .tls_config(ClientTlsConfig::new().with_native_roots())
                        .map_err(|err| Error::Other(format!("tls config: {err}")))?;
                }
                Ok(endpoint.connect_lazy())
            })
            .await
            .cloned()
    }

    async fn client(&self, force_refresh: bool) -> Result<FirestoreClient<AuthService>> {
        grpc_client(
            self.channel().await?,
            &self.options,
            &self.tokens,
            force_refresh,
        )
        .await
    }

    /// Runs `op` with an authenticated client. On `UNAUTHENTICATED`,
    /// forces one token refresh and retries exactly once.
    async fn call<T, F, Fut>(&self, mut op: F) -> Result<T>
    where
        F: FnMut(FirestoreClient<AuthService>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, Status>>,
    {
        let mut force_refresh = false;
        loop {
            let client = self.client(force_refresh).await?;
            match op(client).await {
                Ok(value) => return Ok(value),
                Err(status) if status.code() == Code::Unauthenticated && !force_refresh => {
                    force_refresh = true;
                }
                Err(status) => return Err(map_status(status)),
            }
        }
    }

    // -- Listen (P1) ------------------------------------------------------

    /// Watches a collection query: opens a Listen stream and returns
    /// a [`Listener`] yielding a snapshot at `CURRENT` and at every
    /// later consistent point where the view changed. `parent_path`
    /// follows the same convention as [`Firestore::run_query`].
    ///
    /// The stream reconnects with the last resume token and
    /// exponential backoff when it breaks; terminal failures
    /// (permission denied, invalid target, ...) are delivered as one
    /// final `Err` item.
    pub async fn watch_query(
        &self,
        parent_path: &str,
        query: Query,
    ) -> Result<crate::listen::Listener> {
        let parent = if parent_path.is_empty() {
            self.options.documents_prefix()
        } else {
            validate_document_path(parent_path)?;
            full_document_name(&self.options.documents_prefix(), parent_path)
        };
        let orders = query.effective_orders();
        let target = crate::google::firestore::v1::Target {
            target_id: crate::listen::TARGET_ID,
            once: false,
            expected_count: None,
            target_type: Some(
                crate::google::firestore::v1::target::TargetType::Query(
                    crate::google::firestore::v1::target::QueryTarget {
                        parent,
                        query_type: Some(
                            crate::google::firestore::v1::target::query_target::QueryType::StructuredQuery(
                                query.to_proto(),
                            ),
                        ),
                    },
                ),
            ),
            resume_type: None,
        };
        let channel = self.channel().await?;
        Ok(crate::listen::spawn_watch(
            channel,
            self.options.clone(),
            self.tokens.clone(),
            target,
            orders,
        ))
    }

    /// Watches a single document by relative path. The snapshot
    /// holds the document when it exists and is empty when it does
    /// not (or stops existing).
    pub async fn watch_document(&self, path: &str) -> Result<crate::listen::Listener> {
        validate_document_path(path)?;
        let name = full_document_name(&self.options.documents_prefix(), path);
        let target = crate::google::firestore::v1::Target {
            target_id: crate::listen::TARGET_ID,
            once: false,
            expected_count: None,
            target_type: Some(crate::google::firestore::v1::target::TargetType::Documents(
                crate::google::firestore::v1::target::DocumentsTarget {
                    documents: vec![name],
                },
            )),
            resume_type: None,
        };
        let channel = self.channel().await?;
        Ok(crate::listen::spawn_watch(
            channel,
            self.options.clone(),
            self.tokens.clone(),
            target,
            Vec::new(),
        ))
    }

    // -- Unary CRUD -------------------------------------------------------

    /// Gets a document by its relative path (`notes/abc`). Returns
    /// `None` when the document does not exist.
    pub async fn get_document(&self, path: &str) -> Result<Option<Document>> {
        validate_document_path(path)?;
        let name = full_document_name(&self.options.documents_prefix(), path);
        let result = self
            .call(move |mut client| {
                let name = name.clone();
                async move {
                    client
                        .get_document(GetDocumentRequest {
                            name,
                            mask: None,
                            consistency_selector: None,
                            request_options: None,
                        })
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await;
        match result {
            Ok(proto) => Ok(Some(Document::from_proto(proto)?)),
            Err(Error::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Creates a document in a collection. When `document_id` is `None`
    /// the server assigns an ID.
    pub async fn create_document(
        &self,
        collection_path: &str,
        document_id: Option<&str>,
        fields: Fields,
    ) -> Result<Document> {
        validate_collection_path(collection_path)?;
        let (parent, collection_id) =
            split_collection(&self.options.documents_prefix(), collection_path);
        let proto_document = crate::google::firestore::v1::Document {
            name: String::new(),
            fields: fields_to_proto(&fields),
            create_time: None,
            update_time: None,
        };
        let proto = self
            .call(move |mut client| {
                let parent = parent.clone();
                let collection_id = collection_id.clone();
                let document = proto_document.clone();
                async move {
                    client
                        .create_document(CreateDocumentRequest {
                            parent,
                            collection_id,
                            document_id: document_id.unwrap_or_default().to_string(),
                            document: Some(document),
                            mask: None,
                            request_options: None,
                        })
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await?;
        Document::from_proto(proto)
    }

    /// Sets a document, creating it if needed and replacing the fields
    /// given (fields not listed keep their server values only when
    /// their names are absent from `fields`; see `update_document` for
    /// explicit masks). This is the upsert form built on
    /// `UpdateDocument` with a mask of the provided field names.
    pub async fn set_document(&self, path: &str, fields: Fields) -> Result<Document> {
        let mask: Vec<String> = fields
            .keys()
            .map(|key| crate::path::encode_field_path_segment(key))
            .collect();
        self.update_document_with_mask(path, fields, &mask).await
    }

    /// Updates a document with an explicit field mask.
    pub async fn update_document(
        &self,
        path: &str,
        fields: Fields,
        field_mask: &[String],
    ) -> Result<Document> {
        self.update_document_with_mask(path, fields, field_mask)
            .await
    }

    async fn update_document_with_mask(
        &self,
        path: &str,
        fields: Fields,
        field_mask: &[String],
    ) -> Result<Document> {
        validate_document_path(path)?;
        let name = full_document_name(&self.options.documents_prefix(), path);
        let proto_document = crate::google::firestore::v1::Document {
            name: name.clone(),
            fields: fields_to_proto(&fields),
            create_time: None,
            update_time: None,
        };
        let field_paths = field_mask.to_vec();
        let proto = self
            .call(move |mut client| {
                let document = proto_document.clone();
                let field_paths = field_paths.clone();
                async move {
                    client
                        .update_document(UpdateDocumentRequest {
                            document: Some(document),
                            update_mask: Some(DocumentMask { field_paths }),
                            mask: None,
                            current_document: None,
                            request_options: None,
                        })
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await?;
        Document::from_proto(proto)
    }

    /// Deletes a document. Deleting a missing document succeeds.
    pub async fn delete_document(&self, path: &str) -> Result<()> {
        validate_document_path(path)?;
        let name = full_document_name(&self.options.documents_prefix(), path);
        self.call(move |mut client| {
            let name = name.clone();
            async move {
                client
                    .delete_document(DeleteDocumentRequest {
                        name,
                        current_document: None,
                        request_options: None,
                    })
                    .await
                    .map(|_| ())
            }
        })
        .await
    }

    /// Lists one page of documents in a collection.
    pub async fn list_documents(
        &self,
        collection_path: &str,
        page_size: Option<i32>,
        page_token: Option<String>,
    ) -> Result<ListDocumentsResult> {
        validate_collection_path(collection_path)?;
        let (parent, collection_id) =
            split_collection(&self.options.documents_prefix(), collection_path);
        let response = self
            .call(move |mut client| {
                let parent = parent.clone();
                let collection_id = collection_id.clone();
                let page_token = page_token.clone();
                async move {
                    client
                        .list_documents(ListDocumentsRequest {
                            parent,
                            collection_id,
                            page_size: page_size.unwrap_or(0),
                            page_token: page_token.unwrap_or_default(),
                            mask: None,
                            consistency_selector: None,
                            order_by: String::new(),
                            show_missing: false,
                            request_options: None,
                        })
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await?;
        let mut documents = Vec::with_capacity(response.documents.len());
        for proto in response.documents {
            documents.push(Document::from_proto(proto)?);
        }
        Ok(ListDocumentsResult {
            documents,
            next_page_token: response.next_page_token,
        })
    }

    /// Runs a structured query. `parent_path` is the relative path of
    /// the parent document for subcollections, or empty for a root
    /// collection.
    pub async fn run_query(&self, parent_path: &str, query: Query) -> Result<Vec<Document>> {
        let parent = if parent_path.is_empty() {
            self.options.documents_prefix()
        } else {
            validate_document_path(parent_path)?;
            full_document_name(&self.options.documents_prefix(), parent_path)
        };
        let structured = query.to_proto();
        let responses = self
            .call(move |mut client| {
                let parent = parent.clone();
                let structured = structured.clone();
                async move {
                    let response = client
                        .run_query(RunQueryRequest {
                            parent,
                            query_type: Some(
                                crate::google::firestore::v1::run_query_request::QueryType::StructuredQuery(
                                    structured,
                                ),
                            ),
                            consistency_selector: None,
                            explain_options: None,
                            request_options: None,
                        })
                        .await?;
                    let mut stream = response.into_inner();
                    let mut out = Vec::new();
                    while let Some(item) = stream.message().await? {
                        out.push(item);
                    }
                    Ok(out)
                }
            })
            .await?;
        let mut documents = Vec::new();
        for response in responses {
            if let Some(proto) = response.document {
                documents.push(Document::from_proto(proto)?);
            }
        }
        Ok(documents)
    }
}

/// Maps a gRPC status onto the shared error type.
pub(crate) fn map_status(status: Status) -> Error {
    let message = status.message().to_string();
    match status.code() {
        Code::Unauthenticated => Error::Unauthenticated(message),
        Code::NotFound => Error::NotFound(message),
        Code::PermissionDenied => Error::PermissionDenied(message),
        Code::InvalidArgument => Error::InvalidArgument(message),
        code => Error::Rpc {
            code: format!("{code:?}"),
            message,
        },
    }
}

/// Builds an authenticated gRPC client over an existing channel.
/// Shared by the unary calls and the Listen stream driver: every
/// (re)connection fetches the token for that attempt.
pub(crate) async fn grpc_client(
    channel: Channel,
    options: &FirebaseOptions,
    tokens: &Arc<dyn TokenProvider>,
    force_refresh: bool,
) -> Result<FirestoreClient<AuthService>> {
    let token = tokens.id_token(force_refresh).await?;
    let header_value = format!("Bearer {token}")
        .parse()
        .map_err(|_| Error::Other("ID token is not a valid header value".to_string()))?;
    let request_params = routing_header(&options.project_id, &options.database_id)
        .parse()
        .map_err(|_| Error::Other("routing header is not a valid header value".to_string()))?;
    let service = InterceptedService::new(
        channel,
        AuthInterceptor {
            header_value,
            request_params,
        },
    );
    Ok(FirestoreClient::new(service))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_header_encodes_default_database() {
        assert_eq!(
            routing_header("my-project", "mydb"),
            "project_id=my-project&database_id=mydb"
        );
        assert_eq!(
            routing_header("demo", "(default)"),
            "project_id=demo&database_id=%28default%29"
        );
    }

    #[test]
    fn status_mapping() {
        assert!(matches!(
            map_status(Status::not_found("gone")),
            Error::NotFound(_)
        ));
        assert!(matches!(
            map_status(Status::unauthenticated("bad token")),
            Error::Unauthenticated(_)
        ));
        assert!(matches!(
            map_status(Status::permission_denied("rules")),
            Error::PermissionDenied(_)
        ));
        assert!(matches!(
            map_status(Status::unavailable("later")),
            Error::Rpc { .. }
        ));
    }
}
