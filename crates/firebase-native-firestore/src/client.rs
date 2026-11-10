use std::sync::Arc;

use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request, Status};

use firebase_native_core::{App, Error, FirebaseOptions, Result, TokenProvider};

use crate::google::firestore::v1::firestore_client::FirestoreClient;
use crate::google::firestore::v1::{
    CommitRequest, CreateDocumentRequest, DeleteDocumentRequest, DocumentMask, GetDocumentRequest,
    ListDocumentsRequest, RunQueryRequest, UpdateDocumentRequest,
};
use crate::path::{
    full_document_name, split_collection, validate_collection_path, validate_document_path,
};
use crate::query::Query;
use crate::value::{fields_to_proto, Document, FieldValue};
use crate::{FieldValues, Fields};

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
    channel: Arc<tokio::sync::OnceCell<Channel>>,
    /// The persistence engine, when local persistence is enabled
    /// via [`Firestore::with_local_store`].
    engine: Option<Arc<crate::engine::PersistenceEngine>>,
}

impl Firestore {
    /// Creates a client for the app, obtaining tokens from `tokens`
    /// (in practice, an `Arc<Auth>`).
    pub fn new(app: &App, tokens: Arc<dyn TokenProvider>) -> Self {
        Self {
            options: app.options().clone(),
            tokens,
            endpoint: DEFAULT_ENDPOINT.to_string(),
            channel: Arc::new(tokio::sync::OnceCell::new()),
            engine: None,
        }
    }

    /// Enables local persistence on this client, backed by the
    /// SQLite store at `path`: writes are queued locally and
    /// committed over the Write stream by a background pump, reads
    /// are served from the local view (server-confirmed state plus
    /// queued mutations), and queued writes survive restarts.
    ///
    /// The store's location is the caller's choice — the crate
    /// owns no storage policy. Call this after
    /// [`Firestore::with_endpoint`] when overriding the endpoint:
    /// the write path captures the endpoint (and shares the gRPC
    /// channel) at this point.
    pub fn enable_local_store(mut self, path: &std::path::Path) -> Result<Self> {
        let store = Arc::new(crate::store::LocalStore::open(path)?);
        let write = crate::write::WriteStream::new(
            self.options.clone(),
            self.tokens.clone(),
            self.channel.clone(),
            self.endpoint.clone(),
        );
        self.engine = Some(Arc::new(crate::engine::PersistenceEngine::new(
            store,
            write,
            self.options.documents_prefix(),
        )));
        Ok(self)
    }

    /// Enqueues one mutation in persistent mode and returns the
    /// resulting local view of the document.
    fn enqueue_and_view(
        &self,
        engine: &Arc<crate::engine::PersistenceEngine>,
        mutation: crate::mutation::Mutation,
        path: &str,
    ) -> Result<Document> {
        engine.enqueue(vec![mutation])?;
        engine
            .local_document(path)?
            .ok_or_else(|| Error::Other("local view lost the document after enqueue".to_string()))
    }

    /// Waits until the mutation queue is empty.
    ///
    /// This is a queue-empty wait, not a snapshot of the
    /// writes queued at call time: batches enqueued by other
    /// tasks while this future is pending are waited for too.
    /// When the queue drains the call returns `Ok(())` -
    /// unless a queued write was permanently rejected by the
    /// server along the way, in which case the rejection is
    /// remembered and the first waiter to observe the empty
    /// queue returns it as an `Err` (the rejected batch
    /// itself was dropped from the queue). With no local
    /// store enabled nothing is ever queued, so it returns
    /// `Ok(())` immediately.
    pub async fn wait_for_pending_writes(&self) -> Result<()> {
        match &self.engine {
            Some(engine) => engine.wait_for_pending_writes().await,
            None => Ok(()),
        }
    }

    /// Overrides the gRPC endpoint (Firestore emulator / tests).
    /// Plain `http://` endpoints connect without TLS.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    async fn channel(&self) -> Result<Channel> {
        let channel = self
            .channel
            .get_or_try_init(|| build_channel(&self.endpoint))
            .await?;
        Ok(channel.clone())
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
    /// a [`crate::Listener`] yielding a snapshot at `CURRENT` and at every
    /// later consistent point where the view changed. `parent_path`
    /// follows the same convention as [`Firestore::run_query`].
    ///
    /// The stream reconnects with the last resume token and
    /// exponential backoff when it breaks; terminal failures
    /// (permission denied, invalid target, ...) are delivered as one
    /// final `Err` item.
    ///
    /// With local persistence enabled the watch is store-backed:
    /// the first snapshot comes from the local store immediately,
    /// later snapshots are composed from the store (so local
    /// writes appear as soon as they are queued), streamed
    /// changes are written into the store, and the target resumes
    /// with the resume token persisted by an earlier watch of the
    /// same query.
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
        let store_watch = self.engine.as_ref().map(|engine| {
            crate::listen::StoreWatch::new(
                engine.clone(),
                crate::listen::WatchSource::Query {
                    parent_relative: parent_path.to_string(),
                    query: query.clone(),
                },
                target_key(&target),
            )
        });
        let channel = self.channel().await?;
        Ok(crate::listen::spawn_watch(
            channel,
            self.options.clone(),
            self.tokens.clone(),
            target,
            orders,
            store_watch,
            Some(query),
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
        let store_watch = self.engine.as_ref().map(|engine| {
            crate::listen::StoreWatch::new(
                engine.clone(),
                crate::listen::WatchSource::Document {
                    relative_path: path.to_string(),
                },
                target_key(&target),
            )
        });
        let channel = self.channel().await?;
        Ok(crate::listen::spawn_watch(
            channel,
            self.options.clone(),
            self.tokens.clone(),
            target,
            Vec::new(),
            store_watch,
            None,
        ))
    }

    // -- Unary CRUD -------------------------------------------------------

    /// Gets a document by its relative path (`notes/abc`). Returns
    /// `None` when the document does not exist.
    pub async fn get_document(&self, path: &str) -> Result<Option<Document>> {
        validate_document_path(path)?;
        if let Some(engine) = &self.engine {
            // Persistent mode: the local view answers first. When
            // nothing is known locally, fall back to an online fetch
            // and cache the result.
            if let Some(document) = engine.local_document(path)? {
                return Ok(Some(document));
            }
            if engine.has_pending(path)? {
                // A queued mutation (e.g. a delete) already decides
                // this document's local state.
                return Ok(None);
            }
            // Capture the local-state generation before the fetch:
            // any local change while it is in flight — a write
            // queued, or queued *and already acknowledged* — means
            // the response describes an older read point than the
            // current local state.
            let generation = engine.generation();
            let document_generation = engine.document_generation(path);
            let fetched = self.get_document_online(path).await?;
            if engine.generation() == generation {
                // Nothing raced the fetch: cache and return as-is.
                match &fetched {
                    Some(document) => engine.store_remote_document(document)?,
                    None => engine.store_remote_absent(path)?,
                }
                return Ok(fetched);
            }
            // Reconcile conservatively and answer from the current
            // local view. A missing result evicts nothing (a create
            // may have been acknowledged after the fetch's read
            // point); a fetched document goes through the engine's
            // reconciliation, which also covers the acknowledged-
            // delete case (no cache row left to compare against).
            engine.reconcile_fetched_document(path, fetched.as_ref(), document_generation)?;
            return engine.local_document(path);
        }
        self.get_document_online(path).await
    }

    async fn get_document_online(&self, path: &str) -> Result<Option<Document>> {
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
        if let Some(engine) = &self.engine {
            let document_id = document_id
                .map(str::to_string)
                .unwrap_or_else(generate_document_id);
            let path = format!("{collection_path}/{document_id}");
            validate_document_path(&path)?;
            return self.enqueue_and_view(
                engine,
                crate::mutation::Mutation::Set {
                    path: path.clone(),
                    fields,
                    merge_paths: None,
                    exists: Some(false),
                    transforms: Vec::new(),
                },
                &path,
            );
        }
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
        if let Some(engine) = &self.engine {
            validate_document_path(path)?;
            return self.enqueue_and_view(
                engine,
                crate::mutation::Mutation::Set {
                    path: path.to_string(),
                    fields,
                    merge_paths: Some(mask),
                    exists: None,
                    transforms: Vec::new(),
                },
                path,
            );
        }
        self.update_document_with_mask(path, fields, &mask).await
    }

    /// Updates a document with an explicit field mask.
    pub async fn update_document(
        &self,
        path: &str,
        fields: Fields,
        field_mask: &[String],
    ) -> Result<Document> {
        if let Some(engine) = &self.engine {
            validate_document_path(path)?;
            self.ensure_local_base(engine, path).await?;
            return self.enqueue_and_view(
                engine,
                crate::mutation::Mutation::Patch {
                    path: path.to_string(),
                    fields,
                    mask: field_mask.to_vec(),
                    exists: Some(true),
                    transforms: Vec::new(),
                },
                path,
            );
        }
        self.update_document_with_mask(path, fields, field_mask)
            .await
    }

    /// Establishes a local base for a patch-style update in
    /// persistent mode. An update patches an existing document,
    /// so it needs a known base: when the local view has none,
    /// fetch the document from the server first. A missing
    /// document is NotFound and an unreachable server is an
    /// error — and in both cases nothing is queued, so an update
    /// that is returned as failed never takes effect later
    /// behind the caller's back. (A queued delete ahead of it
    /// also leaves the view empty; the update then fails the
    /// same way it will on the server.)
    async fn ensure_local_base(
        &self,
        engine: &Arc<crate::engine::PersistenceEngine>,
        path: &str,
    ) -> Result<()> {
        if engine.local_document(path)?.is_none() {
            match self.get_document_online(path).await {
                Ok(Some(document)) => engine.store_remote_document(&document)?,
                Ok(None) => {
                    return Err(Error::NotFound(format!("no document exists at {path}")));
                }
                Err(error) => return Err(error),
            }
            if engine.local_document(path)?.is_none() {
                return Err(Error::NotFound(format!(
                    "document at {path} is not present in the local view"
                )));
            }
        }
        Ok(())
    }

    /// Commits one mutation through the unary Commit RPC — the
    /// online form of the Write stream's single-batch commit.
    /// The unary UpdateDocument RPC cannot express field
    /// transforms, so the sentinel writes take this route (as
    /// the official SDKs do).
    async fn commit_mutation(&self, mutation: &crate::mutation::Mutation) -> Result<()> {
        let database = self.options.database_name();
        let write = mutation.to_proto(&self.options.documents_prefix());
        self.call(move |mut client| {
            let database = database.clone();
            let write = write.clone();
            async move {
                client
                    .commit(CommitRequest {
                        database,
                        writes: vec![write],
                        transaction: Vec::new(),
                        request_options: None,
                    })
                    .await
                    .map(|_| ())
            }
        })
        .await
    }

    /// Sets a document from [`FieldValues`]: literal values plus
    /// [`FieldValue`] sentinels (server timestamp, increment,
    /// array union/remove, field delete), addressed by field
    /// path. With `merge`, only the named fields are affected
    /// and `Delete` removes its field; without it the document
    /// is replaced wholesale and `Delete` is rejected.
    ///
    /// In persistent mode the write is queued locally like any
    /// other; online it is committed through the Commit RPC
    /// (the unary update RPC cannot express transforms) and the
    /// result is read back, so the returned document carries
    /// the server's exact transform results.
    pub async fn set_document_with(
        &self,
        path: &str,
        values: FieldValues,
        merge: bool,
    ) -> Result<Document> {
        validate_document_path(path)?;
        let mutation = set_mutation(path, &values, merge)?;
        if let Some(engine) = &self.engine {
            return self.enqueue_and_view(engine, mutation, path);
        }
        self.commit_mutation(&mutation).await?;
        self.get_document_online(path)
            .await?
            .ok_or_else(|| Error::Other(format!("document missing after commit: {path}")))
    }

    /// Updates an existing document from [`FieldValues`]:
    /// literal values plus [`FieldValue`] sentinels, addressed
    /// by field path. The document must exist (in persistent
    /// mode a base is established first, exactly as in
    /// [`Firestore::update_document`]); a missing document is
    /// NotFound.
    pub async fn update_document_with(&self, path: &str, values: FieldValues) -> Result<Document> {
        validate_document_path(path)?;
        let mutation = patch_mutation(path, &values)?;
        if let Some(engine) = &self.engine {
            self.ensure_local_base(engine, path).await?;
            return self.enqueue_and_view(engine, mutation, path);
        }
        self.commit_mutation(&mutation)
            .await
            .map_err(|error| match error {
                // The mutation's exists precondition failed on the
                // server: report it the way the persistent path does.
                Error::Rpc { code, .. } if code == "FailedPrecondition" => {
                    Error::NotFound(format!("no document exists at {path}"))
                }
                other => other,
            })?;
        self.get_document_online(path)
            .await?
            .ok_or_else(|| Error::Other(format!("document missing after commit: {path}")))
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
        if let Some(engine) = &self.engine {
            engine.enqueue(vec![crate::mutation::Mutation::Delete {
                path: path.to_string(),
            }])?;
            return Ok(());
        }
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
    ///
    /// In persistent mode the local store answers, mirroring
    /// `get_document`'s rule: when the store knows the collection
    /// (a cached document or a queued mutation in it), the query
    /// is evaluated over the local views — unacknowledged writes
    /// included — and no network call is made. When the store
    /// knows nothing about the collection, an online query runs
    /// first (unpaginated, so the local evaluation applies
    /// offset/limit exactly once), its results are cached, and
    /// the answer is the local evaluation over the filled store;
    /// if that fetch fails with a connectivity error, the local
    /// evaluation — possibly empty — is the answer rather than
    /// an error. Validation and terminal service errors always
    /// propagate.
    pub async fn run_query(&self, parent_path: &str, query: Query) -> Result<Vec<Document>> {
        if !parent_path.is_empty() {
            validate_document_path(parent_path)?;
        }
        if let Some(engine) = &self.engine {
            if engine.collection_known(parent_path, query.collection_id())? {
                return engine.local_query(parent_path, &query);
            }
            match self
                .run_query_online(parent_path, query.without_pagination())
                .await
            {
                Ok(documents) => {
                    for document in &documents {
                        engine.store_remote_document(document)?;
                    }
                    return engine.local_query(parent_path, &query);
                }
                Err(error) if crate::listen::is_transient_error(&error) => {
                    return engine.local_query(parent_path, &query);
                }
                Err(error) => return Err(error),
            }
        }
        self.run_query_online(parent_path, query).await
    }

    async fn run_query_online(&self, parent_path: &str, query: Query) -> Result<Vec<Document>> {
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

/// The pieces a [`FieldValues`] map decomposes into: the literal
/// fields (path keys folded into nested maps), the paths they
/// occupy, the paths to delete, and the server transforms.
struct TranslatedValues {
    fields: Fields,
    set_paths: Vec<String>,
    delete_paths: Vec<String>,
    transforms: Vec<crate::mutation::FieldTransform>,
}

/// Decomposes [`FieldValues`] for the mutation layer. Rejects
/// an empty map, a malformed field path, two keys denoting the
/// same parsed field or nesting under another entry's path
/// (the mutation model's literal and transform paths must be
/// disjoint, and a scalar cannot also be a parent map), and a
/// non-numeric increment operand.
fn translate_values(values: &FieldValues) -> Result<TranslatedValues> {
    if values.is_empty() {
        return Err(Error::InvalidArgument("no field values given".to_string()));
    }
    // Validate syntax before anything is enqueued, and compare
    // parsed segments: two distinct keys can still denote the
    // same field (`n` vs `` `n` ``), and equality is as much a
    // conflict as ancestry.
    let mut parsed: Vec<(&String, Vec<String>)> = Vec::with_capacity(values.len());
    for path in values.keys() {
        parsed.push((path, crate::path::validate_field_path(path)?));
    }
    for (index, (a, a_segments)) in parsed.iter().enumerate() {
        for (b, b_segments) in &parsed[index + 1..] {
            if a_segments == b_segments {
                return Err(Error::InvalidArgument(format!(
                    "field paths {a} and {b} denote the same field"
                )));
            }
            let (shorter, longer) = if a_segments.len() <= b_segments.len() {
                (a_segments, b_segments)
            } else {
                (b_segments, a_segments)
            };
            if shorter.len() < longer.len() && longer[..shorter.len()] == shorter[..] {
                return Err(Error::InvalidArgument(format!(
                    "field paths {a} and {b} overlap"
                )));
            }
        }
    }
    let mut translated = TranslatedValues {
        fields: Fields::new(),
        set_paths: Vec::new(),
        delete_paths: Vec::new(),
        transforms: Vec::new(),
    };
    for (path, value) in values {
        match value {
            FieldValue::Value(value) => {
                crate::mutation::set_path(&mut translated.fields, path, value.clone());
                translated.set_paths.push(path.clone());
            }
            FieldValue::Delete => translated.delete_paths.push(path.clone()),
            FieldValue::ServerTimestamp => {
                translated.transforms.push(crate::mutation::FieldTransform {
                    path: path.clone(),
                    kind: crate::mutation::TransformKind::ServerTimestamp,
                });
            }
            FieldValue::Increment(operand) => {
                if !operand.is_number() {
                    return Err(Error::InvalidArgument(format!(
                        "increment operand for {path} must be a number"
                    )));
                }
                translated.transforms.push(crate::mutation::FieldTransform {
                    path: path.clone(),
                    kind: crate::mutation::TransformKind::Increment(operand.clone()),
                });
            }
            FieldValue::ArrayUnion(elements) => {
                translated.transforms.push(crate::mutation::FieldTransform {
                    path: path.clone(),
                    kind: crate::mutation::TransformKind::ArrayUnion(elements.clone()),
                });
            }
            FieldValue::ArrayRemove(elements) => {
                translated.transforms.push(crate::mutation::FieldTransform {
                    path: path.clone(),
                    kind: crate::mutation::TransformKind::ArrayRemove(elements.clone()),
                });
            }
        }
    }
    Ok(translated)
}

/// Builds the `Set` mutation for `set_document_with`: with
/// `merge`, the merge paths are exactly the literal and deleted
/// paths (transforms travel separately); without it the
/// document is replaced and a field delete has no meaning.
fn set_mutation(
    path: &str,
    values: &FieldValues,
    merge: bool,
) -> Result<crate::mutation::Mutation> {
    let translated = translate_values(values)?;
    if !merge && !translated.delete_paths.is_empty() {
        return Err(Error::InvalidArgument(
            "FieldValue::Delete cannot be used in a non-merge set; the document is replaced"
                .to_string(),
        ));
    }
    let merge_paths = merge.then(|| {
        let mut paths = translated.set_paths.clone();
        paths.extend(translated.delete_paths.iter().cloned());
        paths.sort();
        paths
    });
    Ok(crate::mutation::Mutation::Set {
        path: path.to_string(),
        fields: translated.fields,
        merge_paths,
        exists: None,
        transforms: translated.transforms,
    })
}

/// Builds the `Patch` mutation for `update_document_with`: the
/// mask is exactly the literal and deleted paths, and the
/// document must exist.
fn patch_mutation(path: &str, values: &FieldValues) -> Result<crate::mutation::Mutation> {
    let translated = translate_values(values)?;
    let mut mask = translated.set_paths.clone();
    mask.extend(translated.delete_paths.iter().cloned());
    mask.sort();
    Ok(crate::mutation::Mutation::Patch {
        path: path.to_string(),
        fields: translated.fields,
        mask,
        exists: Some(true),
        transforms: translated.transforms,
    })
}

/// A stable key for a watch target's row in the local store's
/// targets table, derived from the target definition. Every
/// watch uses the same wire target ID, so the row cannot be
/// keyed by it; a hash of the definition keeps concurrent
/// watches on different targets apart and lets the same target
/// find its row in a later process.
fn target_key(target: &crate::google::firestore::v1::Target) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{target:?}").hash(&mut hasher);
    hasher.finish() as i64
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
/// Builds the shared gRPC channel for an endpoint: native-root TLS
/// for `https://`, with a CONNECT tunnel when the environment
/// routes through a proxy (tonic cannot tunnel by itself).
pub(crate) async fn build_channel(endpoint_str: &str) -> Result<Channel> {
    // The dependency graph enables more than one rustls crypto
    // provider, so rustls cannot pick one automatically; select
    // ring as the process default before any TLS config is built
    // (a no-op when a default was already installed).
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut endpoint = Endpoint::from_shared(endpoint_str.to_string())
        .map_err(|err| Error::Other(format!("invalid endpoint: {err}")))?;
    if endpoint_str.starts_with("https://") {
        if let Some(proxy) = endpoint
            .uri()
            .host()
            .and_then(crate::proxy::ProxyConfig::from_env_for)
        {
            let proxy = std::sync::Arc::new(proxy);
            // The connector returns the CONNECT tunnel; this TLS
            // config runs over it (and is also what makes tonic
            // accept an https URI).
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
}

/// Generates a random-looking 20-character document ID, the same
/// shape Firestore's own clients generate. Entropy comes from the
/// system clock, the process id and a process-wide counter mixed
/// with a splitmix64 finalizer: enough for collision-resistant IDs
/// without a randomness dependency.
fn generate_document_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut state = nanos
        ^ (std::process::id() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ COUNTER
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_mul(0xD1B5_4A32_D192_ED03);
    let mut id = String::with_capacity(20);
    for _ in 0..20 {
        // splitmix64 step.
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        id.push(ALPHABET[(z % 62) as usize] as char);
    }
    id
}

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
    use crate::value::Value;
    use firebase_native_core::{App, FirebaseOptions};

    struct StubTokens;

    #[async_trait::async_trait]
    impl firebase_native_core::TokenProvider for StubTokens {
        async fn id_token(&self, _force_refresh: bool) -> Result<String> {
            Ok("stub-token".to_string())
        }
    }

    /// An update against a document with no known local base, while
    /// the server is unreachable, must fail *without* queueing: a
    /// failed operation may not take effect later.
    #[tokio::test]
    async fn persistent_update_without_base_fails_without_queueing() {
        let dir = std::env::temp_dir().join(format!(
            "firebase-native-client-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store_path = dir.join("store.db");
        let _ = std::fs::remove_file(&store_path);

        let options = FirebaseOptions::new("test-key", "test-project").with_database_id("testdb");
        let app = App::new(options);
        let firestore = Firestore::new(&app, Arc::new(StubTokens))
            .with_endpoint("http://127.0.0.1:9")
            .enable_local_store(&store_path)
            .unwrap();
        let mut fields = Fields::new();
        fields.insert("n".to_string(), Value::Integer(1));
        let result = firestore
            .update_document("notes/a", fields, &["n".to_string()])
            .await;
        assert!(result.is_err(), "update should fail: {result:?}");
        let store = crate::store::LocalStore::open(&store_path).unwrap();
        assert_eq!(store.batch_count().unwrap(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    use super::*;

    /// `run_query` validates the parent path before choosing a
    /// source: in persistent mode an invalid parent must not be
    /// swallowed by the local fallback as an empty result.
    #[tokio::test]
    async fn persistent_run_query_validates_parent_first() {
        let dir = std::env::temp_dir().join(format!(
            "firebase-native-run-query-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store_path = dir.join("store.db");
        let _ = std::fs::remove_file(&store_path);

        let options = FirebaseOptions::new("test-key", "test-project").with_database_id("testdb");
        let app = App::new(options);
        let firestore = Firestore::new(&app, Arc::new(StubTokens))
            .with_endpoint("http://127.0.0.1:9")
            .enable_local_store(&store_path)
            .unwrap();
        // "notes" is a collection path, not a document parent.
        let result = firestore.run_query("notes", Query::new("sub")).await;
        assert!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "{result:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

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

    fn sample_values() -> FieldValues {
        let mut values = FieldValues::new();
        values.insert(
            "title".to_string(),
            FieldValue::Value(Value::String("hello".to_string())),
        );
        values.insert("old".to_string(), FieldValue::Delete);
        values.insert("at".to_string(), FieldValue::ServerTimestamp);
        values.insert(
            "stats.count".to_string(),
            FieldValue::Increment(Value::Integer(2)),
        );
        values.insert(
            "tags".to_string(),
            FieldValue::ArrayUnion(vec![Value::String("x".to_string())]),
        );
        values
    }

    #[test]
    fn patch_translation_separates_literals_deletes_and_transforms() {
        let mutation = patch_mutation("notes/a", &sample_values()).unwrap();
        let crate::mutation::Mutation::Patch {
            fields,
            mask,
            exists,
            transforms,
            ..
        } = mutation
        else {
            panic!("expected a patch mutation");
        };
        assert_eq!(exists, Some(true));
        // The mask is exactly the literal and deleted paths;
        // transform paths travel separately.
        assert_eq!(mask, ["old", "title"]);
        assert_eq!(
            fields.get("title"),
            Some(&Value::String("hello".to_string()))
        );
        assert_eq!(fields.len(), 1);
        let transform_paths: Vec<&str> = transforms
            .iter()
            .map(|transform| transform.path.as_str())
            .collect();
        assert_eq!(transform_paths, ["at", "stats.count", "tags"]);
    }

    #[test]
    fn set_translation_merge_paths_and_delete_rules() {
        let mutation = set_mutation("notes/a", &sample_values(), true).unwrap();
        let crate::mutation::Mutation::Set { merge_paths, .. } = mutation else {
            panic!("expected a set mutation");
        };
        assert_eq!(
            merge_paths,
            Some(vec!["old".to_string(), "title".to_string()])
        );
        // A non-merge set replaces the document: a field delete
        // has no meaning and is rejected.
        let error = set_mutation("notes/a", &sample_values(), false).unwrap_err();
        assert!(matches!(error, Error::InvalidArgument(_)), "{error:?}");
        // Without deletes, a non-merge set carries no mask.
        let mut values = sample_values();
        values.remove("old");
        let mutation = set_mutation("notes/a", &values, false).unwrap();
        let crate::mutation::Mutation::Set { merge_paths, .. } = mutation else {
            panic!("expected a set mutation");
        };
        assert_eq!(merge_paths, None);
    }

    #[test]
    fn translation_rejects_invalid_inputs() {
        // Empty.
        assert!(matches!(
            patch_mutation("notes/a", &FieldValues::new()),
            Err(Error::InvalidArgument(_))
        ));
        // A path nesting under another entry's path.
        let mut values = FieldValues::new();
        values.insert("a".to_string(), FieldValue::Value(Value::Integer(1)));
        values.insert("a.b".to_string(), FieldValue::Increment(Value::Integer(1)));
        assert!(matches!(
            patch_mutation("notes/a", &values),
            Err(Error::InvalidArgument(_))
        ));
        // A non-numeric increment operand.
        let mut values = FieldValues::new();
        values.insert(
            "n".to_string(),
            FieldValue::Increment(Value::String("x".to_string())),
        );
        assert!(matches!(
            patch_mutation("notes/a", &values),
            Err(Error::InvalidArgument(_))
        ));
        // Malformed paths: empty, unquoted specials, an
        // unterminated quote, an empty segment, a reserved name.
        for bad in ["", "a b", "`n", "a..b", "__name__"] {
            let mut values = FieldValues::new();
            values.insert(bad.to_string(), FieldValue::Value(Value::Integer(1)));
            assert!(
                matches!(
                    patch_mutation("notes/a", &values),
                    Err(Error::InvalidArgument(_))
                ),
                "path {bad:?} must be rejected"
            );
        }
        // Two keys denoting the same parsed field.
        let mut values = FieldValues::new();
        values.insert("n".to_string(), FieldValue::Increment(Value::Integer(1)));
        values.insert("`n`".to_string(), FieldValue::Increment(Value::Integer(1)));
        assert!(matches!(
            patch_mutation("notes/a", &values),
            Err(Error::InvalidArgument(_))
        ));
    }

    /// The full local round trip of the sentinel API in
    /// persistent mode: the queued mutations' local views apply
    /// the transforms exactly as the mutation layer defines
    /// (increment accumulates, a pending server timestamp reads
    /// as Null, array remove empties, delete removes).
    #[tokio::test]
    async fn field_values_apply_in_the_local_view() {
        let dir = std::env::temp_dir().join(format!(
            "firebase-native-field-values-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store_path = dir.join("store.db");
        let _ = std::fs::remove_file(&store_path);

        let options = FirebaseOptions::new("test-key", "test-project").with_database_id("testdb");
        let app = App::new(options);
        let firestore = Firestore::new(&app, Arc::new(StubTokens))
            .with_endpoint("http://127.0.0.1:9")
            .enable_local_store(&store_path)
            .unwrap();

        let mut values = FieldValues::new();
        values.insert("n".to_string(), FieldValue::Increment(Value::Integer(3)));
        values.insert("at".to_string(), FieldValue::ServerTimestamp);
        values.insert(
            "tags".to_string(),
            FieldValue::ArrayUnion(vec![Value::String("x".to_string())]),
        );
        let created = firestore
            .set_document_with("notes/a", values, true)
            .await
            .unwrap();
        assert_eq!(created.fields.get("n"), Some(&Value::Integer(3)));
        assert_eq!(created.fields.get("at"), Some(&Value::Null));
        assert_eq!(
            created.fields.get("tags"),
            Some(&Value::Array(vec![Value::String("x".to_string())]))
        );

        let mut values = FieldValues::new();
        values.insert("n".to_string(), FieldValue::Increment(Value::Integer(2)));
        values.insert(
            "tags".to_string(),
            FieldValue::ArrayRemove(vec![Value::String("x".to_string())]),
        );
        values.insert("at".to_string(), FieldValue::Delete);
        let updated = firestore
            .update_document_with("notes/a", values)
            .await
            .unwrap();
        assert_eq!(updated.fields.get("n"), Some(&Value::Integer(5)));
        assert_eq!(updated.fields.get("tags"), Some(&Value::Array(vec![])));
        assert!(!updated.fields.contains_key("at"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
