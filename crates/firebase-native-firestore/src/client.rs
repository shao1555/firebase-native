use std::sync::Arc;

use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
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
/// The default cache-size limit for a local store:
/// 100 MiB, matching the native SDKs' default persistent
/// cache threshold.
pub const DEFAULT_CACHE_SIZE_LIMIT_BYTES: u64 = 100 * 1024 * 1024;

/// Options for a local (persistent) store — the caller's
/// policy knobs. The mechanisms they steer (the mutation
/// queue, the write pump, garbage collection) live in the
/// crate; only the policy values live here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalStoreOptions {
    /// Live-payload size above which the cache garbage
    /// collector evicts least-recently-accessed documents
    /// that no open watch targets and no queued mutation
    /// touches. `None` disables garbage collection (an
    /// unlimited cache). This is a cleanup threshold, not a
    /// hard bound: protected documents are never evicted, so
    /// the store can stay above it.
    pub cache_size_limit: Option<u64>,
}

impl Default for LocalStoreOptions {
    fn default() -> Self {
        Self {
            cache_size_limit: Some(DEFAULT_CACHE_SIZE_LIMIT_BYTES),
        }
    }
}

impl LocalStoreOptions {
    /// Options with garbage collection disabled: the cache
    /// grows without bound.
    pub fn unlimited() -> Self {
        Self {
            cache_size_limit: None,
        }
    }
}

/// The client holds no token string: every call asks the
/// [`TokenProvider`] for the current ID token, and on an
/// `UNAUTHENTICATED` status it forces exactly one refresh and retries
/// the call exactly once.
pub struct Firestore {
    options: FirebaseOptions,
    tokens: Arc<dyn TokenProvider>,
    endpoint: String,
    channel: Arc<SharedChannel>,
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
            channel: Arc::new(SharedChannel::new()),
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
    ///
    /// Equivalent to
    /// [`Firestore::enable_local_store_with_options`] with
    /// [`LocalStoreOptions::default`].
    pub fn enable_local_store(self, path: &std::path::Path) -> Result<Self> {
        self.enable_local_store_with_options(path, LocalStoreOptions::default())
    }

    /// Enables local persistence with explicit
    /// [`LocalStoreOptions`] — currently the cache-size limit
    /// that drives garbage collection of cached documents.
    pub fn enable_local_store_with_options(
        mut self,
        path: &std::path::Path,
        options: LocalStoreOptions,
    ) -> Result<Self> {
        let store = Arc::new(crate::store::LocalStore::open(path)?);
        let write = crate::write::WriteStream::new(
            self.options.clone(),
            self.tokens.clone(),
            self.channel.clone(),
            self.endpoint.clone(),
        );
        self.engine = Some(Arc::new(
            crate::engine::PersistenceEngine::new(store, write, self.options.documents_prefix())
                .with_cache_size_limit(options.cache_size_limit),
        ));
        Ok(self)
    }

    /// The local store's on-disk size in bytes (page-count
    /// based), or `None` when this client has no local store.
    /// A read-only fact for display or logging; the cache's
    /// own garbage collection steers by live payload, so this
    /// number can stay above the configured limit while freed
    /// pages await reuse or vacuuming.
    pub fn local_store_size_bytes(&self) -> Result<Option<u64>> {
        match &self.engine {
            Some(engine) => Ok(Some(engine.store().size_bytes()?)),
            None => Ok(None),
        }
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

    async fn channel(&self) -> Result<(Channel, u64)> {
        self.channel.get(&self.endpoint).await
    }

    async fn client(&self, force_refresh: bool) -> Result<(FirestoreClient<AuthService>, u64)> {
        let (channel, generation) = self.channel().await?;
        let client = grpc_client(channel, &self.options, &self.tokens, force_refresh).await?;
        Ok((client, generation))
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
            let (client, generation) = self.client(force_refresh).await?;
            match tokio::time::timeout(UNARY_CALL_TIMEOUT, op(client)).await {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(status)) if status.code() == Code::Unauthenticated && !force_refresh => {
                    force_refresh = true;
                }
                Ok(Err(status)) if !is_request_deadline_expired(&status) => {
                    return Err(map_status(status));
                }
                _ => {
                    // The call hung, by either clock: this
                    // wrapper's, or tonic's own enforcement of
                    // the request's grpc-timeout (which reports
                    // as Cancelled / "Timeout expired" and can
                    // win the race). Rebuild the generation so
                    // the next call dials fresh, and fail
                    // transiently (see UNARY_CALL_TIMEOUT).
                    let _ = self.channel.refresh(&self.endpoint, generation).await;
                    return Err(Error::Rpc {
                        code: "DeadlineExceeded".to_string(),
                        message: "unary call timed out".to_string(),
                    });
                }
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
        if query.is_collection_group() && !parent_path.is_empty() {
            // Same root-only rule as `run_query`: the server
            // only allows collection group targets at the root
            // parent.
            return Err(Error::InvalidArgument(
                "collection group queries are only allowed at the root parent".to_string(),
            ));
        }
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
        let (channel, generation) = self.channel().await?;
        Ok(crate::listen::spawn_watch(
            channel,
            generation,
            self.channel.clone(),
            self.endpoint.clone(),
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
        let (channel, generation) = self.channel().await?;
        Ok(crate::listen::spawn_watch(
            channel,
            generation,
            self.channel.clone(),
            self.endpoint.clone(),
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
                let _landing = engine.landing_guard();
                match &fetched {
                    Some(document) => engine.store_remote_document(document)?,
                    None => engine.store_remote_absent(path)?,
                }
                engine.note_cache_fill();
                return Ok(fetched);
            }
            // Reconcile conservatively and answer from the current
            // local view. A missing result evicts nothing (a create
            // may have been acknowledged after the fetch's read
            // point); a fetched document goes through the engine's
            // reconciliation, which also covers the acknowledged-
            // delete case (no cache row left to compare against).
            let _landing = engine.landing_guard();
            engine.reconcile_fetched_document(path, fetched.as_ref(), document_generation)?;
            engine.note_cache_fill();
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
                    let mut request = tonic::Request::new(GetDocumentRequest {
                        name,
                        mask: None,
                        consistency_selector: None,
                        request_options: None,
                    });
                    request.set_timeout(UNARY_CALL_TIMEOUT);
                    client
                        .get_document(request)
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
                    let mut request = tonic::Request::new(CreateDocumentRequest {
                        parent,
                        collection_id,
                        document_id: document_id.unwrap_or_default().to_string(),
                        document: Some(document),
                        mask: None,
                        request_options: None,
                    });
                    request.set_timeout(UNARY_CALL_TIMEOUT);
                    client
                        .create_document(request)
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
                let mut request = tonic::Request::new(CommitRequest {
                    database,
                    writes: vec![write],
                    transaction: Vec::new(),
                    request_options: None,
                });
                request.set_timeout(UNARY_CALL_TIMEOUT);
                client.commit(request).await.map(|_| ())
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
                    let mut request = tonic::Request::new(UpdateDocumentRequest {
                        document: Some(document),
                        update_mask: Some(DocumentMask { field_paths }),
                        mask: None,
                        current_document: None,
                        request_options: None,
                    });
                    request.set_timeout(UNARY_CALL_TIMEOUT);
                    client
                        .update_document(request)
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
                let mut request = tonic::Request::new(DeleteDocumentRequest {
                    name,
                    current_document: None,
                    request_options: None,
                });
                request.set_timeout(UNARY_CALL_TIMEOUT);
                client.delete_document(request).await.map(|_| ())
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
                    let mut request = tonic::Request::new(ListDocumentsRequest {
                        parent,
                        collection_id,
                        page_size: page_size.unwrap_or(0),
                        page_token: page_token.unwrap_or_default(),
                        mask: None,
                        consistency_selector: None,
                        order_by: String::new(),
                        show_missing: false,
                        request_options: None,
                    });
                    request.set_timeout(UNARY_CALL_TIMEOUT);
                    client
                        .list_documents(request)
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
    ///
    /// A collection-group query ([`Query::collection_group`],
    /// root parent only — see below) is the exception to the
    /// local-first rule: no single
    /// cached collection stands for the group, so persistent
    /// mode always runs the online query first and caches its
    /// results. The answer is built from the documents the
    /// server returned — never from the wider cache, so a
    /// document the server no longer returns (deleted,
    /// changed out of the filter, no longer readable) cannot
    /// reappear from a stale cached copy. When a queued
    /// mutation touches the group, the returned documents are
    /// composed with the local view (pending writes overlay,
    /// pending deletes remove) and the query's pagination
    /// applies to the composition, so the fetch itself goes
    /// unpaginated; with nothing pending, the server's answer
    /// is final and the fetch keeps the query's pagination.
    /// Only a connectivity failure answers from the cache
    /// alone (the whole cached group, locally evaluated).
    pub async fn run_query(&self, parent_path: &str, query: Query) -> Result<Vec<Document>> {
        if !parent_path.is_empty() {
            validate_document_path(parent_path)?;
        }
        if query.is_collection_group() && !parent_path.is_empty() {
            // The server only allows collection group queries
            // at the root parent (the emulator rejects a
            // document parent with exactly this message, and
            // the official SDKs expose no scoped form), so
            // refuse here rather than send a query that cannot
            // run — or, worse, answer it from the local
            // fallback as if it had.
            return Err(Error::InvalidArgument(
                "collection group queries are only allowed at the root parent".to_string(),
            ));
        }
        if let Some(engine) = &self.engine {
            if query.is_collection_group() {
                let pending = engine.has_pending_in_group(parent_path, query.collection_id())?;
                let fetch = if pending {
                    query.without_pagination()
                } else {
                    query.clone()
                };
                return match self.run_query_online(parent_path, fetch).await {
                    Ok(documents) => {
                        let _landing = engine.landing_guard();
                        for document in &documents {
                            engine.store_remote_document(document)?;
                        }
                        engine.note_cache_fill();
                        if pending {
                            engine.group_query_result(parent_path, &query, &documents)
                        } else {
                            Ok(documents)
                        }
                    }
                    Err(error) if crate::listen::is_transient_error(&error) => {
                        engine.local_query(parent_path, &query)
                    }
                    Err(error) => Err(error),
                };
            }
            if engine.collection_known(parent_path, query.collection_id())? {
                return engine.local_query(parent_path, &query);
            }
            match self
                .run_query_online(parent_path, query.without_pagination())
                .await
            {
                Ok(documents) => {
                    // Fill + evaluate under the landing guard:
                    // the fill wakes the garbage collector, and
                    // a pass must not evict a just-filled
                    // document before the local evaluation
                    // reads it back.
                    let _landing = engine.landing_guard();
                    for document in &documents {
                        engine.store_remote_document(document)?;
                    }
                    // The refill re-establishes the
                    // collection's completeness only when the
                    // server query covered the entire
                    // collection (the fetch strips pagination,
                    // but filters and ordering bounds remain),
                    // and the fill itself is cache growth the
                    // garbage collector should hear about.
                    if query.covers_entire_collection() {
                        engine.note_collection_synced(parent_path, query.collection_id());
                    }
                    engine.note_cache_fill();
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
                    let mut request = tonic::Request::new(RunQueryRequest {
                        parent,
                        query_type: Some(
                            crate::google::firestore::v1::run_query_request::QueryType::StructuredQuery(
                                structured,
                            ),
                        ),
                        consistency_selector: None,
                        explain_options: None,
                        request_options: None,
                    });
                    request.set_timeout(UNARY_CALL_TIMEOUT);
                    let response = client.run_query(request).await?;
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

/// Whether a status is tonic's local enforcement of a
/// request deadline (the `grpc-timeout` every unary request
/// carries): when the deadline passes, tonic cancels the
/// call itself with exactly this shape. Matched explicitly
/// so a cancellation sent by the server keeps its own
/// meaning. (If a future tonic changes the message, the
/// wrapper timeout in `Firestore::call` still bounds the
/// call; only the generation refresh would be skipped.)
fn is_request_deadline_expired(status: &Status) -> bool {
    status.code() == Code::Cancelled && status.message() == "Timeout expired"
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

/// How long opening a streaming RPC (Listen or Write) may
/// take before the attempt is abandoned as hung. The connect
/// timeout bounds TCP/TLS only and keepalive starts once an
/// HTTP/2 connection exists, so neither bounds an open
/// stalled in the handshake that follows (issues #25 and
/// #27). An abandoned open marks its channel generation for
/// rebuild (see [`SharedChannel`]).
///
/// Deliberately longer than [`CONNECT_TIMEOUT`], so the two
/// bounds never race: a connect that stalls is resolved by
/// the connect timeout's error — an ordinary failure, after
/// which tonic redials — while a handshake that stalls
/// after connecting is resolved here, by the rebuild.
#[cfg(not(test))]
pub(crate) const STREAM_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
/// Test scale. The stall tests originally ran on tokio's
/// paused clock, but that races real network I/O: when the
/// runtime idles before a real TCP connect completes, the
/// virtual clock jumps straight to the connect deadline and
/// the attempt ends as an ordinary connect error instead of
/// exercising the open deadline (this is how CI caught it).
/// On real time the loopback connect completes in
/// milliseconds, far inside even the scaled bounds, so each
/// mechanism fires deterministically — at the price of the
/// stall tests taking about a second each.
#[cfg(test)]
pub(crate) const STREAM_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(600);

/// How long one unary call (through [`Firestore::call`]) may
/// take before it is abandoned as hung. A unary call on a
/// connection stalled in the HTTP/2 handshake otherwise never
/// returns — keepalive cannot help, because that connection
/// never became an HTTP/2 connection — and a hang is not an
/// error, so the local-store fallback of the persistent
/// reads would never run. On expiry the channel generation
/// is rebuilt and the call fails with a transient
/// `DeadlineExceeded`, which is exactly the signal the
/// fallback keys on (issue #27). Longer than
/// [`STREAM_OPEN_TIMEOUT`] because the bound covers the
/// whole call, and `run_query` reads its entire result
/// stream inside it.
///
/// The same bound is sent to the server as each request's
/// gRPC deadline, so the server abandons the call at the
/// same moment instead of completing work nobody is waiting
/// for. Two consequences callers should know: a write whose
/// deadline expires may still have been applied, if the
/// server committed it just before the deadline — the error
/// is transient, so a retried create surfaces as
/// `AlreadyExists` — and `run_query` is bounded as a whole,
/// so a very large result on a slow link can time out and
/// fall back to the local store rather than arriving late.
#[cfg(not(test))]
const UNARY_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Test scale (see [`STREAM_OPEN_TIMEOUT`]).
#[cfg(test)]
const UNARY_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// A gRPC channel shared by every part of a client — unary
/// calls, the Listen driver and the Write stream — together
/// with a generation counter.
///
/// Sharing one channel is the normal state: one connection,
/// multiplexed streams, as in the official SDKs. The
/// generation exists for one failure shape (issue #27): a
/// connection that hangs mid-handshake stays the channel's
/// only connection, and every later attempt queues behind
/// it. A component that abandons such an attempt calls
/// [`SharedChannel::refresh`], which swaps in a freshly
/// built channel when — and only when — the generation it
/// was using is still current: concurrent failures race to
/// rebuild, exactly one wins, and the rest adopt the new
/// channel on their next fetch. Users still holding the old
/// channel keep their clone; its connection closes when the
/// last of them finishes.
pub(crate) struct SharedChannel {
    state: tokio::sync::Mutex<SharedChannelState>,
}

struct SharedChannelState {
    channel: Option<Channel>,
    generation: u64,
}

impl SharedChannel {
    pub(crate) fn new() -> Self {
        Self {
            state: tokio::sync::Mutex::new(SharedChannelState {
                channel: None,
                generation: 0,
            }),
        }
    }

    /// The current channel and its generation, building the
    /// first channel on demand. A build error reaches the
    /// caller that needed the channel (the fail-fast
    /// behaviour watch creation has always had).
    pub(crate) async fn get(&self, endpoint: &str) -> Result<(Channel, u64)> {
        let mut state = self.state.lock().await;
        if state.channel.is_none() {
            state.channel = Some(build_channel(endpoint).await?);
        }
        Ok((
            state.channel.clone().expect("channel just ensured"),
            state.generation,
        ))
    }

    /// Rebuilds the channel when `used_generation` is still
    /// current, and returns the current channel and
    /// generation either way: when another component already
    /// rebuilt, the caller simply adopts the fresh channel.
    pub(crate) async fn refresh(
        &self,
        endpoint: &str,
        used_generation: u64,
    ) -> Result<(Channel, u64)> {
        let mut state = self.state.lock().await;
        if state.channel.is_none() || state.generation == used_generation {
            let rebuilt = build_channel(endpoint).await?;
            if state.channel.is_some() {
                state.generation += 1;
            }
            state.channel = Some(rebuilt);
        }
        Ok((
            state.channel.clone().expect("channel just ensured"),
            state.generation,
        ))
    }
}

/// How long establishing one connection may take before the
/// attempt is abandoned. Bounds a connect on a network that
/// silently drops packets, which otherwise hangs until the OS
/// gives up (issue #25).
#[cfg(not(test))]
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Test scale: the stall tests run on real time (see
/// [`STREAM_OPEN_TIMEOUT`]).
#[cfg(test)]
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(300);

/// HTTP/2 keepalive ping interval on the channel, matching
/// Google's own Firestore clients (`grpc.keepalive_time_ms:
/// 30000`): a connection that dies silently — no FIN, no RST —
/// is detected when a ping goes unacknowledged, and the watch
/// driver's failure handling can run (issue #25).
const KEEP_ALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// How long a keepalive ping may go unacknowledged before the
/// connection is closed as dead (the gRPC default).
const KEEP_ALIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// The rustls client configuration for the gRPC transport:
/// certificate verification delegated to the operating
/// system through `rustls-platform-verifier` (issue #29) —
/// the platform evaluates the chain (`SecTrust` on Apple
/// platforms, the Windows certificate store, Android's
/// verifier), so MDM-installed roots are honoured and
/// verification works the same inside the macOS App
/// Sandbox, where reading the trust settings directly
/// (what `rustls-native-certs` does) does not see them. On
/// Linux the verifier falls back to the system roots, as
/// before. Built on the ring crypto provider (named
/// explicitly so the choice does not depend on which
/// providers the dependency graph enables), offering only
/// the `h2` ALPN, as gRPC requires.
fn tls_client_config() -> Result<std::sync::Arc<rustls::ClientConfig>> {
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let verifier = rustls_platform_verifier::Verifier::new(provider.clone())
        .map_err(|err| Error::Other(format!("platform certificate verifier: {err}")))?;
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|err| Error::Other(format!("tls protocol versions: {err}")))?
        .dangerous()
        .with_custom_certificate_verifier(std::sync::Arc::new(verifier))
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(std::sync::Arc::new(config))
}

/// A byte stream for the channel connector: plain TCP, or
/// TLS over TCP (dialed directly, or through a CONNECT
/// tunnel). The named trait exists so the two shapes can
/// share one boxed type (a trait object cannot name two
/// non-auto traits directly).
trait AsyncReadWrite: tokio::io::AsyncRead + tokio::io::AsyncWrite {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite> AsyncReadWrite for T {}
type ChannelStream = std::pin::Pin<Box<dyn AsyncReadWrite + Send>>;

/// Builds the shared gRPC channel for an endpoint.
///
/// The channel is built over a custom connector: it dials
/// with hyper-util's `HttpConnector` — Happy Eyeballs and
/// `TCP_NODELAY`, as tonic's own transport did — or through
/// a CONNECT tunnel when the environment routes through a
/// proxy, and for `https://` endpoints the connector
/// performs the TLS handshake itself with the platform
/// verifier (tonic 0.12's `ClientTlsConfig` cannot take a
/// custom verifier).
pub(crate) async fn build_channel(endpoint_str: &str) -> Result<Channel> {
    // The dependency graph enables more than one rustls crypto
    // provider, so rustls cannot pick one automatically; select
    // ring as the process default before any TLS config is built
    // (a no-op when a default was already installed).
    let _ = rustls::crypto::ring::default_provider().install_default();
    let endpoint = Endpoint::from_shared(endpoint_str.to_string())
        .map_err(|err| Error::Other(format!("invalid endpoint: {err}")))?
        .connect_timeout(CONNECT_TIMEOUT)
        .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
        .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
        // Pings run while calls are in flight — a watch
        // stream is one, which is exactly the window that
        // needs detection. An idle channel stays quiet, as
        // in Google's clients (no keepalive permit without
        // calls).
        .keep_alive_while_idle(false);
    // TLS is this crate's own now, not tonic's: tonic 0.12's
    // `ClientTlsConfig` can only load root certificates — it
    // cannot take a custom verifier — so the channel is built
    // over a connector that dials (directly, or through the
    // CONNECT proxy when one is configured) and performs the
    // handshake itself with the platform verifier. The
    // endpoint's connect timeout bounds the whole connector
    // future, handshake included.
    let tls_config = if endpoint_str.starts_with("https://") {
        Some(tls_client_config()?)
    } else {
        None
    };
    let proxy = if tls_config.is_some() {
        endpoint
            .uri()
            .host()
            .and_then(crate::proxy::ProxyConfig::from_env_for)
            .map(std::sync::Arc::new)
    } else {
        None
    };
    // The direct dialer is hyper-util's HttpConnector, as
    // tonic's own transport used: Happy Eyeballs (an
    // unresponsive address family falls back after 300 ms
    // instead of eating the whole connect timeout, which a
    // sequential TcpStream::connect would allow) and
    // TCP_NODELAY (small gRPC frames do not wait on Nagle).
    let mut dialer = hyper_util::client::legacy::connect::HttpConnector::new();
    dialer.enforce_http(false);
    dialer.set_nodelay(true);
    dialer.set_connect_timeout(Some(CONNECT_TIMEOUT));
    Ok(endpoint.connect_with_connector_lazy(tower::service_fn(
        move |uri: tonic::transport::Uri| {
            let tls_config = tls_config.clone();
            let proxy = proxy.clone();
            let dialer = dialer.clone();
            async move {
                let host = uri.host().unwrap_or_default().to_string();
                let https = uri.scheme_str() == Some("https");
                let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
                let tcp = match &proxy {
                    Some(proxy) => {
                        crate::proxy::connect_via_proxy(proxy.clone(), host.clone(), port).await?
                    }
                    None => {
                        use tower::ServiceExt;
                        dialer
                            .oneshot(uri)
                            .await
                            .map_err(std::io::Error::other)?
                            .into_inner()
                    }
                };
                let stream: ChannelStream = match &tls_config {
                    Some(config) => {
                        // `Uri::host` keeps the brackets of an
                        // IPv6 literal; the server name must not
                        // have them.
                        let server_host = host
                            .strip_prefix('[')
                            .and_then(|rest| rest.strip_suffix(']'))
                            .unwrap_or(&host)
                            .to_string();
                        let server_name = rustls::pki_types::ServerName::try_from(server_host)
                            .map_err(|err| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    format!("invalid TLS server name: {err}"),
                                )
                            })?;
                        let tls = tokio_rustls::TlsConnector::from(config.clone())
                            .connect(server_name, tcp)
                            .await?;
                        Box::pin(tls)
                    }
                    None => Box::pin(tcp),
                };
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        },
    )))
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
    use crate::value::Value;
    use firebase_native_core::{App, FirebaseOptions};

    /// The generation gates rebuilds: a refresh against the
    /// current generation rebuilds exactly once, and a stale
    /// refresh (another component already rebuilt) adopts the
    /// new channel instead of rebuilding again (issue #27).
    /// The endpoint is never dialed — channels are lazy.
    #[tokio::test]
    async fn shared_channel_generations_gate_rebuilds() {
        let shared = SharedChannel::new();
        let endpoint = "http://127.0.0.1:9";
        let (_, generation) = shared.get(endpoint).await.unwrap();
        assert_eq!(generation, 0);
        let (_, generation) = shared.get(endpoint).await.unwrap();
        assert_eq!(generation, 0, "no rebuild without a refresh");
        let (_, generation) = shared.refresh(endpoint, generation).await.unwrap();
        assert_eq!(generation, 1, "refresh against current rebuilds");
        let (_, generation) = shared.refresh(endpoint, 0).await.unwrap();
        assert_eq!(generation, 1, "stale refresh adopts, does not rebuild");
        let (_, generation) = shared.get(endpoint).await.unwrap();
        assert_eq!(generation, 1);
    }

    /// A unary call against a server that accepts TCP and
    /// then never speaks must not wait forever: the call
    /// deadline fires, the channel generation is rebuilt,
    /// and the error is the transient `DeadlineExceeded` the
    /// persistent reads' local-store fallback keys on
    /// (issue #27). Runs on real time with the test-scaled
    /// call deadline (about a second).
    #[tokio::test]
    async fn unary_call_against_a_stalling_server_times_out_transiently() {
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = tcp.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for stream in tcp.incoming() {
                match stream {
                    Ok(socket) => held.push(socket),
                    Err(_) => break,
                }
            }
        });
        let app = App::new(FirebaseOptions::new("test-key", "demo-project"));
        let firestore = Firestore::new(&app, Arc::new(StubTokens))
            .with_endpoint(format!("http://127.0.0.1:{port}"));
        let error = firestore
            .get_document("notes/a")
            .await
            .expect_err("a stalled unary call must fail, not hang");
        assert!(
            matches!(&error, Error::Rpc { code, .. } if code == "DeadlineExceeded"),
            "expected a transient DeadlineExceeded, got {error:?}"
        );
        assert!(
            crate::listen::is_transient_error(&error),
            "the fallback keys on transient errors: {error:?}"
        );
        // The timeout must also have rebuilt the channel
        // generation, or a unary-only client would time out
        // on every call against the hung connection and
        // never recover. This assertion is what makes a
        // future tonic change to its "Timeout expired"
        // message visible: without the recognition in
        // `is_request_deadline_expired`, the error above
        // would surface as Cancelled (still transient) but
        // the generation would stay at 0.
        let (_, generation) = firestore
            .channel
            .get(&firestore.endpoint)
            .await
            .expect("the channel is available");
        assert_eq!(
            generation, 1,
            "the timed-out call must rebuild the generation"
        );
    }

    /// The gRPC TLS configuration builds on this platform
    /// and offers exactly the `h2` ALPN (issue #29).
    #[test]
    fn tls_config_uses_platform_verifier_with_h2_alpn() {
        let config = tls_client_config().expect("the platform verifier config builds");
        assert_eq!(config.alpn_protocols, vec![b"h2".to_vec()]);
    }

    /// A collection-group query always asks the server
    /// first, even when the store knows a concrete collection
    /// with the group's ID (issue #31): the counting server
    /// stalls, so the group call surfaces as a dial plus the
    /// transient-deadline fallback to the local group
    /// evaluation, while the single-collection query over the
    /// same known collection never dials.
    #[tokio::test]
    async fn group_query_goes_online_even_when_a_member_collection_is_known() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = tcp.local_addr().unwrap().port();
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = accepts.clone();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for stream in tcp.incoming() {
                match stream {
                    Ok(socket) => {
                        counter.fetch_add(1, Ordering::SeqCst);
                        held.push(socket);
                    }
                    Err(_) => break,
                }
            }
        });

        let dir = std::env::temp_dir().join(format!(
            "firebase-native-group-query-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store_path = dir.join("store.db");
        let _ = std::fs::remove_file(&store_path);
        let app = App::new(FirebaseOptions::new("test-key", "test-project"));
        let firestore = Firestore::new(&app, Arc::new(StubTokens))
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .enable_local_store(&store_path)
            .unwrap();
        // Seed a root-level `members` document, so the
        // concrete collection is known to the store.
        let engine = firestore.engine.clone().unwrap();
        engine
            .store_remote_document(&crate::value::Document {
                name: "projects/test-project/databases/(default)/documents/members/x".to_string(),
                fields: crate::Fields::new(),
                create_time: None,
                update_time: None,
            })
            .unwrap();

        // The single-collection query answers locally.
        let results = firestore
            .run_query("", Query::new("members"))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            0,
            "a known collection must not dial"
        );

        // The group query consults the server (a dial
        // happens), then falls back to the local evaluation
        // when the stalled call hits its deadline.
        let results = firestore
            .run_query("", Query::collection_group("members"))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(
            accepts.load(Ordering::SeqCst) >= 1,
            "the group query never consulted the server"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A collection-group watch is accepted at the root and
    /// refused under a document parent, like `run_query`
    /// (issue #31, step 2). The endpoint is dead: acceptance
    /// is what is checked here — the listener's driver
    /// retries in the background.
    #[tokio::test]
    async fn watch_query_accepts_collection_group_at_the_root_only() {
        let app = App::new(FirebaseOptions::new("test-key", "test-project"));
        let firestore =
            Firestore::new(&app, Arc::new(StubTokens)).with_endpoint("http://127.0.0.1:9");
        let listener = firestore
            .watch_query("", Query::collection_group("members"))
            .await;
        assert!(listener.is_ok(), "the group watch was refused");
        let result = firestore
            .watch_query("a/1", Query::collection_group("members"))
            .await;
        match result {
            Err(Error::InvalidArgument(message)) => {
                assert!(message.contains("root parent"), "{message}")
            }
            Ok(_) => panic!("expected a refusal, got a listener"),
            Err(other) => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    /// A fake Firestore server speaking just enough gRPC
    /// (HTTP/2 + the RunQuery method) to answer `run_query`
    /// with canned documents. The crate generates client-only
    /// protos (`build_server(false)`), so tests drive the h2
    /// layer directly. `canned` is read per request, so a test
    /// can change what the "server" returns between calls;
    /// `captured` records the last structured query received.
    struct FakeRunQueryServer {
        port: u16,
        canned: Arc<std::sync::Mutex<Vec<crate::google::firestore::v1::Document>>>,
        captured: Arc<std::sync::Mutex<Option<crate::google::firestore::v1::StructuredQuery>>>,
    }

    async fn spawn_fake_run_query_server() -> FakeRunQueryServer {
        use prost::Message as _;
        let canned: Arc<std::sync::Mutex<Vec<crate::google::firestore::v1::Document>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured: Arc<std::sync::Mutex<Option<crate::google::firestore::v1::StructuredQuery>>> =
            Arc::new(std::sync::Mutex::new(None));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_canned = canned.clone();
        let server_captured = captured.clone();
        tokio::spawn(async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => return,
                };
                let canned = server_canned.clone();
                let captured = server_captured.clone();
                tokio::spawn(async move {
                    let mut connection = match h2::server::handshake(socket).await {
                        Ok(connection) => connection,
                        Err(_) => return,
                    };
                    while let Some(accepted) = connection.accept().await {
                        let (request, mut respond) = match accepted {
                            Ok(pair) => pair,
                            Err(_) => return,
                        };
                        let canned = canned.clone();
                        let captured = captured.clone();
                        tokio::spawn(async move {
                            let mut body = request.into_body();
                            let mut data = Vec::new();
                            while let Some(chunk) = body.data().await {
                                let chunk = match chunk {
                                    Ok(chunk) => chunk,
                                    Err(_) => return,
                                };
                                data.extend_from_slice(&chunk);
                                let _ = body.flow_control().release_capacity(chunk.len());
                            }
                            // One gRPC frame: flag + length + message.
                            if data.len() > 5 {
                                if let Ok(run_query) =
                                    crate::google::firestore::v1::RunQueryRequest::decode(
                                        &data[5..],
                                    )
                                {
                                    if let Some(
                                        crate::google::firestore::v1::run_query_request::QueryType::StructuredQuery(
                                            structured,
                                        ),
                                    ) = run_query.query_type
                                    {
                                        *captured.lock().unwrap() = Some(structured);
                                    }
                                }
                            }
                            let headers = http::Response::builder()
                                .status(200)
                                .header("content-type", "application/grpc")
                                .body(())
                                .unwrap();
                            let mut send = match respond.send_response(headers, false) {
                                Ok(send) => send,
                                Err(_) => return,
                            };
                            let documents = canned.lock().unwrap().clone();
                            for document in documents {
                                let response = crate::google::firestore::v1::RunQueryResponse {
                                    document: Some(document),
                                    ..Default::default()
                                };
                                let mut payload = Vec::new();
                                response.encode(&mut payload).unwrap();
                                let mut frame = Vec::with_capacity(5 + payload.len());
                                frame.push(0u8);
                                frame.extend_from_slice(
                                    &u32::try_from(payload.len()).unwrap().to_be_bytes(),
                                );
                                frame.extend_from_slice(&payload);
                                if send.send_data(bytes::Bytes::from(frame), false).is_err() {
                                    return;
                                }
                            }
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                            let _ = send.send_trailers(trailers);
                        });
                    }
                });
            }
        });
        FakeRunQueryServer {
            port,
            canned,
            captured,
        }
    }

    fn proto_document(name: &str, uid: &str) -> crate::google::firestore::v1::Document {
        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "uid".to_string(),
            crate::google::firestore::v1::Value {
                value_type: Some(crate::google::firestore::v1::value::ValueType::StringValue(
                    uid.to_string(),
                )),
            },
        );
        crate::google::firestore::v1::Document {
            name: name.to_string(),
            fields,
            create_time: None,
            update_time: None,
        }
    }

    fn persistent_firestore(endpoint: &str, tag: &str) -> (Firestore, std::path::PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("firebase-native-{tag}-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let store_path = dir.join("store.db");
        let _ = std::fs::remove_file(&store_path);
        let app = App::new(FirebaseOptions::new("test-key", "test-project"));
        let firestore = Firestore::new(&app, Arc::new(StubTokens))
            .with_endpoint(endpoint.to_string())
            .enable_local_store(&store_path)
            .unwrap();
        (firestore, dir)
    }

    /// The group answer comes from what the server returned,
    /// not from the wider cache (issue #31 review): a cached
    /// member the server no longer returns must not reappear,
    /// and a member only the server has is the answer.
    #[tokio::test]
    async fn group_query_answer_comes_from_the_server_result_not_the_cache() {
        let server = spawn_fake_run_query_server().await;
        let (firestore, dir) =
            persistent_firestore(&format!("http://127.0.0.1:{}", server.port), "group-answer");
        let engine = firestore.engine.clone().unwrap();
        // Cache a matching member the server has since deleted.
        engine
            .store_remote_document(&crate::value::Document {
                name: "projects/test-project/databases/(default)/documents/a/1/members/x"
                    .to_string(),
                fields: {
                    let mut fields = crate::Fields::new();
                    fields.insert("uid".to_string(), Value::String("u1".to_string()));
                    fields
                },
                create_time: None,
                update_time: None,
            })
            .unwrap();
        let query = Query::collection_group("members").filter(crate::query::Filter::eq(
            "uid",
            Value::String("u1".to_string()),
        ));

        // The server returns nothing: the answer is empty,
        // however well the stale cached copy matches.
        let results = firestore.run_query("", query.clone()).await.unwrap();
        assert!(
            results.is_empty(),
            "stale cached member reappeared: {results:?}"
        );
        let captured = server.captured.lock().unwrap().clone().unwrap();
        assert!(captured.from[0].all_descendants);

        // The server returns a member the cache has never
        // seen: that, exactly, is the answer.
        *server.canned.lock().unwrap() = vec![proto_document(
            "projects/test-project/databases/(default)/documents/b/2/members/y",
            "u1",
        )];
        let results = firestore.run_query("", query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].name.ends_with("b/2/members/y"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// With nothing pending in the group, the fetch keeps
    /// the query's limit (issue #31 review nit); with a
    /// pending write in scope it goes unpaginated and the
    /// limit applies to the composed answer.
    #[tokio::test]
    async fn group_query_keeps_its_limit_unless_writes_are_pending() {
        let server = spawn_fake_run_query_server().await;
        let (firestore, dir) =
            persistent_firestore(&format!("http://127.0.0.1:{}", server.port), "group-limit");
        let query = Query::collection_group("members").limit(1);

        let results = firestore.run_query("", query.clone()).await.unwrap();
        assert!(results.is_empty());
        let captured = server.captured.lock().unwrap().clone().unwrap();
        assert_eq!(
            captured.limit,
            Some(1),
            "no pending writes: the limit belongs on the wire"
        );

        // Queue a write inside the group: now the answer is
        // composed locally, so the fetch must not carry the
        // limit — and the composed answer still honours it.
        let engine = firestore.engine.clone().unwrap();
        let mut fields = crate::Fields::new();
        fields.insert("uid".to_string(), Value::String("u9".to_string()));
        engine
            .store()
            .enqueue_batch(&[crate::mutation::Mutation::Set {
                path: "c/3/members/z".to_string(),
                fields,
                merge_paths: None,
                exists: None,
                transforms: Vec::new(),
            }])
            .unwrap();
        let results = firestore.run_query("", query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].name.ends_with("c/3/members/z"));
        let captured = server.captured.lock().unwrap().clone().unwrap();
        assert_eq!(
            captured.limit, None,
            "pending writes: the fetch is unpaginated"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A group query with a non-empty parent is refused
    /// before anything is sent: the server only allows
    /// collection group queries at the root parent.
    #[tokio::test]
    async fn group_query_requires_the_root_parent() {
        let app = App::new(FirebaseOptions::new("test-key", "test-project"));
        let firestore =
            Firestore::new(&app, Arc::new(StubTokens)).with_endpoint("http://127.0.0.1:9");
        let result = firestore
            .run_query("a/1", Query::collection_group("members"))
            .await;
        match result {
            Err(Error::InvalidArgument(message)) => {
                assert!(message.contains("root parent"), "{message}")
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

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
    #[test]
    fn local_store_options_and_size_fact() {
        let app = App::new(FirebaseOptions::new("test-key", "p").with_database_id("d"));
        // No local store: the size fact is None.
        let firestore = Firestore::new(&app, Arc::new(StubTokens));
        assert_eq!(firestore.local_store_size_bytes().unwrap(), None);
        // With a store: Some(size), and the options carry the
        // documented defaults.
        assert_eq!(
            LocalStoreOptions::default().cache_size_limit,
            Some(DEFAULT_CACHE_SIZE_LIMIT_BYTES)
        );
        assert_eq!(LocalStoreOptions::unlimited().cache_size_limit, None);
        let dir = std::env::temp_dir().join(format!(
            "firebase-native-gc-options-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db");
        let _ = std::fs::remove_file(&path);
        let firestore = Firestore::new(&app, Arc::new(StubTokens))
            .enable_local_store_with_options(&path, LocalStoreOptions::unlimited())
            .unwrap();
        let size = firestore.local_store_size_bytes().unwrap();
        assert!(size.is_some_and(|bytes| bytes > 0));
        let _ = std::fs::remove_dir_all(&dir);
    }

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
