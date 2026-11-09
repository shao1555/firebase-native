//! The persistence engine: ties the local store and the Write
//! stream together into offline-capable reads and writes.
//!
//! - **Writes** are mutations appended to the store's queue. The
//!   local view reflects them immediately; a background pump task
//!   commits queued batches in FIFO order over the Write stream
//!   and folds each acknowledged batch into the server-confirmed
//!   state with its commit time. A batch the server condemns
//!   permanently is dropped and its error remembered for
//!   [`PersistenceEngine::wait_for_pending_writes`]; any other
//!   failure keeps the batch queued (offline is a normal state,
//!   not an error).
//! - **Reads** overlay the queue on the server-confirmed state:
//!   the local view of a document is its cached server state with
//!   every queued mutation for its path applied in order.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use firebase_native_core::{Error, Result};
use tokio::sync::Notify;

use crate::mutation::Mutation;
use crate::path::full_document_name;
use crate::store::LocalStore;
use crate::value::Document;
use crate::write::{is_permanent_batch_error, WriteStream};

/// Owns the store and the write pump for one persistent client.
pub(crate) struct PersistenceEngine {
    store: Arc<LocalStore>,
    write: WriteStream,
    documents_prefix: String,
    pump_started: AtomicBool,
    /// Signalled whenever the queue changes (enqueue, ack, drop).
    queue_changed: Notify,
    /// Signalled whenever the local view may have changed (any
    /// generation bump). Store-backed watches wait on this as a
    /// wakeup hint and re-derive the view from the generation.
    view_changed: Notify,
    /// A monotone counter bumped on every local-state change
    /// (enqueue, acknowledgement fold, cache store). Readers that
    /// go online capture it before the fetch and compare after:
    /// any change means the fetch raced local writes, however fast
    /// they were acknowledged.
    generation: AtomicU64,
    /// Per-document generations, keyed by full resource name,
    /// bumped on every local transition of that document. This is
    /// what lets a reconciling fetch distinguish "no cache row
    /// because never seen" from "no cache row because a newer
    /// delete was acknowledged": both look identical in the store.
    document_generations: Mutex<std::collections::HashMap<String, u64>>,
    /// The most recent permanently rejected batch's error, kept
    /// until a waiter observes it.
    dropped_error: Mutex<Option<String>>,
}

impl PersistenceEngine {
    pub(crate) fn new(
        store: Arc<LocalStore>,
        write: WriteStream,
        documents_prefix: String,
    ) -> Self {
        Self {
            store,
            write,
            documents_prefix,
            pump_started: AtomicBool::new(false),
            queue_changed: Notify::new(),
            view_changed: Notify::new(),
            generation: AtomicU64::new(0),
            document_generations: Mutex::new(std::collections::HashMap::new()),
            dropped_error: Mutex::new(None),
        }
    }

    /// The underlying store (the sync layer reads and writes
    /// target state through it).
    pub(crate) fn store(&self) -> &LocalStore {
        &self.store
    }

    /// The current local-state generation (see the field docs).
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// The relative path of a full resource name in this
    /// engine's database, if it belongs to it.
    pub(crate) fn relative_path_of<'a>(&self, full_name: &'a str) -> Option<&'a str> {
        full_name
            .strip_prefix(&self.documents_prefix)?
            .strip_prefix('/')
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.view_changed.notify_waiters();
    }

    /// A future that completes on the next local-view change.
    /// Register it (pin + enable) *before* reading the view, the
    /// same discipline as the pump's queue wait; the generation
    /// remains the source of truth for whether anything changed.
    pub(crate) fn view_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.view_changed.notified()
    }

    /// The per-document generation for a relative path (0 when
    /// the document has never transitioned locally).
    pub(crate) fn document_generation(&self, relative_path: &str) -> u64 {
        let name = full_document_name(&self.documents_prefix, relative_path);
        *self
            .document_generations
            .lock()
            .expect("document generations mutex poisoned")
            .get(&name)
            .unwrap_or(&0)
    }

    fn bump_document(&self, full_name: &str) {
        *self
            .document_generations
            .lock()
            .expect("document generations mutex poisoned")
            .entry(full_name.to_string())
            .or_insert(0) += 1;
    }

    /// Bumps the global and per-document generations for a batch.
    fn bump_for_mutations(&self, mutations: &[Mutation]) {
        self.bump_generation();
        for mutation in mutations {
            self.bump_document(&full_document_name(&self.documents_prefix, mutation.path()));
        }
    }

    /// Starts the pump task if it is not running yet. Called on
    /// enqueue and on wait: a process that opens a store with a
    /// queue left over from a previous run must drain it even when
    /// nothing new is enqueued. Must be called from a Tokio runtime
    /// context.
    fn ensure_pump(self: &Arc<Self>) {
        if !self.pump_started.swap(true, Ordering::SeqCst) {
            let engine = Arc::clone(self);
            tokio::spawn(async move {
                engine.pump().await;
            });
        }
    }

    /// Appends one batch to the queue and ensures the pump is
    /// running.
    pub(crate) fn enqueue(self: &Arc<Self>, mutations: Vec<Mutation>) -> Result<i64> {
        let id = self.store.enqueue_batch(&mutations)?;
        self.bump_for_mutations(&mutations);
        self.queue_changed.notify_waiters();
        self.ensure_pump();
        Ok(id)
    }

    /// The pump: commit queued batches oldest-first, forever.
    async fn pump(self: Arc<Self>) {
        loop {
            // Register for the wakeup before inspecting the queue,
            // so an enqueue between the check and the wait is not
            // missed.
            let notified = self.queue_changed.notified();
            tokio::pin!(notified);
            let _ = notified.as_mut().enable();

            let batches = match self.store.peek_batches(1) {
                Ok(batches) => batches,
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            let Some(batch) = batches.into_iter().next() else {
                notified.await;
                continue;
            };

            let writes = batch
                .mutations
                .iter()
                .map(|mutation| mutation.to_proto(&self.documents_prefix))
                .collect::<Vec<_>>();
            match self.write.write_batch(writes).await {
                Ok(ack) => {
                    let commit_time = ack.commit_time.unwrap_or_else(|| {
                        crate::value::Timestamp::from_system_time(std::time::SystemTime::now())
                    });
                    // The server has committed: the local fold must
                    // land exactly once and completely. It is one
                    // transaction (cache updates + queue removal);
                    // if it fails, retry the *local* commit — the
                    // batch is never resent.
                    loop {
                        match self.fold_acknowledgement_with_results(
                            &batch,
                            commit_time,
                            &ack.transform_results,
                        ) {
                            Ok(()) => break,
                            Err(_) => {
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            }
                        }
                    }
                    self.queue_changed.notify_waiters();
                }
                Err(error) if is_permanent_batch_error(&error) => {
                    // The server condemned this batch: drop it (the
                    // local view loses its effect, as in the
                    // official clients) and remember why.
                    let _ = self.store.remove_batch(batch.id);
                    self.bump_for_mutations(&batch.mutations);
                    *self.dropped_error.lock().expect("dropped mutex poisoned") =
                        Some(error.to_string());
                    self.queue_changed.notify_waiters();
                }
                Err(_) => {
                    // write_batch retries transient failures itself;
                    // what reaches here (e.g. no signed-in user) must
                    // not lose the batch. Keep it queued and retry
                    // after a pause.
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Folds one acknowledged batch into the server-confirmed
    /// state: computes each touched document's post-ack state and
    /// commits it together with the queue removal as one store
    /// transaction.
    /// The fold for batches without transform results.
    #[allow(dead_code)]
    pub(crate) fn fold_acknowledgement(
        &self,
        batch: &crate::store::MutationBatch,
        commit_time: crate::value::Timestamp,
    ) -> Result<()> {
        self.fold_acknowledgement_with_results(batch, commit_time, &[])
    }

    /// The fold with the server's per-write transform results
    /// (aligned with the batch's mutations): each mutation's
    /// transformed fields take the server's exact results
    /// instead of the locally computed values.
    pub(crate) fn fold_acknowledgement_with_results(
        &self,
        batch: &crate::store::MutationBatch,
        commit_time: crate::value::Timestamp,
        transform_results: &[Vec<crate::value::Value>],
    ) -> Result<()> {
        let mut updates: Vec<(String, Option<Document>)> = Vec::new();
        for mutation in &batch.mutations {
            let name = full_document_name(&self.documents_prefix, mutation.path());
            let current = self.store.remote_document(&name)?;
            // A watch delivery newer than this commit already
            // reflects the mutation's effect (or a later state);
            // folding the mutation over it would regress the
            // base. The batch still completes; the base stands.
            if let Some(existing) = &current {
                if let Some(update_time) = &existing.update_time {
                    if (update_time.seconds, update_time.nanos)
                        > (commit_time.seconds, commit_time.nanos)
                    {
                        updates.push((name, current));
                        continue;
                    }
                }
            }
            let results = transform_results
                .get(updates.len())
                .map(|results| results.as_slice());
            let mut next = match results {
                Some(results) if !results.is_empty() => {
                    mutation.apply_to_remote_with_results(current, commit_time, results)
                }
                _ => mutation.apply_to_remote(current, commit_time),
            };
            if let Some(document) = next.as_mut() {
                // A newly created document has no name yet (the
                // mutation model works with relative paths): assign
                // the full resource name before it keys a row.
                if document.name.is_empty() {
                    document.name = name.clone();
                }
            }
            updates.push((name, next));
        }
        self.store.commit_acknowledged_batch(batch.id, &updates)?;
        self.bump_generation();
        for (name, _) in &updates {
            self.bump_document(name);
        }
        Ok(())
    }

    /// The local view of one document: server-confirmed state with
    /// every queued mutation for the path applied in order.
    pub(crate) fn local_document(&self, relative_path: &str) -> Result<Option<Document>> {
        let name = full_document_name(&self.documents_prefix, relative_path);
        let mut current = self.store.remote_document(&name)?;
        for batch in self.store.peek_batches(usize::MAX)? {
            for mutation in &batch.mutations {
                if mutation.path() == relative_path {
                    current = if mutation.transforms().is_empty() {
                        mutation.apply_to_local(current)
                    } else {
                        self.apply_local_with_estimates(&batch, mutation, current, &name)?
                    };
                    if let Some(document) = current.as_mut() {
                        if document.name.is_empty() {
                            document.name = name.clone();
                        }
                    }
                }
            }
        }
        Ok(current)
    }

    /// Applies one transform-bearing mutation to the local view
    /// with frozen estimates. An increment is not idempotent: if
    /// the remote base moves under a queued increment — the
    /// watch delivers the write's own server result before the
    /// acknowledgement lands, say — reapplying the mutation to
    /// the new base would count it twice. So the first
    /// application freezes each *increment* field's resulting
    /// value against its batch in the store, and later
    /// applications replay the mutation's literal part on the
    /// current base but substitute the frozen values for the
    /// increment fields. The remaining transforms are recomputed
    /// against the current base on every application (see the
    /// loop). The estimates persist across restarts and are
    /// dropped when the batch settles, when the fold installs
    /// the server's exact transform results into the base.
    fn apply_local_with_estimates(
        &self,
        batch: &crate::store::MutationBatch,
        mutation: &Mutation,
        current: Option<Document>,
        full_name: &str,
    ) -> Result<Option<Document>> {
        let frozen = self.store.transform_estimates(batch.id, full_name)?;
        let Some(computed) = mutation.apply_to_local(current.clone()) else {
            // A patch against a missing base produces nothing,
            // frozen or not.
            return Ok(None);
        };
        // The literal part replays on the current base; applied
        // to the same base it cannot come back empty where the
        // full application succeeded.
        let mut result = mutation
            .without_transforms()
            .apply_to_local(current)
            .unwrap_or_else(|| computed.clone());
        let mut fresh: Vec<(String, crate::value::Value)> = Vec::new();
        for transform in mutation.transforms() {
            // Only the non-idempotent transform freezes: an
            // increment must not count twice when its own result
            // lands in the base early. The array transforms are
            // idempotent — union and removal settle to the same
            // outcome over any base — so they are recomputed
            // against the current base and keep tracking remote
            // changes while the batch is pending; a server
            // timestamp's local estimate (Null) does not depend
            // on the base at all.
            let freezes = matches!(transform.kind, crate::mutation::TransformKind::Increment(_));
            let frozen_value = if freezes {
                frozen
                    .iter()
                    .find(|(path, _)| path == &transform.path)
                    .map(|(_, value)| value.clone())
            } else {
                None
            };
            let is_frozen = frozen_value.is_some();
            match frozen_value.or_else(|| {
                crate::mutation::lookup_path(&computed.fields, &transform.path).cloned()
            }) {
                Some(value) => {
                    if freezes && !is_frozen {
                        fresh.push((transform.path.clone(), value.clone()));
                    }
                    crate::mutation::set_path(&mut result.fields, &transform.path, value);
                }
                None => {
                    crate::mutation::delete_path(&mut result.fields, &transform.path);
                }
            }
        }
        if !fresh.is_empty() {
            self.store
                .put_transform_estimates(batch.id, full_name, &fresh)?;
        }
        Ok(Some(result))
    }

    /// Whether any queued mutation touches `relative_path`.
    pub(crate) fn has_pending(&self, relative_path: &str) -> Result<bool> {
        self.store.has_pending_mutation_for(relative_path)
    }

    /// Whether any queued mutation touches a direct child of
    /// the collection `collection_id` under `parent_relative`
    /// (the members a query over that collection sees).
    pub(crate) fn has_pending_in_collection(
        &self,
        parent_relative: &str,
        collection_id: &str,
    ) -> Result<bool> {
        let prefix = Self::collection_prefix(parent_relative, collection_id);
        for batch in self.store.peek_batches(usize::MAX)? {
            let paths = batch.mutations.iter().map(|mutation| mutation.path());
            if !Self::collection_members(&prefix, paths).is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The collection prefix (relative, trailing slash) a query
    /// under `parent_relative` scans: `notes/` at the root,
    /// `spaces/personal/notes/` below a document.
    fn collection_prefix(parent_relative: &str, collection_id: &str) -> String {
        if parent_relative.is_empty() {
            format!("{collection_id}/")
        } else {
            format!("{parent_relative}/{collection_id}/")
        }
    }

    /// The direct children of the collection among `keys`
    /// (relative paths): a key belongs when it sits exactly one
    /// segment below the collection prefix.
    fn collection_members<'a>(prefix: &str, keys: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
        keys.filter(|key| {
            key.strip_prefix(prefix)
                .is_some_and(|rest| !rest.is_empty() && !rest.contains('/'))
        })
        .collect()
    }

    /// Evaluates a query over the local store: the candidates
    /// are the collection's direct children known to the store —
    /// server-confirmed documents plus every document a queued
    /// mutation touches — each resolved through the local view
    /// (so unacknowledged writes are visible), then filtered,
    /// ordered and limited by the query itself. Index-free, like
    /// the official client: candidates are scanned in memory.
    pub(crate) fn local_query(
        &self,
        parent_relative: &str,
        query: &crate::query::Query,
    ) -> Result<Vec<Document>> {
        let prefix = Self::collection_prefix(parent_relative, query.collection_id());
        let mut keys: Vec<String> = Vec::new();
        for document in self.store.all_remote_documents()? {
            if let Some(relative) = document.name.strip_prefix(&self.documents_prefix) {
                let relative = relative.strip_prefix('/').unwrap_or(relative);
                keys.push(relative.to_string());
            }
        }
        for batch in self.store.peek_batches(usize::MAX)? {
            for mutation in &batch.mutations {
                keys.push(mutation.path().to_string());
            }
        }
        keys.sort();
        keys.dedup();
        let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let mut candidates = Vec::new();
        for key in Self::collection_members(&prefix, key_refs.into_iter()) {
            if let Some(document) = self.local_document(key)? {
                candidates.push(document);
            }
        }
        Ok(query.evaluate(candidates))
    }

    /// Whether the store knows anything about a collection: at
    /// least one server-confirmed document in it, or a queued
    /// mutation touching one of its documents. Reads use this to
    /// decide whether a local answer can be trusted as complete
    /// or an online fetch must fill the gap first (mirroring
    /// `get_document`'s local-first rule).
    pub(crate) fn collection_known(
        &self,
        parent_relative: &str,
        collection_id: &str,
    ) -> Result<bool> {
        let prefix = Self::collection_prefix(parent_relative, collection_id);
        for document in self.store.all_remote_documents()? {
            if let Some(relative) = document.name.strip_prefix(&self.documents_prefix) {
                let relative = relative.strip_prefix('/').unwrap_or(relative);
                if !Self::collection_members(&prefix, [relative].into_iter()).is_empty() {
                    return Ok(true);
                }
            }
        }
        for batch in self.store.peek_batches(usize::MAX)? {
            for mutation in &batch.mutations {
                if !Self::collection_members(&prefix, [mutation.path()].into_iter()).is_empty() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Records a server-confirmed document (e.g. an online fetch
    /// that populated the cache).
    pub(crate) fn store_remote_document(&self, document: &Document) -> Result<()> {
        self.store.put_remote_document(document)?;
        self.bump_generation();
        self.bump_document(&document.name);
        Ok(())
    }

    /// Records a server-confirmed document only when it is at
    /// least as new as the cached one (fetch/ack race guard).
    pub(crate) fn store_remote_document_if_newer(&self, document: &Document) -> Result<bool> {
        let stored = self.store.put_remote_document_if_newer(document)?;
        if stored {
            self.bump_generation();
            self.bump_document(&document.name);
        }
        Ok(stored)
    }

    /// Records that the server has no such document.
    pub(crate) fn store_remote_absent(&self, relative_path: &str) -> Result<()> {
        let name = full_document_name(&self.documents_prefix, relative_path);
        self.store_remote_absent_name(&name)
    }

    /// Records that the server has no document with this full
    /// resource name (the form watch deliveries arrive in).
    pub(crate) fn store_remote_absent_name(&self, full_name: &str) -> Result<()> {
        self.store.remove_remote_document(full_name)?;
        self.bump_generation();
        self.bump_document(full_name);
        Ok(())
    }

    /// Reconciles a document fetched online whose flight raced
    /// local changes (the caller captured
    /// [`PersistenceEngine::document_generation`] before the
    /// fetch). A fetched document may refresh the cache base when
    /// a base row exists (newer-wins applies), or when this
    /// document itself never transitioned locally. When the
    /// document *did* transition and no base row remains — the
    /// signature of an acknowledged delete — the fetched document
    /// predates the delete and is discarded.
    pub(crate) fn reconcile_fetched_document(
        &self,
        relative_path: &str,
        fetched: Option<&Document>,
        captured_document_generation: u64,
    ) -> Result<()> {
        if let Some(document) = fetched {
            let name = full_document_name(&self.documents_prefix, relative_path);
            let has_base = self.store.remote_document(&name)?.is_some();
            if has_base || self.document_generation(relative_path) == captured_document_generation {
                self.store_remote_document_if_newer(document)?;
            }
        }
        Ok(())
    }

    /// Resolves when the queue is empty. Returns the remembered
    /// error if a batch was permanently rejected while waiting (or
    /// since the previous wait).
    pub(crate) async fn wait_for_pending_writes(self: &Arc<Self>) -> Result<()> {
        self.ensure_pump();
        loop {
            if self.store.batch_count()? == 0 {
                let dropped = self
                    .dropped_error
                    .lock()
                    .expect("dropped mutex poisoned")
                    .take();
                return match dropped {
                    Some(message) => Err(Error::Other(format!(
                        "a queued write was rejected by the server: {message}"
                    ))),
                    None => Ok(()),
                };
            }
            let notified = self.queue_changed.notified();
            tokio::pin!(notified);
            let _ = notified.as_mut().enable();
            // A bounded wait as a safety net alongside the notify.
            let _ = tokio::time::timeout(std::time::Duration::from_millis(200), notified).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::Mutation;
    use crate::value::Value;
    use crate::Fields;

    struct StubTokens;

    #[async_trait::async_trait]
    impl firebase_native_core::TokenProvider for StubTokens {
        async fn id_token(&self, _force_refresh: bool) -> Result<String> {
            Ok("stub-token".to_string())
        }
    }

    fn test_engine() -> Arc<PersistenceEngine> {
        // The write stream inside this engine is never exercised by
        // these tests (no pump is started; batches are enqueued
        // directly in the store). Its channel cell is never
        // initialized, so construction performs no I/O.
        let store = Arc::new(LocalStore::open_in_memory().unwrap());
        let options = firebase_native_core::FirebaseOptions {
            api_key: "test".to_string(),
            project_id: "p".to_string(),
            auth_domain: None,
            storage_bucket: None,
            app_id: None,
            database_id: "d".to_string(),
        };
        let write = WriteStream::new(
            options,
            Arc::new(StubTokens),
            Arc::new(tokio::sync::OnceCell::new()),
            "https://firestore.googleapis.com".to_string(),
        );
        Arc::new(PersistenceEngine::new(
            store,
            write,
            "projects/p/databases/d/documents".to_string(),
        ))
    }

    fn set(path: &str, n: i64) -> Mutation {
        let mut fields = Fields::new();
        fields.insert("n".to_string(), Value::Integer(n));
        Mutation::Set {
            path: path.to_string(),
            fields,
            merge_paths: None,
            exists: None,
            transforms: Vec::new(),
        }
    }

    fn remote_doc_at(path: &str, n: i64, update_seconds: i64) -> Document {
        let mut fields = Fields::new();
        fields.insert("n".to_string(), Value::Integer(n));
        Document {
            name: format!("projects/p/databases/d/documents/{path}"),
            fields,
            create_time: None,
            update_time: Some(crate::value::Timestamp {
                seconds: update_seconds,
                nanos: 0,
            }),
        }
    }

    /// A queued increment is applied once, not once per base:
    /// when the watch caches the write's own server result
    /// before the acknowledgement lands, the local view keeps
    /// the frozen estimate instead of counting the increment
    /// again — and the fold then installs the exact result.
    #[test]
    fn queued_increment_is_not_reapplied_to_its_own_result() {
        let engine = test_engine();
        engine
            .store_remote_document(&remote_doc_at("notes/a", 10, 10))
            .unwrap();
        let batch_id = engine
            .store()
            .enqueue_batch(&[Mutation::Patch {
                path: "notes/a".to_string(),
                fields: Fields::new(),
                // Transform paths are not mask paths (the
                // mutation model's disjointness invariant).
                mask: Vec::new(),
                exists: Some(true),
                transforms: vec![crate::mutation::FieldTransform {
                    path: "n".to_string(),
                    kind: crate::mutation::TransformKind::Increment(Value::Integer(1)),
                }],
            }])
            .unwrap();
        let view = engine.local_document("notes/a").unwrap().unwrap();
        assert_eq!(view.fields.get("n"), Some(&Value::Integer(11)));

        // The watch caches the write's own result (n = 11) while
        // the batch is still queued: the view must stay 11, not
        // reapply the increment to 12.
        engine
            .store_remote_document(&remote_doc_at("notes/a", 11, 20))
            .unwrap();
        let view = engine.local_document("notes/a").unwrap().unwrap();
        assert_eq!(view.fields.get("n"), Some(&Value::Integer(11)));

        // Settlement: the fold installs the exact server result
        // and drops the queue entry and its estimates; the view
        // now comes from the base alone.
        let batch = engine
            .store()
            .peek_batches(1)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(batch.id, batch_id);
        engine
            .fold_acknowledgement_with_results(
                &batch,
                crate::value::Timestamp {
                    seconds: 20,
                    nanos: 0,
                },
                &[vec![Value::Integer(11)]],
            )
            .unwrap();
        assert_eq!(engine.store().batch_count().unwrap(), 0);
        assert!(engine
            .store()
            .transform_estimates(batch_id, "projects/p/databases/d/documents/notes/a")
            .unwrap()
            .is_empty());
        let view = engine.local_document("notes/a").unwrap().unwrap();
        assert_eq!(view.fields.get("n"), Some(&Value::Integer(11)));
    }

    fn remote_tags(path: &str, tags: &[&str], update_seconds: i64) -> Document {
        let mut fields = Fields::new();
        fields.insert(
            "tags".to_string(),
            Value::Array(
                tags.iter()
                    .map(|tag| Value::String(tag.to_string()))
                    .collect(),
            ),
        );
        Document {
            name: format!("projects/p/databases/d/documents/{path}"),
            fields,
            create_time: None,
            update_time: Some(crate::value::Timestamp {
                seconds: update_seconds,
                nanos: 0,
            }),
        }
    }

    fn tags_of(document: &Document) -> Vec<String> {
        match document.fields.get("tags") {
            Some(Value::Array(items)) => items
                .iter()
                .map(|item| match item {
                    Value::String(tag) => tag.clone(),
                    other => panic!("unexpected tag value: {other:?}"),
                })
                .collect(),
            other => panic!("unexpected tags field: {other:?}"),
        }
    }

    fn array_patch(path: &str, kind: crate::mutation::TransformKind) -> Mutation {
        Mutation::Patch {
            path: path.to_string(),
            fields: Fields::new(),
            mask: Vec::new(),
            exists: Some(true),
            transforms: vec![crate::mutation::FieldTransform {
                path: "tags".to_string(),
                kind,
            }],
        }
    }

    /// The array transforms are idempotent, so they are NOT
    /// frozen: while the batch is pending they recompute over
    /// the current remote base and keep tracking its changes.
    #[test]
    fn queued_array_transforms_track_the_remote_base() {
        let engine = test_engine();
        // Union: base [a], pending union [b] reads [a, b]; when
        // the base moves to [a, c] the view becomes [a, c, b] —
        // not the frozen [a, b].
        engine
            .store_remote_document(&remote_tags("notes/a", &["a"], 10))
            .unwrap();
        engine
            .store()
            .enqueue_batch(&[array_patch(
                "notes/a",
                crate::mutation::TransformKind::ArrayUnion(vec![Value::String("b".to_string())]),
            )])
            .unwrap();
        let view = engine.local_document("notes/a").unwrap().unwrap();
        assert_eq!(tags_of(&view), ["a", "b"]);
        engine
            .store_remote_document(&remote_tags("notes/a", &["a", "c"], 20))
            .unwrap();
        let view = engine.local_document("notes/a").unwrap().unwrap();
        assert_eq!(tags_of(&view), ["a", "c", "b"]);

        // Remove: base [a, c], pending remove [a] reads [c];
        // when the base gains d the view becomes [c, d].
        engine
            .store_remote_document(&remote_tags("notes/b", &["a", "c"], 10))
            .unwrap();
        engine
            .store()
            .enqueue_batch(&[array_patch(
                "notes/b",
                crate::mutation::TransformKind::ArrayRemove(vec![Value::String("a".to_string())]),
            )])
            .unwrap();
        let view = engine.local_document("notes/b").unwrap().unwrap();
        assert_eq!(tags_of(&view), ["c"]);
        engine
            .store_remote_document(&remote_tags("notes/b", &["a", "c", "d"], 20))
            .unwrap();
        let view = engine.local_document("notes/b").unwrap().unwrap();
        assert_eq!(tags_of(&view), ["c", "d"]);
    }

    #[test]
    fn fold_assigns_name_and_removes_batch() {
        let engine = test_engine();
        let batch_id = engine.store().enqueue_batch(&[set("notes/a", 1)]).unwrap();
        let batch = engine.store().peek_batches(1).unwrap().remove(0);
        assert_eq!(batch.id, batch_id);
        engine
            .fold_acknowledgement(&batch, crate::value::Timestamp::new(50, 0))
            .unwrap();
        // Queue entry gone, and the acknowledged document is cached
        // under its full resource name (not the empty string).
        assert_eq!(engine.store().batch_count().unwrap(), 0);
        let cached = engine
            .store()
            .remote_document("projects/p/databases/d/documents/notes/a")
            .unwrap()
            .expect("acknowledged document cached");
        assert_eq!(cached.name, "projects/p/databases/d/documents/notes/a");
        assert_eq!(
            cached.update_time,
            Some(crate::value::Timestamp::new(50, 0))
        );
        // And the local view still serves it after the ack.
        assert!(engine.local_document("notes/a").unwrap().is_some());
    }

    #[test]
    fn fold_does_not_regress_a_newer_remote_base() {
        // The watch delivered a newer server state of the
        // document before the write's acknowledgement folded:
        // folding the older mutation over it would regress the
        // base. The batch still completes and dequeues.
        let engine = test_engine();
        let remote = Document {
            name: "projects/p/databases/d/documents/notes/a".to_string(),
            fields: {
                let mut fields = Fields::new();
                fields.insert("n".to_string(), Value::Integer(3));
                fields
            },
            create_time: None,
            update_time: Some(crate::value::Timestamp::new(300, 0)),
        };
        engine.store_remote_document(&remote).unwrap();
        engine.store().enqueue_batch(&[set("notes/a", 2)]).unwrap();
        let batch = engine.store().peek_batches(1).unwrap().remove(0);
        engine
            .fold_acknowledgement(&batch, crate::value::Timestamp::new(200, 0))
            .unwrap();
        assert_eq!(engine.store().batch_count().unwrap(), 0);
        let cached = engine
            .store()
            .remote_document("projects/p/databases/d/documents/notes/a")
            .unwrap()
            .expect("base stands");
        assert_eq!(
            cached.fields.get("n"),
            Some(&Value::Integer(3)),
            "the newer watch-delivered state must not regress"
        );
        assert_eq!(
            cached.update_time,
            Some(crate::value::Timestamp::new(300, 0))
        );
    }

    #[test]
    fn generation_tracks_local_changes() {
        // The read-race guard: every kind of local change —
        // enqueue, acknowledgement fold, cache store — must move
        // the generation, including a write that is enqueued AND
        // acknowledged between the two observations.
        let engine = test_engine();
        let g0 = engine.generation();
        engine.store().enqueue_batch(&[set("notes/a", 1)]).unwrap();
        // (Enqueue through the store directly bypasses the engine's
        // bump; go through the public paths below instead.)
        let g_before = engine.generation();
        let batch = engine.store().peek_batches(1).unwrap().remove(0);
        engine
            .fold_acknowledgement(&batch, crate::value::Timestamp::new(1, 0))
            .unwrap();
        assert!(engine.generation() > g_before, "fold must bump");
        let _ = g0;
        let g_before = engine.generation();
        let mut document = engine.local_document("notes/a").unwrap().unwrap();
        document.fields.insert("x".to_string(), Value::Integer(9));
        engine.store_remote_document(&document).unwrap();
        assert!(engine.generation() > g_before, "cache store must bump");
        // A stale if-newer store changes nothing and does not bump.
        let g_before = engine.generation();
        let mut stale = document.clone();
        stale.update_time = Some(crate::value::Timestamp::new(0, 0));
        assert!(!engine.store_remote_document_if_newer(&stale).unwrap());
        assert_eq!(engine.generation(), g_before);
    }

    #[test]
    fn reconcile_rejects_fetch_predating_an_acknowledged_delete() {
        let engine = test_engine();
        let name = "projects/p/databases/d/documents/notes/a";
        let mut fields = Fields::new();
        fields.insert("n".to_string(), Value::Integer(1));
        let document = Document {
            name: name.to_string(),
            fields,
            create_time: None,
            update_time: Some(crate::value::Timestamp::new(10, 0)),
        };
        engine.store_remote_document(&document).unwrap();
        // An online fetch starts here (generation captured)...
        let captured = engine.document_generation("notes/a");
        // ...a delete is queued and acknowledged while it flies...
        engine
            .store()
            .enqueue_batch(&[Mutation::Delete {
                path: "notes/a".to_string(),
            }])
            .unwrap();
        let batch = engine.store().peek_batches(1).unwrap().remove(0);
        engine
            .fold_acknowledgement(&batch, crate::value::Timestamp::new(20, 0))
            .unwrap();
        assert!(engine.local_document("notes/a").unwrap().is_none());
        // ...and the older fetch finally returns the pre-delete
        // document. With no cache row left, only the per-document
        // generation can (and must) reject it.
        engine
            .reconcile_fetched_document("notes/a", Some(&document), captured)
            .unwrap();
        assert!(
            engine.local_document("notes/a").unwrap().is_none(),
            "the deleted document must not be resurrected"
        );
        // Control: an untouched document reconciles normally.
        let captured_b = engine.document_generation("notes/b");
        let mut document_b = document.clone();
        document_b.name = "projects/p/databases/d/documents/notes/b".to_string();
        engine
            .reconcile_fetched_document("notes/b", Some(&document_b), captured_b)
            .unwrap();
        assert!(engine.local_document("notes/b").unwrap().is_some());
    }

    fn remote_doc(relative: &str, n: i64) -> Document {
        let mut fields = Fields::new();
        fields.insert("n".to_string(), Value::Integer(n));
        Document {
            name: format!("projects/p/databases/d/documents/{relative}"),
            fields,
            create_time: None,
            update_time: Some(crate::value::Timestamp::new(10, 0)),
        }
    }

    #[test]
    fn local_query_scans_collection_and_overlays_pending() {
        let engine = test_engine();
        engine
            .store_remote_document(&remote_doc("notes/b", 1))
            .unwrap();
        engine
            .store_remote_document(&remote_doc("notes/a", 2))
            .unwrap();
        // A nested subcollection document is not a member of notes.
        engine
            .store_remote_document(&remote_doc("notes/a/sub/x", 9))
            .unwrap();
        // A pending local write creates a third member.
        engine.store().enqueue_batch(&[set("notes/c", 3)]).unwrap();

        let query = crate::query::Query::new("notes").order_by(crate::query::Order::asc("n"));
        let results = engine.local_query("", &query).unwrap();
        let paths: Vec<&str> = results
            .iter()
            .map(|document| document.name.rsplit('/').next().unwrap())
            .collect();
        assert_eq!(paths, ["b", "a", "c"]);

        // A pending delete removes the document from the results.
        engine
            .store()
            .enqueue_batch(&[Mutation::Delete {
                path: "notes/a".to_string(),
            }])
            .unwrap();
        let results = engine.local_query("", &query).unwrap();
        assert_eq!(results.len(), 2);

        // Subcollection queries address their own parent.
        let sub = crate::query::Query::new("sub");
        let results = engine.local_query("notes/a", &sub).unwrap();
        assert_eq!(results.len(), 1);
        assert!(engine.local_query("", &sub).unwrap().is_empty());
    }

    #[test]
    fn collection_known_reflects_cache_and_queue() {
        let engine = test_engine();
        assert!(!engine.collection_known("", "notes").unwrap());
        assert!(!engine.collection_known("spaces/personal", "notes").unwrap());
        engine
            .store_remote_document(&remote_doc("notes/a", 1))
            .unwrap();
        assert!(engine.collection_known("", "notes").unwrap());
        // The cache row belongs to the root collection only.
        assert!(!engine.collection_known("spaces/personal", "notes").unwrap());
        // A queued mutation also marks its collection known.
        engine.store().enqueue_batch(&[set("other/x", 1)]).unwrap();
        assert!(engine.collection_known("", "other").unwrap());
    }

    #[test]
    fn local_view_overlays_queue_in_order() {
        let engine = test_engine();
        engine.store().enqueue_batch(&[set("notes/a", 1)]).unwrap();
        engine
            .store()
            .enqueue_batch(&[Mutation::Patch {
                path: "notes/a".to_string(),
                fields: {
                    let mut fields = Fields::new();
                    fields.insert("n".to_string(), Value::Integer(2));
                    fields
                },
                mask: vec!["n".to_string()],
                exists: Some(true),
                transforms: Vec::new(),
            }])
            .unwrap();
        let view = engine.local_document("notes/a").unwrap().unwrap();
        assert_eq!(view.name, "projects/p/databases/d/documents/notes/a");
        assert_eq!(view.fields.get("n"), Some(&Value::Integer(2)));
        assert!(engine.has_pending("notes/a").unwrap());

        engine
            .store()
            .enqueue_batch(&[Mutation::Delete {
                path: "notes/a".to_string(),
            }])
            .unwrap();
        assert!(engine.local_document("notes/a").unwrap().is_none());
        // Another path is untouched.
        assert!(engine.local_document("notes/b").unwrap().is_none());
        assert!(!engine.has_pending("notes/b").unwrap());
    }
}
