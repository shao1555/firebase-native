//! The local store: a SQLite database holding everything the
//! offline engine needs across process restarts.
//!
//! - `remote_documents` — server-confirmed documents, stored as
//!   protobuf-encoded `google.firestore.v1.Document` bytes keyed by
//!   full resource name (the plan's storage choice: values stay in
//!   the wire format, conversions happen at the edges).
//! - `mutations` — the pending mutation queue, FIFO by row id; each
//!   row is one batch (a JSON array of [`Mutation`]) waiting for the
//!   Write stream to acknowledge it.
//! - `targets` — per-target sync state (resume token + snapshot
//!   version) so Listen can resume after a restart.
//! - `document_access` / `document_targets` / `active_targets` —
//!   cache-management bookkeeping owned by the garbage
//!   collector: the last-access sequence of each cached
//!   document, which documents form the cached baseline of
//!   each target (persistent: it is what the target's resume
//!   token refers to), and which targets belong to currently
//!   open watches (session state). All live inside the store
//!   on purpose: eviction selects, revalidates protection and
//!   deletes in one transaction, and losing the bookkeeping
//!   must never endanger the queue — documents without an
//!   access row are simply treated as the oldest.
//! - `incomplete_collections` — collections whose cached
//!   contents are known to be partial because eviction
//!   removed some of their documents; local-first query
//!   evaluation must not trust them until a server sync
//!   refills them.
//!
//! The crate chooses no location: the embedding application passes
//! the file path, which is also how the shell keeps session/storage
//! policy (fail-closed included) outside the SDK.

use std::path::Path;
use std::sync::Mutex;

use firebase_native_core::{Error, Result};
use prost::Message;
use rusqlite::{params, Connection, OptionalExtension};

use crate::mutation::Mutation;
use crate::value::{Document, Timestamp};

/// The result of one atomic eviction round
/// ([`LocalStore::evict_lru_batch`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EvictionOutcome {
    /// Full names of the documents that were evicted.
    pub(crate) names: Vec<String>,
    /// Payload bytes freed by the round.
    pub(crate) freed_bytes: u64,
    /// Targets whose resume state was deleted because their
    /// cached baseline lost a document.
    pub(crate) invalidated_targets: Vec<i64>,
}

/// One queued mutation batch.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MutationBatch {
    /// Row id in the `mutations` table (FIFO order).
    pub(crate) id: i64,
    /// Mutations in application order.
    pub(crate) mutations: Vec<Mutation>,
}

/// Sync state for one listen target. Stored from this phase on so
/// the listen-through-store step (P2 part 2) can resume targets
/// across restarts without a schema migration.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct TargetState {
    /// Opaque resume token from the last consistent snapshot.
    pub(crate) resume_token: Vec<u8>,
    /// Snapshot version (read time) the token belongs to.
    pub(crate) snapshot_version: Option<Timestamp>,
}

/// The local store handle. All operations are synchronous and
/// serialized on an internal mutex; the engine calls them from its
/// async tasks without holding the lock across awaits.
pub(crate) struct LocalStore {
    conn: Mutex<Connection>,
}

fn store_error(error: rusqlite::Error) -> Error {
    Error::Other(format!("local store: {error}"))
}

impl LocalStore {
    /// Opens (creating if needed) the store at `path` and applies
    /// the schema.
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).map_err(store_error)?;
        Self::init(conn)
    }

    /// Opens an ephemeral in-memory store (tests).
    #[cfg(test)]
    pub(crate) fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(store_error)?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.execute_batch(
            "PRAGMA auto_vacuum = INCREMENTAL;
             CREATE TABLE IF NOT EXISTS remote_documents (
                 name TEXT PRIMARY KEY,
                 proto BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS mutations (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 batch TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS targets (
                 target_id INTEGER PRIMARY KEY,
                 resume_token BLOB NOT NULL,
                 snapshot_seconds INTEGER,
                 snapshot_nanos INTEGER
             );
             CREATE TABLE IF NOT EXISTS transform_estimates (
                 batch_id INTEGER NOT NULL,
                 name TEXT NOT NULL,
                 field_path TEXT NOT NULL,
                 value TEXT NOT NULL,
                 PRIMARY KEY (batch_id, name, field_path)
             );
             CREATE TABLE IF NOT EXISTS document_access (
                 name TEXT PRIMARY KEY,
                 seq INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS document_targets (
                 name TEXT NOT NULL,
                 target_key INTEGER NOT NULL,
                 PRIMARY KEY (name, target_key)
             );
             CREATE INDEX IF NOT EXISTS document_targets_by_target
                 ON document_targets (target_key);
             CREATE TABLE IF NOT EXISTS active_targets (
                 target_key INTEGER PRIMARY KEY
             );
             CREATE TABLE IF NOT EXISTS incomplete_collections (
                 collection_key TEXT PRIMARY KEY
             );",
        )
        .map_err(store_error)?;
        // One-time migration, gated on the schema version:
        // stores written before GC bookkeeping existed hold
        // resume tokens whose baselines were never recorded
        // in document_targets. Those baselines are
        // unverifiable — the collector may evict their
        // documents without any way to associate them with
        // the token — so the tokens must not survive the
        // upgrade; affected watches re-add their targets in
        // full on next open. (Cached documents need no
        // migration: with no access rows they simply sort
        // as the oldest eviction candidates.)
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(store_error)?;
        if version < 1 {
            conn.execute("DELETE FROM targets", [])
                .map_err(store_error)?;
            conn.execute_batch("PRAGMA user_version = 1")
                .map_err(store_error)?;
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    // --- server-confirmed documents --------------------------------

    /// Returns the server-confirmed document with this full name.
    pub(crate) fn remote_document(&self, name: &str) -> Result<Option<Document>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let bytes: Option<Vec<u8>> = conn
            .query_row(
                "SELECT proto FROM remote_documents WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .optional()
            .map_err(store_error)?;
        match bytes {
            None => Ok(None),
            Some(bytes) => Ok(Some(decode_document(name, &bytes)?)),
        }
    }

    /// Inserts or replaces a server-confirmed document.
    pub(crate) fn put_remote_document(&self, document: &Document) -> Result<()> {
        let bytes = encode_document(document);
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "INSERT INTO remote_documents (name, proto) VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET proto = excluded.proto",
            params![document.name, bytes],
        )
        .map_err(store_error)?;
        Self::touch_access_in(&conn, std::slice::from_ref(&document.name))?;
        Ok(())
    }

    /// Inserts a server-confirmed document only when its update
    /// time is at least as new as the cached one (or nothing is
    /// cached). Guards the cache against a slow fetch overwriting
    /// state a faster acknowledgement already recorded. Returns
    /// whether the document was stored.
    pub(crate) fn put_remote_document_if_newer(&self, document: &Document) -> Result<bool> {
        match self.remote_document(&document.name)? {
            Some(existing)
                if existing.update_time.is_some()
                    && document.update_time < existing.update_time =>
            {
                Ok(false)
            }
            _ => {
                self.put_remote_document(document)?;
                Ok(true)
            }
        }
    }

    /// Applies an acknowledged batch to the store atomically: all
    /// document upserts/removals and the queue removal happen in
    /// one transaction, so a crash or a storage error can never
    /// leave the cache updated without the queue entry (or the
    /// reverse). `updates` pairs each touched document's full name
    /// with its new server state (`None` = deleted server-side).
    pub(crate) fn commit_acknowledged_batch(
        &self,
        batch_id: i64,
        updates: &[(String, Option<Document>)],
    ) -> Result<()> {
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let transaction = conn.transaction().map_err(store_error)?;
        for (name, document) in updates {
            match document {
                Some(document) => {
                    transaction
                        .execute(
                            "INSERT INTO remote_documents (name, proto) VALUES (?1, ?2)
                             ON CONFLICT(name) DO UPDATE SET proto = excluded.proto",
                            params![name, encode_document(document)],
                        )
                        .map_err(store_error)?;
                }
                None => {
                    transaction
                        .execute(
                            "DELETE FROM remote_documents WHERE name = ?1",
                            params![name],
                        )
                        .map_err(store_error)?;
                }
            }
        }
        let upserted: Vec<String> = updates
            .iter()
            .filter(|(_, document)| document.is_some())
            .map(|(name, _)| name.clone())
            .collect();
        Self::touch_access_in(&transaction, &upserted)?;
        transaction
            .execute("DELETE FROM mutations WHERE id = ?1", params![batch_id])
            .map_err(store_error)?;
        transaction
            .execute(
                "DELETE FROM transform_estimates WHERE batch_id = ?1",
                params![batch_id],
            )
            .map_err(store_error)?;
        transaction.commit().map_err(store_error)?;
        Ok(())
    }

    /// Removes a server-confirmed document (server says it is gone).
    pub(crate) fn remove_remote_document(&self, name: &str) -> Result<()> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "DELETE FROM remote_documents WHERE name = ?1",
            params![name],
        )
        .map_err(store_error)?;
        conn.execute("DELETE FROM document_access WHERE name = ?1", params![name])
            .map_err(store_error)?;
        conn.execute(
            "DELETE FROM document_targets WHERE name = ?1",
            params![name],
        )
        .map_err(store_error)?;
        Ok(())
    }

    /// All server-confirmed documents (used to serve offline reads
    /// and, later, local query evaluation).
    #[allow(dead_code)]
    pub(crate) fn all_remote_documents(&self) -> Result<Vec<Document>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut statement = conn
            .prepare("SELECT name, proto FROM remote_documents ORDER BY name")
            .map_err(store_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(store_error)?;
        let mut documents = Vec::new();
        for row in rows {
            let (name, bytes) = row.map_err(store_error)?;
            documents.push(decode_document(&name, &bytes)?);
        }
        Ok(documents)
    }

    // --- mutation queue --------------------------------------------

    /// Appends a batch to the queue and returns its batch id.
    pub(crate) fn enqueue_batch(&self, mutations: &[Mutation]) -> Result<i64> {
        let json = serde_json::to_string(mutations)
            .map_err(|e| Error::Other(format!("mutation encode: {e}")))?;
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute("INSERT INTO mutations (batch) VALUES (?1)", params![json])
            .map_err(store_error)?;
        Ok(conn.last_insert_rowid())
    }

    /// The first `limit` queued batches, oldest first.
    pub(crate) fn peek_batches(&self, limit: usize) -> Result<Vec<MutationBatch>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut statement = conn
            .prepare("SELECT id, batch FROM mutations ORDER BY id LIMIT ?1")
            .map_err(store_error)?;
        let rows = statement
            .query_map(params![limit as i64], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(store_error)?;
        let mut batches = Vec::new();
        for row in rows {
            let (id, json) = row.map_err(store_error)?;
            let mutations: Vec<Mutation> = serde_json::from_str(&json)
                .map_err(|e| Error::Other(format!("mutation decode: {e}")))?;
            batches.push(MutationBatch { id, mutations });
        }
        Ok(batches)
    }

    /// Removes an acknowledged (or server-rejected) batch,
    /// together with its frozen transform estimates.
    pub(crate) fn remove_batch(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute("DELETE FROM mutations WHERE id = ?1", params![id])
            .map_err(store_error)?;
        conn.execute(
            "DELETE FROM transform_estimates WHERE batch_id = ?1",
            params![id],
        )
        .map_err(store_error)?;
        Ok(())
    }

    // --- transform estimates -------------------------------------

    /// Records frozen local estimates for a batch's increment
    /// fields on one document. An increment is not idempotent,
    /// so the value its first local application produced is
    /// kept until the batch settles (see the engine's local
    /// view); the rows die with the batch (the
    /// acknowledged-batch commit and `remove_batch` delete
    /// them), because the fold installs the server's exact
    /// results into the base.
    pub(crate) fn put_transform_estimates(
        &self,
        batch_id: i64,
        name: &str,
        estimates: &[(String, crate::value::Value)],
    ) -> Result<()> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        for (field_path, value) in estimates {
            let json = serde_json::to_string(value)
                .map_err(|e| Error::Other(format!("estimate encode: {e}")))?;
            conn.execute(
                "INSERT INTO transform_estimates (batch_id, name, field_path, value)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(batch_id, name, field_path)
                 DO UPDATE SET value = excluded.value",
                params![batch_id, name, field_path, json],
            )
            .map_err(store_error)?;
        }
        Ok(())
    }

    /// The frozen transform estimates of one batch for one
    /// document, as (field path, value) pairs.
    pub(crate) fn transform_estimates(
        &self,
        batch_id: i64,
        name: &str,
    ) -> Result<Vec<(String, crate::value::Value)>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut statement = conn
            .prepare(
                "SELECT field_path, value FROM transform_estimates
                 WHERE batch_id = ?1 AND name = ?2",
            )
            .map_err(store_error)?;
        let rows = statement
            .query_map(params![batch_id, name], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(store_error)?;
        let mut estimates = Vec::new();
        for row in rows {
            let (field_path, json) = row.map_err(store_error)?;
            let value: crate::value::Value = serde_json::from_str(&json)
                .map_err(|e| Error::Other(format!("estimate decode: {e}")))?;
            estimates.push((field_path, value));
        }
        Ok(estimates)
    }

    /// Number of queued batches.
    pub(crate) fn batch_count(&self) -> Result<usize> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM mutations", [], |row| row.get(0))
            .map_err(store_error)?;
        Ok(count as usize)
    }

    /// True when any queued mutation touches `path` (relative path).
    pub(crate) fn has_pending_mutation_for(&self, path: &str) -> Result<bool> {
        for batch in self.peek_batches(usize::MAX)? {
            if batch.mutations.iter().any(|m| m.path() == path) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    // --- target state ----------------------------------------------

    /// Stores a target's resume token and snapshot version.
    pub(crate) fn put_target_state(&self, target_id: i64, state: &TargetState) -> Result<()> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "INSERT INTO targets (target_id, resume_token, snapshot_seconds, snapshot_nanos)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(target_id) DO UPDATE SET
                 resume_token = excluded.resume_token,
                 snapshot_seconds = excluded.snapshot_seconds,
                 snapshot_nanos = excluded.snapshot_nanos",
            params![
                target_id,
                state.resume_token,
                state.snapshot_version.map(|t| t.seconds),
                state.snapshot_version.map(|t| t.nanos),
            ],
        )
        .map_err(store_error)?;
        Ok(())
    }

    /// Reads a target's stored state, if any.
    pub(crate) fn target_state(&self, target_id: i64) -> Result<Option<TargetState>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let row = conn
            .query_row(
                "SELECT resume_token, snapshot_seconds, snapshot_nanos
                 FROM targets WHERE target_id = ?1",
                params![target_id],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(store_error)?;
        Ok(row.map(|(resume_token, seconds, nanos)| TargetState {
            resume_token,
            snapshot_version: match (seconds, nanos) {
                (Some(seconds), Some(nanos)) => Some(Timestamp::new(seconds, nanos as i32)),
                _ => None,
            },
        }))
    }

    // --- cache management (GC) -----------------------------------

    /// Stamps every named document with a fresh last-access
    /// sequence (one sequence for the whole batch, mirroring
    /// the reference core's per-event ordering). Takes any
    /// connection handle, so callers can fold the stamp into
    /// a transaction they already hold.
    fn touch_access_in(conn: &Connection, names: &[String]) -> Result<()> {
        if names.is_empty() {
            return Ok(());
        }
        let seq: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM document_access",
                [],
                |row| row.get(0),
            )
            .map_err(store_error)?;
        for name in names {
            conn.execute(
                "INSERT INTO document_access (name, seq) VALUES (?1, ?2)
                 ON CONFLICT(name) DO UPDATE SET seq = excluded.seq",
                params![name, seq],
            )
            .map_err(store_error)?;
        }
        Ok(())
    }

    /// Records that the named documents were just used (read
    /// from the cache, or delivered in a watch view).
    pub(crate) fn touch_access(&self, names: &[String]) -> Result<()> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        Self::touch_access_in(&conn, names)
    }

    /// Replaces the recorded membership of one target with
    /// `names` (the target's current view) and stamps those
    /// documents as just-accessed.
    pub(crate) fn replace_target_membership(
        &self,
        target_key: i64,
        names: &[String],
    ) -> Result<()> {
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let transaction = conn.transaction().map_err(store_error)?;
        transaction
            .execute(
                "DELETE FROM document_targets WHERE target_key = ?1",
                params![target_key],
            )
            .map_err(store_error)?;
        for name in names {
            transaction
                .execute(
                    "INSERT INTO document_targets (name, target_key) VALUES (?1, ?2)",
                    params![name, target_key],
                )
                .map_err(store_error)?;
        }
        Self::touch_access_in(&transaction, names)?;
        transaction.commit().map_err(store_error)?;
        Ok(())
    }

    /// Marks a target as belonging to a currently open watch.
    /// Only active targets' membership protects documents
    /// from eviction.
    pub(crate) fn put_active_target(&self, target_key: i64) -> Result<()> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "INSERT INTO active_targets (target_key) VALUES (?1)
             ON CONFLICT(target_key) DO NOTHING",
            params![target_key],
        )
        .map_err(store_error)?;
        Ok(())
    }

    /// Unmarks a target (its last watch closed). Its
    /// membership rows stay: they record the baseline its
    /// resume token refers to.
    pub(crate) fn remove_active_target(&self, target_key: i64) -> Result<()> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "DELETE FROM active_targets WHERE target_key = ?1",
            params![target_key],
        )
        .map_err(store_error)?;
        Ok(())
    }

    /// Clears the active-target set. Active state is
    /// session-scoped (a crashed process's watches are gone),
    /// so a fresh engine starts empty — unlike membership,
    /// which is baseline state and persists.
    pub(crate) fn clear_active_targets(&self) -> Result<()> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute("DELETE FROM active_targets", [])
            .map_err(store_error)?;
        Ok(())
    }

    /// Clears a collection's incomplete mark (a server sync
    /// re-established its full contents).
    pub(crate) fn clear_collection_incomplete(&self, collection_key: &str) -> Result<()> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "DELETE FROM incomplete_collections WHERE collection_key = ?1",
            params![collection_key],
        )
        .map_err(store_error)?;
        Ok(())
    }

    /// Whether a collection's cached contents are known to
    /// be partial.
    pub(crate) fn collection_incomplete(&self, collection_key: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM incomplete_collections WHERE collection_key = ?1",
                params![collection_key],
                |row| row.get(0),
            )
            .map_err(store_error)?;
        Ok(count > 0)
    }

    /// The store file's size in bytes, page-count based
    /// (`page_count * page_size`). This is the on-disk truth
    /// callers display or log; it includes pages freed by
    /// eviction until they are vacuumed or reused.
    pub(crate) fn size_bytes(&self) -> Result<u64> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let page_count: i64 = conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .map_err(store_error)?;
        let page_size: i64 = conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .map_err(store_error)?;
        Ok((page_count * page_size) as u64)
    }

    /// The live payload bytes: cached document bytes plus
    /// queued mutation bytes. This is what eviction can
    /// actually reduce, so it is the measure the GC loop
    /// steers by (unlike the file size, it drops the moment a
    /// document is evicted).
    pub(crate) fn live_bytes(&self) -> Result<u64> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let bytes: i64 = conn
            .query_row(
                "SELECT (SELECT COALESCE(SUM(length(proto)), 0) FROM remote_documents)
                      + (SELECT COALESCE(SUM(length(batch)), 0) FROM mutations)",
                [],
                |row| row.get(0),
            )
            .map_err(store_error)?;
        Ok(bytes as u64)
    }

    /// Eviction candidates in LRU order, as (name, payload
    /// bytes): documents belonging to no *active* target,
    /// least recently accessed first. Documents with no
    /// access row (written before access tracking, or whose
    /// bookkeeping was lost) sort as sequence 0 — the oldest
    /// — so inconsistent GC metadata degrades to evicting
    /// untracked documents first, never to a purge.
    // Retained for tests and diagnostics: the eviction
    // round itself re-derives its candidates inside its
    // transaction.
    #[allow(dead_code)]
    pub(crate) fn lru_candidates(&self, limit: usize) -> Result<Vec<(String, u64)>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut statement = conn
            .prepare(
                "SELECT rd.name, length(rd.proto) FROM remote_documents rd
                 LEFT JOIN document_access da ON da.name = rd.name
                 WHERE NOT EXISTS (
                     SELECT 1 FROM document_targets dt
                     JOIN active_targets at ON at.target_key = dt.target_key
                     WHERE dt.name = rd.name
                 )
                 ORDER BY COALESCE(da.seq, 0) ASC, rd.name ASC
                 LIMIT ?1",
            )
            .map_err(store_error)?;
        let rows = statement
            .query_map(params![limit as i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })
            .map_err(store_error)?;
        let mut candidates = Vec::new();
        for row in rows {
            candidates.push(row.map_err(store_error)?);
        }
        Ok(candidates)
    }

    /// One atomic eviction round: selects LRU candidates,
    /// revalidates their protection, and deletes them — all
    /// in a single transaction, so a mutation enqueued or a
    /// watch opened concurrently cannot slip a document past
    /// the checks (the candidate snapshot and the deletion
    /// never span separate lock acquisitions). Evicts until
    /// at least `free_at_least` payload bytes are freed (one
    /// oversized document can overshoot), capped at 100
    /// documents per round.
    ///
    /// Protection, evaluated inside the transaction:
    /// documents in an *active* target's membership, and
    /// documents any queued mutation touches. If the queue
    /// cannot be decoded, the round fails and nothing is
    /// evicted — protection that cannot be verified is
    /// treated as protection that applies.
    ///
    /// Side effects of evicting a document: its collection
    /// is marked incomplete (local-first queries must refill
    /// it from the server), and every target whose cached
    /// baseline contained the document loses its resume
    /// state — a resumed Listen would deliver only deltas
    /// against a baseline that is no longer there.
    pub(crate) fn evict_lru_batch(
        &self,
        documents_prefix: &str,
        free_at_least: u64,
    ) -> Result<EvictionOutcome> {
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let transaction = conn.transaction().map_err(store_error)?;

        // Queued mutations, decoded inside the transaction:
        // the protected set is exactly the queue this
        // transaction sees.
        let mut pending: std::collections::HashSet<String> = std::collections::HashSet::new();
        {
            let mut statement = transaction
                .prepare("SELECT batch FROM mutations ORDER BY id")
                .map_err(store_error)?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(store_error)?;
            for row in rows {
                let json = row.map_err(store_error)?;
                let mutations: Vec<Mutation> = serde_json::from_str(&json)
                    .map_err(|e| Error::Other(format!("mutation decode during GC: {e}")))?;
                for mutation in &mutations {
                    pending.insert(format!("{documents_prefix}/{}", mutation.path()));
                }
            }
        }

        let mut candidates: Vec<(String, u64)> = Vec::new();
        {
            let mut statement = transaction
                .prepare(
                    "SELECT rd.name, length(rd.proto) FROM remote_documents rd
                     LEFT JOIN document_access da ON da.name = rd.name
                     WHERE NOT EXISTS (
                         SELECT 1 FROM document_targets dt
                         JOIN active_targets at ON at.target_key = dt.target_key
                         WHERE dt.name = rd.name
                     )
                     ORDER BY COALESCE(da.seq, 0) ASC, rd.name ASC
                     LIMIT 512",
                )
                .map_err(store_error)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
                })
                .map_err(store_error)?;
            for row in rows {
                candidates.push(row.map_err(store_error)?);
            }
        }

        let mut chosen: Vec<(String, u64)> = Vec::new();
        let mut freed = 0u64;
        for (name, bytes) in candidates {
            if pending.contains(&name) {
                continue;
            }
            freed += bytes;
            chosen.push((name, bytes));
            if freed >= free_at_least || chosen.len() >= 100 {
                break;
            }
        }

        let mut invalidated_targets: Vec<i64> = Vec::new();
        for (name, _) in &chosen {
            // Targets whose baseline contains this document
            // lose their resume state (active targets are
            // protected above, so only closed ones can be
            // affected).
            {
                let mut statement = transaction
                    .prepare("SELECT target_key FROM document_targets WHERE name = ?1")
                    .map_err(store_error)?;
                let rows = statement
                    .query_map(params![name], |row| row.get::<_, i64>(0))
                    .map_err(store_error)?;
                for row in rows {
                    invalidated_targets.push(row.map_err(store_error)?);
                }
            }
            // The document's collection is now known-partial.
            if let Some(relative) = name
                .strip_prefix(documents_prefix)
                .and_then(|rest| rest.strip_prefix('/'))
            {
                if let Some((collection_key, _)) = relative.rsplit_once('/') {
                    transaction
                        .execute(
                            "INSERT INTO incomplete_collections (collection_key)
                             VALUES (?1) ON CONFLICT(collection_key) DO NOTHING",
                            params![collection_key],
                        )
                        .map_err(store_error)?;
                }
            }
            transaction
                .execute(
                    "DELETE FROM remote_documents WHERE name = ?1",
                    params![name],
                )
                .map_err(store_error)?;
            transaction
                .execute("DELETE FROM document_access WHERE name = ?1", params![name])
                .map_err(store_error)?;
            transaction
                .execute(
                    "DELETE FROM document_targets WHERE name = ?1",
                    params![name],
                )
                .map_err(store_error)?;
        }
        invalidated_targets.sort();
        invalidated_targets.dedup();
        for target_key in &invalidated_targets {
            transaction
                .execute(
                    "DELETE FROM targets WHERE target_id = ?1",
                    params![target_key],
                )
                .map_err(store_error)?;
        }
        transaction.commit().map_err(store_error)?;
        Ok(EvictionOutcome {
            names: chosen.into_iter().map(|(name, _)| name).collect(),
            freed_bytes: freed,
            invalidated_targets,
        })
    }

    /// Returns up to `pages` freed pages to the filesystem,
    /// when the database uses incremental auto-vacuum (new
    /// databases do; older ones silently keep their freelist
    /// for reuse). Best effort by design.
    pub(crate) fn incremental_vacuum(&self, pages: u32) {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let _ = conn.execute_batch(&format!("PRAGMA incremental_vacuum({pages})"));
    }
}

fn encode_document(document: &Document) -> Vec<u8> {
    let proto = crate::google::firestore::v1::Document {
        name: document.name.clone(),
        fields: crate::value::fields_to_proto(&document.fields),
        create_time: document.create_time.map(Into::into),
        update_time: document.update_time.map(Into::into),
    };
    proto.encode_to_vec()
}

fn decode_document(name: &str, bytes: &[u8]) -> Result<Document> {
    let proto = crate::google::firestore::v1::Document::decode(bytes)
        .map_err(|e| Error::Other(format!("stored document decode: {e}")))?;
    Document::from_proto(proto).map_err(|e| match e {
        Error::InvalidArgument(_) => Error::Other(format!("stored document {name} invalid: {e}")),
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn sample_document(name: &str) -> Document {
        let mut fields = crate::Fields::new();
        fields.insert("n".to_string(), Value::Integer(7));
        Document {
            name: name.to_string(),
            fields,
            create_time: Some(Timestamp::new(1, 0)),
            update_time: Some(Timestamp::new(2, 0)),
        }
    }

    fn set_batch(path: &str) -> Vec<Mutation> {
        let mut fields = crate::Fields::new();
        fields.insert("n".to_string(), Value::Integer(1));
        vec![Mutation::Set {
            path: path.to_string(),
            fields,
            merge_paths: None,
            exists: None,
            transforms: Vec::new(),
        }]
    }

    #[test]
    fn gc_bookkeeping_orders_candidates_and_protects_targets() {
        let store = LocalStore::open_in_memory().unwrap();
        let a = sample_document("projects/p/databases/d/documents/notes/a");
        let b = sample_document("projects/p/databases/d/documents/notes/b");
        let c = sample_document("projects/p/databases/d/documents/notes/c");
        store.put_remote_document(&a).unwrap();
        store.put_remote_document(&b).unwrap();
        store.put_remote_document(&c).unwrap();
        // All three were stamped on write; reading a re-stamps it.
        store.touch_access(std::slice::from_ref(&a.name)).unwrap();
        let names: Vec<String> = store
            .lru_candidates(10)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, vec![b.name.clone(), c.name.clone(), a.name.clone()]);
        // Membership alone does not protect: the target must be
        // *active* (a currently open watch).
        store
            .replace_target_membership(7, std::slice::from_ref(&b.name))
            .unwrap();
        assert_eq!(store.lru_candidates(10).unwrap().len(), 3);
        store.put_active_target(7).unwrap();
        let names: Vec<String> = store
            .lru_candidates(10)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, vec![c.name.clone(), a.name.clone()]);
        store.remove_active_target(7).unwrap();
        assert_eq!(store.lru_candidates(10).unwrap().len(), 3);
        // One atomic round frees at least one document's worth:
        // the LRU document (b) goes, and because b was target
        // 7's baseline, its resume state is invalidated and
        // the collection is marked incomplete. (Membership
        // syncing stamped b as accessed, so re-stamp a and c
        // to make b the LRU document again.)
        store.touch_access(std::slice::from_ref(&a.name)).unwrap();
        store.touch_access(std::slice::from_ref(&c.name)).unwrap();
        store
            .put_target_state(
                7,
                &TargetState {
                    resume_token: vec![9, 9],
                    snapshot_version: None,
                },
            )
            .unwrap();
        let live_before = store.live_bytes().unwrap();
        assert!(live_before > 0);
        assert!(store.size_bytes().unwrap() >= live_before);
        let outcome = store
            .evict_lru_batch("projects/p/databases/d/documents", 1)
            .unwrap();
        assert_eq!(outcome.names, vec![b.name.clone()]);
        assert_eq!(outcome.invalidated_targets, vec![7]);
        assert!(store.target_state(7).unwrap().is_none());
        assert!(store.collection_incomplete("notes").unwrap());
        assert_eq!(store.all_remote_documents().unwrap().len(), 2);
        assert!(store.live_bytes().unwrap() < live_before);
        store.clear_collection_incomplete("notes").unwrap();
        assert!(!store.collection_incomplete("notes").unwrap());
    }

    #[test]
    fn eviction_revalidates_pending_mutations_in_its_transaction() {
        let store = LocalStore::open_in_memory().unwrap();
        let a = sample_document("projects/p/databases/d/documents/notes/a");
        let b = sample_document("projects/p/databases/d/documents/notes/b");
        store.put_remote_document(&a).unwrap();
        store.put_remote_document(&b).unwrap();
        // a is the LRU document, but a queued mutation touches
        // it: the round must skip it and take b instead.
        store.enqueue_batch(&set_batch("notes/a")).unwrap();
        let outcome = store
            .evict_lru_batch("projects/p/databases/d/documents", u64::MAX / 2)
            .unwrap();
        assert_eq!(outcome.names, vec![b.name.clone()]);
        assert!(store.remote_document(&a.name).unwrap().is_some());
        assert_eq!(store.batch_count().unwrap(), 1, "queue untouched");
    }

    #[test]
    fn legacy_resume_tokens_are_invalidated_on_first_open() {
        // A store written before GC bookkeeping existed:
        // a resume token with no recorded baseline, and
        // user_version still 0.
        let dir = std::env::temp_dir().join(format!(
            "firebase-native-legacy-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("legacy.db");
        let _ = std::fs::remove_file(&path);
        {
            let raw = rusqlite::Connection::open(&path).unwrap();
            raw.execute_batch(
                "CREATE TABLE remote_documents (
                     name TEXT PRIMARY KEY,
                     proto BLOB NOT NULL
                 );
                 CREATE TABLE targets (
                     target_id INTEGER PRIMARY KEY,
                     resume_token BLOB NOT NULL,
                     snapshot_seconds INTEGER,
                     snapshot_nanos INTEGER
                 );
                 INSERT INTO targets (target_id, resume_token)
                 VALUES (7, X'0102');",
            )
            .unwrap();
        }
        let store = LocalStore::open(&path).unwrap();
        assert!(
            store.target_state(7).unwrap().is_none(),
            "a resume token whose baseline was never recorded must not survive the upgrade"
        );
        // The migration is one-time: tokens written now
        // survive a reopen.
        store
            .put_target_state(
                7,
                &TargetState {
                    resume_token: vec![3],
                    snapshot_version: None,
                },
            )
            .unwrap();
        drop(store);
        let store = LocalStore::open(&path).unwrap();
        assert!(store.target_state(7).unwrap().is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn documents_round_trip() {
        let store = LocalStore::open_in_memory().unwrap();
        let document = sample_document("projects/p/databases/d/documents/notes/a");
        store.put_remote_document(&document).unwrap();
        assert_eq!(
            store.remote_document(&document.name).unwrap(),
            Some(document)
        );
        assert_eq!(store.all_remote_documents().unwrap().len(), 1);
        store
            .remove_remote_document("projects/p/databases/d/documents/notes/a")
            .unwrap();
        assert_eq!(
            store
                .remote_document("projects/p/databases/d/documents/notes/a")
                .unwrap(),
            None
        );
    }

    #[test]
    fn queue_round_trips_non_finite_doubles() {
        // JSON cannot represent NaN or the infinities: the queue
        // encoding must carry them explicitly, or the head batch
        // becomes undecodable and wedges every later write.
        let store = LocalStore::open_in_memory().unwrap();
        let mut fields = crate::Fields::new();
        fields.insert("nan".to_string(), Value::Double(f64::NAN));
        fields.insert("pos".to_string(), Value::Double(f64::INFINITY));
        fields.insert("neg".to_string(), Value::Double(f64::NEG_INFINITY));
        fields.insert("finite".to_string(), Value::Double(1.25));
        fields.insert(
            "geo".to_string(),
            Value::GeoPoint(crate::value::GeoPoint {
                latitude: 35.0,
                longitude: 139.0,
            }),
        );
        store
            .enqueue_batch(&[Mutation::Set {
                path: "notes/a".to_string(),
                fields,
                merge_paths: None,
                exists: None,
                transforms: Vec::new(),
            }])
            .unwrap();
        let batches = store.peek_batches(10).unwrap();
        assert_eq!(batches.len(), 1);
        let Mutation::Set { fields, .. } = &batches[0].mutations[0] else {
            panic!("expected a set mutation");
        };
        let bits = |name: &str| match fields.get(name) {
            Some(Value::Double(value)) => value.to_bits(),
            other => panic!("{name}: unexpected {other:?}"),
        };
        assert_eq!(bits("nan"), f64::NAN.to_bits());
        assert_eq!(bits("pos"), f64::INFINITY.to_bits());
        assert_eq!(bits("neg"), f64::NEG_INFINITY.to_bits());
        assert_eq!(bits("finite"), 1.25f64.to_bits());
        assert_eq!(
            fields.get("geo"),
            Some(&Value::GeoPoint(crate::value::GeoPoint {
                latitude: 35.0,
                longitude: 139.0,
            }))
        );
    }

    #[test]
    fn queue_is_fifo_and_survives_reopen() {
        let dir =
            std::env::temp_dir().join(format!("firebase-native-store-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db");
        let _ = std::fs::remove_file(&path);
        {
            let store = LocalStore::open(&path).unwrap();
            let first = store.enqueue_batch(&set_batch("notes/a")).unwrap();
            let second = store.enqueue_batch(&set_batch("notes/b")).unwrap();
            assert!(second > first);
            store
                .put_target_state(
                    7,
                    &TargetState {
                        resume_token: vec![1, 2, 3],
                        snapshot_version: Some(Timestamp::new(9, 5)),
                    },
                )
                .unwrap();
        }
        // Reopen: queue order and target state persisted.
        let store = LocalStore::open(&path).unwrap();
        let batches = store.peek_batches(10).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].mutations[0].path(), "notes/a");
        assert_eq!(batches[1].mutations[0].path(), "notes/b");
        assert!(store.has_pending_mutation_for("notes/b").unwrap());
        assert!(!store.has_pending_mutation_for("notes/c").unwrap());
        let state = store.target_state(7).unwrap().unwrap();
        assert_eq!(state.resume_token, vec![1, 2, 3]);
        assert_eq!(state.snapshot_version, Some(Timestamp::new(9, 5)));
        store.remove_batch(batches[0].id).unwrap();
        assert_eq!(store.batch_count().unwrap(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
