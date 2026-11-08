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
            "CREATE TABLE IF NOT EXISTS remote_documents (
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
             );",
        )
        .map_err(store_error)?;
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
        transaction
            .execute("DELETE FROM mutations WHERE id = ?1", params![batch_id])
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

    /// Removes an acknowledged (or server-rejected) batch.
    pub(crate) fn remove_batch(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute("DELETE FROM mutations WHERE id = ?1", params![id])
            .map_err(store_error)?;
        Ok(())
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
