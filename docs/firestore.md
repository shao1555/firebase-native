# Firestore

`firebase-native-firestore` talks to Cloud Firestore over
gRPC. The public API is a hand-written model — `Value`,
`Document`, `Query` — and never exposes the generated
protobuf types.

## Documents and values

Paths are slash-separated resource paths relative to the
database root: `cities/SF`, `cities/SF/landmarks/coit-tower`
(even segment counts are documents, odd counts are
collections).

```rust
pub struct Document {
    pub name: String,                        // full resource name
    pub fields: Fields,                      // BTreeMap<String, Value>
    pub create_time: Option<Timestamp>,
    pub update_time: Option<Timestamp>,
}
```

`Value` mirrors Firestore's value model: `Null`, `Bool`,
`Integer(i64)`, `Double(f64)`, `String`, `Bytes`, `Timestamp`,
`GeoPoint`, `Reference(String)`, `Array(Vec<Value>)`,
`Map(BTreeMap<String, Value>)`, and `Decimal128(String)` —
a 128-bit decimal held in canonical string form, which
arises from increment transform operands and server
transform results (on the wire it travels as its `Double`
approximation). `Value::compare` gives the same total order the
server uses (type order first, then value), which local
query evaluation and the spec tests rely on.

Unary operations on `Firestore`:

| Method | Behavior |
|---|---|
| `get_document(path)` | `Ok(None)` when the document does not exist. |
| `create_document(collection_path, document_id, fields)` | `document_id: Option<&str>`; a random ID is generated when `None`. |
| `set_document(path, fields)` | Full overwrite (creates or replaces). |
| `update_document(path, fields, field_mask)` | Patches only the listed field paths (`field_mask: &[String]`). |
| `delete_document(path)` | |
| `list_documents(parent, collection_id, page_size, page_token)` | One page plus a next-page token (`ListDocumentsResult`). |

Field contents are passed as `Fields`
(`BTreeMap<String, Value>`). `set_document` and
`update_document` return the document as the server holds it
after the write (online) or as locally composed (with a
local store, below).

### Server-computed values: `FieldValue`

`set_document_with` / `update_document_with` accept
`FieldValues` (`BTreeMap<String, FieldValue>`), where each
entry is a literal `FieldValue::Value(..)` or a sentinel:

- `FieldValue::ServerTimestamp`
- `FieldValue::Delete`
- `FieldValue::Increment(Value)` — numeric add
- `FieldValue::ArrayUnion(Vec<Value>)` /
  `FieldValue::ArrayRemove(Vec<Value>)`

`set_document_with` additionally takes a `merge` flag
(merge the given fields into the document instead of
replacing it wholesale). Keys are field paths, so a nested field is addressed as
`"stats.count"` rather than by nesting a sentinel inside a
`Value::Map` (matching the official SDKs). Online, sentinel
writes go through a unary `Commit` and read the result back.
In a local store, increments freeze an estimate at enqueue
time so latency-compensated views stay stable until the
server's transform result arrives.

## Queries

```rust
let query = Query::new("cities")
    .filter(Filter::eq("capital", Value::Bool(true)))
    .filter(Filter::gt("population", Value::Integer(1_000_000)))
    .order_by(Order::asc("name"))
    .limit(10);
let documents = firestore.run_query("", query).await?;
```

`run_query(parent_path, query)` runs the query's collection
under the given parent (`""` for the database root). `Filter` covers the unary, comparison and
composite (`and`/`or`) operators of the structured-query
model; `UnaryOp`/`FieldOp` enumerate them.

Query *evaluation* is a local operation too: `Query::evaluate`
runs the same filters, ordering and limits over in-memory
candidates with server-identical semantics (including the
total order above). That is what makes local-first reads
below possible.

## Watches and snapshot metadata

```rust
let mut listener = firestore.watch_query("", query).await?;
// or: firestore.watch_document("cities/SF").await?
while let Some(snapshot) = listener.next().await {
    let snapshot = snapshot?;
    for change in &snapshot.changes {
        // change.kind: Added / Modified / Removed
        // change.document, change.old_index, change.new_index
    }
}
```

A watch delivers an initial snapshot, then a new snapshot
at every consistent point where the view changed. The
stream reconnects with the last resume token and
exponential backoff; terminal failures arrive as one final
`Err` item. Documents that leave a query view for unknown
reasons are resolved through the production **limbo**
mechanism (per-document existence probes), as in the
official SDKs.

Every `Snapshot` carries `metadata: SnapshotMetadata`:

- `is_from_cache` — the snapshot was composed from the
  local store (or, online, precedes the watch's first
  server-confirmed state). The first snapshot with
  `is_from_cache == false` is the watch's initial-sync
  signal.
- `has_pending_writes` — some document in the view has
  mutations not yet acknowledged by the server.

Snapshots are also delivered when *only* the metadata
changes — the cache→server flip, or the pending flag
clearing when the queue drains — exactly like the native
SDKs.

## Offline persistence

```rust
let firestore = Firestore::new(&app, auth.clone())
    .enable_local_store(Path::new("path/to/firestore.db"))?;
```

`enable_local_store` opens (creating if needed) a SQLite
store and switches the client to persistent mode:

- **Writes** are appended to a durable FIFO mutation queue
  and reflected in the local view immediately (**latency
  compensation**); a background Write stream commits them
  in order, surviving restarts. Permanently rejected
  mutations (permission denied, failed precondition, …) are
  dropped from the queue and their optimistic effect is
  rolled back; transient failures retry with the stream.
  `wait_for_pending_writes()` waits until the queue is
  empty - including writes enqueued while it waits - and
  reports a permanent rejection remembered along the way
  as an error once the queue has drained.
- **Reads** (`get_document`, `run_query`) are served from
  the composed local view first; the server is consulted
  only when the local view cannot answer (and transient
  server errors fall back to the cache).
- **Watches** seed from the store, restore their persisted
  resume token, deliver a cache snapshot immediately, and
  land remote changes in the store at accepted consistent
  points.

Without `enable_local_store` the client is online-only and
stateless.

## Per-user stores

On a device shared by several users, give each user their
own store directory so caches, queues and resume tokens
never mix:

```rust
use firebase_native::firestore::{user_store_path, wipe_user_store};

let path = user_store_path(root, uid);   // <root>/<sha-256 hex of uid>/firestore.db
let firestore = Firestore::new(&app, auth.clone()).enable_local_store(&path)?;
```

The directory name is the lowercase hex of the uid's
SHA-256 digest: always 64 characters (within filesystem
component limits for maximum-length uids), a single safe
path component for any input string, and distinct for
case-distinct uids even on case-insensitive filesystems.

`wipe_user_store(root, uid)` removes one user's directory;
`wipe_all_user_stores(root)` removes the whole root. Both
are the store side of sign-out / the kill switch (see
[Auth and sessions](auth-and-sessions.md#the-kill-switch)).
**Current limitation: engines cannot be stopped.** Once a
persistent client has started its write pump, the engine —
and its open SQLite store — lives for the rest of the
process: there is no shutdown API yet, and dropping the
`Firestore` client does not release it, because the pump
task holds its own reference to the engine. Wiping a
store's files is therefore only safe when **no process**
has the store open: the wipe has no cross-process
coordination, so running it from a separate helper process
does *not* help while the owning application is still
alive — its pump keeps the SQLite connection and pending
mutations and goes on attempting writes, and where open
files are locked, the deletion fails in any process. In
practice: wipe before the owning application opens the
store (for example at startup, before sign-in completes),
or after the owning application has exited.

## Named databases

Set the database on `FirebaseOptions::with_database_id`;
the client sends the required routing headers
(`x-goog-request-params`, and
`google-cloud-resource-prefix` on the streaming RPCs)
automatically.
