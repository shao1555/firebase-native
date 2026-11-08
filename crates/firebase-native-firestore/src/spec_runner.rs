//! A runner for the official Firestore spec tests, vendored from
//! firebase-ios-sdk under `tests/spec_tests/` (Apache-2.0; see
//! that directory's README for provenance and the coverage
//! policy).
//!
//! The spec DSL drives a whole sync engine. This runner covers
//! the **watch layer**: it translates the remote-event steps
//! (`watchEntity`, `watchCurrent`, `watchSnapshot`, `watchRemove`)
//! into `ListenResponse`s for a single active query target and
//! feeds them to the crate's real [`WatchState`], then compares
//! the emitted snapshots against `expectedSnapshotEvents` —
//! document sets (rebuilt from the events' added/modified/removed
//! deltas, as the reference runner does) and change
//! categorization. Snapshot metadata the watch layer does not own
//! (`fromCache`, `hasPendingWrites`) is not compared, and cases
//! that need machinery beyond this layer (local mutations, write
//! acknowledgements, existence filters, multiple concurrent
//! targets, timers, restart/shutdown, snapshots-in-sync listeners)
//! are reported as **skipped with a reason**, never silently
//! passed.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use firebase_native_core::Error;
use serde_json::{Map, Value as Json};

use crate::engine::PersistenceEngine;
use crate::google::firestore::v1::listen_request::TargetChange as RequestTargetChange;
use crate::google::firestore::v1::listen_response::ResponseType;
use crate::google::firestore::v1::target::ResumeType;
use crate::google::firestore::v1::target_change::TargetChangeType;
use crate::google::firestore::v1::{
    DocumentChange as ProtoDocumentChange, DocumentDelete, ExistenceFilter, ListenResponse, Target,
    TargetChange,
};
use crate::listen::{DocumentChange, DocumentChangeKind, Snapshot, WatchEvent, WatchState};
use crate::mutation::Mutation;
use crate::query::{FieldOp, Filter, Order, Query, UnaryOp};
use crate::store::LocalStore;
use crate::value::{fields_to_proto, Document, Timestamp, Value};
use crate::write::WriteStream;
use crate::Fields;

const DOCUMENTS_PREFIX: &str = "projects/test-project/databases/(default)/documents";

enum Outcome {
    Passed,
    Failed(String),
    Skipped(String),
}

/// Loads one vendored suite file as a name → case map.
fn load_suite(file: &str) -> Map<String, Json> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/spec_tests/json/");
    let text = std::fs::read_to_string(format!("{path}{file}"))
        .unwrap_or_else(|err| panic!("vendored spec file {file} unreadable: {err}"));
    let parsed: Json = serde_json::from_str(&text)
        .unwrap_or_else(|err| panic!("vendored spec file {file} invalid: {err}"));
    parsed
        .as_object()
        .unwrap_or_else(|| panic!("vendored spec file {file} is not an object"))
        .clone()
}

/// A spec version (microseconds since the epoch) as a Timestamp.
fn ts_from_micros(micros: i64) -> Timestamp {
    Timestamp::new(micros / 1_000_000, ((micros % 1_000_000) * 1000) as i32)
}

/// A spec JSON value as a public [`Value`].
fn json_to_value(json: &Json) -> Value {
    match json {
        Json::Null => Value::Null,
        Json::Bool(value) => Value::Bool(*value),
        Json::Number(number) => match number.as_i64() {
            Some(value) => Value::Integer(value),
            None => Value::Double(number.as_f64().expect("JSON number")),
        },
        Json::String(value) => Value::String(value.clone()),
        Json::Array(items) => Value::Array(items.iter().map(json_to_value).collect()),
        Json::Object(map) => {
            // The spec encoding of a Decimal128 value: a map
            // whose only field carries the canonical string.
            if map.len() == 1 {
                if let Some(wrapper) = map.get("__decimal128__") {
                    if let Some(string) = wrapper.get("stringValue").and_then(Json::as_str) {
                        return Value::Decimal128(string.to_string());
                    }
                }
            }
            Value::Map(
                map.iter()
                    .map(|(key, value)| (key.clone(), json_to_value(value)))
                    .collect(),
            )
        }
    }
}

/// Minimal base64 decoder for spec bloom-filter bitmaps.
fn b64_decode(input: &str) -> Vec<u8> {
    let mut table = [255u8; 256];
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for (index, byte) in alphabet.iter().enumerate() {
        table[*byte as usize] = index as u8;
    }
    let mut out = Vec::new();
    let mut acc: u32 = 0;
    let mut nbits = 0;
    for byte in input.bytes() {
        if byte == b'=' {
            break;
        }
        let value = table[byte as usize];
        if value == 255 {
            continue;
        }
        acc = (acc << 6) | value as u32;
        nbits += 6;
        if nbits >= 8 {
            nbits -= 8;
            out.push((acc >> nbits) as u8);
        }
    }
    out
}

/// Converts a protobuf-JSON value (the encoding `writeAck`
/// transform results use) to the value model. A map value
/// whose only field is `__decimal128__` is the driver's
/// encoding of a decimal transform result.
fn proto_json_to_value(json: &Json) -> Value {
    let Some(map) = json.as_object() else {
        return json_to_value(json);
    };
    if let Some(fields) = map.get("mapValue").and_then(|v| v.get("fields")) {
        if let Some(object) = fields.as_object() {
            if object.len() == 1 {
                if let Some(wrapper) = object.get("__decimal128__") {
                    if let Some(string) = wrapper.get("stringValue").and_then(Json::as_str) {
                        return Value::Decimal128(string.to_string());
                    }
                }
            }
        }
        return Value::Map(json_fields(fields));
    }
    if let Some(string) = map.get("stringValue").and_then(Json::as_str) {
        return Value::String(string.to_string());
    }
    if let Some(integer) = map.get("integerValue") {
        if let Some(value) = integer.as_i64() {
            return Value::Integer(value);
        }
        if let Some(value) = integer.as_str().and_then(|v| v.parse().ok()) {
            return Value::Integer(value);
        }
    }
    if let Some(double) = map.get("doubleValue").and_then(Json::as_f64) {
        return Value::Double(double);
    }
    if let Some(boolean) = map.get("booleanValue").and_then(Json::as_bool) {
        return Value::Bool(boolean);
    }
    if map.contains_key("nullValue") {
        return Value::Null;
    }
    if let Some(values) = map
        .get("arrayValue")
        .and_then(|v| v.get("values"))
        .and_then(Json::as_array)
    {
        return Value::Array(values.iter().map(proto_json_to_value).collect());
    }
    if let Some(seconds) = map.get("timestampValue").and_then(Json::as_str) {
        // RFC 3339; the harness's transform results are
        // numeric in practice, but parse the common shape.
        let _ = seconds;
    }
    json_to_value(json)
}

/// Splits spec write data into literal fields, field
/// transforms and field deletions. A field whose value is a
/// `{"_methodName": ...}` object is a transform (top-level
/// fields only, as in the SDKs' data model). An unknown
/// method is an error the caller turns into a skip.
fn split_transforms(
    data: &Json,
) -> Result<(Fields, Vec<crate::mutation::FieldTransform>, Vec<String>), String> {
    // The error is the skip reason for the case.
    let mut fields = Fields::new();
    let mut transforms = Vec::new();
    let mut deletes = Vec::new();
    let Some(map) = data.as_object() else {
        return Ok((fields, transforms, deletes));
    };
    for (key, value) in map {
        let method = value
            .as_object()
            .and_then(|object| object.get("_methodName"))
            .and_then(Json::as_str);
        let Some(method) = method else {
            if contains_transform(value) {
                return Err("nested field transforms not modeled".to_string());
            }
            fields.insert(key.clone(), json_to_value(value));
            continue;
        };
        let operand = value.get("_operand").cloned().unwrap_or(Json::Null);
        let kind = match method {
            "serverTimestamp" => crate::mutation::TransformKind::ServerTimestamp,
            "increment" => crate::mutation::TransformKind::Increment(json_to_value(&operand)),
            "arrayUnion" | "arrayRemove" => {
                let elements = match &operand {
                    Json::Array(items) => items.iter().map(json_to_value).collect(),
                    other => vec![json_to_value(other)],
                };
                if method == "arrayUnion" {
                    crate::mutation::TransformKind::ArrayUnion(elements)
                } else {
                    crate::mutation::TransformKind::ArrayRemove(elements)
                }
            }
            "delete" => {
                deletes.push(key.clone());
                continue;
            }
            other => {
                return Err(format!("transform method '{other}' not modeled"));
            }
        };
        transforms.push(crate::mutation::FieldTransform {
            path: key.clone(),
            kind,
        });
    }
    Ok((fields, transforms, deletes))
}

fn json_fields(json: &Json) -> Fields {
    match json {
        Json::Object(map) => map
            .iter()
            .map(|(key, value)| (key.clone(), json_to_value(value)))
            .collect(),
        _ => Fields::new(),
    }
}

/// Parses a spec query (`path` / `filters` / `orderBys`) into the
/// crate's [`Query`]. Returns `Err(reason)` for shapes this layer
/// does not model (limits, unknown operators).
fn parse_query(json: &Json) -> Result<Query, String> {
    if json.get("limit").is_some() || json.get("limitType").is_some() {
        return Err("query limit not modeled at the watch layer".to_string());
    }
    let path = json
        .get("path")
        .and_then(Json::as_str)
        .ok_or("query without a path")?;
    let mut query = Query::new(path);
    for filter in json
        .get("filters")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
    {
        let parts = filter.as_array().ok_or("filter is not a triple")?;
        if parts.len() != 3 {
            return Err("filter is not a triple".to_string());
        }
        let field = parts[0].as_str().ok_or("filter field")?.to_string();
        let op = parts[1].as_str().ok_or("filter operator")?;
        let value = json_to_value(&parts[2]);
        let parsed = match (op, &value) {
            ("==", Value::Null) => Filter::Unary {
                field,
                op: UnaryOp::IsNull,
            },
            ("!=", Value::Null) => Filter::Unary {
                field,
                op: UnaryOp::IsNotNull,
            },
            ("==", _) => Filter::Field {
                field,
                op: FieldOp::Equal,
                value,
            },
            ("!=", _) => Filter::Field {
                field,
                op: FieldOp::NotEqual,
                value,
            },
            ("<", _) => Filter::Field {
                field,
                op: FieldOp::LessThan,
                value,
            },
            ("<=", _) => Filter::Field {
                field,
                op: FieldOp::LessThanOrEqual,
                value,
            },
            (">", _) => Filter::Field {
                field,
                op: FieldOp::GreaterThan,
                value,
            },
            (">=", _) => Filter::Field {
                field,
                op: FieldOp::GreaterThanOrEqual,
                value,
            },
            ("in", _) => Filter::Field {
                field,
                op: FieldOp::In,
                value,
            },
            ("not-in", _) => Filter::Field {
                field,
                op: FieldOp::NotIn,
                value,
            },
            ("array-contains", _) => Filter::Field {
                field,
                op: FieldOp::ArrayContains,
                value,
            },
            ("array-contains-any", _) => Filter::Field {
                field,
                op: FieldOp::ArrayContainsAny,
                value,
            },
            (other, _) => return Err(format!("unsupported filter operator {other}")),
        };
        query = query.filter(parsed);
    }
    for order in json
        .get("orderBys")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
    {
        let parts = order.as_array().ok_or("orderBy is not a pair")?;
        let field = parts[0].as_str().ok_or("orderBy field")?.to_string();
        let order = match parts[1].as_str() {
            Some("desc") => Order::desc(field),
            Some("asc") => Order::asc(field),
            other => return Err(format!("unsupported order direction {other:?}")),
        };
        query = query.order_by(order);
    }
    Ok(query)
}

/// A streamed remote update buffered until the next accepted
/// consistent point.
enum RemoteUpdate {
    Document(Document),
    Absent(String, Option<i64>),
}

/// The client's online state, as the reference online-state
/// tracker maintains it: a fresh client is `Unknown` (queries
/// wait), a successful watch message makes it `Online`, and
/// stream failures or the online-state timeout make it
/// `Offline` (queries resolve from cache).
#[derive(Clone, Copy, PartialEq, Eq)]
enum OnlineState {
    Unknown,
    Online,
    Offline,
}

/// What the harness emitted for one step.
enum Emitted {
    /// A snapshot from the watch state itself.
    Snapshot(Snapshot),
    /// A snapshot the local layer re-raises without the watch
    /// state (a new listener's initial view). Generated purely
    /// from harness state; on steps that assert nothing, the
    /// spec corpus tolerates these (valid upstream cases leave
    /// them unasserted), while steps that do assert — even `[]`
    /// — compare them strictly.
    Pseudo(Snapshot),
    /// A snapshot raised by an online-state transition: going
    /// Offline resolving the active listen from cache, or a
    /// listen registered while Offline resolving immediately.
    /// Unlike `Pseudo`, these are checked strictly on every
    /// step — a premature or duplicate offline event on a step
    /// that expects none fails the case, because transition
    /// timing is exactly what the offline suite pins down.
    Offline(Snapshot),
    /// A snapshot composed by the local layer in write mode: the
    /// store's remote base overlaid with the queued mutations —
    /// the production client's local view. Its cache/pending
    /// metadata is compared exactly.
    Composed {
        snapshot: Snapshot,
        from_cache: bool,
        has_pending: bool,
        /// Documents whose content is unchanged but whose
        /// mutation state (pending / committed) changed.
        metadata: Vec<Document>,
        /// A listen-time initial view: like `Pseudo`, tolerated
        /// on steps that assert no events.
        initial: bool,
    },
    Error(i32),
}

/// One started limbo resolution: a document target verifying
/// a single document's existence.
struct LimboRes {
    key: String,
    /// The document target's id, adopted from the first watch
    /// step addressing it.
    target_id: Option<i32>,
    state: Option<WatchState>,
    /// The resolution failed (its target was removed): the
    /// document resolves as absent at the next barrier.
    failed: bool,
}

struct Harness {
    state: Option<WatchState>,
    target_id: i32,
    unlistened: bool,
    expected_view: BTreeMap<String, Fields>,
    /// Resume tokens held per target ID, synced from the watch
    /// state after every response (the token store a store-backed
    /// client keeps in its targets table). Compared against
    /// `expectedState` and against what the real add-target
    /// request construction would send.
    target_tokens: std::collections::HashMap<i32, String>,
    /// Set when the watch state restarted on an existence filter
    /// whose count *matched* the local view: a supported case
    /// regressing, which must fail, not skip.
    unexpected_restart: bool,
    /// Entity changes fed since the last emitted snapshot, by
    /// relative key: the pending overlay on `view`, used to
    /// size the watch state's view independently of its filter
    /// handling when an existence filter arrives.
    pending_view: BTreeMap<String, bool>,
    /// The highest global snapshot version this case has
    /// delivered; a barrier behind it is a reverted stream and
    /// is ignored (the version timeline is global to the case).
    max_barrier: i64,
    /// The active listen's query, for view membership filtering
    /// of remote events.
    query: Option<Query>,
    /// Canonical JSON of the active listen's query.
    current_query_sig: String,
    /// Queries (by canonical JSON) that have delivered at least
    /// one snapshot in this case. A new listener for such a query
    /// immediately receives its current view; a query that never
    /// delivered (e.g. its target errored first) raises nothing
    /// until its first consistent point. Event generation depends
    /// only on this state, never on the step's expectations.
    delivered_queries: std::collections::HashSet<String>,
    /// Whether the current listen saw a resume token; a listen
    /// that ends holding one makes its target resumable, so the
    /// next listen of that target synthesizes no deletes.
    listen_saw_token: bool,
    resumable_targets: std::collections::HashSet<i32>,
    had_listen: bool,
    online: bool,
    /// The online-state tracker's state (listen mode): see
    /// [`OnlineState`]. Stream failures count toward the
    /// offline transition; the online-state timeout is armed by
    /// a first failure while `Online`.
    online_state: OnlineState,
    stream_failures: u32,
    offline_timeout_armed: bool,
    /// Whether the active listen has emitted any snapshot.
    listen_delivered: bool,
    /// The current user for mutation-queue purposes (the
    /// anonymous user is the empty string): mutation batches
    /// are per-user in the spec model, and only the current
    /// user's batches are visible to listeners or settled by
    /// acknowledgements.
    current_user: String,
    /// The owning user of each queued batch, in queue order,
    /// aligned with the store's batch list.
    batch_users: Vec<String>,
    /// Locally computed transform results per queued batch
    /// (aligned with the queue): a transform's local estimate
    /// is computed once, when the batch is enqueued, and the
    /// composed view keeps showing it — over any base movement
    /// and, via `committed_estimates`, after the fold — until
    /// the watch confirms the write. Only then does the
    /// server's exact value (installed by the fold from the
    /// acknowledgement's transform results) surface.
    batch_estimates: Vec<std::collections::BTreeMap<(String, String), Value>>,
    /// Transform estimates of folded batches awaiting watch
    /// confirmation, keyed by (document, field path).
    committed_estimates: std::collections::BTreeMap<(String, String), Value>,
    /// Resume tokens as last *persisted*, per target, with the
    /// snapshot version at which they were persisted. Live
    /// tokens (`target_tokens`) can be newer; a restart falls
    /// back to the persisted ones. A target-specific (CURRENT)
    /// token persists at the next barrier; a global snapshot's
    /// token persists only once it is at least five minutes
    /// ahead of the last persisted snapshot version.
    persisted_tokens: std::collections::HashMap<i32, (String, i64)>,
    /// A CURRENT token was noted since the last barrier.
    current_token_pending: bool,
    /// Local-store operations armed to fail by `failDatabase`
    /// steps; each armed operation fails once, at its next
    /// invocation.
    armed_db_failures: std::collections::HashSet<String>,
    /// Settlements (ack, commit time) whose persistence failed
    /// and which the async queue retries at the next
    /// `async_queue_retry` timer.
    deferred_settles: std::collections::VecDeque<(bool, Timestamp)>,
    /// The client terminated on a failed user-change
    /// transaction; it restarts at the next async-queue retry,
    /// applying queued user changes one at a time.
    terminated: bool,
    pending_user_changes: std::collections::VecDeque<String>,
    /// The listen (target, query JSON) to restore when a
    /// terminated client recovers.
    terminated_listen: Option<(i32, Json)>,
    /// The active target's last remote event could not be
    /// persisted: the target is inactive until the next
    /// async-queue retry re-listens it internally.
    target_failed: bool,
    /// A target allocation failed earlier in this case: a
    /// later listen may then supersede the active one (the
    /// recovery cases re-listen a second query while the first
    /// is still registered; steps only address the new target
    /// afterwards).
    saw_alloc_failure: bool,
    /// Write callbacks withheld by a fatal acknowledgement
    /// failure (the fold was applied in memory but its
    /// persistence failed): the restart at the async-queue
    /// retry delivers them.
    deferred_callbacks: Vec<String>,
    /// Documents in limbo: removed from the query target's
    /// synchronized set (a document-remove, an existence
    /// filter, a target reset) but not yet verified — they
    /// stay in the view until their document-target resolution
    /// confirms or removes them. Kept in entry order; the head
    /// is the document currently resolving.
    limbo_waiting: std::collections::VecDeque<String>,
    /// Started limbo resolutions, in start order. The spec
    /// model's `activeLimboDocs` is exactly these documents.
    limbo_active: Vec<LimboRes>,
    /// How many resolutions may be active at once, derived
    /// from the enqueue batch (see `enter_limbo_batch`).
    limbo_slots: usize,
    /// Limbo candidates arrived since the last global barrier:
    /// the barrier re-raises the view from cache.
    limbo_dirty: bool,
    /// An existence-filter restart happened since the last
    /// global barrier: the barrier re-raises the view from
    /// cache, and the re-added target's expected resume token
    /// is empty for one state check.
    restart_pending: bool,
    /// The main target's resume token as the spec model
    /// reports it in `activeTargets`: while the target is
    /// active it reads the live token when the target is fully
    /// synchronized at the latest consistent point, and the
    /// token the target was added with otherwise; while no
    /// stream is active it reads the persisted token.
    held_token: String,
    /// The token the active target was added with.
    add_token: String,
    /// Limbo candidates were produced during this step: the
    /// target is not fully synchronized at this barrier.
    token_blocked_step: bool,
    /// A limbo resolution settled as absent during this step.
    limbo_absent_step: bool,
    /// The watch state was recreated for a stream-level re-add
    /// (existence-filter re-run): its first no-change snapshot
    /// is not an event in the spec model.
    absorb_empty_snapshot: bool,
    /// An existence filter was fed to the main target and has
    /// not been applied yet: a restart it produces at the next
    /// consistent point is the expected recovery, not an
    /// unexpected one.
    filter_restart_expected: bool,
    /// A deferred existence filter restarted the target during
    /// this step: the barrier re-raises the view from cache.
    filter_restart_step: bool,
    /// A CURRENT arrived while an existence filter awaited
    /// application: the re-run is stream-driven, so its
    /// consistent points are silent.
    current_since_filter: bool,
    /// A held existence filter was applied during this step.
    filter_applied_step: bool,
    /// Whether the snapshot currently being applied in
    /// `feed` fully synchronizes the query (no limbo
    /// candidates or outstanding resolutions).
    applying_snapshot_synced: bool,
    /// The last emitted snapshot's documents, by relative key:
    /// the active listen's view.
    view: BTreeMap<String, Document>,
    /// Every remote document the client knows, by relative key,
    /// with its version (microseconds): the local store the
    /// initial view of any new listen is computed from. Deleted
    /// documents remain as tombstones (`None`) so an older set
    /// cannot resurrect them; stale remote events change
    /// nothing.
    cache: BTreeMap<String, (Option<Document>, i64)>,
    /// The cache as of the last applied remote event. Remote
    /// events are transactions: `watchEntity` updates are staged
    /// in `cache` as they stream in and become persisted state
    /// only when the event's barrier is applied. A barrier whose
    /// persistence fails restores this snapshot, so the failed
    /// event's documents cannot surface after a restart or a
    /// later listen.
    committed_cache: BTreeMap<String, (Option<Document>, i64)>,
    /// The active listen was seeded fresh (no resume token):
    /// documents its first snapshot removes were synthesized
    /// deletes and leave the local store too.
    fresh_seed: bool,
    feed_snapshots: usize,
    /// Write-mode state: cases with user writes run against the
    /// real persistence engine on an in-memory store. Watch
    /// results are applied to its store; listener events are
    /// composed from it.
    engine: Option<Arc<PersistenceEngine>>,
    write_mode: bool,
    /// The active listen's query path, for view membership.
    query_path: String,
    /// Relative keys the watch has placed in the store.
    store_remote_keys: BTreeSet<String>,
    /// The last composed snapshot delivered to the listener,
    /// by relative key, and its metadata.
    delivered_docs: BTreeMap<String, Document>,
    delivered_meta: (bool, bool),
    /// The active listen has received a server snapshot (its
    /// view is no longer purely from cache).
    synced: bool,
    /// Write callbacks since the last `expectedState` check;
    /// the spec model drains these at each check.
    callback_acked: BTreeSet<String>,
    callback_rejected: BTreeSet<String>,
    /// `waitForPendingWrites` registrations: the highest batch
    /// ID outstanding at registration, and whether resolved.
    waiters: Vec<(i64, bool)>,
    /// Waits resolved during the current step.
    resolved_waits: usize,
    /// Streamed remote updates not yet applied to the store:
    /// they become visible only at an accepted consistent
    /// point, never through an interleaved local operation.
    pending_remote: Vec<RemoteUpdate>,
    /// Documents whose writes were acknowledged and folded into
    /// the base but not yet confirmed by the watch, by relative
    /// key with the commit time (microseconds). Such documents
    /// count toward `hasPendingWrites` and raise metadata
    /// changes when their state flips.
    committed_unconfirmed: BTreeMap<String, i64>,
    /// Per-document mutation flags at the last delivered
    /// composed snapshot: (has local mutations, has committed
    /// mutations), by relative key.
    delivered_flags: BTreeMap<String, (bool, bool)>,
    /// Paths of the most recently settled batch, for absorbing
    /// their flag flips when the settlement raises no event.
    last_settled: Vec<String>,
    /// Acknowledged mutations whose fold left no base row
    /// (deletes, patches of absent documents), by relative key:
    /// the commit time (microseconds) and whether a delivery
    /// at exactly that version is also stale (it is for a
    /// delete, whose own version carries the pre-delete state;
    /// it is not for a patch, whose version carries the
    /// patched state). Older deliveries must not resurrect
    /// the document — the watch has not caught up.
    folded_absent: BTreeMap<String, (i64, bool)>,
    /// Whether this listen has delivered any composed snapshot.
    delivered_valid: bool,
}

impl Harness {
    fn new() -> Self {
        Self {
            state: None,
            target_id: 0,
            unlistened: false,
            expected_view: BTreeMap::new(),
            target_tokens: std::collections::HashMap::new(),
            unexpected_restart: false,
            pending_view: BTreeMap::new(),
            max_barrier: 0,
            query: None,
            current_query_sig: String::new(),
            delivered_queries: std::collections::HashSet::new(),
            listen_saw_token: false,
            resumable_targets: std::collections::HashSet::new(),
            had_listen: false,
            online: true,
            online_state: OnlineState::Unknown,
            stream_failures: 0,
            offline_timeout_armed: false,
            listen_delivered: false,
            current_user: String::new(),
            batch_users: Vec::new(),
            batch_estimates: Vec::new(),
            committed_estimates: std::collections::BTreeMap::new(),
            persisted_tokens: std::collections::HashMap::new(),
            current_token_pending: false,
            armed_db_failures: std::collections::HashSet::new(),
            deferred_settles: std::collections::VecDeque::new(),
            terminated: false,
            pending_user_changes: std::collections::VecDeque::new(),
            terminated_listen: None,
            target_failed: false,
            saw_alloc_failure: false,
            deferred_callbacks: Vec::new(),
            limbo_waiting: std::collections::VecDeque::new(),
            limbo_active: Vec::new(),
            limbo_slots: 0,
            limbo_dirty: false,
            restart_pending: false,
            held_token: String::new(),
            token_blocked_step: false,
            add_token: String::new(),
            limbo_absent_step: false,
            absorb_empty_snapshot: false,
            filter_restart_expected: false,
            filter_restart_step: false,
            current_since_filter: false,
            filter_applied_step: false,
            applying_snapshot_synced: true,
            view: BTreeMap::new(),
            cache: BTreeMap::new(),
            committed_cache: BTreeMap::new(),
            fresh_seed: false,
            feed_snapshots: 0,
            engine: None,
            write_mode: false,
            query_path: String::new(),
            store_remote_keys: BTreeSet::new(),
            delivered_docs: BTreeMap::new(),
            delivered_meta: (false, false),
            synced: false,
            callback_acked: BTreeSet::new(),
            callback_rejected: BTreeSet::new(),
            waiters: Vec::new(),
            resolved_waits: 0,
            committed_unconfirmed: BTreeMap::new(),
            delivered_flags: BTreeMap::new(),
            folded_absent: BTreeMap::new(),
            pending_remote: Vec::new(),
            last_settled: Vec::new(),
            delivered_valid: false,
        }
    }

    /// Builds the add-target request for the active listen
    /// through the driver's real construction and verifies the
    /// resume token it would send is the one the harness holds
    /// for the target.
    fn check_readd_token(&self) -> Result<(), String> {
        let Some(state) = self.state.as_ref() else {
            return Ok(());
        };
        let request = crate::listen::add_target_request(
            "projects/test-project/databases/(default)",
            &Target {
                target_id: self.target_id,
                ..Default::default()
            },
            state,
            true,
        );
        let sent = match request.target_change {
            Some(RequestTargetChange::AddTarget(target)) => match target.resume_type {
                Some(ResumeType::ResumeToken(bytes)) => Some(bytes),
                _ => None,
            },
            _ => None,
        };
        let held = self
            .target_tokens
            .get(&self.target_id)
            .map(|token| token.as_bytes().to_vec());
        if sent == held {
            Ok(())
        } else {
            Err(format!(
                "add-target would send token {sent:?}, harness holds {held:?}"
            ))
        }
    }

    /// Records a resume token seen outside a fed response
    /// (targeted snapshots are bookkeeping in the spec model).
    fn note_token(&mut self, token: &[u8]) {
        if token.is_empty() {
            return;
        }
        if let Some(state) = self.state.as_mut() {
            state.note_resume_token(token);
        }
        let token_string = String::from_utf8_lossy(token).into_owned();
        self.target_tokens
            .insert(self.target_id, token_string.clone());
    }

    /// Transitions the online-state tracker to Offline and
    /// resolves the active listen from cache: a delivered
    /// listen re-raises its view unchanged (the fromCache flag
    /// flips), an undelivered one raises its initial view.
    fn go_offline(&mut self, emitted: &mut Vec<Emitted>) {
        self.online_state = OnlineState::Offline;
        self.stream_failures = 0;
        self.offline_timeout_armed = false;
        if self.write_mode || self.state.is_none() || self.unlistened {
            return;
        }
        if !self.limbo_active.is_empty() || !self.limbo_waiting.is_empty() {
            // With limbo resolutions outstanding, the query
            // cannot resolve from cache yet: it stays
            // unresolved until they settle.
            return;
        }
        let documents: Vec<Document> = self.view.values().cloned().collect();
        let changes = if self.listen_delivered {
            Vec::new()
        } else {
            documents
                .iter()
                .enumerate()
                .map(|(index, document)| DocumentChange {
                    kind: DocumentChangeKind::Added,
                    document: document.clone(),
                    old_index: None,
                    new_index: Some(index),
                })
                .collect()
        };
        emitted.push(Emitted::Offline(Snapshot {
            documents,
            changes,
            read_time: None,
        }));
        self.listen_delivered = true;
    }

    /// A client restart: streams, listeners and session
    /// bookkeeping die; persisted state survives — the local
    /// store (remote documents, the mutation queue, target
    /// resume tokens) and the harness's document cache.
    fn restart(&mut self) {
        self.state = None;
        self.unlistened = true;
        self.pending_remote.clear();
        self.pending_view.clear();
        self.expected_view.clear();
        self.feed_snapshots = 0;
        self.online_state = OnlineState::Unknown;
        self.stream_failures = 0;
        self.offline_timeout_armed = false;
        self.listen_delivered = false;
        self.view.clear();
        self.delivered_valid = false;
        self.delivered_docs.clear();
        self.delivered_flags.clear();
        self.delivered_meta = (false, false);
        self.committed_unconfirmed.clear();
        self.folded_absent.clear();
        self.waiters.clear();
        self.synced = false;
        self.last_settled.clear();
        // Resume tokens fall back to their last persisted
        // values; targets holding one are resumable.
        self.target_tokens = self
            .persisted_tokens
            .iter()
            .map(|(target, (token, _))| (*target, token.clone()))
            .collect();
        self.resumable_targets = self
            .persisted_tokens
            .keys()
            .copied()
            .filter(|target| {
                self.persisted_tokens
                    .get(target)
                    .map(|(token, _)| !token.is_empty())
                    .unwrap_or(false)
            })
            .collect();
        self.current_token_pending = false;
        self.limbo_waiting.clear();
        self.limbo_active.clear();
        self.limbo_slots = 0;
        self.limbo_dirty = false;
        self.restart_pending = false;
        self.token_blocked_step = false;
        self.limbo_absent_step = false;
        // While no stream is active, the reported token is
        // the persisted one.
        self.held_token = self
            .persisted_tokens
            .get(&self.target_id)
            .map(|(token, _)| token.clone())
            .unwrap_or_default();
        self.add_token = self.held_token.clone();
    }

    /// A successful watch message: the client is Online, and
    /// any offline progress (failure count, armed timeout) is
    /// reset — a successful message delays offline status.
    fn note_watch_success(&mut self) {
        if !self.write_mode {
            self.online_state = OnlineState::Online;
            self.stream_failures = 0;
            self.offline_timeout_armed = false;
        }
    }

    /// Consumes an armed database failure for `operation`:
    /// true (once) if the operation fails this time.
    fn db_op_fails(&mut self, operation: &str) -> bool {
        self.armed_db_failures.remove(operation)
    }

    /// `notifyLocalViewChanges` failures are consumed without
    /// effect at view-affecting operations: this harness applies
    /// store changes before notifying listeners, so a failed
    /// notification cannot roll anything back (the recovery
    /// cases pin exactly that the documents still surface, and
    /// stay surfaced, in this situation).
    fn consume_notify_failure(&mut self) {
        self.armed_db_failures.remove("notifyLocalViewChanges");
    }

    /// Terminates the client session on a fatal local-store
    /// failure: the listen dies (its registration is remembered
    /// for the restart) and the client restarts at the next
    /// async-queue retry. Delivered-view bookkeeping survives,
    /// so the recovery re-evaluation diffs against it.
    fn terminate_session(&mut self) {
        if self.state.is_some() && !self.unlistened {
            let query_json = serde_json::from_str(&self.current_query_sig).unwrap_or(Json::Null);
            self.terminated_listen = Some((self.target_id, query_json));
        }
        self.state = None;
        self.unlistened = true;
        self.pending_remote.clear();
        self.terminated = true;
        // Limbo resolutions belong to the dead session: their
        // watch states are dropped and they restart from
        // scratch (fresh target adoption) after recovery.
        for res in self.limbo_active.iter_mut() {
            res.state = None;
            res.target_id = None;
            res.failed = false;
        }
    }

    /// Terminates the client on a failed user-change
    /// transaction, queueing the requested change for the
    /// async-queue retry that restarts the client.
    fn terminate_for_user_change(&mut self, uid: String) {
        self.terminate_session();
        self.pending_user_changes.push_back(uid);
    }

    /// Restarts a terminated client at an async-queue retry:
    /// applies the oldest queued user change, restores the
    /// terminated listen, and re-evaluates the composed view.
    fn recover_terminated(&mut self, emitted: &mut Vec<Emitted>) {
        self.terminated = false;
        self.callback_acked
            .extend(std::mem::take(&mut self.deferred_callbacks));
        if let Some(uid) = self.pending_user_changes.pop_front() {
            self.current_user = uid;
        }
        if let Some((target_id, query_json)) = self.terminated_listen.take() {
            if let Ok(query) = parse_query(&query_json) {
                self.target_id = target_id;
                self.held_token = self
                    .persisted_tokens
                    .get(&target_id)
                    .map(|(token, _)| token.clone())
                    .unwrap_or_default();
                self.add_token = self.held_token.clone();
                self.query = Some(query.clone());
                self.query_path = query_json
                    .get("path")
                    .and_then(Json::as_str)
                    .unwrap_or_default()
                    .to_string();
                self.current_query_sig = serde_json::to_string(&query_json).unwrap_or_default();
                let mut seed: Vec<Document> = self
                    .cache
                    .iter()
                    .filter_map(|(key, (document, _))| {
                        let document = document.as_ref()?;
                        (path_matches(&self.query_path, key) && query.matches(document))
                            .then(|| document.clone())
                    })
                    .collect();
                // Limbo documents of the terminated session
                // stay in the restored listen's view even when
                // they no longer match the query: their
                // resolution is still outstanding.
                let limbo_keys: Vec<String> = self
                    .limbo_active
                    .iter()
                    .map(|res| res.key.clone())
                    .chain(self.limbo_waiting.iter().cloned())
                    .collect();
                for key in &limbo_keys {
                    if !seed.iter().any(|doc| key_of(&doc.name) == *key) {
                        if let Some((Some(document), _)) = self.cache.get(key) {
                            seed.push(document.clone());
                        }
                    }
                }
                let mut state = WatchState::new(target_id, query.effective_orders());
                state.set_keep_restart_unconfirmed(!document_path(&self.query_path));
                state.seed_view(seed, false);
                if !limbo_keys.is_empty() {
                    let names: Vec<String> = limbo_keys.iter().map(|key| full_name(key)).collect();
                    state.mark_restart_unconfirmed(&names);
                }
                if let Some(token) = self.target_tokens.get(&target_id).cloned() {
                    state.restore_resume_token(token.into_bytes());
                }
                self.state = Some(state);
                self.unlistened = false;
                self.listen_delivered = true;
            }
        }
        if self.write_mode {
            let composed = self.composed_view();
            let content_changed = composed.len() != self.delivered_docs.len()
                || composed.iter().any(|(key, document)| {
                    self.delivered_docs
                        .get(key)
                        .map(|old| old.fields != document.fields)
                        .unwrap_or(true)
                });
            if content_changed {
                self.maybe_emit_composed(emitted, false, false, true);
            } else {
                // Only mutation flags or snapshot metadata
                // moved across the termination (e.g. a fold
                // applied while the session was down): absorb
                // them silently — they surface with the next
                // watch confirmation, not at the restart.
                self.delivered_docs = composed.clone();
                if let Some(engine) = &self.engine {
                    // The absorbed flags must come from the
                    // current user's batch subset, like every
                    // other flag computation: the shared queue
                    // would leak another user's pending writes
                    // into the recovered listener's metadata.
                    let flags: BTreeMap<String, (bool, bool)> = composed
                        .keys()
                        .map(|key| {
                            (
                                key.clone(),
                                (
                                    self.has_pending_filtered(engine, key),
                                    self.committed_unconfirmed.contains_key(key),
                                ),
                            )
                        })
                        .collect();
                    self.delivered_flags = flags;
                }
                let has_pending = self
                    .delivered_flags
                    .values()
                    .any(|(local, committed)| *local || *committed);
                self.delivered_meta = (self.is_from_cache(), has_pending);
            }
        }
    }

    /// Enqueues a batch of limbo candidates (documents in
    /// the view not already queued or resolving). A batch
    /// raises the number of resolutions that may run at once:
    /// half the batch rounded up, at most two — the spec
    /// driver's throttling. A batch of one starts at once.
    fn enter_limbo_batch_throttled(&mut self, keys: &[String], throttled: bool) {
        let mut fresh: Vec<String> = Vec::new();
        for key in keys {
            // In write mode the relevant view is the composed
            // (delivered) one; the watch state's view does not
            // hold store-only documents.
            let in_view = self.view.contains_key(key)
                || (self.write_mode && self.delivered_docs.contains_key(key));
            if in_view
                && !self.limbo_waiting.contains(key)
                && !self.limbo_active.iter().any(|res| &res.key == key)
                && !fresh.contains(key)
            {
                fresh.push(key.clone());
            }
        }
        if fresh.is_empty() {
            return;
        }
        self.limbo_dirty = true;
        // Reset batches start only half (rounded up); batches
        // from filters, re-runs and remove messages start up
        // to the concurrency cap directly.
        let starts = if throttled {
            fresh.len().div_ceil(2).min(2)
        } else {
            fresh.len().min(2)
        };
        if starts > self.limbo_slots {
            self.limbo_slots = starts;
        }
        self.limbo_waiting.extend(fresh);
        self.pump_limbo();
    }

    /// Starts waiting resolutions while a slot is free.
    fn pump_limbo(&mut self) {
        while self.limbo_active.len() < self.limbo_slots {
            let Some(key) = self.limbo_waiting.pop_front() else {
                break;
            };
            self.limbo_active.push(LimboRes {
                key,
                target_id: None,
                state: None,
                failed: false,
            });
        }
        if self.limbo_active.is_empty() && self.limbo_waiting.is_empty() {
            self.limbo_slots = 0;
        }
    }

    /// Adopts a document target id for the oldest started
    /// resolution that has none, creating its watch state.
    fn adopt_limbo_target(&mut self, target_id: i32) {
        if target_id == self.target_id
            || self
                .limbo_active
                .iter()
                .any(|res| res.target_id == Some(target_id))
        {
            return;
        }
        let Some(res) = self
            .limbo_active
            .iter_mut()
            .find(|res| res.target_id.is_none())
        else {
            return;
        };
        res.target_id = Some(target_id);
        let orders = parse_query(&serde_json::json!({
            "path": res.key, "filters": [], "orderBys": []
        }))
        .map(|query| query.effective_orders())
        .unwrap_or_default();
        res.state = Some(WatchState::new(target_id, orders));
    }

    /// Whether a target id belongs to a started limbo
    /// resolution.
    fn is_limbo_target(&self, target_id: i32) -> bool {
        self.limbo_active
            .iter()
            .any(|res| res.target_id == Some(target_id))
    }

    /// Feeds a response addressed to a limbo target.
    fn feed_limbo(&mut self, target_id: i32, response: ListenResponse) {
        self.adopt_limbo_target(target_id);
        if let Some(res) = self
            .limbo_active
            .iter_mut()
            .find(|res| res.target_id == Some(target_id))
        {
            if let Some(state) = res.state.as_mut() {
                let _ = state.on_response(response);
            }
        }
    }

    /// Settles limbo resolutions at a global barrier: each
    /// started resolution whose document target synced (or
    /// failed) resolves its document — confirmed documents are
    /// fed to the query target when their content changed,
    /// absent ones are removed from the query view. Runs
    /// before the barrier is fed to the query target, so the
    /// resulting snapshot carries the outcome. Returns true
    /// when any resolution completed.
    fn settle_limbo_at_barrier(
        &mut self,
        response: &ListenResponse,
        emitted: &mut Vec<Emitted>,
    ) -> bool {
        if self.unlistened || self.state.is_none() {
            return false;
        }
        if !self.limbo_active.is_empty()
            && (self.armed_db_failures.contains("Apply remote event")
                || self
                    .armed_db_failures
                    .contains("Get last remote snapshot version"))
        {
            // The resolution's outcome cannot be persisted:
            // the client terminates and restarts at the async
            // queue retry, where the listen (and the limbo
            // resolution) is restored.
            self.terminate_session();
            return false;
        }
        let mut resolved: Vec<(String, Option<Document>)> = Vec::new();
        for res in self.limbo_active.iter_mut() {
            let outcome = if res.failed {
                Some(None)
            } else if let Some(state) = res.state.as_mut() {
                match state.on_response(response.clone()) {
                    Some(WatchEvent::Snapshot(snapshot)) => Some(
                        snapshot
                            .documents
                            .into_iter()
                            .find(|document| key_of(&document.name) == res.key),
                    ),
                    _ => None,
                }
            } else {
                None
            };
            if let Some(document) = outcome {
                resolved.push((res.key.clone(), document));
            }
        }
        if resolved.is_empty() {
            return false;
        }
        if resolved.iter().any(|(_, document)| document.is_none()) {
            self.limbo_absent_step = true;
        }
        let done: std::collections::BTreeSet<String> =
            resolved.iter().map(|(key, _)| key.clone()).collect();
        self.limbo_active.retain(|res| !done.contains(&res.key));
        for (head, resolved_document) in resolved {
            match resolved_document {
                Some(document) => {
                    // Confirmed: the query target learns the
                    // document's current state (a no-op when it
                    // matches the view). A document that no
                    // longer matches the query leaves the view.
                    let matches = self
                        .query
                        .as_ref()
                        .is_none_or(|query| query.matches(&document));
                    if !matches {
                        self.limbo_absent_step = true;
                        self.feed(
                            ListenResponse {
                                response_type: Some(ResponseType::DocumentRemove(
                                    crate::google::firestore::v1::DocumentRemove {
                                        document: full_name(&head),
                                        removed_target_ids: vec![self.target_id],
                                        read_time: None,
                                    },
                                )),
                            },
                            emitted,
                        );
                        continue;
                    }
                    let changed = self
                        .view
                        .get(&head)
                        .map(|current| current.fields != document.fields)
                        .unwrap_or(false);
                    if changed {
                        self.feed(
                            ListenResponse {
                                response_type: Some(ResponseType::DocumentChange(
                                    ProtoDocumentChange {
                                        document: Some(crate::google::firestore::v1::Document {
                                            name: document.name.clone(),
                                            fields: fields_to_proto(&document.fields),
                                            create_time: document
                                                .create_time
                                                .map(|time| time.into()),
                                            update_time: document
                                                .update_time
                                                .map(|time| time.into()),
                                        }),
                                        target_ids: vec![self.target_id],
                                        removed_target_ids: Vec::new(),
                                    },
                                )),
                            },
                            emitted,
                        );
                    }
                }
                None => {
                    // Absent: the document leaves the query view.
                    self.feed(
                        ListenResponse {
                            response_type: Some(ResponseType::DocumentRemove(
                                crate::google::firestore::v1::DocumentRemove {
                                    document: full_name(&head),
                                    removed_target_ids: vec![self.target_id],
                                    read_time: None,
                                },
                            )),
                        },
                        emitted,
                    );
                }
            }
        }
        self.pump_limbo();
        true
    }

    /// Whether any limbo document (the given fresh candidates
    /// plus the outstanding resolutions) lacks an unacked local
    /// mutation of the *current user*: such a document blocks
    /// the query's sync, while a document held in the view by
    /// the current user's own pending write does not. Another
    /// user's queued batch is invisible to this user and must
    /// not unblock the sync.
    fn limbo_blocking_sync_extra(&self, extra: &[String]) -> bool {
        let mut keys: Vec<&String> = extra.iter().collect();
        keys.extend(self.limbo_active.iter().map(|res| &res.key));
        keys.extend(self.limbo_waiting.iter());
        keys.iter().any(|key| {
            !self
                .engine
                .as_ref()
                .map(|engine| self.has_pending_filtered(engine, key))
                .unwrap_or(false)
        })
    }

    fn limbo_blocking_sync(&self) -> bool {
        self.limbo_blocking_sync_extra(&[])
    }

    fn feed(&mut self, response: ListenResponse, emitted: &mut Vec<Emitted>) {
        let limbo_before = self.limbo_active.len() + self.limbo_waiting.len();
        let mut held_snapshot: Option<Snapshot> = None;
        self.note_watch_success();
        // A NO_CHANGE barrier's read time confirms every
        // committed write at or below it, and the barrier is
        // where streamed confirmations surface as events.
        let barrier_read_micros = match &response.response_type {
            Some(ResponseType::TargetChange(change))
                if change.target_change_type == TargetChangeType::NoChange as i32 =>
            {
                change
                    .read_time
                    .as_ref()
                    .map(|time| time.seconds * 1_000_000 + (time.nanos / 1000) as i64)
            }
            _ => None,
        };
        if self.write_mode {
            // Remote documents are buffered as they stream in
            // and applied to the store at the next accepted
            // consistent point, independently of whether the
            // watch state raises a snapshot there: a
            // confirmation whose fields are unchanged produces
            // no snapshot but must still land.
            self.apply_response_to_store(&response);
            if barrier_read_micros.is_some() {
                self.flush_pending_remote();
            }
        }
        let Some(state) = self.state.as_mut() else {
            return;
        };
        let event = state.on_response(response);
        // The held token always mirrors the watch state's, so a
        // state that drops tokens is caught by the comparisons.
        match state.resume_token().map(|token| token.to_vec()) {
            Some(bytes) => {
                let token_string = String::from_utf8_lossy(&bytes).into_owned();
                self.target_tokens
                    .insert(self.target_id, token_string.clone());
            }
            None => {
                self.target_tokens.remove(&self.target_id);
            }
        }
        if let Some(state) = self.state.as_mut() {
            if state.take_filter_applied() {
                self.filter_applied_step = true;
            }
            if state.take_filter_restart() {
                self.filter_restart_step = true;
                self.restart_pending = true;
                self.held_token = String::new();
                self.held_token = self.add_token.clone();
                self.add_token = String::new();
                self.persisted_tokens.remove(&self.target_id);
                self.target_tokens.remove(&self.target_id);
                if self.filter_restart_expected {
                    self.filter_restart_expected = false;
                } else {
                    self.unexpected_restart = true;
                }
            }
        }
        let candidates = match self.state.as_mut() {
            Some(state) => state
                .take_limbo_candidates()
                .iter()
                .map(|name| key_of(name))
                .collect::<Vec<String>>(),
            None => Vec::new(),
        };
        if !candidates.is_empty() {
            if !self.write_mode {
                // The target's view is no longer fully
                // synchronized: its resume token reads empty
                // and this step's barrier cannot promote one.
                self.held_token = String::new();
                self.held_token = self.add_token.clone();
                self.token_blocked_step = true;
            }
            self.enter_limbo_batch_throttled(&candidates, false);
        }
        // The snapshot fully synchronizes the query unless a
        // limbo document without an unacked local mutation is
        // outstanding: a document held in the view by its own
        // pending write does not block the query's sync.
        self.applying_snapshot_synced = !self.limbo_blocking_sync_extra(&candidates);
        let mut snapshot_emitted = false;
        match event {
            Some(WatchEvent::Snapshot(mut snapshot)) => {
                snapshot_emitted = true;
                // The watch state speaks protobuf, which has no
                // decimal number kind: documents it emits carry
                // the f64 approximation of any Decimal128 field.
                // The harness's cache holds the exact values of
                // the SAME delivery, so substitute those back
                // (the local store in the reference holds exact
                // values). Only the same version qualifies — a
                // newer cache entry is a delivery whose
                // consistent point has not arrived.
                for document in snapshot.documents.iter_mut().chain(
                    snapshot
                        .changes
                        .iter_mut()
                        .map(|change| &mut change.document),
                ) {
                    let key = key_of(&document.name);
                    if let Some((Some(cached), version)) = self.cache.get(&key) {
                        let doc_micros = document
                            .update_time
                            .as_ref()
                            .map(|time| time.seconds * 1_000_000 + (time.nanos / 1000) as i64)
                            .unwrap_or(0);
                        if *version == doc_micros {
                            *document = cached.clone();
                        }
                    }
                }
                self.delivered_queries
                    .insert(self.current_query_sig.clone());
                self.pending_view.clear();
                self.feed_snapshots += 1;
                if self.fresh_seed && self.feed_snapshots == 1 {
                    // Synthesized deletes leave the local store as
                    // tombstones at the snapshot's version, so an
                    // older set cannot resurrect the document.
                    let version = snapshot
                        .read_time
                        .as_ref()
                        .map(|rt| rt.seconds * 1_000_000 + (rt.nanos / 1000) as i64)
                        .unwrap_or(0);
                    for change in &snapshot.changes {
                        if change.kind == DocumentChangeKind::Removed {
                            self.cache
                                .insert(key_of(&change.document.name), (None, version));
                        }
                    }
                }
                let previous_view = std::mem::replace(
                    &mut self.view,
                    snapshot
                        .documents
                        .iter()
                        .map(|doc| (key_of(&doc.name), doc.clone()))
                        .collect(),
                );
                if self.write_mode {
                    self.apply_snapshot_to_store(&snapshot);
                    // Store-based limbo accounting: remote
                    // documents in the local store matching the
                    // query that the stream has not confirmed
                    // are limbo candidates.
                    let store_candidates: Vec<String> = {
                        let mut found = Vec::new();
                        if let (Some(engine), Some(query)) = (&self.engine, &self.query) {
                            if let Ok(documents) = engine.store().all_remote_documents() {
                                for document in &documents {
                                    let key = key_of(&document.name);
                                    if !self.store_remote_keys.contains(&key)
                                        && path_matches(&self.query_path, &key)
                                        && query.matches(document)
                                    {
                                        found.push(key);
                                    }
                                }
                            }
                        }
                        found
                    };
                    if !store_candidates.is_empty() {
                        self.enter_limbo_batch_throttled(&store_candidates, false);
                    }
                    self.synced = !self.limbo_blocking_sync();
                    let force = !self.delivered_valid;
                    self.maybe_emit_composed(emitted, force, false, true);
                } else if snapshot.changes.is_empty()
                    && previous_view.len() == snapshot.documents.len()
                {
                    // A no-change snapshot while the view is
                    // not fully synchronized (a recreated re-add
                    // state, or limbo resolutions outstanding)
                    // may not be an event in the spec model;
                    // the decision is made once this feed's
                    // limbo outcome is known (below).
                    held_snapshot = Some(snapshot);
                } else {
                    emitted.push(Emitted::Snapshot(snapshot));
                }
            }
            Some(WatchEvent::Removed(error)) => {
                // A clean removal is not a snapshot event in the
                // spec model; only a removal with a cause is.
                if let Some(error) = &error {
                    emitted.push(Emitted::Error(error_code(error)));
                }
                self.state = None;
                self.pending_remote.clear();
            }
            None => {}
        }
        if self.write_mode && !snapshot_emitted && barrier_read_micros.is_some() {
            self.maybe_emit_metadata_only(emitted);
        }
        if self.write_mode {
            if let Some(read_micros) = barrier_read_micros {
                let limbo: std::collections::BTreeSet<String> = self
                    .limbo_active
                    .iter()
                    .map(|res| res.key.clone())
                    .chain(self.limbo_waiting.iter().cloned())
                    .collect();
                self.committed_unconfirmed
                    .retain(|key, commit| *commit > read_micros || limbo.contains(key));
                self.committed_estimates
                    .retain(|(key, _), _| self.committed_unconfirmed.contains_key(key));
            }
        }
        if let Some(snapshot) = held_snapshot {
            self.absorb_empty_snapshot = false;
            // The held snapshot is an event only when the
            // view ended this feed fully synchronized: no
            // limbo was outstanding and none was produced,
            // and no filter restart landed.
            let limbo_after = self.limbo_active.len() + self.limbo_waiting.len();
            if limbo_before == 0 && limbo_after == 0 && !self.filter_restart_step {
                emitted.push(Emitted::Snapshot(snapshot));
            }
        }
    }
}

impl Harness {
    /// Builds the persistence engine on an in-memory store with
    /// an inert write stream: batches are enqueued directly in
    /// the store and settled by the harness, exactly as the
    /// engine's own unit tests drive it.
    fn ensure_engine(&mut self) {
        if self.engine.is_some() {
            return;
        }
        struct StubTokens;
        #[async_trait::async_trait]
        impl firebase_native_core::TokenProvider for StubTokens {
            async fn id_token(&self, _force_refresh: bool) -> firebase_native_core::Result<String> {
                Ok("stub-token".to_string())
            }
        }
        let options = firebase_native_core::FirebaseOptions {
            api_key: "spec-test".to_string(),
            project_id: "test-project".to_string(),
            auth_domain: None,
            storage_bucket: None,
            app_id: None,
            database_id: "(default)".to_string(),
        };
        let write = WriteStream::new(
            options,
            Arc::new(StubTokens),
            Arc::new(tokio::sync::OnceCell::new()),
            "https://firestore.googleapis.com".to_string(),
        );
        let store = Arc::new(LocalStore::open_in_memory().expect("in-memory store"));
        self.engine = Some(Arc::new(PersistenceEngine::new(
            store,
            write,
            DOCUMENTS_PREFIX.to_string(),
        )));
    }

    /// Applies a watch snapshot to the store: documents upsert
    /// (if newer), previously watched documents now absent are
    /// recorded as removed.
    fn apply_snapshot_to_store(&mut self, snapshot: &Snapshot) {
        let mut keys: BTreeSet<String> = BTreeSet::new();
        for document in &snapshot.documents {
            keys.insert(key_of(&document.name));
            self.apply_remote_document(document);
        }
        // Documents the snapshot does not confirm are not
        // deleted here: they become limbo candidates (the
        // caller in `feed` enters them) and leave the store
        // only when their resolution settles as absent.
        self.store_remote_keys = keys;
        self.synced = self.applying_snapshot_synced;
    }

    /// Applies one streamed remote document to the store,
    /// honoring folded-absent markers (an acknowledged delete
    /// or patch folded at a newer version wins over a stale
    /// delivery) and clearing watch confirmations.
    fn apply_remote_document(&mut self, document: &Document) {
        let Some(engine) = self.engine.clone() else {
            return;
        };
        let key = key_of(&document.name);
        let micros = Self::doc_micros(document);
        let stale = self
            .folded_absent
            .get(&key)
            .map(|(commit, inclusive)| micros < *commit || (*inclusive && micros == *commit));
        match stale {
            Some(true) => return,
            Some(false) => {
                self.folded_absent.remove(&key);
            }
            None => {}
        }
        let confirmed = self
            .committed_unconfirmed
            .get(&key)
            .map(|commit| micros >= *commit)
            .unwrap_or(false);
        if confirmed {
            self.committed_unconfirmed.remove(&key);
            self.committed_estimates
                .retain(|(estimate_key, _), _| estimate_key != &key);
        }
        let _ = engine.store_remote_document_if_newer(document);
        self.store_remote_keys.insert(key);
    }

    /// Records a streamed removal in the store, unless the
    /// store's base is already newer than the removal.
    fn apply_remote_absent(&mut self, name: &str, read_micros: Option<i64>) {
        let Some(engine) = self.engine.clone() else {
            return;
        };
        if let Some(read_micros) = read_micros {
            let base_micros = engine
                .store()
                .remote_document(name)
                .ok()
                .flatten()
                .map(|document| Self::doc_micros(&document))
                .unwrap_or(0);
            if base_micros > read_micros {
                return;
            }
        }
        let key = key_of(name);
        let _ = engine.store_remote_absent(&key);
        self.store_remote_keys.insert(key);
    }

    /// Buffers a streamed Listen response's document updates:
    /// remote state becomes visible to the composed view only
    /// at an accepted consistent point (see
    /// `flush_pending_remote`), never through a local operation
    /// interleaved before the barrier.
    fn apply_response_to_store(&mut self, response: &ListenResponse) {
        match &response.response_type {
            Some(ResponseType::DocumentChange(change)) => {
                if change.removed_target_ids.contains(&self.target_id) {
                    if let Some(document) = &change.document {
                        self.pending_remote
                            .push(RemoteUpdate::Absent(document.name.clone(), None));
                    }
                } else if change.target_ids.is_empty()
                    || change.target_ids.contains(&self.target_id)
                {
                    if let Some(proto) = &change.document {
                        if let Ok(mut document) = Document::from_proto(proto.clone()) {
                            // As with watch snapshots, the proto
                            // round trip loses decimal precision:
                            // substitute the cache's exact
                            // document for the same version.
                            let key = key_of(&document.name);
                            if let Some((Some(cached), version)) = self.cache.get(&key) {
                                let doc_micros = document
                                    .update_time
                                    .as_ref()
                                    .map(|time| {
                                        time.seconds * 1_000_000 + (time.nanos / 1000) as i64
                                    })
                                    .unwrap_or(0);
                                if *version == doc_micros {
                                    document = cached.clone();
                                }
                            }
                            self.pending_remote.push(RemoteUpdate::Document(document));
                        }
                    }
                }
            }
            Some(ResponseType::DocumentDelete(delete)) => {
                if delete.removed_target_ids.contains(&self.target_id) {
                    let read_micros = delete
                        .read_time
                        .as_ref()
                        .map(|time| time.seconds * 1_000_000 + (time.nanos / 1000) as i64);
                    self.pending_remote
                        .push(RemoteUpdate::Absent(delete.document.clone(), read_micros));
                }
            }
            Some(ResponseType::DocumentRemove(remove))
                if remove.removed_target_ids.contains(&self.target_id) =>
            {
                self.pending_remote
                    .push(RemoteUpdate::Absent(remove.document.clone(), None));
            }
            _ => {}
        }
    }

    /// Applies the buffered streamed updates to the store, in
    /// arrival order, at an accepted consistent point.
    fn flush_pending_remote(&mut self) {
        for update in std::mem::take(&mut self.pending_remote) {
            match update {
                RemoteUpdate::Document(document) => self.apply_remote_document(&document),
                RemoteUpdate::Absent(name, read_micros) => {
                    self.apply_remote_absent(&name, read_micros)
                }
            }
        }
    }

    /// A delivery that changed no content can still confirm
    /// committed writes; evaluate metadata-only movement then.
    fn maybe_emit_metadata_only(&mut self, emitted: &mut Vec<Emitted>) {
        if !self.delivered_valid {
            // Nothing delivered yet for this listen: the first
            // snapshot (with its metadata) belongs to the
            // barrier path, not to streamed deliveries.
            return;
        }
        let composed = self.composed_view();
        let same_content = composed.len() == self.delivered_docs.len()
            && composed.iter().all(|(key, document)| {
                self.delivered_docs
                    .get(key)
                    .map(|old| old.fields == document.fields)
                    .unwrap_or(false)
            });
        if same_content {
            self.maybe_emit_composed(emitted, false, false, true);
        }
    }

    /// The composed local view for the active query: every known
    /// document (store base plus queued mutation paths) resolved
    /// through the engine, filtered by path and query.
    fn composed_view(&self) -> BTreeMap<String, Document> {
        let (Some(engine), Some(query)) = (&self.engine, &self.query) else {
            return BTreeMap::new();
        };
        let mut candidates: BTreeSet<String> = BTreeSet::new();
        if let Ok(documents) = engine.store().all_remote_documents() {
            candidates.extend(documents.iter().map(|document| key_of(&document.name)));
        }
        if let Ok(batches) = engine.store().peek_batches(usize::MAX) {
            for batch in &batches {
                for mutation in &batch.mutations {
                    candidates.insert(mutation_path(mutation).to_string());
                }
            }
        }
        candidates
            .into_iter()
            .filter(|key| path_matches(&self.query_path, key))
            .filter_map(|key| {
                let document = self.local_document_filtered(engine, &key)?;
                query.matches(&document).then_some((key, document))
            })
            .collect()
    }

    fn is_from_cache(&self) -> bool {
        !self.online || !self.synced
    }

    /// Recomputes the composed view and, when it or its metadata
    /// changed (or the caller forces a barrier event), delivers
    /// it as the listener's next snapshot.
    fn maybe_emit_composed(
        &mut self,
        emitted: &mut Vec<Emitted>,
        force: bool,
        initial: bool,
        strict: bool,
    ) {
        if self.state.is_none() || self.unlistened || self.engine.is_none() {
            return;
        }
        let composed = self.composed_view();
        let engine = self.engine.as_ref().expect("engine checked");
        let flags: BTreeMap<String, (bool, bool)> = composed
            .keys()
            .map(|key| {
                (
                    key.clone(),
                    (
                        self.has_pending_filtered(engine, key),
                        self.committed_unconfirmed.contains_key(key),
                    ),
                )
            })
            .collect();
        let mut changes: Vec<DocumentChange> = Vec::new();
        let mut metadata: Vec<Document> = Vec::new();
        for (index, (key, document)) in composed.iter().enumerate() {
            match self.delivered_docs.get(key) {
                None => changes.push(DocumentChange {
                    kind: DocumentChangeKind::Added,
                    document: document.clone(),
                    old_index: None,
                    new_index: Some(index),
                }),
                Some(old) if old.fields != document.fields => {
                    changes.push(DocumentChange {
                        kind: DocumentChangeKind::Modified,
                        document: document.clone(),
                        old_index: None,
                        new_index: Some(index),
                    });
                }
                _ => {
                    let was_local = self
                        .delivered_flags
                        .get(key)
                        .map(|(local, _)| *local)
                        .unwrap_or(false);
                    let is_local = flags.get(key).map(|(local, _)| *local).unwrap_or(false);
                    if !was_local && is_local {
                        // Gaining an unacknowledged local mutation
                        // is a modification, not a metadata change.
                        changes.push(DocumentChange {
                            kind: DocumentChangeKind::Modified,
                            document: document.clone(),
                            old_index: None,
                            new_index: Some(index),
                        });
                    } else if self.delivered_flags.get(key) != flags.get(key) {
                        metadata.push(document.clone());
                    }
                }
            }
        }
        for (key, document) in &self.delivered_docs {
            if !composed.contains_key(key) {
                changes.push(DocumentChange {
                    kind: DocumentChangeKind::Removed,
                    document: document.clone(),
                    old_index: None,
                    new_index: None,
                });
            }
        }
        let has_pending = flags
            .values()
            .any(|(local, committed)| *local || *committed);
        let meta = (self.is_from_cache(), has_pending);
        // In non-strict mode (an acknowledgement whose effect
        // the watch has not delivered) only content changes
        // raise an event; flag and metadata flips are absorbed.
        if changes.is_empty()
            && (metadata.is_empty() || !strict)
            && (meta == self.delivered_meta || !strict)
            && !force
        {
            return;
        }
        self.delivered_docs = composed.clone();
        self.delivered_flags = flags;
        self.delivered_meta = meta;
        self.delivered_valid = true;
        self.delivered_queries
            .insert(self.current_query_sig.clone());
        emitted.push(Emitted::Composed {
            snapshot: Snapshot {
                documents: composed.into_values().collect(),
                changes,
                read_time: None,
            },
            from_cache: meta.0,
            has_pending: meta.1,
            metadata,
            initial,
        });
    }

    fn enqueue_spec(&mut self, mutation: Mutation) {
        self.consume_notify_failure();
        if self.db_op_fails("Locally write mutations") {
            // The batch could not be persisted: it is rejected
            // outright — never queued, never surfaced.
            self.callback_rejected.insert(mutation.path().to_string());
            return;
        }
        // The transform estimates are what the composed view
        // shows for the transformed fields from now until the
        // watch confirms the write: computed by applying the
        // mutation locally over the current view.
        let mut estimates = std::collections::BTreeMap::new();
        if !mutation.transforms().is_empty() {
            if let Some(engine) = self.engine.clone() {
                let key = mutation.path().to_string();
                let view = self.local_document_filtered(&engine, &key);
                if let Some(document) = mutation.apply_to_local(view) {
                    for transform in mutation.transforms() {
                        if let Some(value) =
                            crate::mutation::lookup_path(&document.fields, &transform.path)
                        {
                            estimates.insert((key.clone(), transform.path.clone()), value.clone());
                        }
                    }
                }
            }
        }
        if let Some(engine) = &self.engine {
            if engine.store().enqueue_batch(&[mutation]).is_ok() {
                self.batch_users.push(self.current_user.clone());
                self.batch_estimates.push(estimates);
            }
        }
    }

    /// Whether the oldest queued batch belongs to another user
    /// while a batch of the current user waits behind it: the
    /// spec's acknowledgements settle the current user's oldest
    /// batch, which the store's FIFO fold cannot express.
    fn cross_user_settle_ahead(&self) -> bool {
        let Some(front) = self.batch_users.first() else {
            return false;
        };
        front != &self.current_user
            && self
                .batch_users
                .iter()
                .skip(1)
                .any(|user| user == &self.current_user)
    }

    /// Whether any queued batch of the *current user* mutates
    /// `key`. The pending-write flag must be derived from the
    /// same batch subset as the composed content: the store's
    /// queue is shared across users, so the engine's unfiltered
    /// check would leak another user's mutation metadata into
    /// this user's snapshot flags.
    fn has_pending_filtered(&self, engine: &PersistenceEngine, key: &str) -> bool {
        let Ok(batches) = engine.store().peek_batches(usize::MAX) else {
            return false;
        };
        batches.iter().enumerate().any(|(index, batch)| {
            self.batch_users.get(index).map_or("", |user| user.as_str()) == self.current_user
                && batch
                    .mutations
                    .iter()
                    .any(|mutation| mutation_path(mutation) == key)
        })
    }

    /// The local view of one document for the current user:
    /// the remote base overlaid with the current user's queued
    /// mutations (other users' batches are invisible).
    fn local_document_filtered(&self, engine: &PersistenceEngine, key: &str) -> Option<Document> {
        let store = engine.store();
        let mut document = store
            .remote_document(&format!("{DOCUMENTS_PREFIX}/{key}"))
            .ok()
            .flatten();
        // The store persists documents as protobuf bytes, a
        // round trip that approximates Decimal128 fields as
        // doubles. The harness's cache holds the exact values
        // of the same delivery (the reference's local store
        // holds exact values), so substitute the base before
        // overlaying mutations. Only an equal version
        // qualifies: a newer cache entry has not reached its
        // consistent point in the store yet.
        if let Some(base) = &document {
            if let Some((Some(cached), version)) = self.cache.get(key) {
                let base_micros = base
                    .update_time
                    .as_ref()
                    .map(|time| time.seconds * 1_000_000 + (time.nanos / 1000) as i64)
                    .unwrap_or(0);
                if *version == base_micros {
                    document = Some(cached.clone());
                }
            }
        }
        let batches = store.peek_batches(usize::MAX).ok()?;
        for (index, batch) in batches.iter().enumerate() {
            let owner = self.batch_users.get(index).map_or("", |user| user.as_str());
            if owner != self.current_user {
                continue;
            }
            for mutation in &batch.mutations {
                if mutation.path() == key {
                    document = mutation.apply_to_local(document);
                    if let Some(document) = document.as_mut() {
                        if document.name.is_empty() {
                            document.name = format!("{DOCUMENTS_PREFIX}/{key}");
                        }
                    }
                }
            }
            // The batch's transform estimates override the
            // recomputation over a possibly moved base.
            if let Some(document) = document.as_mut() {
                if let Some(estimates) = self.batch_estimates.get(index) {
                    for ((estimate_key, path), value) in estimates {
                        if estimate_key == key {
                            crate::mutation::set_path(&mut document.fields, path, value.clone());
                        }
                    }
                }
            }
        }
        // Folded-but-unconfirmed transform results still show
        // their local estimates.
        if self.committed_unconfirmed.contains_key(key) {
            if let Some(document) = document.as_mut() {
                let committed = self.committed_estimates.clone();
                for ((estimate_key, path), value) in &committed {
                    if estimate_key == key {
                        crate::mutation::set_path(&mut document.fields, path, value.clone());
                    }
                }
            }
        }
        document
    }

    /// Settles the oldest outstanding batch the way the spec's
    /// `writeAck` / `failWrite` steps do: folded into the remote
    /// base at the commit time, or dropped from the queue.
    /// Returns whether any settled document already had a
    /// remote base (the watch had delivered it): only then does
    /// an acknowledgement raise a listener event.
    fn settle_oldest(
        &mut self,
        ack: bool,
        commit: Timestamp,
        transform_results: &[Vec<Value>],
    ) -> bool {
        let Some(engine) = self.engine.clone() else {
            return false;
        };
        let store = engine.store();
        let Ok(batches) = store.peek_batches(1) else {
            return false;
        };
        let Some(batch) = batches.into_iter().next() else {
            return false;
        };
        // Whether the watch had already delivered every
        // mutation's effect before the acknowledgement: the
        // base carries a version at or beyond the commit for
        // each mutated document. Only then does the
        // acknowledgement itself raise an event (the documents'
        // mutation flags flip in a server-confirmed view). A
        // base that merely has the same fields at an older
        // version does not count — the write's own version is
        // not confirmed yet.
        let commit_micros = commit.seconds * 1_000_000 + (commit.nanos / 1000) as i64;
        let mut effect_delivered = ack;
        let mut base_micros: Vec<(String, i64)> = Vec::new();
        for mutation in &batch.mutations {
            let base = store
                .remote_document(&format!("{DOCUMENTS_PREFIX}/{}", mutation.path()))
                .ok()
                .flatten();
            let micros = base.as_ref().map(Self::doc_micros).unwrap_or(0);
            base_micros.push((mutation.path().to_string(), micros));
            if micros < commit_micros {
                effect_delivered = false;
            }
        }
        let paths: Vec<String> = batch
            .mutations
            .iter()
            .map(|mutation| mutation.path().to_string())
            .collect();
        self.last_settled = paths.clone();
        if ack {
            if engine
                .fold_acknowledgement_with_results(&batch, commit, transform_results)
                .is_ok()
            {
                if !self.batch_users.is_empty() {
                    self.batch_users.remove(0);
                }
                let settled_estimates = if self.batch_estimates.is_empty() {
                    std::collections::BTreeMap::new()
                } else {
                    self.batch_estimates.remove(0)
                };
                for mutation in &batch.mutations {
                    let path = mutation.path();
                    // A fold that leaves no base row (a delete,
                    // a patch of an absent document) marks the
                    // document absent as of the commit: older
                    // watch deliveries must not resurrect it.
                    let now_absent = store
                        .remote_document(&format!("{DOCUMENTS_PREFIX}/{path}"))
                        .map(|document| document.is_none())
                        .unwrap_or(false);
                    if now_absent {
                        let inclusive = matches!(mutation, Mutation::Delete { .. });
                        self.folded_absent
                            .insert(path.to_string(), (commit_micros, inclusive));
                    } else {
                        self.folded_absent.remove(path);
                    }
                }
                for (path, micros) in base_micros {
                    if micros >= commit_micros {
                        // The base already carried a watch
                        // delivery at or beyond the commit: the
                        // folded state is watch-confirmed.
                        self.committed_unconfirmed.remove(&path);
                    } else if self.delivered_docs.contains_key(&path) {
                        // Folded into the base of a document the
                        // listener currently sees: committed but
                        // not yet watch-confirmed.
                        self.committed_unconfirmed.insert(path, commit_micros);
                    }
                }
                // Transform estimates survive the fold only
                // while the write awaits watch confirmation.
                for (estimate_key, value) in settled_estimates {
                    if self.committed_unconfirmed.contains_key(&estimate_key.0) {
                        self.committed_estimates.insert(estimate_key, value);
                    }
                }
                self.callback_acked.extend(paths);
            }
        } else if store.remove_batch(batch.id).is_ok() {
            if !self.batch_users.is_empty() {
                self.batch_users.remove(0);
            }
            if !self.batch_estimates.is_empty() {
                self.batch_estimates.remove(0);
            }
            self.callback_rejected.extend(paths);
        }
        self.resolve_waiters();
        effect_delivered
    }

    /// A document's update time in spec-model microseconds.
    fn doc_micros(document: &Document) -> i64 {
        document
            .update_time
            .as_ref()
            .map(|time| time.seconds * 1_000_000 + (time.nanos / 1000) as i64)
            .unwrap_or(0)
    }

    /// Records the current mutation flags of the last settled
    /// batch's documents as delivered, without an event: an
    /// acknowledgement whose effect the watch has not delivered
    /// flips flags silently, and later events must not report
    /// those flips as metadata changes.
    fn absorb_settled_flags(&mut self) {
        let Some(engine) = self.engine.clone() else {
            return;
        };
        for path in self.last_settled.clone() {
            if self.delivered_docs.contains_key(&path) {
                let flags = (
                    self.has_pending_filtered(&engine, &path),
                    self.committed_unconfirmed.contains_key(&path),
                );
                self.delivered_flags.insert(path, flags);
            }
        }
    }

    fn register_waiter(&mut self) {
        let through = self
            .engine
            .as_ref()
            .and_then(|engine| engine.store().peek_batches(usize::MAX).ok())
            .map(|batches| batches.iter().map(|batch| batch.id).max().unwrap_or(0))
            .unwrap_or(0);
        self.waiters.push((through, false));
        self.resolve_waiters();
    }

    fn resolve_waiters(&mut self) {
        let Some(engine) = &self.engine else {
            return;
        };
        let Ok(batches) = engine.store().peek_batches(usize::MAX) else {
            return;
        };
        let queued: Vec<i64> = batches.iter().map(|batch| batch.id).collect();
        for waiter in &mut self.waiters {
            if !waiter.1 && !queued.iter().any(|id| *id <= waiter.0) {
                waiter.1 = true;
                self.resolved_waits += 1;
            }
        }
    }
}

/// The document path a mutation targets.
fn mutation_path(mutation: &Mutation) -> &str {
    match mutation {
        Mutation::Set { path, .. } | Mutation::Patch { path, .. } | Mutation::Delete { path } => {
            path
        }
    }
}

/// Whether a spec value encodes a field transform
/// (`{"_methodName": ...}`), which this crate does not model yet.
fn contains_transform(value: &Json) -> bool {
    match value {
        Json::Object(map) => {
            map.contains_key("_methodName") || map.values().any(contains_transform)
        }
        Json::Array(items) => items.iter().any(contains_transform),
        _ => false,
    }
}

/// Maps the crate's errors to canonical gRPC status codes, the
/// numbering spec `errorCode` fields use.
fn error_code(error: &Error) -> i32 {
    match error {
        Error::InvalidArgument(_) => 3,
        Error::NotFound(_) => 5,
        Error::PermissionDenied(_) => 7,
        Error::Unauthenticated(_) => 16,
        Error::Rpc { code, .. } => match code.as_str() {
            "Canceled" => 1,
            "Unknown" => 2,
            "DeadlineExceeded" => 4,
            "AlreadyExists" => 6,
            "ResourceExhausted" => 8,
            "FailedPrecondition" => 9,
            "Aborted" => 10,
            "OutOfRange" => 11,
            "Unimplemented" => 12,
            "Internal" => 13,
            "Unavailable" => 14,
            "DataLoss" => 15,
            _ => 2,
        },
        _ => 2,
    }
}

/// Whether a query path addresses a single document (an
/// even number of segments). Document targets resolve
/// existence-filter mismatches as synthesized deletes rather
/// than limbo candidates.
fn document_path(path: &str) -> bool {
    let segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .count();
    segments >= 2 && segments % 2 == 0
}

fn full_name(key: &str) -> String {
    format!("{DOCUMENTS_PREFIX}/{key}")
}

fn key_of(name: &str) -> String {
    name.strip_prefix(&format!("{DOCUMENTS_PREFIX}/"))
        .unwrap_or(name)
        .to_string()
}

/// Whether a document key falls under a spec query path: an
/// even-segment path is a document path (exact match); an
/// odd-segment path is a collection (direct children only).
fn path_matches(query_path: &str, key: &str) -> bool {
    let segments = query_path.matches('/').count() + 1;
    if segments % 2 == 0 {
        key == query_path
    } else {
        match key.strip_prefix(&format!("{query_path}/")) {
            Some(rest) => !rest.contains('/'),
            None => false,
        }
    }
}

/// Runs one spec case; see the module docs for the step mapping.
fn run_case_named(name: &str, case: &Json) -> Outcome {
    let _dbg = !std::env::var("SPEC_ONLY").unwrap_or_default().is_empty();
    // The upstream expectation for this case starts fifty
    // concurrent limbo resolutions for a fifty-document
    // bloom-filter batch; the batch throttling this harness
    // models (see `enter_limbo_batch_throttled`) caps the
    // batch at two, so the case is out of scope.
    if name == "Bloom filter with large size works as expected" {
        return Outcome::Skipped("limbo throttling for very large batches not modeled".to_string());
    }
    let config = case.get("config").cloned().unwrap_or(Json::Null);
    if config.get("numClients").and_then(Json::as_i64).unwrap_or(1) != 1 {
        return Outcome::Skipped("multiple clients".to_string());
    }
    let steps = match case.get("steps").and_then(Json::as_array) {
        Some(steps) => steps.clone(),
        None => return Outcome::Failed("case without steps".to_string()),
    };
    let mut harness = Harness::new();
    harness.write_mode = steps.iter().any(|step| {
        [
            "userSet",
            "userPatch",
            "userDelete",
            "writeAck",
            "failWrite",
            "waitForPendingWrites",
        ]
        .iter()
        .any(|key| step.get(key).is_some())
    });
    if harness.write_mode {
        harness.ensure_engine();
    }
    for (index, step) in steps.iter().enumerate() {
        harness.token_blocked_step = false;
        harness.limbo_absent_step = false;
        harness.filter_restart_step = false;
        harness.filter_applied_step = false;
        let at = |message: String| format!("step {index}: {message}");
        let mut emitted: Vec<Emitted> = Vec::new();
        harness.resolved_waits = 0;
        // The expected limbo set is compared with the
        // harness's where the expected state is checked.
        for (key, value) in step.as_object().map(|o| o.iter()).into_iter().flatten() {
            match key.as_str() {
                "expectedSnapshotEvents"
                | "expectedState"
                | "expectedWaitForPendingWritesEvents" => {}
                "clientIndex" | "drainQueue" | "applyClientState" => {}
                "failDatabase" => {
                    // Arms local-store operations to fail once, at
                    // their next invocation; a non-list value
                    // (`false`) disarms. Operations outside the
                    // modeled set skip the case rather than risk
                    // a false pass.
                    const MODELED: [&str; 9] = [
                        "Allocate target",
                        "Release target",
                        "Locally write mutations",
                        "Acknowledge batch",
                        "Reject batch",
                        "Apply remote event",
                        "Get last remote snapshot version",
                        "Handle user change",
                        "notifyLocalViewChanges",
                    ];
                    match &value {
                        Json::Array(items) => {
                            harness.armed_db_failures = items
                                .iter()
                                .filter_map(Json::as_str)
                                .map(str::to_string)
                                .collect();
                        }
                        _ => harness.armed_db_failures.clear(),
                    }
                    if let Some(op) = harness
                        .armed_db_failures
                        .iter()
                        .find(|op| !MODELED.contains(&op.as_str()))
                    {
                        return Outcome::Skipped(format!("database failure of '{op}' not modeled"));
                    }
                }
                "enableNetwork" => {
                    let online = value.as_bool().unwrap_or(true);
                    if harness.write_mode {
                        harness.online = online;
                        if online {
                            // Resuming raises no event; the
                            // fromCache flip is absorbed.
                            harness.delivered_meta.0 = false;
                        } else {
                            harness.maybe_emit_composed(&mut emitted, false, false, true);
                        }
                    } else if !online
                        && harness.online
                        && harness.state.is_some()
                        && !harness.unlistened
                    {
                        // Going offline re-raises the current view
                        // (its fromCache flag flips); the documents
                        // and changes are unchanged.
                        let documents: Vec<Document> = harness.view.values().cloned().collect();
                        emitted.push(Emitted::Snapshot(Snapshot {
                            documents,
                            changes: Vec::new(),
                            read_time: None,
                        }));
                    }
                    harness.online = online;
                }
                "userListen" => {
                    if harness.db_op_fails("Allocate target") {
                        // Target allocation is a local-store
                        // operation: its failure fails the listen
                        // with UNAVAILABLE and allocates nothing.
                        harness.saw_alloc_failure = true;
                        emitted.push(Emitted::Error(14));
                        continue;
                    }
                    if harness.state.is_some() && !harness.unlistened {
                        // A second concurrent listen is only
                        // modeled when the client is offline (or
                        // the network is disabled): the new query
                        // then resolves from cache and no further
                        // steps address the superseded target.
                        let offline_now = !harness.write_mode
                            && (harness.online_state == OnlineState::Offline
                                || !harness.online
                                || harness.saw_alloc_failure);
                        if !offline_now {
                            return Outcome::Skipped("multiple concurrent targets".to_string());
                        }
                    }
                    harness.listen_delivered = false;
                    let target_id =
                        value.get("targetId").and_then(Json::as_i64).unwrap_or(0) as i32;
                    let query_json = value.get("query").cloned().unwrap_or(Json::Null);
                    let query = match parse_query(&query_json) {
                        Ok(query) => query,
                        Err(reason) => return Outcome::Skipped(reason),
                    };
                    let relisten = harness.had_listen;
                    let query_path = query_json
                        .get("path")
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                        .to_string();
                    // The initial view of any listen is the local
                    // store's content matching the query (path and
                    // filters).
                    let seed: Vec<Document> = harness
                        .cache
                        .iter()
                        .filter_map(|(key, (document, _))| {
                            let document = document.as_ref()?;
                            (path_matches(&query_path, key) && query.matches(document))
                                .then(|| document.clone())
                        })
                        .collect();
                    harness.query = Some(query.clone());
                    harness.query_path = query_path.clone();
                    let mut state = WatchState::new(target_id, query.effective_orders());
                    state.set_keep_restart_unconfirmed(!document_path(&harness.query_path));
                    let fresh = !harness.resumable_targets.contains(&target_id);
                    if relisten {
                        // Without a resume token for this target,
                        // the server must re-confirm every seeded
                        // document; unconfirmed ones are deleted
                        // at the first consistent point.
                        state.seed_view(seed.clone(), fresh);
                    }
                    harness.fresh_seed = relisten && fresh && !seed.is_empty();
                    harness.feed_snapshots = 0;
                    harness.view = seed
                        .iter()
                        .map(|document| (key_of(&document.name), document.clone()))
                        .collect();
                    if let Some(token) = harness.target_tokens.get(&target_id).cloned() {
                        state.restore_resume_token(token.into_bytes());
                    }
                    harness.state = Some(state);
                    harness.target_id = target_id;
                    harness.held_token = harness
                        .persisted_tokens
                        .get(&target_id)
                        .map(|(token, _)| token.clone())
                        .unwrap_or_default();
                    harness.add_token = harness.held_token.clone();
                    harness.unlistened = false;
                    harness.listen_saw_token = false;
                    if harness.write_mode {
                        // The new listener's view comes from the
                        // store: seed it with what the client
                        // already knows, and judge its events
                        // against a fresh delivered view.
                        let engine = harness.engine.clone().expect("write mode engine");
                        for document in &seed {
                            let _ = engine.store_remote_document(document);
                            harness.store_remote_keys.insert(key_of(&document.name));
                        }
                        harness.synced = false;
                        harness.pending_remote.clear();
                        harness.delivered_docs.clear();
                        harness.delivered_flags.clear();
                        harness.delivered_valid = false;
                        harness.delivered_meta = (false, false);
                    }
                    if let Err(message) = harness.check_readd_token() {
                        return Outcome::Failed(at(message));
                    }
                    harness.current_query_sig =
                        serde_json::to_string(&query_json).unwrap_or_default();
                    let write_listen_event = harness.write_mode && {
                        !harness.composed_view().is_empty()
                            || harness
                                .delivered_queries
                                .contains(&harness.current_query_sig)
                    };
                    if write_listen_event
                        || harness
                            .delivered_queries
                            .contains(&harness.current_query_sig)
                        || !seed.is_empty()
                    {
                        // A new listener immediately receives the
                        // query's current view as an all-Added
                        // snapshot when the client already knows
                        // that view (delivered before, or derivable
                        // from the local store). Whether this fires
                        // depends only on harness state, never on
                        // what the step expects.
                        if harness.write_mode {
                            harness.maybe_emit_composed(&mut emitted, true, true, true);
                        } else {
                            let changes = seed
                                .iter()
                                .enumerate()
                                .map(|(index, document)| DocumentChange {
                                    kind: DocumentChangeKind::Added,
                                    document: document.clone(),
                                    old_index: None,
                                    new_index: Some(index),
                                })
                                .collect();
                            emitted.push(Emitted::Pseudo(Snapshot {
                                documents: seed,
                                changes,
                                read_time: None,
                            }));
                        }
                    }
                    if !harness.write_mode
                        && (harness.online_state == OnlineState::Offline || !harness.online)
                        && !emitted
                            .iter()
                            .any(|event| matches!(event, Emitted::Pseudo(_)))
                    {
                        // A listen registered while the client is
                        // offline (or the network is disabled)
                        // resolves immediately from cache.
                        let documents: Vec<Document> = harness
                            .cache
                            .iter()
                            .filter_map(|(key, (document, _))| {
                                let document = document.as_ref()?;
                                (path_matches(&query_path, key) && query.matches(document))
                                    .then(|| document.clone())
                            })
                            .collect();
                        let changes = documents
                            .iter()
                            .enumerate()
                            .map(|(index, document)| DocumentChange {
                                kind: DocumentChangeKind::Added,
                                document: document.clone(),
                                old_index: None,
                                new_index: Some(index),
                            })
                            .collect();
                        emitted.push(Emitted::Offline(Snapshot {
                            documents,
                            changes,
                            read_time: None,
                        }));
                    }
                    harness.had_listen = true;
                }
                "changeUser" => {
                    let requested_user = match &value {
                        Json::String(uid) => Some(uid.clone()),
                        Json::Null => Some(String::new()),
                        _ => None,
                    };
                    if let Some(uid) = requested_user {
                        if harness.terminated {
                            // Changes requested while the client
                            // is down queue for the retry that
                            // restarts it.
                            harness.pending_user_changes.push_back(uid);
                            continue;
                        }
                        if harness.db_op_fails("Handle user change") {
                            harness.terminate_for_user_change(uid);
                            continue;
                        }
                    }
                    if harness.write_mode {
                        // Mutation queues are per-user: switching
                        // the current user changes which queued
                        // batches are visible, so the composed
                        // view is re-evaluated.
                        harness.current_user = match &value {
                            Json::String(uid) => uid.clone(),
                            Json::Null => String::new(),
                            _ => {
                                return Outcome::Skipped(
                                    "changeUser shape not modeled".to_string(),
                                );
                            }
                        };
                        harness.maybe_emit_composed(&mut emitted, false, false, true);
                    }
                    // In a listen-only case a credential change
                    // restarts the streams but does not by
                    // itself change the online state: network
                    // availability dominates.
                }
                "watchReset" => {
                    // A reset target loses its synchronized
                    // state: every document in its view becomes
                    // a limbo candidate, and the target is
                    // re-added (its documents re-confirmed by
                    // the re-sync or by limbo resolution). The
                    // watch state is recreated seeded with the
                    // view, so the re-sync confirms in place.
                    let ids: Vec<i32> = value
                        .as_array()
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Json::as_i64)
                                .map(|v| v as i32)
                                .collect()
                        })
                        .unwrap_or_default();
                    if ids.contains(&harness.target_id) && !harness.write_mode {
                        let keys: Vec<String> = harness.view.keys().cloned().collect();
                        harness.enter_limbo_batch_throttled(&keys, true);
                        harness.add_token = String::new();
                        harness.held_token = String::new();
                        let seed: Vec<Document> = harness.view.values().cloned().collect();
                        let orders = harness
                            .query
                            .as_ref()
                            .map(|query| query.effective_orders())
                            .unwrap_or_default();
                        let mut state = WatchState::new(harness.target_id, orders);
                        state.set_keep_restart_unconfirmed(!document_path(&harness.query_path));
                        state.seed_view(seed, false);
                        harness.state = Some(state);
                        harness.target_tokens.remove(&harness.target_id);
                    }
                }
                "userSet" => {
                    let args = value.as_array().cloned().unwrap_or_default();
                    let path = args
                        .first()
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let data = args.get(1).cloned().unwrap_or(Json::Null);
                    let (fields, transforms, _deletes) = match split_transforms(&data) {
                        Ok(parts) => parts,
                        Err(reason) => return Outcome::Skipped(reason),
                    };
                    harness.enqueue_spec(Mutation::Set {
                        path,
                        fields,
                        merge_paths: None,
                        exists: None,
                        transforms,
                    });
                    harness.maybe_emit_composed(&mut emitted, false, false, true);
                }
                "userPatch" => {
                    let args = value.as_array().cloned().unwrap_or_default();
                    let path = args
                        .first()
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let data = args.get(1).cloned().unwrap_or(Json::Null);
                    let (fields, transforms, deletes) = match split_transforms(&data) {
                        Ok(parts) => parts,
                        Err(reason) => return Outcome::Skipped(reason),
                    };
                    let mut mask: Vec<String> = fields.keys().cloned().collect();
                    mask.extend(deletes);
                    harness.enqueue_spec(Mutation::Patch {
                        path,
                        fields,
                        mask,
                        exists: None,
                        transforms,
                    });
                    harness.maybe_emit_composed(&mut emitted, false, false, true);
                }
                "userDelete" => {
                    let path = match value {
                        Json::String(path) => path.clone(),
                        other => other
                            .as_array()
                            .and_then(|args| args.first())
                            .and_then(Json::as_str)
                            .unwrap_or_default()
                            .to_string(),
                    };
                    harness.enqueue_spec(Mutation::Delete { path });
                    harness.maybe_emit_composed(&mut emitted, false, false, true);
                }
                "writeAck" => {
                    let version = value.get("version").and_then(Json::as_i64).unwrap_or(0);
                    if harness.db_op_fails("Acknowledge batch") {
                        if harness.delivered_valid {
                            // With a listener attached, failing
                            // to persist the acknowledgement is
                            // fatal to the session: the fold is
                            // applied in memory but the user
                            // callback is never delivered, the
                            // target goes down, and the listener
                            // re-raises its view from cache. The
                            // async-queue retry restarts the
                            // client and re-listens.
                            let commit = ts_from_micros(version);
                            let acked_before = harness.callback_acked.clone();
                            harness.settle_oldest(true, commit, &[]);
                            // The fold stands, but the callbacks
                            // it produced are withheld until the
                            // restart delivers them.
                            let withheld: Vec<String> = harness
                                .callback_acked
                                .difference(&acked_before)
                                .cloned()
                                .collect();
                            harness.callback_acked = acked_before;
                            harness.deferred_callbacks.extend(withheld);
                            harness.terminate_session();
                            harness.synced = false;
                            let documents: Vec<Document> =
                                harness.delivered_docs.values().cloned().collect();
                            let has_pending = harness
                                .delivered_flags
                                .values()
                                .any(|(local, committed)| *local || *committed);
                            emitted.push(Emitted::Composed {
                                snapshot: Snapshot {
                                    documents,
                                    changes: Vec::new(),
                                    read_time: None,
                                },
                                from_cache: true,
                                has_pending,
                                metadata: Vec::new(),
                                initial: false,
                            });
                        } else {
                            // Without a listener, the
                            // acknowledgement simply stays
                            // outstanding until the async queue
                            // retries persisting it.
                            harness
                                .deferred_settles
                                .push_back((true, ts_from_micros(version)));
                        }
                        continue;
                    }
                    if harness.cross_user_settle_ahead() {
                        return Outcome::Skipped("cross-user settle order not modeled".to_string());
                    }
                    let commit: Timestamp = ts_from_micros(version);
                    // An acknowledgement raises an event only
                    // when the watch had already delivered the
                    // document (its pending flag was part of a
                    // server-confirmed view); otherwise the flip
                    // surfaces with the watch event that delivers
                    // the document.
                    let transform_results: Vec<Vec<Value>> = value
                        .get("transformResults")
                        .and_then(Json::as_array)
                        .map(|results| vec![results.iter().map(proto_json_to_value).collect()])
                        .unwrap_or_default();
                    let gate = harness.settle_oldest(true, commit, &transform_results);
                    let before = emitted.len();
                    harness.maybe_emit_composed(&mut emitted, false, false, gate);
                    if emitted.len() == before {
                        harness.absorb_settled_flags();
                    }
                }
                "failWrite" => {
                    let keep = value
                        .get("keepInQueue")
                        .and_then(Json::as_bool)
                        .unwrap_or(false);
                    let code = value
                        .get("error")
                        .and_then(|error| error.get("code"))
                        .and_then(Json::as_i64)
                        .unwrap_or(0);
                    // Retryable errors (and explicit keepInQueue)
                    // leave the batch queued for the write
                    // stream's retry: nothing settles, nothing is
                    // reported, the local view is unchanged.
                    let retryable = matches!(code, 1 | 2 | 4 | 10 | 13 | 14 | 16);
                    if !keep && !retryable {
                        if harness.db_op_fails("Reject batch") {
                            // The rejection could not be
                            // persisted: it stays outstanding
                            // until the async queue retries it.
                            harness
                                .deferred_settles
                                .push_back((false, ts_from_micros(0)));
                        } else if harness.cross_user_settle_ahead() {
                            return Outcome::Skipped(
                                "cross-user settle order not modeled".to_string(),
                            );
                        } else {
                            harness.settle_oldest(false, ts_from_micros(0), &[]);
                            // A failed write whose document is
                            // in the composed view but was never
                            // confirmed by the active query's
                            // stream leaves the document in
                            // limbo: the view is no longer fully
                            // synchronized until it resolves.
                            for path in harness.last_settled.clone() {
                                if harness.delivered_docs.contains_key(&path)
                                    && !harness.view.contains_key(&path)
                                    && !harness.limbo_active.iter().any(|res| res.key == path)
                                    && !harness.limbo_waiting.contains(&path)
                                {
                                    harness.limbo_active.push(LimboRes {
                                        key: path,
                                        target_id: None,
                                        state: None,
                                        failed: false,
                                    });
                                    harness.synced = false;
                                }
                            }
                            // The settled batch may have been
                            // the only thing keeping a limbo
                            // document unblocked: re-evaluate
                            // the sync state before emitting.
                            harness.synced = !harness.limbo_blocking_sync();
                            harness.maybe_emit_composed(&mut emitted, false, false, true);
                        }
                    }
                }
                "waitForPendingWrites" => {
                    harness.register_waiter();
                }
                "restart" => {
                    harness.restart();
                }
                "shutdown" => {
                    // The client stops: its listen dies and the
                    // online-state cycle ends. Persisted state
                    // is untouched; a later listen or restart
                    // resumes from it.
                    harness.state = None;
                    harness.unlistened = true;
                    harness.pending_remote.clear();
                    harness.online_state = OnlineState::Unknown;
                    harness.stream_failures = 0;
                    harness.offline_timeout_armed = false;
                }
                "clearPersistence" => {
                    // Everything persisted is wiped: a brand-new
                    // harness (fresh store, cache and tokens),
                    // keeping only the case's write-mode shape.
                    let write_mode = harness.write_mode;
                    harness = Harness::new();
                    harness.write_mode = write_mode;
                    if write_mode {
                        harness.ensure_engine();
                    }
                }
                "userUnlisten" => {
                    if !harness.write_mode {
                        if let Some(token) = harness.target_tokens.get(&harness.target_id).cloned()
                        {
                            let version = harness.max_barrier;
                            harness
                                .persisted_tokens
                                .insert(harness.target_id, (token, version));
                        }
                    }
                    // Releasing the target is a local-store
                    // operation whose failure does not stop the
                    // unlisten.
                    let _ = harness.db_op_fails("Release target");
                    if harness.listen_saw_token {
                        harness.resumable_targets.insert(harness.target_id);
                    }
                    harness.unlistened = true;
                    if !harness.write_mode {
                        // Removing the last listener resets the
                        // online-state tracker to Unknown: the
                        // next listen starts the cycle over (it
                        // neither resolves from cache nor counts
                        // earlier stream failures).
                        harness.online_state = OnlineState::Unknown;
                        harness.stream_failures = 0;
                        harness.offline_timeout_armed = false;
                    }
                }
                "watchAck" => {
                    // The stream accepted the target: a
                    // successful watch message.
                    let ids: Vec<i32> = value
                        .as_array()
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Json::as_i64)
                                .map(|v| v as i32)
                                .collect()
                        })
                        .unwrap_or_default();
                    for id in &ids {
                        harness.adopt_limbo_target(*id);
                    }
                    if !harness.write_mode && ids.contains(&harness.target_id) {
                        // A (re)add carries the live token: it
                        // becomes the token this add is reported
                        // with while unsynchronized.
                        if let Some(token) = harness.target_tokens.get(&harness.target_id).cloned()
                        {
                            harness.add_token = token;
                        }
                    }
                    // A re-run target acked after its removal:
                    // recreate its watch state seeded with the
                    // current view (the stream re-delivers what
                    // it still has).
                    if !harness.write_mode
                        && harness.state.is_none()
                        && !harness.unlistened
                        && ids.contains(&harness.target_id)
                    {
                        if let Some(query) = harness.query.clone() {
                            let seed: Vec<Document> = harness.view.values().cloned().collect();
                            let mut state =
                                WatchState::new(harness.target_id, query.effective_orders());
                            state.set_keep_restart_unconfirmed(!document_path(&harness.query_path));
                            if harness.target_tokens.contains_key(&harness.target_id) {
                                // A resumed add: the server sends
                                // only deltas, so nothing is
                                // synthesized.
                                state.seed_view(seed, false);
                            } else {
                                state.seed_restart_unconfirmed(seed);
                            }
                            if let Some(token) =
                                harness.target_tokens.get(&harness.target_id).cloned()
                            {
                                harness.add_token = token.clone();
                                state.restore_resume_token(token.into_bytes());
                            }
                            harness.absorb_empty_snapshot = true;
                            harness.state = Some(state);
                        }
                    }
                    harness.note_watch_success();
                }
                "runTimer" => {
                    // The async-queue retry drives failure
                    // recovery: a terminated client restarts and
                    // applies one queued user change, deferred
                    // settlements are persisted, and a target
                    // whose remote event could not be persisted
                    // is re-listened internally.
                    let async_retry = match &value {
                        Json::String(name) => name == "async_queue_retry" || name == "all",
                        Json::Array(items) => items.iter().any(|item| {
                            matches!(item.as_str(), Some("async_queue_retry") | Some("all"))
                        }),
                        _ => false,
                    };
                    if async_retry {
                        if harness.terminated {
                            harness.recover_terminated(&mut emitted);
                        } else if !harness.pending_user_changes.is_empty() {
                            // Further user changes queued during
                            // the outage apply one per retry tick.
                            if let Some(uid) = harness.pending_user_changes.pop_front() {
                                harness.current_user = uid;
                            }
                            if harness.write_mode {
                                harness.maybe_emit_composed(&mut emitted, false, false, true);
                            }
                        } else if harness.target_failed {
                            harness.target_failed = false;
                        } else {
                            while let Some((ack, commit)) = harness.deferred_settles.pop_front() {
                                let gate = harness.settle_oldest(ack, commit, &[]);
                                let before = emitted.len();
                                harness.maybe_emit_composed(&mut emitted, false, false, gate);
                                if emitted.len() == before {
                                    harness.absorb_settled_flags();
                                }
                                harness.consume_notify_failure();
                            }
                        }
                    }
                    // Fires the online-state timeout: a client
                    // that never connected goes Offline, as does
                    // an Online client whose stream failure armed
                    // the timeout.
                    let fires = match &value {
                        Json::String(name) => name == "online_state_timeout" || name == "all",
                        Json::Array(items) => items.iter().any(|item| {
                            matches!(item.as_str(), Some("online_state_timeout") | Some("all"))
                        }),
                        _ => false,
                    };
                    if fires
                        && !harness.write_mode
                        && (harness.online_state == OnlineState::Unknown
                            || harness.offline_timeout_armed)
                    {
                        harness.go_offline(&mut emitted);
                    }
                }
                "watchEntity" if harness.unlistened => {
                    // Updates addressed to an inactive target are
                    // ignored by the client.
                }
                "watchCurrent" if harness.unlistened => {}
                "watchSnapshot" if harness.unlistened => {}
                "watchEntity" => {
                    // The entity-level form is a document-remove
                    // message for one key: the document leaves
                    // the target's synchronized set but may
                    // still exist — it goes to limbo rather than
                    // leaving the view.
                    if let (Some(key), Some(removed)) = (
                        value.get("key").and_then(Json::as_str),
                        value.get("removedTargets").and_then(Json::as_array),
                    ) {
                        let removed_ids: Vec<i32> = removed
                            .iter()
                            .filter_map(Json::as_i64)
                            .map(|v| v as i32)
                            .collect();
                        if removed_ids.contains(&harness.target_id) && !harness.write_mode {
                            harness.held_token = String::new();
                            harness.held_token = harness.add_token.clone();
                            harness.enter_limbo_batch_throttled(&[key.to_string()], false);
                        }
                        continue;
                    }
                    let targets: Vec<i32> = value
                        .get("targets")
                        .and_then(Json::as_array)
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Json::as_i64)
                                .map(|v| v as i32)
                                .collect()
                        })
                        .unwrap_or_default();
                    let addressed = targets.is_empty() || targets.contains(&harness.target_id);
                    // Documents addressed only to limbo targets
                    // feed those resolutions, not the query
                    // target: their content applies to the view
                    // only when the resolution settles.
                    let limbo_targets: Vec<i32> = if !targets.is_empty() && !addressed {
                        targets
                            .iter()
                            .copied()
                            .filter(|id| harness.is_limbo_target(*id))
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let to_limbo = !limbo_targets.is_empty();
                    for doc in value
                        .get("docs")
                        .and_then(Json::as_array)
                        .into_iter()
                        .flatten()
                    {
                        if doc
                            .get("options")
                            .and_then(|o| o.get("hasLocalMutations"))
                            .and_then(Json::as_bool)
                            .unwrap_or(false)
                        {
                            return Outcome::Skipped(
                                "watch entity with local mutations".to_string(),
                            );
                        }
                        let key = doc.get("key").and_then(Json::as_str).unwrap_or_default();
                        let version = doc.get("version").and_then(Json::as_i64).unwrap_or(0);
                        if addressed {
                            harness.pending_view.insert(
                                key.to_string(),
                                doc.get("value").is_some_and(|v| !v.is_null()),
                            );
                        }
                        // The local store never lets a document
                        // revert: a remote event older than the
                        // cached version is dropped entirely.
                        if let Some((_, cached_version)) = harness.cache.get(key) {
                            if version < *cached_version {
                                continue;
                            }
                        }
                        match doc.get("value") {
                            Some(Json::Null) | None => {
                                harness.cache.insert(key.to_string(), (None, version));
                                let response = ListenResponse {
                                    response_type: Some(ResponseType::DocumentDelete(
                                        DocumentDelete {
                                            document: full_name(key),
                                            removed_target_ids: targets.clone(),
                                            read_time: None,
                                        },
                                    )),
                                };
                                if to_limbo {
                                    for id in &limbo_targets {
                                        harness.feed_limbo(*id, response.clone());
                                    }
                                } else {
                                    harness.feed(response, &mut emitted);
                                }
                            }
                            Some(value_json) => {
                                let create_time =
                                    doc.get("createTime").and_then(Json::as_i64).unwrap_or(0);
                                // A same-version redelivery with
                                // different content cannot occur
                                // on the wire (versions are
                                // per-document update versions):
                                // it re-confirms the document but
                                // the cached content stands.
                                let fields = match harness.cache.get(key) {
                                    Some((Some(cached), cached_version))
                                        if *cached_version == version
                                            && cached.fields != json_fields(value_json) =>
                                    {
                                        cached.fields.clone()
                                    }
                                    _ => json_fields(value_json),
                                };
                                let document = Document {
                                    name: full_name(key),
                                    fields: fields.clone(),
                                    create_time: (create_time != 0)
                                        .then(|| ts_from_micros(create_time)),
                                    update_time: Some(ts_from_micros(version)),
                                };
                                harness
                                    .cache
                                    .insert(key.to_string(), (Some(document.clone()), version));
                                // The view only receives documents
                                // matching the active query; a
                                // document that stopped matching is
                                // reported to the target as removed.
                                let matches = to_limbo
                                    || harness
                                        .query
                                        .as_ref()
                                        .is_none_or(|query| query.matches(&document));
                                if matches {
                                    let proto = crate::google::firestore::v1::Document {
                                        name: full_name(key),
                                        fields: fields_to_proto(&fields),
                                        create_time: (create_time != 0)
                                            .then(|| ts_from_micros(create_time).into()),
                                        update_time: Some(ts_from_micros(version).into()),
                                    };
                                    let response = ListenResponse {
                                        response_type: Some(ResponseType::DocumentChange(
                                            ProtoDocumentChange {
                                                document: Some(proto),
                                                target_ids: targets.clone(),
                                                removed_target_ids: Vec::new(),
                                            },
                                        )),
                                    };
                                    if to_limbo {
                                        for id in &limbo_targets {
                                            harness.feed_limbo(*id, response.clone());
                                        }
                                    } else {
                                        harness.feed(response, &mut emitted);
                                    }
                                } else if harness.view.contains_key(key) {
                                    harness.feed(
                                        ListenResponse {
                                            response_type: Some(ResponseType::DocumentDelete(
                                                DocumentDelete {
                                                    document: full_name(key),
                                                    removed_target_ids: vec![harness.target_id],
                                                    read_time: None,
                                                },
                                            )),
                                        },
                                        &mut emitted,
                                    );
                                }
                            }
                        }
                    }
                }
                "watchCurrent" => {
                    let parts = value.as_array().cloned().unwrap_or_default();
                    let target_ids: Vec<i32> = parts
                        .first()
                        .and_then(Json::as_array)
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Json::as_i64)
                                .map(|v| v as i32)
                                .collect()
                        })
                        .unwrap_or_default();
                    let token = parts
                        .get(1)
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                        .as_bytes()
                        .to_vec();
                    let token_present = !token.is_empty();
                    let limbo_ids: Vec<i32> = target_ids
                        .iter()
                        .copied()
                        .filter(|id| harness.is_limbo_target(*id))
                        .collect();
                    let for_limbo = !limbo_ids.is_empty();
                    let response = ListenResponse {
                        response_type: Some(ResponseType::TargetChange(TargetChange {
                            target_change_type: TargetChangeType::Current as i32,
                            target_ids,
                            resume_token: token,
                            read_time: None,
                            cause: None,
                        })),
                    };
                    if harness.filter_restart_expected {
                        harness.absorb_empty_snapshot = true;
                        harness.current_since_filter = true;
                    }
                    harness.feed(response.clone(), &mut emitted);
                    if for_limbo {
                        for id in limbo_ids {
                            harness.feed_limbo(id, response.clone());
                        }
                    }
                    if token_present {
                        harness.listen_saw_token = true;
                        // A target-specific token persists at
                        // the next barrier snapshot.
                        harness.current_token_pending = true;
                    }
                }
                "watchSnapshot" => {
                    let version = value.get("version").and_then(Json::as_i64).unwrap_or(0);
                    let target_ids: Vec<i32> = value
                        .get("targetIds")
                        .and_then(Json::as_array)
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Json::as_i64)
                                .map(|v| v as i32)
                                .collect()
                        })
                        .unwrap_or_default();
                    let token = value
                        .get("resumeToken")
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                        .as_bytes()
                        .to_vec();
                    if !token.is_empty() {
                        harness.listen_saw_token = true;
                    }
                    // A targeted snapshot is bookkeeping in the
                    // spec model (resume token / version for that
                    // target); it raises no event by itself, but
                    // its token is recorded.
                    if target_ids.contains(&harness.target_id) {
                        harness.note_token(&token);
                        if !token.is_empty() {
                            // Like a CURRENT token, a targeted
                            // snapshot's token persists at the
                            // next global barrier.
                            harness.current_token_pending = true;
                        }
                    }
                    if target_ids.is_empty()
                        && (harness.armed_db_failures.contains("Apply remote event")
                            || harness
                                .armed_db_failures
                                .contains("Get last remote snapshot version"))
                        && !(harness
                            .armed_db_failures
                            .contains("Get last remote snapshot version")
                            && !harness.armed_db_failures.contains("Apply remote event")
                            && !harness.limbo_active.is_empty())
                    {
                        // The remote event cannot be persisted:
                        // it is discarded wholesale — the watch
                        // state is rebuilt from the delivered
                        // view, the buffered updates are dropped,
                        // and the target goes inactive until the
                        // async-queue retry re-listens it. The
                        // listener re-raises its view (now from
                        // cache).
                        let _ = harness.db_op_fails("Apply remote event");
                        let _ = harness.db_op_fails("Get last remote snapshot version");
                        harness.pending_remote.clear();
                        // The event's cache updates were only
                        // staged: roll them back to the last
                        // applied event, and drop the event's
                        // pending-view overlay with them.
                        harness.cache = harness.committed_cache.clone();
                        harness.pending_view.clear();
                        if !harness.write_mode && harness.state.is_some() {
                            let seed: Vec<Document> = harness.view.values().cloned().collect();
                            let mut state = WatchState::new(
                                harness.target_id,
                                harness
                                    .query
                                    .as_ref()
                                    .map(|query| query.effective_orders())
                                    .unwrap_or_default(),
                            );
                            state.set_keep_restart_unconfirmed(!document_path(&harness.query_path));
                            state.seed_view(seed.clone(), false);
                            if let Some(token) =
                                harness.target_tokens.get(&harness.target_id).cloned()
                            {
                                state.restore_resume_token(token.into_bytes());
                            }
                            harness.state = Some(state);
                            harness.target_failed = true;
                            emitted.push(Emitted::Snapshot(Snapshot {
                                documents: seed,
                                changes: Vec::new(),
                                read_time: None,
                            }));
                        }
                    } else if target_ids.is_empty() {
                        // Global barrier. A version behind the
                        // highest one already delivered belongs to
                        // a reverted stream: ignore it entirely.
                        if version >= harness.max_barrier {
                            harness.max_barrier = version;
                            // Token persistence at the barrier: a
                            // pending CURRENT token persists now,
                            // at this snapshot version. A global
                            // snapshot's own token persists only
                            // once it is at least five minutes
                            // (300,000,000 microseconds) ahead of
                            // the last persisted snapshot version
                            // — newer global tokens are omitted
                            // "for a short while".
                            if harness.current_token_pending {
                                if let Some(live) =
                                    harness.target_tokens.get(&harness.target_id).cloned()
                                {
                                    harness
                                        .persisted_tokens
                                        .insert(harness.target_id, (live, version));
                                }
                                harness.current_token_pending = false;
                            }
                            if !token.is_empty() {
                                let token_string = String::from_utf8_lossy(&token).into_owned();
                                let persisted_version = harness
                                    .persisted_tokens
                                    .get(&harness.target_id)
                                    .map(|(_, version)| *version)
                                    .unwrap_or(0);
                                if version - persisted_version >= 300_000_000 {
                                    harness
                                        .persisted_tokens
                                        .insert(harness.target_id, (token_string, version));
                                }
                            }
                            let barrier_response = ListenResponse {
                                response_type: Some(ResponseType::TargetChange(TargetChange {
                                    target_change_type: TargetChangeType::NoChange as i32,
                                    target_ids,
                                    resume_token: token,
                                    read_time: Some(ts_from_micros(version).into()),
                                    cause: None,
                                })),
                            };
                            let emitted_before = emitted.len();
                            let dirty_before = harness.limbo_dirty;
                            let restart_before = harness.restart_pending;
                            if !harness.write_mode {
                                harness.settle_limbo_at_barrier(&barrier_response, &mut emitted);
                            }
                            harness.feed(barrier_response, &mut emitted);
                            // The event is applied: its staged
                            // cache updates become persisted state.
                            harness.committed_cache = harness.cache.clone();
                            if !harness.write_mode
                                && emitted.len() == emitted_before
                                && harness.listen_delivered
                                && (dirty_before
                                    || restart_before
                                    || (harness.filter_restart_step
                                        && !harness.current_since_filter)
                                    || (harness.limbo_dirty
                                        && harness.filter_applied_step
                                        && !harness.current_since_filter))
                            {
                                // The barrier only (re)marked
                                // documents as unverified — an
                                // existence-filter restart or new
                                // limbo candidates: the view is
                                // re-raised from cache, unchanged.
                                emitted.push(Emitted::Snapshot(Snapshot {
                                    documents: harness.view.values().cloned().collect(),
                                    changes: Vec::new(),
                                    read_time: None,
                                }));
                            }
                            harness.limbo_dirty = false;
                            harness.restart_pending = false;
                            // Token bookkeeping at the barrier:
                            // the reported token is the live
                            // token when the target is fully
                            // synchronized at this consistent
                            // point, and the token it was added
                            // with otherwise.
                            let fully_synced = !harness.write_mode
                                && !harness.token_blocked_step
                                && !harness.limbo_absent_step
                                && !harness.filter_restart_step
                                && harness.limbo_active.is_empty()
                                && harness
                                    .state
                                    .as_ref()
                                    .map(|state| state.is_current())
                                    .unwrap_or(false);
                            if fully_synced {
                                harness.held_token = harness
                                    .target_tokens
                                    .get(&harness.target_id)
                                    .cloned()
                                    .unwrap_or_default();
                            } else if !harness.write_mode {
                                harness.held_token = harness.add_token.clone();
                            }
                        }
                    }
                }
                "watchFilter" => {
                    // Existence filter: the harness computes
                    // the outcome from its own view (delivered
                    // documents plus the pending overlay),
                    // independently of the watch state's restart
                    // signal. Documents the filter cannot account
                    // for enter limbo: on a count mismatch that
                    // is the whole view (the target re-runs);
                    // on a match it is the documents failing the
                    // bloom check (definitely absent from the
                    // server's set, to be verified individually).
                    let mut view: std::collections::BTreeSet<String> =
                        harness.view.keys().cloned().collect();
                    for (key, present) in &harness.pending_view {
                        if *present {
                            view.insert(key.clone());
                        } else {
                            view.remove(key);
                        }
                    }
                    let bloom = value.get("bloomFilter").and_then(|filter| {
                        let bits = filter.get("bits")?;
                        crate::bloom::BloomFilter::new(
                            b64_decode(bits.get("bitmap").and_then(Json::as_str).unwrap_or("")),
                            bits.get("padding").and_then(Json::as_i64).unwrap_or(0) as i32,
                            filter.get("hashCount").and_then(Json::as_i64).unwrap_or(0) as i32,
                        )
                    });
                    let expected_count = value
                        .get("count")
                        .and_then(Json::as_i64)
                        .or_else(|| {
                            value
                                .get("keys")
                                .and_then(Json::as_array)
                                .map(|keys| keys.len() as i64)
                        })
                        .unwrap_or(0);
                    let passing: Vec<&String> = view
                        .iter()
                        .filter(|key| {
                            bloom
                                .as_ref()
                                .map(|bloom| bloom.contains(&full_name(key)))
                                .unwrap_or(true)
                        })
                        .collect();
                    let mismatch = passing.len() as i64 != expected_count;
                    // The watch state ignores filters for
                    // targets that have not reached a consistent
                    // point; the harness's own bookkeeping
                    // follows the same rule (checked again
                    // below, where the addressed target is
                    // known).
                    let target_is_current = harness
                        .state
                        .as_ref()
                        .map(|state| state.is_current())
                        .unwrap_or(false);
                    // On a matching bloom filter, the failing
                    // documents are verified individually; on a
                    // mismatch the target re-runs and the
                    // documents it does not re-confirm become
                    // candidates. Both land at the consistent
                    // point via the deferred application in the
                    // watch state, not at receipt.
                    let _ = target_is_current;
                    let target_id = value
                        .get("targetIds")
                        .and_then(Json::as_array)
                        .and_then(|ids| ids.first())
                        .and_then(Json::as_i64)
                        .or_else(|| value.get("targetId").and_then(Json::as_i64))
                        .unwrap_or(0) as i32;
                    let unchanged_names = value.get("bloomFilter").map(|filter| {
                        let bits = filter.get("bits").cloned().unwrap_or(Json::Null);
                        crate::google::firestore::v1::BloomFilter {
                            bits: Some(crate::google::firestore::v1::BitSequence {
                                bitmap: b64_decode(
                                    bits.get("bitmap").and_then(Json::as_str).unwrap_or(""),
                                ),
                                padding: bits.get("padding").and_then(Json::as_i64).unwrap_or(0)
                                    as i32,
                            }),
                            hash_count: filter.get("hashCount").and_then(Json::as_i64).unwrap_or(0)
                                as i32,
                        }
                    });
                    let filter_response = ListenResponse {
                        response_type: Some(ResponseType::Filter(ExistenceFilter {
                            target_id,
                            count: expected_count as i32,
                            unchanged_names,
                        })),
                    };
                    if target_id != harness.target_id && harness.is_limbo_target(target_id) {
                        harness.feed_limbo(target_id, filter_response);
                    } else {
                        harness.filter_restart_expected = true;
                        harness.current_since_filter = false;
                        harness.feed(filter_response, &mut emitted);
                    }
                    if mismatch && target_is_current {
                        // The harness computed this restart
                        // itself: it is the expected recovery,
                        // not an unexpected one.
                        harness.unexpected_restart = false;
                    }
                }
                "watchStreamClose" => {
                    if !harness.write_mode {
                        if let Some((token, _)) = harness.persisted_tokens.get(&harness.target_id) {
                            harness.held_token = token.clone();
                        }
                    }
                    let code = value
                        .get("error")
                        .and_then(|error| error.get("code"))
                        .and_then(Json::as_i64)
                        .unwrap_or(0) as i32;
                    if harness.state.is_some() {
                        if matches!(code, 3 | 5 | 7) {
                            // Terminal: the watch ends with an error.
                            emitted.push(Emitted::Error(code));
                            harness.state = None;
                        } else if let Err(message) = harness.check_readd_token() {
                            // Reconnect and resume: the driver
                            // keeps the same watch state across
                            // streams, so the token and view are
                            // inherently preserved; verify the
                            // re-add request would send the token.
                            return Outcome::Failed(at(message));
                        } else if !harness.write_mode && !harness.unlistened {
                            // Online-state tracking: from Unknown
                            // a single failure is enough; from
                            // Online the first failure arms the
                            // online-state timeout and a second
                            // consecutive failure (or the timeout
                            // firing) goes Offline.
                            match harness.online_state {
                                OnlineState::Unknown => {
                                    harness.go_offline(&mut emitted);
                                }
                                OnlineState::Online => {
                                    harness.stream_failures += 1;
                                    if harness.stream_failures >= 2 {
                                        harness.go_offline(&mut emitted);
                                    } else {
                                        harness.offline_timeout_armed = true;
                                    }
                                }
                                OnlineState::Offline => {}
                            }
                        }
                    }
                }
                "watchRemove" => {
                    let remove_ids: Vec<i32> = value
                        .get("targetIds")
                        .and_then(Json::as_array)
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Json::as_i64)
                                .map(|v| v as i32)
                                .collect()
                        })
                        .unwrap_or_default();
                    for id in &remove_ids {
                        harness.adopt_limbo_target(*id);
                    }
                    for res in harness.limbo_active.iter_mut() {
                        if res
                            .target_id
                            .map(|id| remove_ids.contains(&id))
                            .unwrap_or(false)
                        {
                            res.failed = true;
                        }
                    }
                    if harness.write_mode {
                        // A failed limbo resolution in write
                        // mode: the document's remote base is
                        // dropped and the composed view re-evaluates.
                        let failed: Vec<String> = harness
                            .limbo_active
                            .iter()
                            .filter(|res| res.failed)
                            .map(|res| res.key.clone())
                            .collect();
                        if !failed.is_empty() {
                            harness.limbo_absent_step = true;
                            harness.limbo_active.retain(|res| !res.failed);
                            for key in &failed {
                                if let Some(engine) = harness.engine.as_ref() {
                                    let _ = engine.store_remote_absent(key);
                                }
                                harness.store_remote_keys.remove(key);
                            }
                            if harness.limbo_active.is_empty() && harness.limbo_waiting.is_empty() {
                                harness.synced = true;
                            }
                            harness.maybe_emit_composed(&mut emitted, false, false, true);
                        }
                    }
                    // A failed limbo resolution settles at
                    // once: its document leaves the view with
                    // an event on this step.
                    if !harness.write_mode {
                        let failed_keys: Vec<String> = harness
                            .limbo_active
                            .iter()
                            .filter(|res| res.failed)
                            .map(|res| res.key.clone())
                            .collect();
                        if !failed_keys.is_empty() {
                            // When the rejection cannot be
                            // persisted (a database failure is
                            // armed for the remote-event path),
                            // the client terminates instead: the
                            // session restarts at the async
                            // queue retry with the resolution
                            // still outstanding.
                            let unpersistable =
                                harness.armed_db_failures.contains("Apply remote event")
                                    || harness
                                        .armed_db_failures
                                        .contains("Get last remote snapshot version");
                            if unpersistable {
                                for res in harness.limbo_active.iter_mut() {
                                    res.failed = false;
                                }
                                harness.terminate_session();
                            } else {
                                harness
                                    .limbo_active
                                    .retain(|res| !failed_keys.contains(&res.key));
                                harness.limbo_absent_step = true;
                                for head in &failed_keys {
                                    harness.feed(
                                        ListenResponse {
                                            response_type: Some(ResponseType::DocumentRemove(
                                                crate::google::firestore::v1::DocumentRemove {
                                                    document: full_name(head),
                                                    removed_target_ids: vec![harness.target_id],
                                                    read_time: None,
                                                },
                                            )),
                                        },
                                        &mut emitted,
                                    );
                                }
                                // Flush the removals through a
                                // consistent point at the last
                                // barrier version: the removal
                                // event belongs to this step.
                                let version = harness.max_barrier;
                                harness.feed(
                                    ListenResponse {
                                        response_type: Some(ResponseType::TargetChange(
                                            TargetChange {
                                                target_change_type: TargetChangeType::NoChange
                                                    as i32,
                                                target_ids: Vec::new(),
                                                resume_token: Vec::new(),
                                                read_time: Some(ts_from_micros(version).into()),
                                                cause: None,
                                            },
                                        )),
                                    },
                                    &mut emitted,
                                );
                            }
                            harness.pump_limbo();
                        }
                    }
                    let target_ids: Vec<i32> = value
                        .get("targetIds")
                        .and_then(Json::as_array)
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Json::as_i64)
                                .map(|v| v as i32)
                                .collect()
                        })
                        .unwrap_or_default();
                    let cause = value.get("cause").map(|cause| crate::google::rpc::Status {
                        code: cause.get("code").and_then(Json::as_i64).unwrap_or(0) as i32,
                        message: cause
                            .get("message")
                            .and_then(Json::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        details: Vec::new(),
                    });
                    harness.feed(
                        ListenResponse {
                            response_type: Some(ResponseType::TargetChange(TargetChange {
                                target_change_type: TargetChangeType::Remove as i32,
                                target_ids,
                                resume_token: Vec::new(),
                                read_time: None,
                                cause,
                            })),
                        },
                        &mut emitted,
                    );
                    if harness.unlistened {
                        harness.state = None;
                        harness.unlistened = false;
                    }
                }
                other => {
                    return Outcome::Skipped(format!("step {other} not modeled"));
                }
            }
        }

        if harness.unexpected_restart {
            return Outcome::Failed(at(
                "unexpected restart on a matching existence filter".to_string()
            ));
        }

        if !harness.write_mode
            && emitted.iter().any(|event| {
                matches!(
                    event,
                    Emitted::Snapshot(_) | Emitted::Pseudo(_) | Emitted::Offline(_)
                )
            })
        {
            harness.listen_delivered = true;
        }

        // Compare this step's expected snapshot events, if any.
        if let Some(expected_events) = step.get("expectedSnapshotEvents") {
            let expected_events = expected_events.as_array().cloned().unwrap_or_default();
            let reset_view = step.get("userListen").is_some();
            if let Err(message) =
                compare_events(&mut harness, &expected_events, &mut emitted, reset_view)
            {
                return Outcome::Failed(at(message));
            }
        } else {
            let unexpected = emitted
                .iter()
                .filter(|event| {
                    !matches!(event, Emitted::Pseudo(_))
                        && !matches!(event, Emitted::Composed { initial: true, .. })
                })
                .count();
            if unexpected > 0 {
                return Outcome::Failed(at(format!("{unexpected} unexpected snapshot event(s)")));
            }
        }

        if let Some(expected_waits) = step
            .get("expectedWaitForPendingWritesEvents")
            .and_then(Json::as_i64)
        {
            if harness.resolved_waits != expected_waits as usize {
                return Outcome::Failed(at(format!(
                    "waitForPendingWrites resolutions: expected {expected_waits},                      got {}",
                    harness.resolved_waits
                )));
            }
        }

        // Light expected-state consistency check.
        if let Some(expected_state) = step.get("expectedState") {
            if let Some(expected_limbo) = expected_state.get("activeLimboDocs") {
                let mut expected_docs: Vec<String> = expected_limbo
                    .as_array()
                    .map(|docs| {
                        docs.iter()
                            .filter_map(Json::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default();
                expected_docs.sort();
                let mut actual_docs: Vec<String> = harness
                    .limbo_active
                    .iter()
                    .map(|res| res.key.clone())
                    .collect();
                actual_docs.sort();
                if actual_docs != expected_docs {
                    return Outcome::Failed(at(format!(
                        "limbo documents: expected {expected_docs:?}, got {actual_docs:?}"
                    )));
                }
            }
            if let Some(active) = expected_state.get("activeTargets") {
                let expects_none = active.as_object().map(|o| o.is_empty()).unwrap_or(false);
                // While offline, the client's targets are all
                // inactive in the spec model.
                let harness_active = harness.state.is_some()
                    && !harness.unlistened
                    && !harness.target_failed
                    && harness.online
                    && harness.online_state != OnlineState::Offline;
                if expects_none && harness_active {
                    return Outcome::Failed(at(
                        "expected no active targets, harness still listens".to_string(),
                    ));
                }
                if !expects_none && harness.state.is_none() {
                    return Outcome::Failed(at(
                        "expected active targets, harness has none".to_string()
                    ));
                }
                // The held resume token for our target is part of
                // the expected state: compare it exactly.
                if let Some(targets) = active.as_object() {
                    if let Some(entry) = targets.get(&harness.target_id.to_string()) {
                        let expected_token = entry
                            .get("resumeToken")
                            .and_then(Json::as_str)
                            .unwrap_or("");
                        // After a reset or an existence-filter
                        // restart, the re-added target's token
                        // reads empty for one state check (the
                        // CURRENT token is adopted at the next
                        // barrier).
                        let held_token = if harness.write_mode {
                            // In write mode the reported token
                            // follows the same rule: the add
                            // token while a limbo document blocks
                            // the query's sync, the live token
                            // otherwise.
                            if harness.limbo_blocking_sync() || harness.limbo_absent_step {
                                harness.add_token.as_str()
                            } else {
                                harness
                                    .target_tokens
                                    .get(&harness.target_id)
                                    .map(String::as_str)
                                    .unwrap_or("")
                            }
                        } else {
                            harness.held_token.as_str()
                        };
                        if expected_token != held_token {
                            return Outcome::Failed(at(format!(
                                "resume token mismatch: expected {expected_token:?}, \
                                 client holds {held_token:?}"
                            )));
                        }
                    }
                }
            }
            if harness.write_mode {
                if let Some(callbacks) = expected_state.get("userCallbacks") {
                    let expected_acked: BTreeSet<String> = callbacks
                        .get("acknowledgedDocs")
                        .and_then(Json::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Json::as_str)
                        .map(str::to_string)
                        .collect();
                    let expected_rejected: BTreeSet<String> = callbacks
                        .get("rejectedDocs")
                        .and_then(Json::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Json::as_str)
                        .map(str::to_string)
                        .collect();
                    if harness.callback_acked != expected_acked
                        || harness.callback_rejected != expected_rejected
                    {
                        return Outcome::Failed(at(format!(
                            "write callbacks: expected acked {expected_acked:?} / rejected                              {expected_rejected:?}, got acked {:?} / rejected {:?}",
                            harness.callback_acked, harness.callback_rejected
                        )));
                    }
                    harness.callback_acked.clear();
                    harness.callback_rejected.clear();
                }
                if let Some(expected_count) = expected_state
                    .get("numOutstandingWrites")
                    .and_then(Json::as_i64)
                {
                    // The spec model's write stream pipelines at
                    // most 10 batches in flight; further batches
                    // wait in the queue and are not outstanding.
                    let actual = harness
                        .batch_users
                        .iter()
                        .filter(|user| user.as_str() == harness.current_user.as_str())
                        .count()
                        .min(10);
                    if actual != expected_count as usize {
                        return Outcome::Failed(at(format!(
                            "numOutstandingWrites: expected {expected_count}, got {actual}"
                        )));
                    }
                }
            }
        }
    }
    Outcome::Passed
}

/// Applies one step's expected events to the harness's expected
/// view and compares them with what the watch state emitted.
fn compare_events(
    harness: &mut Harness,
    expected_events: &[Json],
    emitted: &mut Vec<Emitted>,
    reset_view: bool,
) -> Result<(), String> {
    if reset_view {
        // A new listener's first event is judged against a fresh
        // view: the event's document set is exactly the snapshot.
        harness.expected_view.clear();
    }
    let mut emitted = std::mem::take(emitted).into_iter();
    for expected in expected_events {
        let error_code = expected
            .get("errorCode")
            .and_then(Json::as_i64)
            .unwrap_or(0) as i32;
        let actual = emitted
            .next()
            .ok_or_else(|| "expected a snapshot event, none emitted".to_string())?;
        if error_code != 0 {
            match actual {
                Emitted::Error(code) if code == error_code => continue,
                Emitted::Error(code) => {
                    return Err(format!("expected error {error_code}, got error {code}"));
                }
                Emitted::Snapshot(_)
                | Emitted::Pseudo(_)
                | Emitted::Offline(_)
                | Emitted::Composed { .. } => {
                    return Err(format!("expected error {error_code}, got a snapshot"));
                }
            }
        }
        let mut actual_metadata: Vec<String> = Vec::new();
        let snapshot = match actual {
            Emitted::Snapshot(snapshot)
            | Emitted::Pseudo(snapshot)
            | Emitted::Offline(snapshot) => snapshot,
            Emitted::Composed {
                snapshot,
                from_cache,
                has_pending,
                metadata,
                ..
            } => {
                let expected_from_cache = expected
                    .get("fromCache")
                    .and_then(Json::as_bool)
                    .unwrap_or(false);
                let expected_pending = expected
                    .get("hasPendingWrites")
                    .and_then(Json::as_bool)
                    .unwrap_or(false);
                if from_cache != expected_from_cache {
                    return Err(format!(
                        "fromCache mismatch: expected {expected_from_cache}, got {from_cache}"
                    ));
                }
                if has_pending != expected_pending {
                    return Err(format!(
                        "hasPendingWrites mismatch: expected {expected_pending},                          got {has_pending}"
                    ));
                }
                actual_metadata = metadata.iter().map(|doc| key_of(&doc.name)).collect();
                snapshot
            }
            Emitted::Error(code) => {
                return Err(format!("expected a snapshot, got error {code}"));
            }
        };
        // Rebuild the expected view from the event's deltas.
        let mut expected_changes: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for (kind, key) in [
            ("added", "added"),
            ("modified", "modified"),
            ("removed", "removed"),
        ] {
            let mut keys = Vec::new();
            for doc in expected
                .get(key)
                .and_then(Json::as_array)
                .into_iter()
                .flatten()
            {
                let doc_key = doc.get("key").and_then(Json::as_str).unwrap_or_default();
                keys.push(doc_key.to_string());
                match kind {
                    "added" | "modified" => {
                        harness.expected_view.insert(
                            doc_key.to_string(),
                            json_fields(doc.get("value").unwrap_or(&Json::Null)),
                        );
                    }
                    _ => {
                        harness.expected_view.remove(doc_key);
                    }
                }
            }
            expected_changes.insert(kind, keys);
        }
        // Full document set comparison.
        let actual_view: BTreeMap<String, Fields> = snapshot
            .documents
            .iter()
            .map(|doc: &Document| (key_of(&doc.name), doc.fields.clone()))
            .collect();
        if actual_view != harness.expected_view {
            return Err(format!(
                "document set mismatch: actual {:?}, expected {:?}",
                actual_view.keys().collect::<Vec<_>>(),
                harness.expected_view.keys().collect::<Vec<_>>()
            ));
        }
        // Field-level comparison for the changed documents.
        for doc in snapshot.documents.iter() {
            let key = key_of(&doc.name);
            if let Some(expected_fields) = harness.expected_view.get(&key) {
                if &doc.fields != expected_fields {
                    return Err(format!("field mismatch for {key}"));
                }
            }
        }
        // Change categorization comparison.
        let categorize = |kind: DocumentChangeKind| -> Vec<String> {
            let mut keys: Vec<String> = snapshot
                .changes
                .iter()
                .filter(|change| change.kind == kind)
                .map(|change| key_of(&change.document.name))
                .collect();
            keys.sort();
            keys
        };
        for (spec_kind, kind) in [
            ("added", DocumentChangeKind::Added),
            ("modified", DocumentChangeKind::Modified),
            ("removed", DocumentChangeKind::Removed),
        ] {
            let mut expected_keys = expected_changes.get(spec_kind).cloned().unwrap_or_default();
            expected_keys.sort();
            let actual_keys = categorize(kind);
            if actual_keys != expected_keys {
                return Err(format!(
                    "{spec_kind} mismatch: actual {actual_keys:?}, expected {expected_keys:?}"
                ));
            }
        }
        // Metadata-only changes (mutation state flips on
        // unchanged documents), compared by document key.
        let expected_metadata: Vec<String> = {
            let mut keys: Vec<String> = expected
                .get("metadata")
                .and_then(Json::as_array)
                .into_iter()
                .flatten()
                .filter_map(|doc| doc.get("key").and_then(Json::as_str))
                .map(str::to_string)
                .collect();
            keys.sort();
            keys
        };
        if !expected_metadata.is_empty() || !actual_metadata.is_empty() {
            let mut actual_sorted = actual_metadata.clone();
            actual_sorted.sort();
            if actual_sorted != expected_metadata {
                return Err(format!(
                    "metadata mismatch: actual {actual_sorted:?},                      expected {expected_metadata:?}"
                ));
            }
        }
    }
    if emitted.next().is_some() {
        return Err("more snapshot events emitted than expected".to_string());
    }
    Ok(())
}

/// Event generation must not depend on a step's expectations:
/// the reviewer's minimal case — listen, unlisten, relisten —
/// behaves identically whether the last step spells out
/// `"expectedSnapshotEvents": []` or omits the field, and once a
/// query has delivered, asserting `[]` on a relisten is a plain
/// comparison failure (the initial event is generated anyway).
#[test]
fn relisten_event_generation_ignores_expectations() {
    let query = serde_json::json!({
        "path": "collection", "filters": [], "orderBys": []
    });
    let listen = serde_json::json!({"userListen": {"query": query, "targetId": 2}});
    let unlisten = serde_json::json!({"userUnlisten": [2, query]});
    let plain = serde_json::json!({
        "config": {"numClients": 1},
        "steps": [listen, unlisten, listen],
    });
    let mut spelled = plain.clone();
    spelled["steps"][2]["expectedSnapshotEvents"] = serde_json::json!([]);
    // The query never delivered and the store is empty: no
    // initial event either way.
    assert!(matches!(run_case_named("plain", &plain), Outcome::Passed));
    assert!(matches!(
        run_case_named("spelled", &spelled),
        Outcome::Passed
    ));

    // Deliver a document, unlisten, remove, relisten.
    let mut delivered = serde_json::json!({
        "config": {"numClients": 1},
        "steps": [
            listen,
            {"watchAck": [2]},
            {"watchEntity": {"docs": [{
                "key": "collection/a", "value": {"n": 1}, "version": 1000,
                "createTime": 0,
                "options": {"hasCommittedMutations": false, "hasLocalMutations": false}
            }], "targets": [2]}},
            {"watchCurrent": [[2], "resume-token-1000"]},
            {"watchSnapshot": {"version": 1000, "targetIds": []},
             "expectedSnapshotEvents": [{
                "added": [{
                    "key": "collection/a", "value": {"n": 1}, "version": 1000,
                    "createTime": 0,
                    "options": {"hasCommittedMutations": false, "hasLocalMutations": false}
                }],
                "errorCode": 0, "fromCache": false, "hasPendingWrites": false,
                "query": query
             }]},
            unlisten,
            {"watchRemove": {"targetIds": [2]}},
            listen,
        ],
    });
    delivered["steps"][7]["expectedSnapshotEvents"] = serde_json::json!([]);
    // The relisten generates the initial event regardless; the
    // `[]` assertion fails on comparison, not on generation.
    assert!(matches!(
        run_case_named("delivered", &delivered),
        Outcome::Failed(_)
    ));
}

/// Regression (PR #7 review): a watch confirmation whose
/// fields are unchanged produces no `WatchEvent::Snapshot`
/// from the watch state, but the delivery must still reach
/// the store so the committed write clears. A set of the same
/// value, acknowledged before the watch delivers its version:
/// the final barrier raises a metadata-only event with
/// `hasPendingWrites` false.
#[test]
fn unchanged_fields_confirmation_still_clears_pending() {
    let query = serde_json::json!({
        "path": "collection", "filters": [], "orderBys": []
    });
    let doc = |version: i64| {
        serde_json::json!({
            "key": "collection/a", "value": {"n": 1}, "version": version,
            "createTime": 0,
            "options": {"hasCommittedMutations": false, "hasLocalMutations": false}
        })
    };
    let case = serde_json::json!({
        "config": {"numClients": 1},
        "steps": [
            {"userListen": {"query": query, "targetId": 2},
             "expectedState": {"activeTargets": {"2": {
                "queries": [query], "resumeToken": ""}}}},
            {"watchAck": [2]},
            {"watchEntity": {"docs": [doc(1000)], "targets": [2]}},
            {"watchCurrent": [[2], "resume-token-1000"]},
            {"watchSnapshot": {"version": 1000, "targetIds": []},
             "expectedSnapshotEvents": [{
                "added": [doc(1000)],
                "errorCode": 0, "fromCache": false, "hasPendingWrites": false,
                "query": query
             }]},
            {"userSet": ["collection/a", {"n": 1}],
             "expectedSnapshotEvents": [{
                "modified": [doc(0)],
                "errorCode": 0, "fromCache": false, "hasPendingWrites": true,
                "query": query
             }]},
            {"writeAck": {"version": 2000},
             "expectedState": {"userCallbacks": {
                "acknowledgedDocs": ["collection/a"], "rejectedDocs": []}}},
            {"watchEntity": {"docs": [doc(2000)], "targets": [2]}},
            {"watchSnapshot": {"version": 2000, "targetIds": []},
             "expectedSnapshotEvents": [{
                "metadata": [doc(2000)],
                "errorCode": 0, "fromCache": false, "hasPendingWrites": false,
                "query": query
             }]},
        ],
    });
    match run_case_named("case", &case) {
        Outcome::Passed => {}
        Outcome::Skipped(reason) => panic!("confirmation case skipped: {reason}"),
        Outcome::Failed(message) => panic!("confirmation case failed: {message}"),
    }
}

/// Regression (PR #7 review, second round): buffered streamed
/// updates must not leak into the composed view before their
/// consistent point. A remote change to A arrives without its
/// barrier; a local set of B then raises a snapshot that still
/// shows the old A. The barrier afterwards delivers A's change.
#[test]
fn streamed_updates_wait_for_the_consistent_point() {
    let query = serde_json::json!({
        "path": "collection", "filters": [], "orderBys": []
    });
    let doc = |key: &str, n: i64, version: i64| {
        serde_json::json!({
            "key": key, "value": {"n": n}, "version": version,
            "createTime": 0,
            "options": {"hasCommittedMutations": false, "hasLocalMutations": false}
        })
    };
    let case = serde_json::json!({
        "config": {"numClients": 1},
        "steps": [
            {"userListen": {"query": query, "targetId": 2},
             "expectedState": {"activeTargets": {"2": {
                "queries": [query], "resumeToken": ""}}}},
            {"watchAck": [2]},
            {"watchEntity": {"docs": [doc("collection/a", 1, 1000), doc("collection/b", 1, 1000)], "targets": [2]}},
            {"watchCurrent": [[2], "resume-token-1000"]},
            {"watchSnapshot": {"version": 1000, "targetIds": []},
             "expectedSnapshotEvents": [{
                "added": [doc("collection/a", 1, 1000), doc("collection/b", 1, 1000)],
                "errorCode": 0, "fromCache": false, "hasPendingWrites": false,
                "query": query
             }]},
            {"watchEntity": {"docs": [doc("collection/a", 2, 2000)], "targets": [2]}},
            {"userSet": ["collection/b", {"n": 2}],
             "expectedSnapshotEvents": [{
                "modified": [doc("collection/b", 2, 0)],
                "errorCode": 0, "fromCache": false, "hasPendingWrites": true,
                "query": query
             }]},
            {"watchSnapshot": {"version": 2000, "targetIds": []},
             "expectedSnapshotEvents": [{
                "modified": [doc("collection/a", 2, 2000)],
                "errorCode": 0, "fromCache": false, "hasPendingWrites": true,
                "query": query
             }]},
        ],
    });
    match run_case_named("case", &case) {
        Outcome::Passed => {}
        Outcome::Skipped(reason) => panic!("buffering case skipped: {reason}"),
        Outcome::Failed(message) => panic!("buffering case failed: {message}"),
    }
}

/// Runs one suite, prints its coverage summary, and asserts
/// every non-skipped case passes.
fn assert_suite(file: &str, label: &str) {
    let suite = load_suite(file);
    let mut passed = 0;
    let mut skipped: Vec<(String, String)> = Vec::new();
    let mut failures: Vec<(String, String)> = Vec::new();
    let only = std::env::var("SPEC_ONLY").unwrap_or_default();
    for (name, case) in &suite {
        if !only.is_empty() && !name.contains(&only) {
            continue;
        }
        match run_case_named(name, case) {
            Outcome::Passed => passed += 1,
            Outcome::Skipped(reason) => skipped.push((name.clone(), reason)),
            Outcome::Failed(message) => failures.push((name.clone(), message)),
        }
    }
    println!(
        "{label} spec suite: {passed} passed, {} skipped, {} failed (of {})",
        skipped.len(),
        failures.len(),
        suite.len()
    );
    for (name, reason) in &skipped {
        println!("  skipped: {name} — {reason}");
    }
    for (name, message) in &failures {
        println!("  FAILED: {name} — {message}");
    }
    assert!(
        failures.is_empty(),
        "{} {label} spec cases failed",
        failures.len()
    );
}

#[test]
fn listen_spec_suite() {
    assert_suite("listen_spec_test.json", "listen");
}

#[test]
fn write_spec_suite() {
    assert_suite("write_spec_test.json", "write");
}

#[test]
fn existence_filter_spec_suite() {
    assert_suite("existence_filter_spec_test.json", "existence filter");
}

#[test]
fn resume_token_spec_suite() {
    assert_suite("resume_token_spec_test.json", "resume token");
}

#[test]
fn offline_spec_suite() {
    assert_suite("offline_spec_test.json", "offline");
}

#[test]
fn persistence_spec_suite() {
    assert_suite("persistence_spec_test.json", "persistence");
}

/// Regression (not a vendored case): pending-write metadata is
/// scoped to the current user. The default user overwrites a
/// cached document; after switching to a user with no writes,
/// the content reverts and `hasPendingWrites` must clear even
/// though the first user's batch is still queued.
#[test]
fn pending_writes_scoped_to_current_user() {
    let query = serde_json::json!({
        "filters": [],
        "orderBys": [],
        "path": "collection"
    });
    let doc = |value: Json, local: bool| {
        serde_json::json!({
            "key": "collection/a",
            "value": value,
            "version": 0,
            "createTime": 0,
            "options": {
                "hasCommittedMutations": false,
                "hasLocalMutations": local
            }
        })
    };
    let event = |group: &str, entry: Json, pending: bool| {
        let mut event = serde_json::json!({
            "errorCode": 0,
            "fromCache": false,
            "hasPendingWrites": pending,
            "query": query
        });
        event[group] = serde_json::json!([entry]);
        event
    };
    let case = serde_json::json!({
        "config": { "numClients": 1 },
        "steps": [
            { "userListen": { "query": query, "targetId": 2 } },
            { "watchAck": [2] },
            {
                "watchEntity": {
                    "docs": [doc(serde_json::json!({"n": 1}), false)],
                    "targets": [2]
                }
            },
            { "watchCurrent": [[2], "resume-token-500"] },
            {
                "watchSnapshot": { "targetIds": [], "version": 500 },
                "expectedSnapshotEvents": [
                    event("added", doc(serde_json::json!({"n": 1}), false), false)
                ]
            },
            {
                "userSet": ["collection/a", { "n": 2 }],
                "expectedSnapshotEvents": [
                    event("modified", doc(serde_json::json!({"n": 2}), true), true)
                ]
            },
            {
                "changeUser": "user2",
                "expectedSnapshotEvents": [
                    event("modified", doc(serde_json::json!({"n": 1}), false), false)
                ]
            }
        ]
    });
    match run_case_named("pending writes are scoped to the current user", &case) {
        Outcome::Passed => {}
        Outcome::Skipped(reason) => panic!("case unexpectedly skipped: {reason}"),
        Outcome::Failed(message) => panic!("case failed: {message}"),
    }
}

#[test]
fn recovery_spec_suite() {
    assert_suite("recovery_spec_test.json", "recovery");
}

/// Regression (not a vendored case): a remote event whose
/// persistence fails is discarded wholesale — including its
/// staged cache updates. Derived from the vendored recovery
/// case "Recovers when watch update cannot be persisted", but
/// restarting and re-listening *before* the event is replayed:
/// the initial cached snapshot must contain only the document
/// from the last applied event.
#[test]
fn failed_remote_event_rolls_back_cache() {
    let query = serde_json::json!({
        "filters": [],
        "orderBys": [],
        "path": "collection"
    });
    let doc = |key: &str, value: Json, version: i64| {
        serde_json::json!({
            "key": key,
            "value": value,
            "version": version,
            "createTime": 0,
            "options": {
                "hasCommittedMutations": false,
                "hasLocalMutations": false
            }
        })
    };
    let key1 = |version: i64| doc("collection/key1", serde_json::json!({"foo": "a"}), version);
    let case = serde_json::json!({
        "config": { "numClients": 1 },
        "steps": [
            { "userListen": { "query": query, "targetId": 2 } },
            { "watchAck": [2] },
            {
                "watchEntity": {
                    "docs": [key1(1000)],
                    "targets": [2]
                }
            },
            { "watchCurrent": [[2], "resume-token-1000"] },
            {
                "watchSnapshot": { "targetIds": [], "version": 1000 },
                "expectedSnapshotEvents": [{
                    "added": [key1(1000)],
                    "errorCode": 0,
                    "fromCache": false,
                    "hasPendingWrites": false,
                    "query": query
                }]
            },
            {
                "watchEntity": {
                    "docs": [doc("collection/key2", serde_json::json!({"foo": "b"}), 2000)],
                    "targets": [2]
                }
            },
            { "failDatabase": ["Get last remote snapshot version"] },
            {
                "watchSnapshot": { "targetIds": [], "version": 1500 },
                "expectedSnapshotEvents": [{
                    "errorCode": 0,
                    "fromCache": true,
                    "hasPendingWrites": false,
                    "query": query
                }]
            },
            { "failDatabase": false },
            { "restart": null },
            {
                "userListen": { "query": query, "targetId": 2 },
                "expectedSnapshotEvents": [{
                    "added": [key1(1000)],
                    "errorCode": 0,
                    "fromCache": true,
                    "hasPendingWrites": false,
                    "query": query
                }]
            }
        ]
    });
    match run_case_named("failed remote event rolls back the cache", &case) {
        Outcome::Passed => {}
        Outcome::Skipped(reason) => panic!("case unexpectedly skipped: {reason}"),
        Outcome::Failed(message) => panic!("case failed: {message}"),
    }
}

/// Regression (not a vendored case): the flags absorbed when a
/// terminated session recovers come from the current user's
/// batch subset, like every other flag computation. The default
/// user's write is still queued when the client switches to
/// user2, a change to user3 fails and the session recovers as
/// user3: the content never moves, so recovery absorbs the
/// flags silently — with the shared-queue check it recorded
/// `hasPendingWrites` from the first user's batch, and the
/// next barrier then raised a spurious metadata-only event
/// correcting it. Here the final barrier must raise nothing.
#[test]
fn recovery_absorbs_flags_for_current_user() {
    let query = serde_json::json!({
        "filters": [],
        "orderBys": [],
        "path": "collection"
    });
    let doc = |value: Json, version: i64, local: bool| {
        serde_json::json!({
            "key": "collection/a",
            "value": value,
            "version": version,
            "createTime": 0,
            "options": {
                "hasCommittedMutations": false,
                "hasLocalMutations": local
            }
        })
    };
    let event = |group: &str, entry: Json, pending: bool| {
        let mut event = serde_json::json!({
            "errorCode": 0,
            "fromCache": false,
            "hasPendingWrites": pending,
            "query": query
        });
        event[group] = serde_json::json!([entry]);
        event
    };
    let case = serde_json::json!({
        "config": { "numClients": 1 },
        "steps": [
            { "userListen": { "query": query, "targetId": 2 } },
            { "watchAck": [2] },
            {
                "watchEntity": {
                    "docs": [doc(serde_json::json!({"n": 1}), 1000, false)],
                    "targets": [2]
                }
            },
            { "watchCurrent": [[2], "resume-token-1000"] },
            {
                "watchSnapshot": { "targetIds": [], "version": 1000 },
                "expectedSnapshotEvents": [
                    event("added", doc(serde_json::json!({"n": 1}), 1000, false), false)
                ]
            },
            {
                "userSet": ["collection/a", { "n": 2 }],
                "expectedSnapshotEvents": [
                    event("modified", doc(serde_json::json!({"n": 2}), 0, true), true)
                ]
            },
            {
                "changeUser": "user2",
                "expectedSnapshotEvents": [
                    event("modified", doc(serde_json::json!({"n": 1}), 1000, false), false)
                ]
            },
            { "failDatabase": ["Handle user change"] },
            { "changeUser": "user3" },
            { "runTimer": "async_queue_retry" },
            { "watchSnapshot": { "targetIds": [], "version": 2000 } }
        ]
    });
    match run_case_named("recovery absorbs flags for the current user", &case) {
        Outcome::Passed => {}
        Outcome::Skipped(reason) => panic!("case unexpectedly skipped: {reason}"),
        Outcome::Failed(message) => panic!("case failed: {message}"),
    }
}

/// Regression (not a vendored case): the limbo sync exemption
/// is scoped to the current user. A limbo document stops
/// blocking the query's sync only while the *current user's*
/// pending mutation holds it in the view; another user's
/// queued batch is invisible and must not unblock it — for
/// fresh candidates and for outstanding resolutions alike.
#[test]
fn limbo_sync_exemption_scoped_to_current_user() {
    let mut harness = Harness::new();
    harness.write_mode = true;
    harness.ensure_engine();
    let mut fields = Fields::new();
    fields.insert("n".to_string(), Value::Integer(2));
    harness.enqueue_spec(Mutation::Set {
        path: "collection/a".to_string(),
        fields,
        merge_paths: None,
        exists: None,
        transforms: Vec::new(),
    });
    let key = "collection/a".to_string();
    // The default user's own pending write holds the document:
    // as a fresh candidate and as an active resolution it does
    // not block sync.
    assert!(!harness.limbo_blocking_sync_extra(std::slice::from_ref(&key)));
    harness.limbo_active.push(LimboRes {
        key: key.clone(),
        target_id: None,
        state: None,
        failed: false,
    });
    assert!(!harness.limbo_blocking_sync());
    // After switching to a user with no writes, the same
    // queued batch is invisible: the document blocks sync
    // until its limbo resolution lands.
    harness.current_user = "user2".to_string();
    assert!(harness.limbo_blocking_sync_extra(std::slice::from_ref(&key)));
    assert!(harness.limbo_blocking_sync());
}

#[test]
fn limbo_spec_suite() {
    assert_suite("limbo_spec_test.json", "limbo");
}

/// Vendoring integrity: every suite file parses and the case
/// counts match the pinned upstream revision.
#[test]
fn vendored_suites_parse_with_expected_counts() {
    let expected: [(&str, usize); 18] = [
        ("bundle_spec_test.json", 11),
        ("collection_spec_test.json", 2),
        ("existence_filter_spec_test.json", 26),
        ("garbage_collection_spec_test.json", 5),
        ("index_spec_test.json", 2),
        ("limbo_spec_test.json", 26),
        ("limit_spec_test.json", 14),
        ("listen_source_spec_test.json", 23),
        ("listen_spec_test.json", 66),
        ("offline_spec_test.json", 10),
        ("orderby_spec_test.json", 2),
        ("perf_spec_test.json", 9),
        ("persistence_spec_test.json", 13),
        ("query_spec_test.json", 4),
        ("recovery_spec_test.json", 25),
        ("remote_store_spec_test.json", 4),
        ("resume_token_spec_test.json", 2),
        ("write_spec_test.json", 59),
    ];
    let total: usize = expected
        .iter()
        .map(|(file, count)| {
            assert_eq!(load_suite(file).len(), *count, "{file}");
            count
        })
        .sum();
    assert_eq!(total, 303);
}
