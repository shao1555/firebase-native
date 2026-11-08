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

use std::collections::BTreeMap;

use firebase_native_core::Error;
use serde_json::{Map, Value as Json};

use crate::google::firestore::v1::listen_response::ResponseType;
use crate::google::firestore::v1::target_change::TargetChangeType;
use crate::google::firestore::v1::{
    DocumentChange as ProtoDocumentChange, DocumentDelete, ListenResponse, TargetChange,
};
use crate::listen::{DocumentChange, DocumentChangeKind, Snapshot, WatchEvent, WatchState};
use crate::query::{FieldOp, Filter, Order, Query, UnaryOp};
use crate::value::{fields_to_proto, Document, Timestamp, Value};
use crate::Fields;

const DOCUMENTS_PREFIX: &str = "projects/test-project/databases/test-db/documents";

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
        Json::Object(map) => Value::Map(
            map.iter()
                .map(|(key, value)| (key.clone(), json_to_value(value)))
                .collect(),
        ),
    }
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

/// What the harness emitted for one step.
enum Emitted {
    /// A snapshot from the watch state itself.
    Snapshot(Snapshot),
    /// A snapshot the local layer re-raises without the watch
    /// state (a new listener's initial view, the offline
    /// re-raise). Generated purely from harness state; on steps
    /// that assert nothing, the spec corpus tolerates these
    /// (valid upstream cases leave them unasserted), while steps
    /// that do assert — even `[]` — compare them strictly.
    Pseudo(Snapshot),
    Error(i32),
}

struct Harness {
    state: Option<WatchState>,
    target_id: i32,
    unlistened: bool,
    expected_view: BTreeMap<String, Fields>,
    /// Whether the server acknowledged the current target
    /// (`watchAck`): the spec harness raises snapshots at global
    /// barriers for acknowledged targets without an explicit
    /// CURRENT step.
    acked: bool,
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
    /// The active listen was seeded fresh (no resume token):
    /// documents its first snapshot removes were synthesized
    /// deletes and leave the local store too.
    fresh_seed: bool,
    feed_snapshots: usize,
}

impl Harness {
    fn new() -> Self {
        Self {
            state: None,
            target_id: 0,
            unlistened: false,
            expected_view: BTreeMap::new(),
            acked: false,
            max_barrier: 0,
            query: None,
            current_query_sig: String::new(),
            delivered_queries: std::collections::HashSet::new(),
            listen_saw_token: false,
            resumable_targets: std::collections::HashSet::new(),
            had_listen: false,
            online: true,
            view: BTreeMap::new(),
            cache: BTreeMap::new(),
            fresh_seed: false,
            feed_snapshots: 0,
        }
    }

    fn feed(&mut self, response: ListenResponse, emitted: &mut Vec<Emitted>) {
        let Some(state) = self.state.as_mut() else {
            return;
        };
        match state.on_response(response) {
            Some(WatchEvent::Snapshot(snapshot)) => {
                self.delivered_queries
                    .insert(self.current_query_sig.clone());
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
                self.view = snapshot
                    .documents
                    .iter()
                    .map(|doc| (key_of(&doc.name), doc.clone()))
                    .collect();
                emitted.push(Emitted::Snapshot(snapshot));
            }
            Some(WatchEvent::Restart) => {
                emitted.push(Emitted::Error(-1));
            }
            Some(WatchEvent::Removed(error)) => {
                // A clean removal is not a snapshot event in the
                // spec model; only a removal with a cause is.
                if let Some(error) = &error {
                    emitted.push(Emitted::Error(error_code(error)));
                }
                self.state = None;
            }
            None => {}
        }
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
fn run_case(case: &Json) -> Outcome {
    let config = case.get("config").cloned().unwrap_or(Json::Null);
    if config.get("numClients").and_then(Json::as_i64).unwrap_or(1) != 1 {
        return Outcome::Skipped("multiple clients".to_string());
    }
    let steps = match case.get("steps").and_then(Json::as_array) {
        Some(steps) => steps.clone(),
        None => return Outcome::Failed("case without steps".to_string()),
    };
    let mut harness = Harness::new();
    for (index, step) in steps.iter().enumerate() {
        let at = |message: String| format!("step {index}: {message}");
        let mut emitted: Vec<Emitted> = Vec::new();
        for (key, value) in step.as_object().map(|o| o.iter()).into_iter().flatten() {
            match key.as_str() {
                "expectedSnapshotEvents" | "expectedState" => {}
                "clientIndex" | "drainQueue" | "applyClientState" => {}
                "enableNetwork" => {
                    let online = value.as_bool().unwrap_or(true);
                    if !online && harness.online && harness.state.is_some() && !harness.unlistened {
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
                    if harness.state.is_some() && !harness.unlistened {
                        return Outcome::Skipped("multiple concurrent targets".to_string());
                    }
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
                    let mut state = WatchState::new(target_id, query.effective_orders());
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
                    harness.state = Some(state);
                    harness.target_id = target_id;
                    harness.unlistened = false;
                    harness.acked = false;
                    harness.listen_saw_token = false;
                    harness.current_query_sig =
                        serde_json::to_string(&query_json).unwrap_or_default();
                    if harness
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
                    harness.had_listen = true;
                }
                "userUnlisten" => {
                    if harness.listen_saw_token {
                        harness.resumable_targets.insert(harness.target_id);
                    }
                    harness.unlistened = true;
                }
                "watchAck" => {
                    let ids: Vec<i64> = value
                        .as_array()
                        .map(|items| items.iter().filter_map(Json::as_i64).collect())
                        .unwrap_or_default();
                    if ids.is_empty() || ids.contains(&(harness.target_id as i64)) {
                        harness.acked = true;
                    }
                }
                "watchEntity" if harness.unlistened => {
                    // Updates addressed to an inactive target are
                    // ignored by the client.
                }
                "watchCurrent" if harness.unlistened => {}
                "watchSnapshot" if harness.unlistened => {}
                "watchEntity" => {
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
                                harness.feed(
                                    ListenResponse {
                                        response_type: Some(ResponseType::DocumentDelete(
                                            DocumentDelete {
                                                document: full_name(key),
                                                removed_target_ids: targets.clone(),
                                                read_time: None,
                                            },
                                        )),
                                    },
                                    &mut emitted,
                                );
                            }
                            Some(value_json) => {
                                let create_time =
                                    doc.get("createTime").and_then(Json::as_i64).unwrap_or(0);
                                let fields = json_fields(value_json);
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
                                let matches = harness
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
                                    harness.feed(
                                        ListenResponse {
                                            response_type: Some(ResponseType::DocumentChange(
                                                ProtoDocumentChange {
                                                    document: Some(proto),
                                                    target_ids: targets.clone(),
                                                    removed_target_ids: Vec::new(),
                                                },
                                            )),
                                        },
                                        &mut emitted,
                                    );
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
                    harness.feed(
                        ListenResponse {
                            response_type: Some(ResponseType::TargetChange(TargetChange {
                                target_change_type: TargetChangeType::Current as i32,
                                target_ids,
                                resume_token: token,
                                read_time: None,
                                cause: None,
                            })),
                        },
                        &mut emitted,
                    );
                    if token_present {
                        harness.listen_saw_token = true;
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
                    if target_ids.is_empty() {
                        // Global barrier. A version behind the
                        // highest one already delivered belongs to
                        // a reverted stream: ignore it entirely.
                        if version >= harness.max_barrier {
                            harness.max_barrier = version;
                            harness.feed(
                                ListenResponse {
                                    response_type: Some(ResponseType::TargetChange(TargetChange {
                                        target_change_type: TargetChangeType::NoChange as i32,
                                        target_ids,
                                        resume_token: token,
                                        read_time: Some(ts_from_micros(version).into()),
                                        cause: None,
                                    })),
                                },
                                &mut emitted,
                            );
                        }
                    }
                    // A targeted snapshot is bookkeeping in the
                    // spec model (resume token / version for that
                    // target); it raises no event by itself.
                }
                "watchRemove" => {
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
                .filter(|event| !matches!(event, Emitted::Pseudo(_)))
                .count();
            if unexpected > 0 {
                return Outcome::Failed(at(format!("{unexpected} unexpected snapshot event(s)")));
            }
        }

        // Light expected-state consistency check.
        if let Some(expected_state) = step.get("expectedState") {
            if let Some(active) = expected_state.get("activeTargets") {
                let expects_none = active.as_object().map(|o| o.is_empty()).unwrap_or(false);
                // While offline, the client's targets are all
                // inactive in the spec model.
                let harness_active =
                    harness.state.is_some() && !harness.unlistened && harness.online;
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
                Emitted::Snapshot(_) | Emitted::Pseudo(_) => {
                    return Err(format!("expected error {error_code}, got a snapshot"));
                }
            }
        }
        let snapshot = match actual {
            Emitted::Snapshot(snapshot) | Emitted::Pseudo(snapshot) => snapshot,
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
    assert!(matches!(run_case(&plain), Outcome::Passed));
    assert!(matches!(run_case(&spelled), Outcome::Passed));

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
    assert!(matches!(run_case(&delivered), Outcome::Failed(_)));
}

/// Runs the listen suite and asserts every non-skipped case
/// passes; prints the coverage summary.
#[test]
fn listen_spec_suite() {
    let suite = load_suite("listen_spec_test.json");
    let mut passed = 0;
    let mut skipped: Vec<(String, String)> = Vec::new();
    let mut failures: Vec<(String, String)> = Vec::new();
    for (name, case) in &suite {
        match run_case(case) {
            Outcome::Passed => passed += 1,
            Outcome::Skipped(reason) => skipped.push((name.clone(), reason)),
            Outcome::Failed(message) => failures.push((name.clone(), message)),
        }
    }
    println!(
        "listen spec suite: {passed} passed, {} skipped, {} failed (of {})",
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
        "{} listen spec cases failed",
        failures.len()
    );
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
