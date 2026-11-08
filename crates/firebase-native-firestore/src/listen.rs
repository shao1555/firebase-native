//! The Firestore Listen stream: live snapshots for a query or a
//! single document, following the official wire protocol.
//!
//! The protocol logic lives in [`WatchState`], a pure state machine
//! (responses in, snapshot/re-add/terminal events out) so it can be
//! unit tested — and later spec tested — without a server. A driver
//! task owns the bidirectional gRPC stream, feeds responses into the
//! state machine, restarts the stream when told to, and reconnects
//! with the last resume token and exponential backoff when the
//! stream breaks (online only; there is no local store yet).
//!
//! Snapshot semantics mirror the official clients: nothing is
//! emitted before the target reports `CURRENT`; afterwards a new
//! snapshot is emitted at each consistent point (a `NO_CHANGE` or
//! `CURRENT` target change carrying a `read_time`) whenever the view
//! actually changed. Changes are computed against the previously
//! emitted snapshot, with documents ordered by the query's ordering
//! (Firestore value ordering, document name as the tiebreak).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tonic::transport::Channel;
use tonic::{Code, Request};

use firebase_native_core::{Error, FirebaseOptions, Result, TokenProvider};

use crate::client::{grpc_client, map_status};
use crate::google::firestore::v1::listen_request::TargetChange as RequestTargetChange;
use crate::google::firestore::v1::listen_response::ResponseType;
use crate::google::firestore::v1::target_change::TargetChangeType;
use crate::google::firestore::v1::{ListenRequest, ListenResponse, Target, TargetChange};
use crate::query::Order;
use crate::value::{Document, Timestamp, Value};

/// The target ID used for every watch. A Listen stream created by
/// this client carries exactly one target.
pub(crate) const TARGET_ID: i32 = 2;

/// The kind of a [`DocumentChange`] within a [`Snapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentChangeKind {
    /// The document newly matches and was not in the last snapshot.
    Added,
    /// The document was in the last snapshot with different fields.
    Modified,
    /// The document no longer matches (deleted or updated out).
    Removed,
}

/// One document's change between two consecutive snapshots.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentChange {
    /// What happened to the document.
    pub kind: DocumentChangeKind,
    /// The document: its new state for `Added`/`Modified`, its last
    /// known state for `Removed`.
    pub document: Document,
    /// Index in the previous snapshot, for `Modified`/`Removed`.
    pub old_index: Option<usize>,
    /// Index in this snapshot, for `Added`/`Modified`.
    pub new_index: Option<usize>,
}

/// A consistent view of a watch target at a point in time.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    /// All documents currently matching, in query order.
    pub documents: Vec<Document>,
    /// How this snapshot differs from the previously emitted one.
    pub changes: Vec<DocumentChange>,
    /// The server time this snapshot is consistent at, when reported.
    pub read_time: Option<Timestamp>,
}

/// A live watch on a query or document. Snapshots arrive through
/// [`Listener::next`]; dropping the listener stops the stream.
pub struct Listener {
    receiver: tokio::sync::mpsc::Receiver<Result<Snapshot>>,
    task: tokio::task::JoinHandle<()>,
}

impl Listener {
    /// Waits for the next snapshot. Returns `None` when the watch has
    /// ended (after a terminal error was delivered, or cleanly when
    /// the server removed the target).
    pub async fn next(&mut self) -> Option<Result<Snapshot>> {
        self.receiver.recv().await
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Events the driver must act on, produced by [`WatchState`].
#[derive(Debug)]
pub(crate) enum WatchEvent {
    /// A new consistent snapshot to deliver.
    Snapshot(Snapshot),
    /// Restart the stream and re-add the target from scratch (no
    /// resume token). Produced on an existence-filter mismatch: the
    /// target is still active on this stream, and the protocol
    /// forbids adding an active target ID again, so recovery goes
    /// through a fresh stream rather than a second AddTarget.
    Restart,
    /// The server removed the target: the watch is over. Carries the
    /// error when the removal had a cause.
    Removed(Option<Error>),
}

/// The Listen protocol state machine for one target.
pub(crate) struct WatchState {
    target_id: i32,
    orders: Vec<Order>,
    /// Current view by full document name, before pending changes.
    docs: BTreeMap<String, Document>,
    /// Changes received since the last snapshot point; `None` is a
    /// removal.
    pending: BTreeMap<String, Option<Document>>,
    /// The last emitted snapshot's documents, in order.
    emitted: Vec<Document>,
    ever_emitted: bool,
    current_received: bool,
    resume_token: Option<Vec<u8>>,
    /// Seeded documents the (re)added target has not yet
    /// re-confirmed. On a fresh add (no resume token), documents
    /// the server does not resend are deleted from the view at
    /// the first consistent point: the reference client's
    /// synthesized deletes. Empty unless seeded with synthesis.
    unconfirmed: std::collections::BTreeSet<String>,
    /// The highest consistent-point read time accepted so far,
    /// whether or not it produced a snapshot: consistent points
    /// behind it belong to a reverted stream.
    highest_consistent_read_time: Option<Timestamp>,
}

impl WatchState {
    pub(crate) fn new(target_id: i32, orders: Vec<Order>) -> Self {
        Self {
            target_id,
            orders,
            docs: BTreeMap::new(),
            pending: BTreeMap::new(),
            emitted: Vec::new(),
            ever_emitted: false,
            current_received: false,
            resume_token: None,
            unconfirmed: std::collections::BTreeSet::new(),
            highest_consistent_read_time: None,
        }
    }

    /// Seeds the view with documents known before the target is
    /// (re)added — the local store's cached view, once watches are
    /// store-backed. The seeded set becomes the diff baseline, so
    /// the first snapshot reports changes relative to it. With
    /// `synthesize_deletes` (a fresh add without a resume token),
    /// seeded documents the server never re-confirms are removed
    /// at the first consistent point; on a resumed add the server
    /// sends only deltas, so nothing is synthesized.
    // Staged for store-backed watches; exercised today by the
    // spec-test harness.
    #[allow(dead_code)]
    pub(crate) fn seed_view(&mut self, documents: Vec<Document>, synthesize_deletes: bool) {
        for document in &documents {
            self.docs.insert(document.name.clone(), document.clone());
            if synthesize_deletes {
                self.unconfirmed.insert(document.name.clone());
            }
        }
        self.emitted = documents;
    }

    /// The latest resume token reported by the server, if any.
    pub(crate) fn resume_token(&self) -> Option<&[u8]> {
        self.resume_token.as_deref()
    }

    /// Records a resume token carried by a target change the
    /// caller handles as bookkeeping; `on_response` does the
    /// same for full responses. Used by the spec-test harness
    /// for targeted snapshots it does not feed.
    #[allow(dead_code)]
    pub(crate) fn note_resume_token(&mut self, token: &[u8]) {
        if !token.is_empty() {
            self.resume_token = Some(token.to_vec());
        }
    }

    /// Restores a resume token persisted for this target (the
    /// local store's targets table) into a fresh state, for a
    /// watch created on a previously watched query. Staged for
    /// store-backed watches; exercised today by the spec-test
    /// harness.
    #[allow(dead_code)]
    pub(crate) fn restore_resume_token(&mut self, token: Vec<u8>) {
        self.resume_token = Some(token);
    }

    fn applies_to_me(&self, target_ids: &[i32]) -> bool {
        target_ids.is_empty() || target_ids.contains(&self.target_id)
    }

    /// Feeds one Listen response into the state machine.
    pub(crate) fn on_response(&mut self, response: ListenResponse) -> Option<WatchEvent> {
        match response.response_type? {
            ResponseType::DocumentChange(change) => {
                let document = change.document?;
                if change.removed_target_ids.contains(&self.target_id)
                    || self.applies_to_me(&change.target_ids)
                {
                    // Any word about this document confirms its
                    // state; it needs no synthesized delete.
                    self.unconfirmed.remove(&document.name);
                }
                if change.removed_target_ids.contains(&self.target_id) {
                    self.pending.insert(document.name.clone(), None);
                } else if self.applies_to_me(&change.target_ids) {
                    match Document::from_proto(document.clone()) {
                        Ok(doc) => {
                            self.pending.insert(doc.name.clone(), Some(doc));
                        }
                        Err(_) => {
                            // A document we cannot model (a value
                            // kind outside the public model): treat as
                            // absent rather than poisoning the watch.
                            self.pending.insert(document.name, None);
                        }
                    }
                }
                None
            }
            ResponseType::DocumentDelete(delete) => {
                if self.applies_to_me(&delete.removed_target_ids) {
                    self.unconfirmed.remove(&delete.document);
                    self.pending.insert(delete.document, None);
                }
                None
            }
            ResponseType::DocumentRemove(remove) => {
                if self.applies_to_me(&remove.removed_target_ids) {
                    self.unconfirmed.remove(&remove.document);
                    self.pending.insert(remove.document, None);
                }
                None
            }
            ResponseType::Filter(filter) => {
                if filter.target_id != self.target_id {
                    return None;
                }
                // Existence filter: the server's count must match our
                // view (pending changes applied). A mismatch means we
                // missed removals while resuming; discard the view
                // and re-add without the resume token, like a RESET.
                if filter.count as usize != self.view_len() {
                    self.reset_view();
                    return Some(WatchEvent::Restart);
                }
                None
            }
            ResponseType::TargetChange(change) => self.on_target_change(change),
        }
    }

    fn view_len(&self) -> usize {
        let mut names: std::collections::BTreeSet<&String> = self.docs.keys().collect();
        for (name, change) in &self.pending {
            match change {
                Some(_) => {
                    names.insert(name);
                }
                None => {
                    names.remove(name);
                }
            }
        }
        names.len()
    }

    fn reset_view(&mut self) {
        self.docs.clear();
        self.pending.clear();
        self.unconfirmed.clear();
        self.current_received = false;
        self.resume_token = None;
    }

    fn on_target_change(&mut self, change: TargetChange) -> Option<WatchEvent> {
        let applies = self.applies_to_me(&change.target_ids);
        if applies && !change.resume_token.is_empty() {
            self.resume_token = Some(change.resume_token.clone());
        }
        match change.target_change_type() {
            TargetChangeType::Add => None,
            TargetChangeType::Remove => {
                if !applies {
                    return None;
                }
                let error = change
                    .cause
                    .map(|status| map_rpc_status(status.code, &status.message));
                Some(WatchEvent::Removed(error))
            }
            TargetChangeType::Current => {
                if !applies {
                    return None;
                }
                self.current_received = true;
                // Becoming current (again) is itself news for the
                // listener: the next consistent point raises a
                // snapshot even when no document changed, matching
                // the reference client after a resume/reconnect.
                self.ever_emitted = false;
                // CURRENT marks the target current, but a snapshot
                // may only be emitted at a consistent point: if the
                // read_time is omitted the target is not at one yet,
                // so keep the pending changes and wait for the
                // following consistent NO_CHANGE.
                match change.read_time {
                    Some(read_time) => {
                        let read_time = Timestamp::from(read_time);
                        if self.is_stale(&read_time) {
                            None
                        } else {
                            self.accept_consistent_point(&read_time);
                            self.emit(Some(read_time), true)
                        }
                    }
                    None => None,
                }
            }
            TargetChangeType::Reset => {
                if !applies {
                    return None;
                }
                // RESET does not remove the target: it stays active
                // and the server promises to send its replacement
                // initial state. Adding the same target ID again on
                // this stream would be a protocol violation, so this
                // is a purely local reset; the resent state arrives
                // as ordinary document changes and a CURRENT.
                self.reset_view();
                None
            }
            TargetChangeType::NoChange => {
                if !applies {
                    return None;
                }
                let read_time = Timestamp::from(change.read_time?);
                if self.is_stale(&read_time) {
                    // The stream reverted behind the highest
                    // accepted version: nothing to deliver, and
                    // pending changes stay pending.
                    return None;
                }
                self.accept_consistent_point(&read_time);
                // A targeted NO_CHANGE is a consistent point only
                // once CURRENT arrived. A stream-wide one (no
                // target IDs) may also deliver pending changes
                // before CURRENT, but an empty initial snapshot
                // still waits for CURRENT (`allow_empty`).
                if !self.current_received && !change.target_ids.is_empty() {
                    return None;
                }
                self.emit(Some(read_time), self.current_received)
            }
        }
    }

    /// Whether a consistent point predates the highest accepted
    /// one (the stream reverted to an older version).
    fn is_stale(&self, read_time: &Timestamp) -> bool {
        self.highest_consistent_read_time
            .as_ref()
            .is_some_and(|last| {
                read_time.seconds < last.seconds
                    || (read_time.seconds == last.seconds && read_time.nanos < last.nanos)
            })
    }

    /// Records a consistent point as accepted: the stale
    /// watermark advances even when the point produces no
    /// snapshot (an unchanged barrier still moves the stream's
    /// version forward).
    fn accept_consistent_point(&mut self, read_time: &Timestamp) {
        let dominated = self
            .highest_consistent_read_time
            .as_ref()
            .is_some_and(|last| {
                last.seconds > read_time.seconds
                    || (last.seconds == read_time.seconds && last.nanos >= read_time.nanos)
            });
        if !dominated {
            self.highest_consistent_read_time = Some(*read_time);
        }
    }

    /// Applies pending changes and, if the view changed (or
    /// nothing was ever emitted and an empty snapshot is allowed
    /// at this point), produces the next snapshot.
    fn emit(&mut self, read_time: Option<Timestamp>, allow_empty: bool) -> Option<WatchEvent> {
        // First consistent point of a freshly added target:
        // seeded documents the server never re-confirmed are
        // deleted from the view (synthesized deletes).
        if self.current_received && !self.unconfirmed.is_empty() {
            for name in std::mem::take(&mut self.unconfirmed) {
                self.docs.remove(&name);
            }
        }
        for (name, change) in std::mem::take(&mut self.pending) {
            match change {
                Some(doc) => {
                    self.docs.insert(name, doc);
                }
                None => {
                    self.docs.remove(&name);
                }
            }
        }
        let ordered = self.ordered_documents();
        let changes = diff_snapshots(&self.emitted, &ordered);
        if changes.is_empty() && (self.ever_emitted || !allow_empty) {
            return None;
        }
        self.ever_emitted = true;
        self.emitted = ordered.clone();
        Some(WatchEvent::Snapshot(Snapshot {
            documents: ordered,
            changes,
            read_time,
        }))
    }

    fn ordered_documents(&self) -> Vec<Document> {
        let mut documents: Vec<Document> = self.docs.values().cloned().collect();
        documents.sort_by(|a, b| self.compare_documents(a, b));
        documents
    }

    fn compare_documents(&self, a: &Document, b: &Document) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        for order in &self.orders {
            let cmp = field_value(a, &order.field).compare(&field_value(b, &order.field));
            let cmp = if order.descending { cmp.reverse() } else { cmp };
            if cmp != Equal {
                return cmp;
            }
        }
        // Implicit __name__ tiebreak; its direction follows the last
        // explicit ordering, per the server semantics.
        let name_cmp = compare_names(&a.name, &b.name);
        if self.orders.last().is_some_and(|order| order.descending) {
            name_cmp.reverse()
        } else {
            name_cmp
        }
    }
}

/// The value a document sorts by for one ordering field: `__name__`
/// is the document reference; any other field is a field *path*
/// whose segments traverse nested maps. A path that cannot be fully
/// resolved sorts as `Null`.
fn field_value(document: &Document, field: &str) -> Value {
    if field == "__name__" {
        return Value::Reference(document.name.clone());
    }
    let segments = crate::path::parse_field_path(field);
    let mut segments = segments.iter();
    let Some(first) = segments.next() else {
        return Value::Null;
    };
    let mut current = match document.fields.get(first) {
        Some(value) => value,
        None => return Value::Null,
    };
    for segment in segments {
        match current {
            Value::Map(map) => match map.get(segment) {
                Some(value) => current = value,
                None => return Value::Null,
            },
            _ => return Value::Null,
        }
    }
    current.clone()
}

/// Compares full document resource names segment by segment.
fn compare_names(a: &str, b: &str) -> std::cmp::Ordering {
    let mut left = a.split('/');
    let mut right = b.split('/');
    loop {
        match (left.next(), right.next()) {
            (None, None) => return std::cmp::Ordering::Equal,
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
            (Some(l), Some(r)) => match l.cmp(r) {
                std::cmp::Ordering::Equal => continue,
                ordering => return ordering,
            },
        }
    }
}

/// Computes the change list between two ordered snapshots. A
/// document counts as modified when its fields differ (update_time
/// alone does not count).
fn diff_snapshots(old: &[Document], new: &[Document]) -> Vec<DocumentChange> {
    let old_index: BTreeMap<&str, usize> = old
        .iter()
        .enumerate()
        .map(|(index, doc)| (doc.name.as_str(), index))
        .collect();
    let new_index: BTreeMap<&str, usize> = new
        .iter()
        .enumerate()
        .map(|(index, doc)| (doc.name.as_str(), index))
        .collect();
    let mut changes = Vec::new();
    for (index, doc) in old.iter().enumerate() {
        if !new_index.contains_key(doc.name.as_str()) {
            changes.push(DocumentChange {
                kind: DocumentChangeKind::Removed,
                document: doc.clone(),
                old_index: Some(index),
                new_index: None,
            });
        }
    }
    for (index, doc) in new.iter().enumerate() {
        match old_index.get(doc.name.as_str()) {
            None => changes.push(DocumentChange {
                kind: DocumentChangeKind::Added,
                document: doc.clone(),
                old_index: None,
                new_index: Some(index),
            }),
            Some(&previous) => {
                if old[previous].fields != doc.fields {
                    changes.push(DocumentChange {
                        kind: DocumentChangeKind::Modified,
                        document: doc.clone(),
                        old_index: Some(previous),
                        new_index: Some(index),
                    });
                }
            }
        }
    }
    changes
}

/// Maps a `google.rpc.Status` (as carried by a REMOVE target change)
/// to the shared error type, by numeric code.
fn map_rpc_status(code: i32, message: &str) -> Error {
    match code {
        16 => Error::Unauthenticated(message.to_string()),
        5 => Error::NotFound(message.to_string()),
        7 => Error::PermissionDenied(message.to_string()),
        3 => Error::InvalidArgument(message.to_string()),
        _ => Error::Rpc {
            // Store the canonical code *name*, consistent with
            // `map_status` (numeric strings here once made
            // downstream code-name comparisons silently miss).
            code: match code {
                1 => "Canceled",
                2 => "Unknown",
                4 => "DeadlineExceeded",
                6 => "AlreadyExists",
                8 => "ResourceExhausted",
                9 => "FailedPrecondition",
                10 => "Aborted",
                11 => "OutOfRange",
                12 => "Unimplemented",
                13 => "Internal",
                14 => "Unavailable",
                15 => "DataLoss",
                _ => "Unknown",
            }
            .to_string(),
            message: message.to_string(),
        },
    }
}

/// Whether a stream failure is terminal for the watch (no reconnect
/// will fix it). Mirrors the official client's classification.
fn is_terminal(code: Code) -> bool {
    matches!(
        code,
        Code::PermissionDenied
            | Code::InvalidArgument
            | Code::NotFound
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::Unimplemented
            | Code::AlreadyExists
            | Code::DataLoss
    )
}

/// Exponential backoff for stream re-establishment: 1s doubling to a
/// 30s cap, with up-to-half jitter. Reset after a snapshot is
/// delivered, i.e. the stream proved healthy.
pub(crate) struct Backoff {
    current: Duration,
}

impl Backoff {
    pub(crate) fn new() -> Self {
        Self {
            current: Duration::from_secs(1),
        }
    }

    pub(crate) fn reset(&mut self) {
        self.current = Duration::from_secs(1);
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        let base = self.current;
        self.current = (self.current * 2).min(Duration::from_secs(30));
        // Jitter from the wall clock's sub-second part; the exact
        // distribution does not matter, only decorrelation.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        base + Duration::from_nanos(u64::from(nanos) % (base.as_nanos() as u64 / 2 + 1))
    }
}

/// Spawns the driver task for one watch and returns its handle.
pub(crate) fn spawn_watch(
    channel: Channel,
    options: FirebaseOptions,
    tokens: Arc<dyn TokenProvider>,
    target: Target,
    orders: Vec<Order>,
) -> Listener {
    let (sender, receiver) = tokio::sync::mpsc::channel(16);
    let task = tokio::spawn(async move {
        run_watch(channel, options, tokens, target, orders, sender).await;
    });
    Listener { receiver, task }
}

async fn run_watch(
    channel: Channel,
    options: FirebaseOptions,
    tokens: Arc<dyn TokenProvider>,
    target: Target,
    orders: Vec<Order>,
    sender: tokio::sync::mpsc::Sender<Result<Snapshot>>,
) {
    let database = options.database_name();
    let mut state = WatchState::new(target.target_id, orders);
    let mut backoff = Backoff::new();
    let mut force_refresh = false;

    'reconnect: loop {
        if sender.is_closed() {
            return;
        }
        let mut client = match grpc_client(channel.clone(), &options, &tokens, force_refresh).await
        {
            Ok(client) => client,
            Err(error) if is_transient_token_error(&error) => {
                // Acquiring the token hit a transient failure (e.g.
                // the network is down while a refresh is due). The
                // watch must survive outages: back off and retry
                // instead of ending permanently.
                tokio::time::sleep(backoff.next_delay()).await;
                continue 'reconnect;
            }
            Err(error) => {
                // Permanent failure (no signed-in user, a rejected
                // refresh token, a configuration error): deliver and
                // stop.
                let _ = sender.send(Err(error)).await;
                return;
            }
        };
        force_refresh = false;

        let (request_sender, request_receiver) = tokio::sync::mpsc::channel(8);
        if request_sender
            .send(add_target_request(&database, &target, &state, true))
            .await
            .is_err()
        {
            continue 'reconnect;
        }
        // The streaming RPCs cross-check the request's database
        // against a routing header and reject mismatches; the
        // Listen (and Write) streams require the database resource
        // name in `google-cloud-resource-prefix` in addition to the
        // `x-goog-request-params` the interceptor already sends.
        let mut request = Request::new(tokio_stream::wrappers::ReceiverStream::new(
            request_receiver,
        ));
        if let Ok(prefix) = database.parse() {
            request
                .metadata_mut()
                .insert("google-cloud-resource-prefix", prefix);
        }
        let mut stream = match client.listen(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                if is_terminal(status.code()) {
                    let _ = sender.send(Err(map_status(status))).await;
                    return;
                }
                if status.code() == Code::Unauthenticated {
                    force_refresh = true;
                }
                tokio::time::sleep(backoff.next_delay()).await;
                continue 'reconnect;
            }
        };

        // Mid-stream loop. `restart` asks for an immediate fresh
        // stream (existence-filter recovery), skipping the backoff.
        let mut restart = false;
        loop {
            match stream.message().await {
                Ok(Some(response)) => match state.on_response(response) {
                    Some(WatchEvent::Snapshot(snapshot)) => {
                        backoff.reset();
                        if sender.send(Ok(snapshot)).await.is_err() {
                            return;
                        }
                    }
                    Some(WatchEvent::Restart) => {
                        restart = true;
                        break;
                    }
                    Some(WatchEvent::Removed(error)) => {
                        if let Some(error) = error {
                            let _ = sender.send(Err(error)).await;
                        }
                        return;
                    }
                    None => {}
                },
                Ok(None) => break, // Server closed the stream; reconnect.
                Err(status) => {
                    if is_terminal(status.code()) {
                        let _ = sender.send(Err(map_status(status))).await;
                        return;
                    }
                    if status.code() == Code::Unauthenticated {
                        force_refresh = true;
                    }
                    break;
                }
            }
        }

        if sender.is_closed() {
            return;
        }
        if !restart {
            tokio::time::sleep(backoff.next_delay()).await;
        }
    }
}

/// Whether acquiring a token failed transiently (transport or
/// retryable service errors) as opposed to permanently (no session,
/// rejected credentials, configuration errors).
pub(crate) fn is_transient_token_error(error: &Error) -> bool {
    match error {
        Error::Http(_) => true,
        // REST failures from the Auth token endpoints: throttling
        // and server-side statuses are transient; client errors
        // (invalid grant, bad key, ...) are not.
        Error::Api {
            status: Some(status),
            ..
        } => matches!(status, 429 | 500 | 502 | 503 | 504),
        Error::Rpc { code, .. } => matches!(
            code.as_str(),
            "Unavailable"
                | "Unknown"
                | "DeadlineExceeded"
                | "ResourceExhausted"
                | "Aborted"
                | "Internal"
                | "Cancelled"
        ),
        _ => false,
    }
}

/// Builds the `AddTarget` request that opens a stream. The current
/// resume token is included when `resume` is set (it is not after a
/// view reset, where the server must resend everything).
pub(crate) fn add_target_request(
    database: &str,
    target: &Target,
    state: &WatchState,
    resume: bool,
) -> ListenRequest {
    let mut target = target.clone();
    target.resume_type = if resume {
        state.resume_token().map(|token| {
            crate::google::firestore::v1::target::ResumeType::ResumeToken(token.to_vec())
        })
    } else {
        None
    };
    ListenRequest {
        database: database.to_string(),
        target_change: Some(RequestTargetChange::AddTarget(target)),
        labels: Default::default(),
        request_options: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::firestore::v1::target_change::TargetChangeType as Type;
    use crate::google::firestore::v1::{
        DocumentChange as ProtoDocumentChange, DocumentDelete as ProtoDocumentDelete,
        ExistenceFilter,
    };
    use crate::Fields;

    const PREFIX: &str = "projects/p/databases/d/documents";

    fn doc(name: &str, fields: &[(&str, Value)]) -> crate::google::firestore::v1::Document {
        let mut proto_fields = std::collections::HashMap::new();
        for (key, value) in fields {
            proto_fields.insert(key.to_string(), value.to_proto());
        }
        crate::google::firestore::v1::Document {
            name: format!("{PREFIX}/{name}"),
            fields: proto_fields,
            create_time: None,
            update_time: None,
        }
    }

    fn change(name: &str, fields: &[(&str, Value)]) -> ListenResponse {
        ListenResponse {
            response_type: Some(ResponseType::DocumentChange(ProtoDocumentChange {
                document: Some(doc(name, fields)),
                target_ids: vec![TARGET_ID],
                removed_target_ids: vec![],
            })),
        }
    }

    fn target_change(kind: Type, read_time: Option<(i64, i32)>, token: &[u8]) -> ListenResponse {
        ListenResponse {
            response_type: Some(ResponseType::TargetChange(TargetChange {
                target_change_type: kind as i32,
                target_ids: vec![],
                cause: None,
                resume_token: token.to_vec(),
                read_time: read_time
                    .map(|(seconds, nanos)| prost_types::Timestamp { seconds, nanos }),
            })),
        }
    }

    fn no_change_at(seconds: i64) -> ListenResponse {
        target_change(Type::NoChange, Some((seconds, 0)), b"")
    }

    fn watch() -> WatchState {
        WatchState::new(TARGET_ID, vec![])
    }

    #[test]
    fn initial_empty_snapshot_waits_for_current() {
        let mut state = watch();
        // A stream-wide consistent point with nothing pending
        // does not raise the initial (empty) snapshot before
        // CURRENT.
        assert!(state.on_response(no_change_at(1)).is_none());
        // Pending changes, however, are delivered at a
        // stream-wide consistent point even before CURRENT: the
        // reference client's aggregator behavior, exercised by
        // the spec suite ("Does not synthesize deletes..." raises
        // its first snapshot at a bare barrier).
        assert!(state
            .on_response(change("notes/a", &[("n", Value::Integer(1))]))
            .is_none());
        let event = state.on_response(no_change_at(2));
        let Some(WatchEvent::Snapshot(snapshot)) = event else {
            panic!("expected snapshot, got {event:?}");
        };
        assert_eq!(snapshot.documents.len(), 1);
        assert_eq!(snapshot.changes.len(), 1);
        assert_eq!(snapshot.changes[0].kind, DocumentChangeKind::Added);
        assert_eq!(snapshot.read_time, Some(Timestamp::new(2, 0)));
        // Becoming CURRENT still raises a snapshot (with an empty
        // diff): the listener learns the target is current.
        let event = state.on_response(target_change(Type::Current, Some((3, 0)), b"t1"));
        assert!(matches!(event, Some(WatchEvent::Snapshot(_))));
        assert_eq!(state.resume_token(), Some(b"t1".as_slice()));
    }

    fn seed_doc(name: &str) -> crate::value::Document {
        crate::value::Document::from_proto(doc(name, &[("n", Value::Integer(1))]))
            .expect("seed document")
    }

    #[test]
    fn seeded_fresh_view_synthesizes_deletes_for_unconfirmed() {
        let mut state = watch();
        state.seed_view(vec![seed_doc("notes/a"), seed_doc("notes/b")], true);
        // The server re-confirms only notes/a.
        state.on_response(change("notes/a", &[("n", Value::Integer(1))]));
        state.on_response(target_change(Type::Current, None, b""));
        let event = state.on_response(no_change_at(5));
        let Some(WatchEvent::Snapshot(snapshot)) = event else {
            panic!("expected snapshot, got {event:?}");
        };
        assert_eq!(snapshot.documents.len(), 1);
        assert!(snapshot.changes.iter().any(|change| {
            change.kind == DocumentChangeKind::Removed && change.document.name.ends_with("notes/b")
        }));
    }

    #[test]
    fn seeded_resumed_view_keeps_unconfirmed() {
        let mut state = watch();
        state.seed_view(vec![seed_doc("notes/a")], false);
        state.on_response(target_change(Type::Current, None, b""));
        let event = state.on_response(no_change_at(5));
        let Some(WatchEvent::Snapshot(snapshot)) = event else {
            panic!("expected snapshot, got {event:?}");
        };
        assert_eq!(snapshot.documents.len(), 1);
        assert!(snapshot.changes.is_empty());
    }

    #[test]
    fn accepted_barrier_advances_the_stale_watermark() {
        let mut state = watch();
        state.on_response(change("notes/a", &[("n", Value::Integer(1))]));
        state.on_response(target_change(Type::Current, None, b""));
        assert!(state.on_response(no_change_at(10)).is_some());
        // An unchanged barrier at 20 emits nothing, but its
        // version is still accepted...
        assert!(state.on_response(no_change_at(20)).is_none());
        // ...so a change followed by a barrier at 15 is stale and
        // must not be delivered.
        state.on_response(change("notes/b", &[("n", Value::Integer(2))]));
        assert!(state.on_response(no_change_at(15)).is_none());
        // The pending change arrives at the next fresh barrier.
        let event = state.on_response(no_change_at(25));
        let Some(WatchEvent::Snapshot(snapshot)) = event else {
            panic!("expected snapshot, got {event:?}");
        };
        assert_eq!(snapshot.documents.len(), 2);
    }

    #[test]
    fn reverted_barrier_is_ignored() {
        let mut state = watch();
        state.on_response(change("notes/a", &[("n", Value::Integer(1))]));
        state.on_response(target_change(Type::Current, None, b""));
        assert!(state.on_response(no_change_at(10)).is_some());
        // A consistent point behind the delivered snapshot's
        // version delivers nothing.
        assert!(state.on_response(no_change_at(5)).is_none());
    }

    #[test]
    fn modify_and_remove_diff() {
        let mut state = watch();
        state.on_response(change("notes/a", &[("n", Value::Integer(1))]));
        state.on_response(change("notes/b", &[("n", Value::Integer(2))]));
        state.on_response(target_change(Type::Current, Some((1, 0)), b""));
        // Modify a, delete b via DocumentDelete, add c.
        state.on_response(change("notes/a", &[("n", Value::Integer(10))]));
        state.on_response(ListenResponse {
            response_type: Some(ResponseType::DocumentDelete(ProtoDocumentDelete {
                document: format!("{PREFIX}/notes/b"),
                removed_target_ids: vec![],
                read_time: None,
            })),
        });
        state.on_response(change("notes/c", &[("n", Value::Integer(3))]));
        let event = state.on_response(no_change_at(2));
        let Some(WatchEvent::Snapshot(snapshot)) = event else {
            panic!("expected snapshot, got {event:?}");
        };
        let kinds: Vec<(DocumentChangeKind, String)> = snapshot
            .changes
            .iter()
            .map(|c| (c.kind, c.document.id().to_string()))
            .collect();
        assert!(kinds.contains(&(DocumentChangeKind::Modified, "a".to_string())));
        assert!(kinds.contains(&(DocumentChangeKind::Removed, "b".to_string())));
        assert!(kinds.contains(&(DocumentChangeKind::Added, "c".to_string())));
        assert_eq!(snapshot.documents.len(), 2);
        // A further no-change point with no pending changes emits nothing.
        assert!(state.on_response(no_change_at(3)).is_none());
    }

    #[test]
    fn reset_clears_view_locally_without_readd() {
        let mut state = watch();
        state.on_response(change("notes/a", &[("n", Value::Integer(1))]));
        state.on_response(target_change(Type::Current, Some((1, 0)), b"tok"));
        // RESET keeps the target active server-side: no event, no
        // second AddTarget - just a local reset.
        let event = state.on_response(target_change(Type::Reset, None, b""));
        assert!(event.is_none());
        assert_eq!(state.resume_token(), None);
        // Server resends the (now different) full state.
        state.on_response(change("notes/z", &[("n", Value::Integer(9))]));
        let event = state.on_response(target_change(Type::Current, Some((2, 0)), b""));
        let Some(WatchEvent::Snapshot(snapshot)) = event else {
            panic!("expected snapshot, got {event:?}");
        };
        // z added, a removed relative to the previously emitted view.
        assert_eq!(snapshot.documents.len(), 1);
        assert_eq!(snapshot.documents[0].id(), "z");
        assert_eq!(snapshot.changes.len(), 2);
    }

    #[test]
    fn existence_filter_mismatch_restarts_stream() {
        let mut state = watch();
        state.on_response(change("notes/a", &[("n", Value::Integer(1))]));
        state.on_response(target_change(Type::Current, Some((1, 0)), b""));
        // Server says two documents match; we hold one.
        let event = state.on_response(ListenResponse {
            response_type: Some(ResponseType::Filter(ExistenceFilter {
                target_id: TARGET_ID,
                count: 2,
                unchanged_names: None,
            })),
        });
        assert!(matches!(event, Some(WatchEvent::Restart)));
        // A matching count is a no-op.
        let mut state = watch();
        state.on_response(change("notes/a", &[("n", Value::Integer(1))]));
        state.on_response(target_change(Type::Current, Some((1, 0)), b""));
        let event = state.on_response(ListenResponse {
            response_type: Some(ResponseType::Filter(ExistenceFilter {
                target_id: TARGET_ID,
                count: 1,
                unchanged_names: None,
            })),
        });
        assert!(event.is_none());
    }

    #[test]
    fn remove_with_cause_is_terminal() {
        let mut state = watch();
        let mut change = target_change(Type::Remove, None, b"");
        if let Some(ResponseType::TargetChange(tc)) = &mut change.response_type {
            tc.cause = Some(crate::google::rpc::Status {
                code: 7,
                message: "permission denied for target".to_string(),
                details: vec![],
            });
        }
        let event = state.on_response(change);
        match event {
            Some(WatchEvent::Removed(Some(Error::PermissionDenied(message)))) => {
                assert!(message.contains("permission"));
            }
            other => panic!("expected terminal removal, got {other:?}"),
        }
    }

    #[test]
    fn ordering_uses_query_order_then_name() {
        let mut state = WatchState::new(
            TARGET_ID,
            vec![Order {
                field: "rank".to_string(),
                descending: false,
            }],
        );
        state.on_response(change("notes/b", &[("rank", Value::Integer(2))]));
        state.on_response(change("notes/a", &[("rank", Value::Integer(2))]));
        state.on_response(change("notes/c", &[("rank", Value::Integer(1))]));
        let event = state.on_response(target_change(Type::Current, Some((1, 0)), b""));
        let Some(WatchEvent::Snapshot(snapshot)) = event else {
            panic!("expected snapshot");
        };
        let ids: Vec<&str> = snapshot.documents.iter().map(|d| d.id()).collect();
        assert_eq!(ids, vec!["c", "a", "b"]);
    }

    #[test]
    fn current_without_read_time_waits_for_consistent_point() {
        let mut state = watch();
        state.on_response(change("notes/a", &[("n", Value::Integer(1))]));
        // CURRENT without read_time: marked current, but no emission.
        let event = state.on_response(target_change(Type::Current, None, b""));
        assert!(event.is_none());
        // The consistent NO_CHANGE that follows emits, with the
        // pending document included and its read_time.
        let event = state.on_response(no_change_at(7));
        let Some(WatchEvent::Snapshot(snapshot)) = event else {
            panic!("expected snapshot, got {event:?}");
        };
        assert_eq!(snapshot.documents.len(), 1);
        assert_eq!(snapshot.read_time, Some(Timestamp::new(7, 0)));
    }

    #[test]
    fn ordering_resolves_nested_field_paths() {
        let mut meta_b = Fields::new();
        meta_b.insert("rank".to_string(), Value::Integer(1));
        let mut meta_a = Fields::new();
        meta_a.insert("rank".to_string(), Value::Integer(2));
        let mut state = WatchState::new(
            TARGET_ID,
            vec![Order {
                field: "meta.rank".to_string(),
                descending: false,
            }],
        );
        state.on_response(change("notes/a", &[("meta", Value::Map(meta_a))]));
        state.on_response(change("notes/b", &[("meta", Value::Map(meta_b))]));
        let event = state.on_response(target_change(Type::Current, Some((1, 0)), b""));
        let Some(WatchEvent::Snapshot(snapshot)) = event else {
            panic!("expected snapshot");
        };
        let ids: Vec<&str> = snapshot.documents.iter().map(|d| d.id()).collect();
        assert_eq!(ids, vec!["b", "a"]);
    }

    #[test]
    fn token_error_classification() {
        assert!(is_transient_token_error(&Error::Http(
            "offline".to_string()
        )));
        assert!(is_transient_token_error(&Error::Rpc {
            code: "Unavailable".to_string(),
            message: "down".to_string(),
        }));
        assert!(!is_transient_token_error(&Error::Unauthenticated(
            "no user".to_string()
        )));
        assert!(!is_transient_token_error(&Error::Other(
            "bad config".to_string()
        )));
        // Auth REST statuses: throttling/server errors retry,
        // client errors and status-less API errors do not.
        let api = |status: Option<u16>| Error::Api {
            status,
            code: "X".to_string(),
            message: "x".to_string(),
        };
        for status in [429, 500, 502, 503, 504] {
            assert!(is_transient_token_error(&api(Some(status))), "{status}");
        }
        for status in [400, 401, 403, 404] {
            assert!(!is_transient_token_error(&api(Some(status))), "{status}");
        }
        assert!(!is_transient_token_error(&api(None)));
    }

    #[tokio::test]
    async fn watch_survives_transient_token_failures() {
        use firebase_native_core::{App, FirebaseOptions};
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        #[derive(Debug)]
        struct FlakyTokens {
            calls: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl TokenProvider for FlakyTokens {
            async fn id_token(&self, _force_refresh: bool) -> Result<String> {
                self.calls.fetch_add(1, AtomicOrdering::SeqCst);
                Err(Error::Http("temporary network outage".to_string()))
            }
            async fn uid(&self) -> Option<String> {
                None
            }
        }

        let tokens = Arc::new(FlakyTokens {
            calls: AtomicUsize::new(0),
        });
        let app = App::new(FirebaseOptions::new("test-key", "demo-project"));
        let firestore = crate::Firestore::new(&app, tokens.clone());
        let mut listener = firestore.watch_document("notes/x").await.unwrap();
        // The driver must keep retrying token acquisition (backoff),
        // not deliver a terminal error. Wait until it retried at
        // least once (initial attempt + one retry > 1 call).
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while tokens.calls.load(AtomicOrdering::SeqCst) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "token acquisition was not retried"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // And nothing was delivered to the consumer meanwhile.
        let delivered = tokio::time::timeout(Duration::from_millis(200), listener.next()).await;
        assert!(delivered.is_err(), "no item should be delivered yet");
    }
}
