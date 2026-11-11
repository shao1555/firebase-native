//! The Firestore Listen stream: live snapshots for a query or a
//! single document, following the official wire protocol.
//!
//! The protocol logic lives in [`WatchState`], a pure state machine
//! (responses in, snapshot/re-add/terminal events out) so it can be
//! unit tested — and later spec tested — without a server. A driver
//! task owns the bidirectional gRPC stream, feeds responses into the
//! state machine, restarts the stream when told to, and reconnects
//! with the last resume token and exponential backoff when the
//! stream breaks.
//!
//! When the client has local persistence, the driver is
//! store-backed (see [`StoreWatch`]): the state machine is seeded
//! from the store and resumes with the target's persisted resume
//! token, streamed remote changes are applied to the store at
//! consistent points, and the snapshots delivered to the listener
//! are composed from the store's local views — so a snapshot
//! reflects unacknowledged local writes as soon as they are
//! queued (latency compensation), arrives from the cache before
//! the stream first connects, and survives restarts.
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
use crate::engine::PersistenceEngine;
use crate::google::firestore::v1::listen_request::TargetChange as RequestTargetChange;
use crate::google::firestore::v1::listen_response::ResponseType;
use crate::google::firestore::v1::target_change::TargetChangeType;
use crate::google::firestore::v1::{ListenRequest, ListenResponse, Target, TargetChange};
use crate::query::{Order, Query};
use crate::store::TargetState;
use crate::value::{Document, Timestamp};

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
    /// Where this snapshot came from and whether unacknowledged
    /// local writes affect it.
    pub metadata: SnapshotMetadata,
}

/// Provenance of a [`Snapshot`], mirroring the native SDKs'
/// `SnapshotMetadata`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotMetadata {
    /// This snapshot reflects the local store's composed view —
    /// the initial cache snapshot and latency-compensated local
    /// updates — rather than a server round-trip. Only a watch
    /// backed by a local store produces these; the first
    /// snapshot with this `false` marks the watch's initial
    /// sync (one full server round-trip for the target).
    pub is_from_cache: bool,
    /// The local store holds mutations affecting this watch
    /// that the server has not acknowledged yet: a mutation on
    /// a document in the snapshot, on the watched document, or
    /// on any member of the watched collection.
    pub has_pending_writes: bool,
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
    /// The server removed the target: the watch is over. Carries the
    /// error when the removal had a cause.
    Removed(Option<Error>),
}

/// The decided outcome of an existence filter, applied at
/// the next consistent point after its receipt.
enum FilterOutcome {
    /// The server's count does not match the view as of
    /// receipt: the target restarts.
    Mismatch,
    /// The count matches; with a bloom filter, these view
    /// documents fail the bloom check and are definitely gone
    /// from the server's set.
    Match { failing: Vec<String> },
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
    /// Documents that were in the view when an existence-filter
    /// mismatch restarted the target. Like `unconfirmed` they
    /// must be re-confirmed by the re-run; at the first
    /// consistent point they are either deleted (the default) or,
    /// when `keep_restart_unconfirmed` is set, reported as limbo
    /// candidates while staying in the view.
    restart_unconfirmed: std::collections::BTreeSet<String>,
    /// Limbo candidates produced at a consistent point when
    /// `keep_restart_unconfirmed` is set; drained by the caller.
    limbo_candidates: std::collections::BTreeSet<String>,
    keep_restart_unconfirmed: bool,
    /// The target was current when the last restart reset it:
    /// with the delete policy (not keeping restart-unconfirmed
    /// documents), the synthesized deletes are already due at
    /// the next consistent point, CURRENT or not.
    restart_was_current: bool,
    /// An existence filter received since the last consistent
    /// point. Filters are applied at consistent points, not on
    /// receipt: the count is checked against the view as of
    /// the snapshot (the reference client's "handled at global
    /// snapshot").
    pending_filter: Option<FilterOutcome>,
    /// A deferred filter mismatched at a consistent point and
    /// restarted the target; drained by the caller (the driver
    /// re-adds the target; the spec harness masks the token).
    filter_restart: bool,
    /// A held existence filter was applied at a consistent
    /// point (either outcome); drained by the caller.
    filter_applied: bool,
    /// The highest consistent-point read time accepted so far,
    /// whether or not it produced a snapshot: consistent points
    /// behind it belong to a reverted stream.
    highest_consistent_read_time: Option<Timestamp>,
    /// A consistent point was accepted while processing the
    /// last response (not stale, addressed to this target);
    /// drained by the caller. The store-backed driver flushes
    /// its buffered remote event only on this signal — the raw
    /// message shape alone would also admit stale barriers the
    /// state machine rejected.
    consistent_point: bool,
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
            restart_unconfirmed: std::collections::BTreeSet::new(),
            limbo_candidates: std::collections::BTreeSet::new(),
            keep_restart_unconfirmed: false,
            restart_was_current: false,
            pending_filter: None,
            filter_restart: false,
            filter_applied: false,
            highest_consistent_read_time: None,
            consistent_point: false,
        }
    }

    /// Seeds the view with documents known before the target is
    /// (re)added — the local store's cached view, for store-backed
    /// watches. The seeded set becomes the diff baseline, so the
    /// first snapshot reports changes relative to it. With
    /// `synthesize_deletes` (a fresh add without a resume token),
    /// seeded documents the server never re-confirms are removed
    /// at the first consistent point; on a resumed add the server
    /// sends only deltas, so nothing is synthesized.
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
    /// Sets whether documents left unconfirmed by an
    /// existence-filter restart are reported as limbo
    /// candidates (true) or deleted at the first consistent
    /// point (false, the default).
    pub(crate) fn set_keep_restart_unconfirmed(&mut self, keep: bool) {
        self.keep_restart_unconfirmed = keep;
    }

    /// Drains the limbo candidates produced at the last
    /// consistent point (full resource names).
    pub(crate) fn take_limbo_candidates(&mut self) -> Vec<String> {
        std::mem::take(&mut self.limbo_candidates)
            .into_iter()
            .collect()
    }

    /// The view as it will stand once pending changes land:
    /// `docs` overlaid with `pending`. Decisions that must see
    /// the in-flight remote event as well as the last emitted
    /// view (limbo outcome application) use this.
    pub(crate) fn effective_view(&self) -> Vec<Document> {
        let mut view = self.docs.clone();
        for (name, change) in &self.pending {
            match change {
                Some(document) => {
                    view.insert(name.clone(), document.clone());
                }
                None => {
                    view.remove(name);
                }
            }
        }
        view.into_values().collect()
    }

    /// Whether the target has been marked CURRENT since its
    /// (re)add.
    pub(crate) fn is_current(&self) -> bool {
        self.current_received
    }

    /// Seeds the view for a re-run re-add: the seeded
    /// documents must be re-confirmed by the re-run and are
    /// accounted for at its first consistent point (see
    /// `restart_unconfirmed`).
    // Exercised by the spec-test harness (and staged for
    // store-backed watches in the driver).
    #[allow(dead_code)]
    pub(crate) fn seed_restart_unconfirmed(&mut self, documents: Vec<Document>) {
        for document in &documents {
            self.restart_unconfirmed.insert(document.name.clone());
        }
        self.seed_view(documents, false);
    }

    /// Marks already-seeded documents as restart-unconfirmed
    /// (see `restart_unconfirmed`), e.g. limbo documents of a
    /// session restored after a termination.
    // Exercised by the spec-test harness (and staged for
    // store-backed watches in the driver).
    #[allow(dead_code)]
    pub(crate) fn mark_restart_unconfirmed(&mut self, names: &[String]) {
        for name in names {
            if self.docs.contains_key(name) {
                self.restart_unconfirmed.insert(name.clone());
            }
        }
    }

    // Exercised by the spec-test harness.
    #[allow(dead_code)]
    pub(crate) fn note_resume_token(&mut self, token: &[u8]) {
        if !token.is_empty() {
            self.resume_token = Some(token.to_vec());
        }
    }

    /// Restores a resume token persisted for this target (the
    /// local store's targets table) into a fresh state, for a
    /// watch created on a previously watched query.
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
                    self.restart_unconfirmed.remove(&document.name);
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
                    self.restart_unconfirmed.remove(&delete.document);
                    self.pending.insert(delete.document, None);
                }
                None
            }
            ResponseType::DocumentRemove(remove) => {
                if self.applies_to_me(&remove.removed_target_ids) {
                    self.unconfirmed.remove(&remove.document);
                    self.restart_unconfirmed.remove(&remove.document);
                    self.pending.insert(remove.document, None);
                }
                None
            }
            ResponseType::Filter(filter) => {
                if filter.target_id != self.target_id {
                    return None;
                }
                // A filter for a target that has not reached a
                // consistent point yet is ignored: the target
                // is still (re)running, so there is no resumed
                // state for the filter to validate. Otherwise
                // the filter is decided now, against the view
                // as of receipt, and its effect lands at the
                // next consistent point (see
                // `apply_pending_filter`).
                if !self.current_received {
                    return None;
                }
                let bloom = filter.unchanged_names.as_ref().and_then(|filter| {
                    let bits = filter.bits.as_ref()?;
                    crate::bloom::BloomFilter::new(
                        bits.bitmap.clone(),
                        bits.padding,
                        filter.hash_count,
                    )
                });
                if filter.count as usize != self.view_len(&bloom) {
                    self.pending_filter = Some(FilterOutcome::Mismatch);
                } else if let Some(bloom) = bloom {
                    let mut names: std::collections::BTreeSet<String> =
                        self.docs.keys().cloned().collect();
                    for (name, change) in &self.pending {
                        match change {
                            Some(_) => {
                                names.insert(name.clone());
                            }
                            None => {
                                names.remove(name);
                            }
                        }
                    }
                    let failing = names
                        .into_iter()
                        .filter(|name| !bloom.contains(name))
                        .collect();
                    self.pending_filter = Some(FilterOutcome::Match { failing });
                } else {
                    self.pending_filter = Some(FilterOutcome::Match {
                        failing: Vec::new(),
                    });
                }
                None
            }
            ResponseType::TargetChange(change) => self.on_target_change(change),
        }
    }

    fn view_len(&self, bloom: &Option<crate::bloom::BloomFilter>) -> usize {
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
        match bloom {
            Some(bloom) => names.iter().filter(|name| bloom.contains(name)).count(),
            None => names.len(),
        }
    }

    fn reset_view(&mut self) {
        // The view survives a restart: its documents must be
        // re-confirmed by the re-run and are accounted for at
        // the first consistent point (see `restart_unconfirmed`).
        self.restart_unconfirmed.extend(self.docs.keys().cloned());
        self.pending.clear();
        self.restart_was_current = self.current_received;
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

    /// Drains whether a consistent point was accepted while
    /// processing the last response.
    pub(crate) fn take_consistent_point(&mut self) -> bool {
        std::mem::take(&mut self.consistent_point)
    }

    /// Records a consistent point as accepted: the stale
    /// watermark advances even when the point produces no
    /// snapshot (an unchanged barrier still moves the stream's
    /// version forward).
    fn accept_consistent_point(&mut self, read_time: &Timestamp) {
        self.consistent_point = true;
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
    /// Applies a held existence filter at a consistent point.
    /// The server's count must match our view (pending changes
    /// applied). With a bloom filter, only the view documents
    /// that pass it count — the ones that fail are definitely
    /// gone from the server's set and become
    /// restart-unconfirmed (limbo candidates or synthesized
    /// deletes, per the caller's policy). On a count mismatch
    /// we missed changes while resuming: the whole view becomes
    /// restart-unconfirmed, the resume token is dropped, and
    /// the restart is reported via `take_filter_restart`.
    /// Deliveries already in flight still land in this
    /// snapshot; the re-run re-confirms the rest.
    fn apply_pending_filter(&mut self) {
        let Some(outcome) = self.pending_filter.take() else {
            return;
        };
        self.filter_applied = true;
        match outcome {
            FilterOutcome::Mismatch => {
                self.restart_unconfirmed.extend(self.docs.keys().cloned());
                self.restart_was_current = self.current_received;
                self.current_received = false;
                self.resume_token = None;
                self.filter_restart = true;
            }
            FilterOutcome::Match { failing } => {
                for name in failing {
                    if self.docs.contains_key(&name) {
                        self.restart_unconfirmed.insert(name);
                    }
                }
            }
        }
    }

    /// Drains the deferred-filter restart signal.
    pub(crate) fn take_filter_restart(&mut self) -> bool {
        std::mem::take(&mut self.filter_restart)
    }

    /// Drains the deferred-filter application signal.
    // Exercised by the spec-test harness (and staged for
    // store-backed watches in the driver).
    #[allow(dead_code)]
    pub(crate) fn take_filter_applied(&mut self) -> bool {
        std::mem::take(&mut self.filter_applied)
    }

    fn emit(&mut self, read_time: Option<Timestamp>, allow_empty: bool) -> Option<WatchEvent> {
        self.apply_pending_filter();
        // First consistent point of a freshly added target:
        // seeded documents the server never re-confirmed are
        // deleted from the view (synthesized deletes), or —
        // with the limbo policy — reported as limbo candidates
        // while staying in the view.
        if self.current_received && !self.unconfirmed.is_empty() {
            let names = std::mem::take(&mut self.unconfirmed);
            if self.keep_restart_unconfirmed {
                self.limbo_candidates.extend(names);
            } else {
                for name in names {
                    self.docs.remove(&name);
                }
            }
        }
        if (self.current_received || (self.restart_was_current && !self.keep_restart_unconfirmed))
            && !self.restart_unconfirmed.is_empty()
        {
            let names = std::mem::take(&mut self.restart_unconfirmed);
            if self.keep_restart_unconfirmed {
                // The documents stay in the view; the caller
                // resolves them individually (limbo resolution).
                self.limbo_candidates.extend(names);
            } else {
                for name in names {
                    self.docs.remove(&name);
                }
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
            // Until the target is CURRENT the initial result
            // set is incomplete: partial deliveries are cache
            // state, not the initial-sync signal.
            metadata: SnapshotMetadata {
                is_from_cache: !self.current_received,
                has_pending_writes: false,
            },
        }))
    }

    fn ordered_documents(&self) -> Vec<Document> {
        let mut documents: Vec<Document> = self.docs.values().cloned().collect();
        documents.sort_by(|a, b| self.compare_documents(a, b));
        documents
    }

    fn compare_documents(&self, a: &Document, b: &Document) -> std::cmp::Ordering {
        crate::query::compare_by_orders(&self.orders, a, b)
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

/// The backend intermittently rejects a stream with a
/// database-name mismatch even though the identical request
/// succeeds on retry (observed on fresh Write streams during
/// part 2a and on a fresh Listen stream during the part 2l live
/// check). That specific failure is a routing hiccup, not a
/// configuration error, so it is exempt from the terminal
/// classification below and the stream restarts instead.
fn is_database_mismatch(status: &tonic::Status) -> bool {
    status.code() == Code::InvalidArgument
        && status.message().contains("Mismatch between database name")
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

/// What a store-backed watch reads its composed view from.
pub(crate) enum WatchSource {
    /// A collection query under a relative parent path.
    Query {
        /// Parent document's relative path ("" at the root).
        parent_relative: String,
        /// The query itself.
        query: Query,
    },
    /// A single document by relative path.
    Document {
        /// The document's relative path.
        relative_path: String,
    },
}

/// The store side of a store-backed watch: everything the
/// driver needs to seed the state machine, resume the target,
/// apply streamed remote changes to the store and compose the
/// snapshots the listener receives.
pub(crate) struct StoreWatch {
    engine: Arc<PersistenceEngine>,
    source: WatchSource,
    /// Key of this target's row in the store's targets table.
    /// Every watch uses the same wire target ID, so the row is
    /// keyed by a stable hash of the target definition instead:
    /// concurrent watches on different targets keep separate
    /// rows, and the same target in a later process finds its
    /// own row again.
    target_key: i64,
}

impl StoreWatch {
    pub(crate) fn new(
        engine: Arc<PersistenceEngine>,
        source: WatchSource,
        target_key: i64,
    ) -> Self {
        engine.register_target(target_key);
        Self {
            engine,
            source,
            target_key,
        }
    }

    /// Records `documents` (this watch's current view) as the
    /// target's membership with the engine, protecting them
    /// from cache garbage collection while the watch is open.
    /// `from_server` marks syncs that reflect server state (a
    /// consistent point, not the cache-only seed): for a query
    /// watch they also re-establish the collection's
    /// completeness after any eviction.
    fn sync_membership(&self, documents: &[Document], from_server: bool) {
        self.engine
            .sync_target_membership(self.target_key, documents);
        if from_server {
            if let WatchSource::Query {
                parent_relative,
                query,
            } = &self.source
            {
                // Only a sync covering the *entire* collection
                // re-establishes its completeness: a filtered
                // or windowed watch legitimately never sees
                // the documents eviction may have taken.
                if query.covers_entire_collection() {
                    self.engine
                        .note_collection_synced(parent_relative, query.collection_id());
                }
            }
        }
    }

    /// The composed local view of the watched target: the
    /// store's server-confirmed documents with queued mutations
    /// applied — evaluated for the query, or the single
    /// document's local view.
    fn composed(&self) -> Result<Vec<Document>> {
        match &self.source {
            WatchSource::Query {
                parent_relative,
                query,
            } => self.engine.local_query(parent_relative, query),
            WatchSource::Document { relative_path } => Ok(self
                .engine
                .local_document(relative_path)?
                .into_iter()
                .collect()),
        }
    }

    /// Seeds a fresh watch state from the store and restores
    /// the target's persisted resume token, if any. Returns the
    /// seeded documents (the state view's baseline).
    fn seed(&self, state: &mut WatchState) -> Vec<Document> {
        // The composed view and the membership registration
        // must be atomic against garbage collection: a
        // document the seed just read must not be evicted
        // before its protection lands.
        let _landing = self.engine.landing_guard();
        let documents = self.composed().unwrap_or_default();
        let token = self
            .engine
            .store()
            .target_state(self.target_key)
            .ok()
            .flatten()
            .map(|persisted| persisted.resume_token)
            .filter(|token| !token.is_empty());
        if let Some(token) = &token {
            state.restore_resume_token(token.clone());
        }
        state.seed_view(documents.clone(), token.is_none());
        self.sync_membership(&documents, false);
        documents
    }

    /// Whether unacknowledged mutations affect this watch:
    /// a mutation on a document of `view`, on the watched
    /// document, or — for a query — on any member of the
    /// watched collection (which covers pending creates and
    /// deletes whose document is not in the current view).
    fn has_pending_writes(&self, view: &[Document]) -> bool {
        match &self.source {
            // Independent of the view: a pending delete removes
            // the document from the composed view entirely, so
            // view membership can never observe it.
            WatchSource::Document { relative_path } => {
                self.engine.has_pending(relative_path).unwrap_or(false)
            }
            WatchSource::Query {
                parent_relative,
                query,
            } => {
                let in_view = view.iter().any(|document| {
                    self.engine
                        .relative_path_of(&document.name)
                        .is_some_and(|relative| self.engine.has_pending(relative).unwrap_or(false))
                });
                in_view
                    || self
                        .engine
                        .has_pending_in_collection(parent_relative, query.collection_id())
                        .unwrap_or(false)
            }
        }
    }

    /// Persists the target's resume token and snapshot version
    /// at a consistent point. Best effort: a store failure must
    /// not kill a healthy watch — the token is an optimization,
    /// and the seeded view keeps a tokenless re-add correct.
    fn persist_target(&self, state: &WatchState, read_time: Option<Timestamp>) {
        let _ = self.engine.store().put_target_state(
            self.target_key,
            &TargetState {
                resume_token: state.resume_token().map(<[u8]>::to_vec).unwrap_or_default(),
                snapshot_version: read_time,
            },
        );
    }
}

impl Drop for StoreWatch {
    fn drop(&mut self) {
        // The watch is gone: its documents stop being
        // protected from garbage collection (the engine
        // reference-counts watches per target).
        self.engine.unregister_target(self.target_key);
    }
}

/// Tracks what a store-backed listener has been delivered and
/// composes the next snapshot from the store on demand.
struct StoreDelivery {
    delivered: Vec<Document>,
    delivered_once: bool,
    last_read_time: Option<Timestamp>,
    seen_generation: u64,
    /// Metadata of the last delivered snapshot. A snapshot is
    /// delivered when its documents change *or* its metadata
    /// does — the cache-to-server flip (initial sync) and the
    /// pending-writes flag clearing on acknowledgement are
    /// observable events even when the view itself is unchanged,
    /// matching the native SDKs.
    last_metadata: Option<SnapshotMetadata>,
}

impl StoreDelivery {
    fn new() -> Self {
        Self {
            delivered: Vec::new(),
            delivered_once: false,
            last_read_time: None,
            seen_generation: 0,
            last_metadata: None,
        }
    }

    /// Emits the current composed view when it differs from
    /// what was delivered (the first call always emits: the
    /// cache snapshot). Returns false when the listener is gone
    /// or the store failed (the error is delivered first).
    /// `from_cache` records the event's origin: `true` for the
    /// initial cache snapshot and local (latency-compensated)
    /// updates, `false` when a server snapshot was just applied.
    async fn emit(
        &mut self,
        watch: &StoreWatch,
        sender: &tokio::sync::mpsc::Sender<Result<Snapshot>>,
        from_cache: bool,
    ) -> bool {
        let view = match watch.composed() {
            Ok(view) => view,
            Err(error) => {
                let _ = sender.send(Err(error)).await;
                return false;
            }
        };
        self.seen_generation = watch.engine.generation();
        let changes = diff_snapshots(&self.delivered, &view);
        let metadata = SnapshotMetadata {
            is_from_cache: from_cache,
            has_pending_writes: watch.has_pending_writes(&view),
        };
        if changes.is_empty() && self.delivered_once && self.last_metadata == Some(metadata) {
            return true;
        }
        self.delivered_once = true;
        self.delivered = view.clone();
        self.last_metadata = Some(metadata);
        sender
            .send(Ok(Snapshot {
                documents: view,
                changes,
                read_time: self.last_read_time,
                metadata,
            }))
            .await
            .is_ok()
    }
}

/// Extracts the remote-store effect of one Listen response for
/// the driver's target into `buffer` (the latest state per
/// document name wins; `None` is a server-confirmed deletion).
/// Names removed from the view without new state (a document
/// *remove*) are recorded in `view_only`: they never touch the
/// store directly, and view reconciliation exempts them.
/// Addressing mirrors `WatchState::on_response`: a document
/// change carried for a target it was removed from still
/// reports the document's latest remote state. A `RESET`
/// discards the buffer: the event it belonged to is void and
/// its state is resent. Whether the response completes the
/// event at an *accepted* consistent point is the state
/// machine's call (`WatchState::take_consistent_point`), not
/// this function's: the raw message shape would also admit
/// stale barriers and barriers addressed to other targets.
fn buffer_remote(
    response: &ListenResponse,
    target_id: i32,
    buffer: &mut BTreeMap<String, Option<Document>>,
    view_only: &mut std::collections::BTreeSet<String>,
) {
    let Some(response_type) = &response.response_type else {
        return;
    };
    match response_type {
        ResponseType::DocumentChange(change) => {
            let applies = change.removed_target_ids.contains(&target_id)
                || change.target_ids.is_empty()
                || change.target_ids.contains(&target_id);
            if applies {
                if let Some(document) = &change.document {
                    if let Ok(document) = Document::from_proto(document.clone()) {
                        buffer.insert(document.name.clone(), Some(document));
                    }
                }
            }
        }
        ResponseType::DocumentDelete(delete) => {
            if delete.removed_target_ids.is_empty()
                || delete.removed_target_ids.contains(&target_id)
            {
                buffer.insert(delete.document.clone(), None);
            }
        }
        ResponseType::DocumentRemove(remove) => {
            if remove.removed_target_ids.is_empty()
                || remove.removed_target_ids.contains(&target_id)
            {
                view_only.insert(remove.document.clone());
            }
        }
        ResponseType::TargetChange(change)
            if change.target_change_type() == TargetChangeType::Reset =>
        {
            buffer.clear();
            view_only.clear();
        }
        _ => {}
    }
}

/// Reconciles the store with the state machine's view at a
/// consistent point: documents that were in the view at the
/// previous point, are absent from the current one, and were
/// neither re-reported by this event with new state nor
/// removed view-only have left the server state the view was
/// built from — the synthesized deletes of a fresh add or an
/// existence-filter restart, and bloom-confirmed removals.
/// Their remote rows leave the store, under two guards: a
/// document held by a pending local mutation keeps its base
/// row until the mutation settles, and the removal obeys the
/// same newer-wins rule as puts — the point establishes the
/// absence only of versions up to its `read_time`, so a row
/// the store has since advanced past it (an acknowledgement
/// fold, another watch) is not covered and stays. An
/// unversioned point establishes nothing.
fn reconcile_view_removals(
    watch: &StoreWatch,
    previous: &[Document],
    current: &[Document],
    buffer: &BTreeMap<String, Option<Document>>,
    view_only: &std::collections::BTreeSet<String>,
    read_time: Option<&crate::value::Timestamp>,
) {
    let surviving: std::collections::BTreeSet<&str> = current
        .iter()
        .map(|document| document.name.as_str())
        .collect();
    let reported: std::collections::BTreeSet<&str> = buffer
        .iter()
        .filter(|(_, change)| change.is_some())
        .map(|(name, _)| name.as_str())
        .collect();
    for document in previous {
        if surviving.contains(document.name.as_str())
            || reported.contains(document.name.as_str())
            || view_only.contains(&document.name)
        {
            continue;
        }
        let held = watch
            .engine
            .relative_path_of(&document.name)
            .is_some_and(|relative| watch.engine.has_pending(relative).unwrap_or(false));
        if held {
            continue;
        }
        let covered = match read_time {
            Some(read_time) => match watch.engine.store().remote_document(&document.name) {
                Ok(Some(row)) => row.update_time.map_or(true, |update| {
                    (update.seconds, update.nanos) <= (read_time.seconds, read_time.nanos)
                }),
                // No row (or unreadable): nothing to remove.
                _ => false,
            },
            None => false,
        };
        if covered {
            let _ = watch.engine.store_remote_absent_name(&document.name);
        }
    }
}

/// Sleeps for `delay`, but keeps a store-backed listener
/// current while waiting: local view changes are composed and
/// delivered as they happen. Offline latency compensation must
/// not pause for reconnect backoff — a queued write is visible
/// to the listener while the client is still failing to open a
/// stream. Returns false when the listener is gone.
async fn sleep_with_delivery(
    delay: Duration,
    store_watch: &Option<StoreWatch>,
    delivery: &mut StoreDelivery,
    sender: &tokio::sync::mpsc::Sender<Result<Snapshot>>,
) -> bool {
    let Some(watch) = store_watch else {
        tokio::time::sleep(delay).await;
        return true;
    };
    let deadline = tokio::time::Instant::now() + delay;
    loop {
        if watch.engine.generation() != delivery.seen_generation {
            if !delivery.emit(watch, sender, true).await {
                return false;
            }
            continue;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return true;
        }
        let notified = watch.engine.view_notified();
        tokio::pin!(notified);
        let _ = notified.as_mut().enable();
        tokio::select! {
            _ = tokio::time::sleep(remaining) => return true,
            _ = notified => {
                if !delivery.emit(watch, sender, true).await {
                    return false;
                }
            }
        }
    }
}

/// Applies buffered remote changes to the store, completing one
/// remote event. Puts never regress a newer cached state (a
/// write acknowledgement may have folded fresher state while
/// the event was in flight).
fn flush_remote(engine: &PersistenceEngine, buffer: &mut BTreeMap<String, Option<Document>>) {
    for (name, change) in std::mem::take(buffer) {
        match change {
            Some(document) => {
                let _ = engine.store_remote_document_if_newer(&document);
            }
            None => {
                let _ = engine.store_remote_absent_name(&name);
            }
        }
    }
}

/// Spawns the driver task for one watch and returns its handle.
/// `query` is the watch's query for query targets (used to
/// decide limbo resolution outcomes); document watches pass
/// `None` and never resolve limbo.
pub(crate) fn spawn_watch(
    channel: Channel,
    options: FirebaseOptions,
    tokens: Arc<dyn TokenProvider>,
    target: Target,
    orders: Vec<Order>,
    store_watch: Option<StoreWatch>,
    query: Option<Query>,
) -> Listener {
    let (sender, receiver) = tokio::sync::mpsc::channel(16);
    let task = tokio::spawn(async move {
        run_watch(
            channel,
            options,
            tokens,
            target,
            orders,
            store_watch,
            query,
            sender,
        )
        .await;
    });
    Listener { receiver, task }
}

/// How the watch driver reacts to one processed response.
enum ResponseAction {
    /// Keep listening; nothing to deliver.
    Continue,
    /// Deliver this snapshot and keep listening.
    Deliver(Snapshot),
    /// Deliver this snapshot first (if any), then re-add the
    /// target on a fresh stream: an existence-filter mismatch
    /// was applied at a consistent point and the target's view
    /// was reset.
    Restart(Option<Snapshot>),
    /// The target was removed; the listener ends. Carries the
    /// removal error, if any, to deliver before ending.
    Removed(Option<Error>),
}

/// Feeds one response to the watch state and decides the
/// driver's reaction. The filter-restart signal is drained
/// after *every* processed response, not only when a snapshot
/// was emitted: applying a mismatching filter to an
/// already-delivered empty view emits nothing (the diff is
/// empty and the initial snapshot already went out), but the
/// target must still be re-added without its token, or the
/// listener would stay empty indefinitely while the server
/// reports documents it should hold.
fn response_action(state: &mut WatchState, response: ListenResponse) -> ResponseAction {
    let event = state.on_response(response);
    let restart = state.take_filter_restart();
    match event {
        Some(WatchEvent::Snapshot(snapshot)) => {
            if restart {
                ResponseAction::Restart(Some(snapshot))
            } else {
                ResponseAction::Deliver(snapshot)
            }
        }
        Some(WatchEvent::Removed(error)) => ResponseAction::Removed(error),
        None => {
            if restart {
                ResponseAction::Restart(None)
            } else {
                ResponseAction::Continue
            }
        }
    }
}

// The parameter list mirrors `spawn_watch`'s: the watch's full
// construction context, assembled once by the client.
#[allow(clippy::too_many_arguments)]
async fn run_watch(
    channel: Channel,
    options: FirebaseOptions,
    tokens: Arc<dyn TokenProvider>,
    target: Target,
    orders: Vec<Order>,
    store_watch: Option<StoreWatch>,
    query: Option<Query>,
    sender: tokio::sync::mpsc::Sender<Result<Snapshot>>,
) {
    let database = options.database_name();
    let mut state = WatchState::new(target.target_id, orders);
    // Query watches verify unconfirmed documents individually
    // (limbo resolution) instead of dropping them at the first
    // consistent point; document watches keep the drop policy.
    let limbo_enabled = query.is_some();
    state.set_keep_restart_unconfirmed(limbo_enabled);
    let mut limbo = LimboTracker::new();
    let mut backoff = Backoff::new();
    let mut force_refresh = false;

    // A store-backed watch seeds its state machine from the
    // store (restoring the target's persisted resume token)
    // before the first stream opens, and delivers one cache
    // snapshot immediately. `last_state_docs` tracks the state
    // machine's own view at the last consistent point — the
    // baseline an existence-filter restart's casualties are
    // computed against.
    let mut delivery = StoreDelivery::new();
    let mut last_state_docs: Vec<Document> = Vec::new();
    if let Some(watch) = &store_watch {
        // A watch-only workload may never enqueue a write, so
        // make sure the pump (and with it idle-time garbage
        // collection) is running for this engine.
        watch.engine.ensure_pump();
        last_state_docs = watch.seed(&mut state);
        if !delivery.emit(watch, &sender, true).await {
            return;
        }
    }

    'reconnect: loop {
        if sender.is_closed() {
            return;
        }
        let mut client = match grpc_client(channel.clone(), &options, &tokens, force_refresh).await
        {
            Ok(client) => client,
            Err(error) if is_transient_error(&error) => {
                // Acquiring the token hit a transient failure (e.g.
                // the network is down while a refresh is due). The
                // watch must survive outages: back off and retry
                // instead of ending permanently.
                if !sleep_with_delivery(backoff.next_delay(), &store_watch, &mut delivery, &sender)
                    .await
                {
                    return;
                }
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
                if is_terminal(status.code()) && !is_database_mismatch(&status) {
                    let _ = sender.send(Err(map_status(status))).await;
                    return;
                }
                if status.code() == Code::Unauthenticated {
                    force_refresh = true;
                }
                if !sleep_with_delivery(backoff.next_delay(), &store_watch, &mut delivery, &sender)
                    .await
                {
                    return;
                }
                continue 'reconnect;
            }
        };

        // (Re)start any limbo resolutions the previous stream
        // left queued.
        if limbo_enabled {
            limbo.pump(&request_sender, &database).await;
        }

        // Mid-stream loop. `restart` asks for an immediate fresh
        // stream (existence-filter recovery), skipping the backoff.
        // The remote-change buffer is per-stream: a stream that
        // dies mid-event is resumed from the last consistent
        // point and the partial event is resent whole.
        let mut restart = false;
        let mut buffer: BTreeMap<String, Option<Document>> = BTreeMap::new();
        let mut view_only: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        'stream: loop {
            // Register the view-change wakeup before checking
            // the generation, so a change in between is not
            // missed; the generation is the source of truth.
            let view_wait = async {
                match &store_watch {
                    Some(watch) => {
                        let notified = watch.engine.view_notified();
                        tokio::pin!(notified);
                        let _ = notified.as_mut().enable();
                        notified.await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            if let Some(watch) = &store_watch {
                if watch.engine.generation() != delivery.seen_generation {
                    if !delivery.emit(watch, &sender, true).await {
                        return;
                    }
                    continue 'stream;
                }
            }
            let message = tokio::select! {
                message = stream.message() => message,
                _ = view_wait => {
                    // Local state changed (a queued write, an
                    // acknowledgement fold, a cached fetch):
                    // recompose and deliver if the view moved.
                    if let Some(watch) = &store_watch {
                        if !delivery.emit(watch, &sender, true).await {
                            return;
                        }
                    }
                    continue 'stream;
                }
            };
            match message {
                Ok(Some(response)) => {
                    // Limbo bookkeeping around the main state's
                    // processing. Settled resolutions apply to
                    // the query state before the barrier that
                    // settled them is processed, so the snapshot
                    // it produces already carries their outcomes;
                    // a document-remove on the query target is
                    // not a removal at all — the document is
                    // verified individually instead of being fed
                    // to the state; a failed resolution settles
                    // as absent in the state's pending set, to
                    // publish at the next real barrier.
                    // `to_process` is the ordered list of
                    // responses the main state sees.
                    let mut to_process: Vec<ListenResponse> = Vec::new();
                    if limbo_enabled {
                        if is_settle_barrier(&response, target.target_id, &limbo) {
                            for (limbo_target, key, outcome) in limbo.settle(&response) {
                                apply_limbo_outcome(
                                    &mut state,
                                    query.as_ref(),
                                    target.target_id,
                                    &key,
                                    outcome,
                                    store_watch.is_some(),
                                    &mut buffer,
                                );
                                let remove = ListenRequest {
                                    database: database.clone(),
                                    target_change: Some(RequestTargetChange::RemoveTarget(
                                        limbo_target,
                                    )),
                                    labels: Default::default(),
                                    request_options: None,
                                };
                                let _ = request_sender.send(remove).await;
                            }
                        }
                        match &response.response_type {
                            Some(ResponseType::DocumentRemove(remove))
                                if remove.removed_target_ids.contains(&target.target_id) =>
                            {
                                limbo.enqueue(remove.document.clone());
                                if store_watch.is_some() {
                                    buffer_remote(
                                        &response,
                                        target.target_id,
                                        &mut buffer,
                                        &mut view_only,
                                    );
                                }
                            }
                            _ => {
                                let failed = limbo.feed(&response);
                                // A failed resolution settles as
                                // absent, but only its own outcome
                                // is applied (to the state's pending
                                // set and the store buffer): it
                                // publishes at the next real accepted
                                // barrier. A synthetic barrier at
                                // the last accepted read time would
                                // also emit and flush unrelated
                                // changes of the in-flight remote
                                // event, at a stale version.
                                for key in &failed {
                                    apply_limbo_outcome(
                                        &mut state,
                                        query.as_ref(),
                                        target.target_id,
                                        key,
                                        None,
                                        store_watch.is_some(),
                                        &mut buffer,
                                    );
                                }
                                to_process.push(response);
                            }
                        }
                    } else {
                        to_process.push(response);
                    }
                    for response in to_process {
                        if store_watch.is_some() {
                            buffer_remote(&response, target.target_id, &mut buffer, &mut view_only);
                        }
                        let action = response_action(&mut state, response);
                        // Whether the state machine accepted a
                        // consistent point in this response (not
                        // stale, addressed to this target): only
                        // then does the buffered remote event
                        // complete and land in the store.
                        let accepted = state.take_consistent_point();
                        match action {
                            ResponseAction::Deliver(snapshot) => {
                                backoff.reset();
                                if let Some(watch) = &store_watch {
                                    // Landing is serialized
                                    // against garbage
                                    // collection: the flushed
                                    // documents are protected
                                    // only once their membership
                                    // is recorded, and no pass
                                    // may run in between.
                                    {
                                        let _landing = watch.engine.landing_guard();
                                        reconcile_view_removals(
                                            watch,
                                            &last_state_docs,
                                            &snapshot.documents,
                                            &buffer,
                                            &view_only,
                                            snapshot.read_time.as_ref(),
                                        );
                                        flush_remote(&watch.engine, &mut buffer);
                                        view_only.clear();
                                        watch.persist_target(&state, snapshot.read_time);
                                        watch.sync_membership(&snapshot.documents, true);
                                        watch.engine.wake_pump();
                                    }
                                    last_state_docs = snapshot.documents.clone();
                                    delivery.last_read_time = snapshot.read_time;
                                    if !delivery.emit(watch, &sender, !state.is_current()).await {
                                        return;
                                    }
                                } else if sender.send(Ok(snapshot)).await.is_err() {
                                    return;
                                }
                            }
                            ResponseAction::Restart(snapshot) => {
                                restart = true;
                                if let Some(watch) = &store_watch {
                                    // The event that restarted the
                                    // target is discarded: documents
                                    // the state dropped from its view
                                    // (the synthesized deletes) leave
                                    // the store through the same
                                    // reconciliation as an ordinary
                                    // delivery; the event's own puts
                                    // still land. (Landing/GC
                                    // serialization as in the
                                    // Deliver branch.)
                                    {
                                        let _landing = watch.engine.landing_guard();
                                        reconcile_view_removals(
                                            watch,
                                            &last_state_docs,
                                            snapshot
                                                .as_ref()
                                                .map(|snapshot| snapshot.documents.as_slice())
                                                .unwrap_or(&[]),
                                            &buffer,
                                            &view_only,
                                            snapshot
                                                .as_ref()
                                                .and_then(|snapshot| snapshot.read_time.as_ref()),
                                        );
                                        flush_remote(&watch.engine, &mut buffer);
                                        view_only.clear();
                                        watch.persist_target(
                                            &state,
                                            snapshot
                                                .as_ref()
                                                .and_then(|snapshot| snapshot.read_time),
                                        );
                                        if let Some(snapshot) = &snapshot {
                                            delivery.last_read_time = snapshot.read_time;
                                            last_state_docs = snapshot.documents.clone();
                                        } else {
                                            last_state_docs.clear();
                                        }
                                        watch.sync_membership(&last_state_docs, true);
                                        watch.engine.wake_pump();
                                    }
                                    if !delivery.emit(watch, &sender, !state.is_current()).await {
                                        return;
                                    }
                                } else if let Some(snapshot) = snapshot {
                                    let _ = sender.send(Ok(snapshot)).await;
                                }
                                break 'stream;
                            }
                            ResponseAction::Removed(error) => {
                                if let Some(error) = error {
                                    let _ = sender.send(Err(error)).await;
                                }
                                return;
                            }
                            ResponseAction::Continue => {
                                // An accepted consistent point that
                                // produced no snapshot still completes
                                // a remote event: apply its buffered
                                // changes. (A stale or foreign barrier
                                // is not accepted and flushes nothing.)
                                if accepted {
                                    if let Some(watch) = &store_watch {
                                        let _landing = watch.engine.landing_guard();
                                        flush_remote(&watch.engine, &mut buffer);
                                        view_only.clear();
                                        let view = state.effective_view();
                                        watch.sync_membership(&view, true);
                                        watch.engine.wake_pump();
                                    }
                                }
                            }
                        }
                    }
                    // Candidates this processing produced start
                    // resolving (and slots freed by settlements
                    // are refilled).
                    if limbo_enabled {
                        for name in state.take_limbo_candidates() {
                            limbo.enqueue(name);
                        }
                        limbo.pump(&request_sender, &database).await;
                    }
                }
                Ok(None) => break, // Server closed the stream; reconnect.
                Err(status) => {
                    if is_terminal(status.code()) && !is_database_mismatch(&status) {
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

        // The stream ended: in-flight limbo resolutions lose
        // their wire targets and restart on the next stream.
        limbo.restart_session();

        if sender.is_closed() {
            return;
        }
        if !restart
            && !sleep_with_delivery(backoff.next_delay(), &store_watch, &mut delivery, &sender)
                .await
        {
            return;
        }
    }
}

/// Whether a failure is transient (transport or retryable
/// service errors) as opposed to permanent (no session, rejected
/// credentials, configuration errors, validation). Used for
/// token-acquisition retries in the watch driver and for the
/// persistent `run_query` offline fallback, which must only
/// engage on connectivity failures.
pub(crate) fn is_transient_error(error: &Error) -> bool {
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

/// The most limbo resolutions (document targets) one watch
/// runs at once; the rest queue. Document targets are cheap,
/// but an existence-filter restart can nominate a whole cached
/// collection at once.
const MAX_CONCURRENT_LIMBO_RESOLUTIONS: usize = 10;

/// One started limbo resolution: a document target verifying
/// a single document the query target could not confirm (an
/// existence-filter casualty, a document-remove on the query
/// target, an unconfirmed seeded document).
struct LimboRes {
    /// The document's full resource name.
    key: String,
    target_id: i32,
    state: WatchState,
    /// The resolution's outcome, captured when its state first
    /// emitted a snapshot (at a consistent point of its own):
    /// `Some(Some(doc))` confirmed, `Some(None)` absent. The
    /// outcome is applied at the next settle barrier, so a
    /// state that emitted early (e.g. at a CURRENT carrying a
    /// read time) is not lost.
    ready: Option<Option<Document>>,
}

/// The driver's limbo bookkeeping for one watch: candidates
/// waiting for a slot and the resolutions in flight. Mirrors
/// the reference client's model (and the spec harness's): a
/// candidate is verified by its own document target; the
/// outcome lands on the query target at a stream-wide barrier.
struct LimboTracker {
    waiting: std::collections::VecDeque<String>,
    active: Vec<LimboRes>,
    next_target_id: i32,
}

impl LimboTracker {
    fn new() -> Self {
        Self {
            waiting: std::collections::VecDeque::new(),
            active: Vec::new(),
            // The query target holds TARGET_ID (2); limbo
            // targets take the following even ids.
            next_target_id: TARGET_ID + 2,
        }
    }

    /// Queues a candidate unless it is already waiting or in
    /// flight.
    fn enqueue(&mut self, name: String) {
        if self.waiting.contains(&name) || self.active.iter().any(|res| res.key == name) {
            return;
        }
        self.waiting.push_back(name);
    }

    fn is_limbo_target(&self, target_id: i32) -> bool {
        self.active.iter().any(|res| res.target_id == target_id)
    }

    /// Starts waiting resolutions while a slot is free,
    /// sending their document-target adds on the stream.
    async fn pump(
        &mut self,
        request_sender: &tokio::sync::mpsc::Sender<ListenRequest>,
        database: &str,
    ) {
        while self.active.len() < MAX_CONCURRENT_LIMBO_RESOLUTIONS {
            let Some(key) = self.waiting.pop_front() else {
                break;
            };
            let target_id = self.next_target_id;
            self.next_target_id += 2;
            let target = Target {
                target_id,
                once: false,
                expected_count: None,
                target_type: Some(crate::google::firestore::v1::target::TargetType::Documents(
                    crate::google::firestore::v1::target::DocumentsTarget {
                        documents: vec![key.clone()],
                    },
                )),
                resume_type: None,
            };
            let request = ListenRequest {
                database: database.to_string(),
                target_change: Some(RequestTargetChange::AddTarget(target)),
                labels: Default::default(),
                request_options: None,
            };
            if request_sender.send(request).await.is_err() {
                // The stream is gone; the resolution restarts
                // with the next stream (see `restart_session`).
                self.waiting.push_front(key);
                break;
            }
            self.active.push(LimboRes {
                key,
                target_id,
                state: WatchState::new(target_id, Vec::new()),
                ready: None,
            });
        }
    }

    /// Feeds a non-barrier response to every in-flight
    /// resolution (each state ignores what is not addressed to
    /// it), capturing outcomes as they emit. Returns the keys
    /// whose targets the server removed with an error: a failed
    /// resolution, settled by the caller as absent at once.
    /// Failed resolutions leave `active` here.
    fn feed(&mut self, response: &ListenResponse) -> Vec<String> {
        let mut failed = Vec::new();
        let mut index = 0;
        while index < self.active.len() {
            let res = &mut self.active[index];
            match res.state.on_response(response.clone()) {
                Some(WatchEvent::Snapshot(snapshot)) => {
                    if res.ready.is_none() {
                        res.ready = Some(
                            snapshot
                                .documents
                                .into_iter()
                                .find(|document| document.name == res.key),
                        );
                    }
                    index += 1;
                }
                Some(WatchEvent::Removed(_)) => {
                    failed.push(res.key.clone());
                    self.active.remove(index);
                }
                None => index += 1,
            }
        }
        failed
    }

    /// Settles resolutions at a barrier: every in-flight state
    /// is fed the barrier, and resolutions whose outcome is
    /// known (captured earlier, or emitted by this barrier)
    /// complete. Returns (target id, key, outcome) for each
    /// settled resolution; settled resolutions leave `active`.
    fn settle(&mut self, barrier: &ListenResponse) -> Vec<(i32, String, Option<Document>)> {
        let mut settled = Vec::new();
        let mut index = 0;
        while index < self.active.len() {
            let outcome = {
                let res = &mut self.active[index];
                // Always advance the state through the barrier:
                // it may carry newer state than the early capture
                // (changes that arrived after the resolution first
                // emitted would otherwise be discarded with the
                // resolution). The capture is only the fallback
                // for when the barrier produces nothing new.
                match res.state.on_response(barrier.clone()) {
                    Some(WatchEvent::Snapshot(snapshot)) => Some(
                        snapshot
                            .documents
                            .into_iter()
                            .find(|document| document.name == res.key),
                    ),
                    _ => res.ready.take(),
                }
            };
            match outcome {
                Some(outcome) => {
                    let res = self.active.remove(index);
                    settled.push((res.target_id, res.key, outcome));
                }
                None => index += 1,
            }
        }
        settled
    }

    /// The stream ended: in-flight resolutions lose their wire
    /// targets and restart from scratch on the next stream
    /// (their candidates return to the queue, in order).
    fn restart_session(&mut self) {
        let mut keys: Vec<String> = self.active.drain(..).map(|res| res.key).collect();
        keys.extend(self.waiting.drain(..));
        self.waiting = keys.into();
    }
}

/// Whether a response is a barrier at which limbo resolutions
/// settle: a NO_CHANGE addressed stream-wide, to the query
/// target, or to any limbo target. (A barrier addressed only
/// to foreign targets completes nothing here.)
fn is_settle_barrier(response: &ListenResponse, main_target_id: i32, limbo: &LimboTracker) -> bool {
    match &response.response_type {
        Some(ResponseType::TargetChange(change)) => {
            change.target_change_type() == TargetChangeType::NoChange
                && (change.target_ids.is_empty()
                    || change.target_ids.contains(&main_target_id)
                    || change
                        .target_ids
                        .iter()
                        .any(|id| limbo.is_limbo_target(*id)))
        }
        _ => false,
    }
}

/// Feeds a synthetic document-remove for `key` to the query
/// state: the mechanism a settled limbo resolution uses to
/// take its document out of the query view.
fn feed_synthetic_remove(state: &mut WatchState, main_target_id: i32, key: &str) {
    let _ = state.on_response(ListenResponse {
        response_type: Some(ResponseType::DocumentRemove(
            crate::google::firestore::v1::DocumentRemove {
                document: key.to_string(),
                removed_target_ids: vec![main_target_id],
                read_time: None,
            },
        )),
    });
}

/// Applies one settled limbo resolution to the query state
/// (and, for store-backed watches, to the remote buffer the
/// enclosing consistent point will flush): a confirmed
/// document the query still matches is fed as a change; a
/// confirmed document that no longer matches, and an absent
/// document, are fed as removes. The store learns the
/// confirmed document's latest state either way, and the
/// absent document's tombstone.
#[allow(clippy::too_many_arguments)]
fn apply_limbo_outcome(
    state: &mut WatchState,
    query: Option<&Query>,
    main_target_id: i32,
    key: &str,
    outcome: Option<Document>,
    store_backed: bool,
    buffer: &mut BTreeMap<String, Option<Document>>,
) {
    match outcome {
        Some(document) => {
            if store_backed {
                buffer.insert(document.name.clone(), Some(document.clone()));
            }
            // Filter membership alone does not entitle the
            // document to a place in the view: the query's
            // semantics decide (filter match, order-field
            // existence, ordering and limit). Evaluate the
            // result window over the effective view with the
            // confirmed document in it; the document enters only
            // if it lands in the window, and view documents the
            // recomputed window displaces leave with it. The
            // offset is NOT re-applied: the view is the server's
            // already paginated window and does not contain the
            // documents the offset skipped, so offsetting it
            // again would skip valid results a second time.
            let (in_window, displaced) = match query {
                Some(query) => {
                    let view = state.effective_view();
                    let mut candidates: Vec<Document> = view
                        .iter()
                        .filter(|candidate| candidate.name != document.name)
                        .cloned()
                        .collect();
                    candidates.push(document.clone());
                    let window: std::collections::BTreeSet<String> = query
                        .without_offset()
                        .evaluate(candidates)
                        .into_iter()
                        .map(|doc| doc.name)
                        .collect();
                    let displaced: Vec<String> = view
                        .iter()
                        .map(|doc| doc.name.clone())
                        .filter(|name| name != &document.name && !window.contains(name))
                        .collect();
                    (window.contains(&document.name), displaced)
                }
                None => (true, Vec::new()),
            };
            if in_window {
                let proto = crate::google::firestore::v1::Document {
                    name: document.name.clone(),
                    fields: crate::value::fields_to_proto(&document.fields),
                    create_time: document.create_time.map(Into::into),
                    update_time: document.update_time.map(Into::into),
                };
                let _ = state.on_response(ListenResponse {
                    response_type: Some(ResponseType::DocumentChange(
                        crate::google::firestore::v1::DocumentChange {
                            document: Some(proto),
                            target_ids: vec![main_target_id],
                            removed_target_ids: Vec::new(),
                        },
                    )),
                });
            } else {
                feed_synthetic_remove(state, main_target_id, key);
            }
            for name in &displaced {
                feed_synthetic_remove(state, main_target_id, name);
            }
        }
        None => {
            if store_backed {
                buffer.insert(key.to_string(), None);
            }
            feed_synthetic_remove(state, main_target_id, key);
        }
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
    use crate::value::Value;
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

    #[test]
    fn pre_current_snapshots_stay_from_cache_until_current() {
        let mut state = watch();
        state.on_response(change("notes/a", &[("n", Value::Integer(1))]));
        // A stream-wide NO_CHANGE delivers the partial initial
        // set before CURRENT: not the initial sync yet.
        let event = state.on_response(no_change_at(2));
        let Some(WatchEvent::Snapshot(snapshot)) = event else {
            panic!("expected a pre-CURRENT snapshot, got {event:?}");
        };
        assert_eq!(snapshot.documents.len(), 1);
        assert!(snapshot.metadata.is_from_cache);
        // The rest of the initial set, then CURRENT: this
        // snapshot completes the initial sync.
        state.on_response(change("notes/b", &[("n", Value::Integer(1))]));
        let event = state.on_response(target_change(Type::Current, Some((3, 0)), b"t1"));
        let Some(WatchEvent::Snapshot(snapshot)) = event else {
            panic!("expected a CURRENT snapshot, got {event:?}");
        };
        assert_eq!(snapshot.documents.len(), 2);
        assert!(!snapshot.metadata.is_from_cache);
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
        // Server says two documents match; we hold one. The
        // filter is held until the next consistent point: no
        // immediate event.
        let event = state.on_response(ListenResponse {
            response_type: Some(ResponseType::Filter(ExistenceFilter {
                target_id: TARGET_ID,
                count: 2,
                unchanged_names: None,
            })),
        });
        assert!(event.is_none());
        assert!(!state.take_filter_restart());
        // At the consistent point the mismatch applies: the
        // restart is signaled and the resume token is dropped.
        state.on_response(no_change_at(2));
        assert!(state.take_filter_restart());
        assert_eq!(state.resume_token(), None);
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
        state.on_response(no_change_at(2));
        assert!(!state.take_filter_restart());
    }

    #[test]
    fn driver_restarts_on_filter_mismatch_without_snapshot() {
        // The empty-view mismatch: an empty CURRENT snapshot is
        // delivered, then an existence filter reports a document
        // the client does not hold. At the next consistent point
        // the watch state emits nothing (the diff is empty and
        // the initial snapshot already went out), but the driver
        // must still re-add the target — draining the restart
        // signal only on snapshots would leave the listener
        // empty indefinitely.
        let mut state = watch();
        let action = response_action(
            &mut state,
            target_change(Type::Current, Some((1, 0)), b"tok"),
        );
        assert!(
            matches!(action, ResponseAction::Deliver(_)),
            "the empty CURRENT snapshot is delivered"
        );
        let action = response_action(
            &mut state,
            ListenResponse {
                response_type: Some(ResponseType::Filter(ExistenceFilter {
                    target_id: TARGET_ID,
                    count: 1,
                    unchanged_names: None,
                })),
            },
        );
        assert!(
            matches!(action, ResponseAction::Continue),
            "the filter is held until the consistent point"
        );
        let action = response_action(&mut state, no_change_at(2));
        assert!(
            matches!(action, ResponseAction::Restart(None)),
            "the mismatch restarts the target even without a snapshot"
        );
        assert_eq!(state.resume_token(), None);
        // A matching filter at the barrier is not a restart.
        let mut state = watch();
        response_action(
            &mut state,
            target_change(Type::Current, Some((1, 0)), b"tok"),
        );
        response_action(
            &mut state,
            ListenResponse {
                response_type: Some(ResponseType::Filter(ExistenceFilter {
                    target_id: TARGET_ID,
                    count: 0,
                    unchanged_names: None,
                })),
            },
        );
        let action = response_action(&mut state, no_change_at(2));
        assert!(matches!(action, ResponseAction::Continue));
        // A mismatch on a non-empty view still restarts, with
        // the removal snapshot delivered first.
        let mut state = watch();
        response_action(&mut state, change("notes/a", &[("n", Value::Integer(1))]));
        response_action(&mut state, target_change(Type::Current, Some((1, 0)), b""));
        response_action(
            &mut state,
            ListenResponse {
                response_type: Some(ResponseType::Filter(ExistenceFilter {
                    target_id: TARGET_ID,
                    count: 2,
                    unchanged_names: None,
                })),
            },
        );
        let action = response_action(&mut state, no_change_at(2));
        assert!(
            matches!(action, ResponseAction::Restart(Some(_))),
            "a non-empty mismatch restarts with its snapshot"
        );
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
        assert!(is_transient_error(&Error::Http("offline".to_string())));
        assert!(is_transient_error(&Error::Rpc {
            code: "Unavailable".to_string(),
            message: "down".to_string(),
        }));
        assert!(!is_transient_error(&Error::Unauthenticated(
            "no user".to_string()
        )));
        assert!(!is_transient_error(&Error::Other("bad config".to_string())));
        // Auth REST statuses: throttling/server errors retry,
        // client errors and status-less API errors do not.
        let api = |status: Option<u16>| Error::Api {
            status,
            code: "X".to_string(),
            message: "x".to_string(),
        };
        for status in [429, 500, 502, 503, 504] {
            assert!(is_transient_error(&api(Some(status))), "{status}");
        }
        for status in [400, 401, 403, 404] {
            assert!(!is_transient_error(&api(Some(status))), "{status}");
        }
        assert!(!is_transient_error(&api(None)));
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

    #[test]
    fn buffer_remote_extracts_store_effects() {
        let mut buffer = BTreeMap::new();
        let mut view_only = std::collections::BTreeSet::new();
        // A change addressed to this target buffers a put.
        buffer_remote(
            &change("notes/a", &[("n", Value::Integer(1))]),
            TARGET_ID,
            &mut buffer,
            &mut view_only,
        );
        assert_eq!(buffer.len(), 1);
        assert!(buffer
            .get(&format!("{PREFIX}/notes/a"))
            .is_some_and(|change| change.is_some()));

        // A change addressed only to another target buffers
        // nothing; a change this target was removed from still
        // reports the document's latest remote state.
        let other = ListenResponse {
            response_type: Some(ResponseType::DocumentChange(ProtoDocumentChange {
                document: Some(doc("notes/b", &[("n", Value::Integer(1))])),
                target_ids: vec![99],
                removed_target_ids: vec![],
            })),
        };
        buffer_remote(&other, TARGET_ID, &mut buffer, &mut view_only);
        assert_eq!(buffer.len(), 1);
        let removed_from = ListenResponse {
            response_type: Some(ResponseType::DocumentChange(ProtoDocumentChange {
                document: Some(doc("notes/b", &[("n", Value::Integer(2))])),
                target_ids: vec![],
                removed_target_ids: vec![TARGET_ID],
            })),
        };
        buffer_remote(&removed_from, TARGET_ID, &mut buffer, &mut view_only);
        assert!(buffer.contains_key(&format!("{PREFIX}/notes/b")));

        // A delete buffers a tombstone; a remove is view-only
        // (recorded as such, the buffered put stands).
        let delete = ListenResponse {
            response_type: Some(ResponseType::DocumentDelete(
                crate::google::firestore::v1::DocumentDelete {
                    document: format!("{PREFIX}/notes/a"),
                    removed_target_ids: vec![TARGET_ID],
                    read_time: None,
                },
            )),
        };
        buffer_remote(&delete, TARGET_ID, &mut buffer, &mut view_only);
        assert!(buffer
            .get(&format!("{PREFIX}/notes/a"))
            .is_some_and(|change| change.is_none()));
        let remove = ListenResponse {
            response_type: Some(ResponseType::DocumentRemove(
                crate::google::firestore::v1::DocumentRemove {
                    document: format!("{PREFIX}/notes/b"),
                    removed_target_ids: vec![TARGET_ID],
                    read_time: None,
                },
            )),
        };
        buffer_remote(&remove, TARGET_ID, &mut buffer, &mut view_only);
        assert!(buffer
            .get(&format!("{PREFIX}/notes/b"))
            .is_some_and(|change| change.is_some()));
        assert!(view_only.contains(&format!("{PREFIX}/notes/b")));

        // A RESET voids the buffered event entirely.
        buffer_remote(
            &target_change(Type::Reset, None, b""),
            TARGET_ID,
            &mut buffer,
            &mut view_only,
        );
        assert!(buffer.is_empty());
        assert!(view_only.is_empty());
    }

    #[test]
    fn consistent_point_signal_tracks_acceptance() {
        let mut state = watch();
        // An accepted barrier sets the signal (drained once).
        state.on_response(change("notes/a", &[("n", Value::Integer(1))]));
        assert!(!state.take_consistent_point());
        state.on_response(no_change_at(10));
        assert!(state.take_consistent_point());
        assert!(!state.take_consistent_point());
        // A stale (reverted) barrier is not accepted: the state
        // machine rejects it, so the driver must not flush the
        // event it carries into the store.
        state.on_response(no_change_at(5));
        assert!(!state.take_consistent_point());
    }

    #[test]
    fn reconcile_view_removals_drops_synthesized_deletes() {
        let (engine, store_watch) = test_store_watch();
        engine
            .store_remote_document(&stored_doc("notes/a", 1))
            .unwrap();
        engine
            .store_remote_document(&stored_doc("notes/b", 2))
            .unwrap();
        let buffer = BTreeMap::new();
        let view_only = std::collections::BTreeSet::new();
        // stored_doc carries update time 10; a point at 15
        // covers it.
        let at_15 = Timestamp::new(15, 0);
        // The previous state view held both documents; the new
        // one holds neither and the event reported nothing: both
        // were synthesized deletes and leave the store.
        let previous = vec![stored_doc("notes/a", 1), stored_doc("notes/b", 2)];
        reconcile_view_removals(
            &store_watch,
            &previous,
            &[],
            &buffer,
            &view_only,
            Some(&at_15),
        );
        assert!(engine.local_document("notes/a").unwrap().is_none());
        assert!(engine.local_document("notes/b").unwrap().is_none());

        // A document held by a pending local mutation keeps its
        // base row until the mutation settles.
        engine
            .store_remote_document(&stored_doc("notes/a", 1))
            .unwrap();
        engine
            .store()
            .enqueue_batch(&[crate::mutation::Mutation::Patch {
                path: "notes/a".to_string(),
                fields: Fields::new(),
                mask: vec!["n".to_string()],
                exists: Some(true),
                transforms: vec![crate::mutation::FieldTransform {
                    path: "n".to_string(),
                    kind: crate::mutation::TransformKind::Increment(Value::Integer(1)),
                }],
            }])
            .unwrap();
        let previous = vec![stored_doc("notes/a", 1)];
        reconcile_view_removals(
            &store_watch,
            &previous,
            &[],
            &buffer,
            &view_only,
            Some(&at_15),
        );
        assert!(engine
            .store()
            .remote_document(&format!("{PREFIX}/notes/a"))
            .unwrap()
            .is_some());

        // Newer-wins: the point establishes the absence only of
        // versions up to its read time. A row advanced past it
        // after the baseline (an acknowledgement fold, another
        // watch) survives a point at 15, falls to a point at 25,
        // and an unversioned point establishes nothing.
        let (engine, store_watch) = test_store_watch();
        let versioned = |seconds: i64| {
            let mut document = stored_doc("notes/a", seconds);
            document.update_time = Some(Timestamp::new(seconds, 0));
            document
        };
        engine.store_remote_document(&versioned(10)).unwrap();
        let previous = vec![versioned(10)];
        engine.store_remote_document(&versioned(20)).unwrap();
        let row = |engine: &PersistenceEngine| {
            engine
                .store()
                .remote_document(&format!("{PREFIX}/notes/a"))
                .unwrap()
        };
        reconcile_view_removals(
            &store_watch,
            &previous,
            &[],
            &buffer,
            &view_only,
            Some(&at_15),
        );
        assert!(row(&engine).is_some(), "version 20 survives a point at 15");
        let at_25 = Timestamp::new(25, 0);
        reconcile_view_removals(
            &store_watch,
            &previous,
            &[],
            &buffer,
            &view_only,
            Some(&at_25),
        );
        assert!(row(&engine).is_none(), "a point at 25 covers version 20");
        engine.store_remote_document(&versioned(10)).unwrap();
        reconcile_view_removals(&store_watch, &previous, &[], &buffer, &view_only, None);
        assert!(row(&engine).is_some(), "an unversioned point keeps the row");
    }

    fn limbo_change(target_id: i32, name: &str, n: i64) -> ListenResponse {
        ListenResponse {
            response_type: Some(ResponseType::DocumentChange(ProtoDocumentChange {
                document: Some(doc(name, &[("n", Value::Integer(n))])),
                target_ids: vec![target_id],
                removed_target_ids: vec![],
            })),
        }
    }

    fn targeted_current(target_id: i32, seconds: i64) -> ListenResponse {
        ListenResponse {
            response_type: Some(ResponseType::TargetChange(TargetChange {
                target_change_type: Type::Current as i32,
                target_ids: vec![target_id],
                resume_token: Vec::new(),
                read_time: Some(prost_types::Timestamp { seconds, nanos: 0 }),
                cause: None,
            })),
        }
    }

    #[tokio::test]
    async fn limbo_tracker_pumps_with_cap_and_dedup() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(32);
        let mut limbo = LimboTracker::new();
        limbo.enqueue(format!("{PREFIX}/notes/a"));
        limbo.enqueue(format!("{PREFIX}/notes/a")); // duplicate: ignored
        for index in 0..11 {
            limbo.enqueue(format!("{PREFIX}/notes/d{index}"));
        }
        limbo.pump(&sender, "projects/p/databases/d").await;
        assert_eq!(limbo.active.len(), MAX_CONCURRENT_LIMBO_RESOLUTIONS);
        assert_eq!(limbo.waiting.len(), 2);
        let mut ids = std::collections::BTreeSet::new();
        for _ in 0..MAX_CONCURRENT_LIMBO_RESOLUTIONS {
            let request = receiver.recv().await.unwrap();
            let Some(RequestTargetChange::AddTarget(target)) = request.target_change else {
                panic!("expected an add-target request");
            };
            assert!(ids.insert(target.target_id), "ids must be unique");
            assert_ne!(target.target_id, TARGET_ID);
        }
        // A stream end requeues the in-flight resolutions in
        // order; they restart with fresh target ids.
        limbo.restart_session();
        assert!(limbo.active.is_empty());
        assert_eq!(limbo.waiting.len(), 12);
        limbo.pump(&sender, "projects/p/databases/d").await;
        let request = receiver.recv().await.unwrap();
        let Some(RequestTargetChange::AddTarget(target)) = request.target_change else {
            panic!("expected an add-target request");
        };
        assert!(!ids.contains(&target.target_id), "ids are not reused");
    }

    #[tokio::test]
    async fn limbo_resolution_confirms_at_the_settle_barrier() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(8);
        let mut limbo = LimboTracker::new();
        let key = format!("{PREFIX}/notes/a");
        limbo.enqueue(key.clone());
        limbo.pump(&sender, "projects/p/databases/d").await;
        let target_id = limbo.active[0].target_id;
        // The document target reports the document and becomes
        // current with a read time: the outcome is captured but
        // the resolution settles only at the barrier.
        assert!(limbo
            .feed(&limbo_change(target_id, "notes/a", 3))
            .is_empty());
        assert!(limbo.feed(&targeted_current(target_id, 5)).is_empty());
        assert_eq!(limbo.active.len(), 1);
        let settled = limbo.settle(&no_change_at(10));
        assert_eq!(settled.len(), 1);
        let (settled_id, settled_key, outcome) = settled.into_iter().next().unwrap();
        assert_eq!(settled_id, target_id);
        assert_eq!(settled_key, key);
        let document = outcome.expect("confirmed outcome");
        assert_eq!(document.fields.get("n"), Some(&Value::Integer(3)));
        assert!(limbo.active.is_empty());
    }

    #[tokio::test]
    async fn limbo_resolution_absent_and_failed() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(8);
        let mut limbo = LimboTracker::new();
        let key = format!("{PREFIX}/notes/a");
        limbo.enqueue(key.clone());
        limbo.pump(&sender, "projects/p/databases/d").await;
        let target_id = limbo.active[0].target_id;
        // Current with no document reported: the resolution is
        // absent, settled at the barrier.
        assert!(limbo.feed(&targeted_current(target_id, 5)).is_empty());
        let settled = limbo.settle(&no_change_at(10));
        assert_eq!(settled.len(), 1);
        assert_eq!(settled[0].1, key);
        assert!(settled[0].2.is_none());

        // A target removed with an error fails the resolution:
        // feed reports it and it leaves the active set at once.
        limbo.enqueue(key.clone());
        limbo.pump(&sender, "projects/p/databases/d").await;
        let target_id = limbo.active[0].target_id;
        let removed = ListenResponse {
            response_type: Some(ResponseType::TargetChange(TargetChange {
                target_change_type: Type::Remove as i32,
                target_ids: vec![target_id],
                resume_token: Vec::new(),
                read_time: None,
                cause: Some(crate::google::rpc::Status {
                    code: 7,
                    message: "denied".to_string(),
                    details: Vec::new(),
                }),
            })),
        };
        assert_eq!(limbo.feed(&removed), [key]);
        assert!(limbo.active.is_empty());
    }

    #[tokio::test]
    async fn limbo_settle_prefers_the_barriers_newer_state() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(8);
        let mut limbo = LimboTracker::new();
        let key = format!("{PREFIX}/notes/a");
        limbo.enqueue(key.clone());
        limbo.pump(&sender, "projects/p/databases/d").await;
        let target_id = limbo.active[0].target_id;
        // The outcome is captured early at n=1...
        assert!(limbo
            .feed(&limbo_change(target_id, "notes/a", 1))
            .is_empty());
        assert!(limbo.feed(&targeted_current(target_id, 5)).is_empty());
        // ...then a newer change arrives before the barrier.
        // Settlement must use the latest state, not the capture.
        assert!(limbo
            .feed(&limbo_change(target_id, "notes/a", 2))
            .is_empty());
        let settled = limbo.settle(&no_change_at(10));
        assert_eq!(settled.len(), 1);
        let document = settled
            .into_iter()
            .next()
            .unwrap()
            .2
            .expect("confirmed outcome");
        assert_eq!(document.fields.get("n"), Some(&Value::Integer(2)));
    }

    #[test]
    fn limbo_outcome_respects_the_query_window() {
        let key_a = format!("{PREFIX}/notes/a");
        let key_b = format!("{PREFIX}/notes/b");
        let query = Query::new("notes").order_by(Order::asc("n")).limit(1);
        let mut buffer: BTreeMap<String, Option<Document>> = BTreeMap::new();

        // The view holds b(n=1) and a(n=2), a being the limbo
        // candidate that stayed in the view. Confirming a must
        // not keep it: the top-1 window over {a, b} is [b].
        let mut state = WatchState::new(TARGET_ID, vec![Order::asc("n")]);
        state.set_keep_restart_unconfirmed(true);
        state.on_response(change("notes/b", &[("n", Value::Integer(1))]));
        state.on_response(change("notes/a", &[("n", Value::Integer(2))]));
        assert!(state.on_response(no_change_at(6)).is_some());
        let confirmed = Document::from_proto(doc("notes/a", &[("n", Value::Integer(2))])).unwrap();
        apply_limbo_outcome(
            &mut state,
            Some(&query),
            TARGET_ID,
            &key_a,
            Some(confirmed),
            false,
            &mut buffer,
        );
        let Some(WatchEvent::Snapshot(snapshot)) = state.on_response(no_change_at(10)) else {
            panic!("expected a snapshot at the barrier");
        };
        assert_eq!(snapshot.documents.len(), 1);
        assert_eq!(snapshot.documents[0].name, key_b);

        // Confirming a(n=0) instead lands it in the window and
        // displaces b.
        let mut state = WatchState::new(TARGET_ID, vec![Order::asc("n")]);
        state.set_keep_restart_unconfirmed(true);
        state.on_response(change("notes/b", &[("n", Value::Integer(1))]));
        state.on_response(change("notes/a", &[("n", Value::Integer(2))]));
        assert!(state.on_response(no_change_at(6)).is_some());
        let confirmed = Document::from_proto(doc("notes/a", &[("n", Value::Integer(0))])).unwrap();
        apply_limbo_outcome(
            &mut state,
            Some(&query),
            TARGET_ID,
            &key_a,
            Some(confirmed),
            false,
            &mut buffer,
        );
        let Some(WatchEvent::Snapshot(snapshot)) = state.on_response(no_change_at(10)) else {
            panic!("expected a snapshot at the barrier");
        };
        assert_eq!(snapshot.documents.len(), 1);
        assert_eq!(snapshot.documents[0].name, key_a);
        assert_eq!(
            snapshot.documents[0].fields.get("n"),
            Some(&Value::Integer(0))
        );
    }

    #[test]
    fn limbo_outcome_does_not_reapply_the_offset() {
        let key_b = format!("{PREFIX}/notes/b");
        // Collection a(n=1), b(n=2), c(n=3); the query orders by
        // n with offset 1, so the server's view is [b, c] — a
        // was skipped server-side and is not in the view.
        let query = Query::new("notes").order_by(Order::asc("n")).offset(1);
        let mut buffer: BTreeMap<String, Option<Document>> = BTreeMap::new();
        let mut state = WatchState::new(TARGET_ID, vec![Order::asc("n")]);
        state.set_keep_restart_unconfirmed(true);
        state.on_response(change("notes/b", &[("n", Value::Integer(2))]));
        state.on_response(change("notes/c", &[("n", Value::Integer(3))]));
        assert!(state.on_response(no_change_at(6)).is_some());
        // Confirming b unchanged must not shrink the view:
        // re-applying the offset to [b, c] would skip b.
        let confirmed = Document::from_proto(doc("notes/b", &[("n", Value::Integer(2))])).unwrap();
        apply_limbo_outcome(
            &mut state,
            Some(&query),
            TARGET_ID,
            &key_b,
            Some(confirmed),
            false,
            &mut buffer,
        );
        state.on_response(no_change_at(10));
        // A later change makes the view observable: it must
        // still hold b alongside c and the new d.
        state.on_response(change("notes/d", &[("n", Value::Integer(4))]));
        let Some(WatchEvent::Snapshot(snapshot)) = state.on_response(no_change_at(11)) else {
            panic!("expected a snapshot at the barrier");
        };
        assert_eq!(snapshot.documents.len(), 3);
        assert!(snapshot
            .documents
            .iter()
            .any(|document| document.name == key_b));
    }

    #[test]
    fn failed_limbo_outcome_waits_for_the_real_barrier() {
        let mut state = watch();
        state.set_keep_restart_unconfirmed(true);
        state.on_response(change("notes/a", &[("n", Value::Integer(1))]));
        let Some(WatchEvent::Snapshot(snapshot)) = state.on_response(no_change_at(10)) else {
            panic!("expected the initial snapshot");
        };
        assert_eq!(snapshot.documents.len(), 1);
        // The next remote event starts: b arrives, its barrier
        // has not.
        state.on_response(change("notes/b", &[("n", Value::Integer(2))]));
        // a's resolution fails; the driver applies the absent
        // outcome as a synthetic remove only. It must not
        // publish b early at the old version.
        let remove = ListenResponse {
            response_type: Some(ResponseType::DocumentRemove(
                crate::google::firestore::v1::DocumentRemove {
                    document: format!("{PREFIX}/notes/a"),
                    removed_target_ids: vec![TARGET_ID],
                    read_time: None,
                },
            )),
        };
        assert!(
            state.on_response(remove).is_none(),
            "no premature snapshot at the old version"
        );
        // At the event's own barrier, both changes publish
        // together at its read time.
        let Some(WatchEvent::Snapshot(snapshot)) = state.on_response(no_change_at(11)) else {
            panic!("expected a snapshot at the real barrier");
        };
        assert_eq!(snapshot.read_time, Some(Timestamp::new(11, 0)));
        assert_eq!(snapshot.documents.len(), 1);
        assert!(snapshot.documents[0].name.ends_with("/b"));
    }

    #[test]
    fn settle_barrier_classification() {
        let limbo = LimboTracker::new();
        // Stream-wide and query-addressed barriers settle.
        assert!(is_settle_barrier(&no_change_at(3), TARGET_ID, &limbo));
        // A barrier addressed only to a foreign target does not.
        let foreign = ListenResponse {
            response_type: Some(ResponseType::TargetChange(TargetChange {
                target_change_type: Type::NoChange as i32,
                target_ids: vec![99],
                resume_token: Vec::new(),
                read_time: Some(prost_types::Timestamp {
                    seconds: 3,
                    nanos: 0,
                }),
                cause: None,
            })),
        };
        assert!(!is_settle_barrier(&foreign, TARGET_ID, &limbo));
    }

    struct StubTokens;

    #[async_trait::async_trait]
    impl firebase_native_core::TokenProvider for StubTokens {
        async fn id_token(&self, _force_refresh: bool) -> Result<String> {
            Ok("stub-token".to_string())
        }
    }

    fn test_store_watch() -> (Arc<PersistenceEngine>, StoreWatch) {
        let store = Arc::new(crate::store::LocalStore::open_in_memory().unwrap());
        let options = FirebaseOptions::new("test-key", "p");
        let write = crate::write::WriteStream::new(
            options.clone(),
            Arc::new(StubTokens),
            Arc::new(tokio::sync::OnceCell::new()),
            "https://firestore.googleapis.com".to_string(),
        );
        let engine = Arc::new(PersistenceEngine::new(store, write, PREFIX.to_string()));
        let watch = StoreWatch::new(
            engine.clone(),
            WatchSource::Query {
                parent_relative: String::new(),
                query: Query::new("notes"),
            },
            42,
        );
        (engine, watch)
    }

    #[test]
    fn filtered_watch_sync_does_not_clear_collection_incompleteness() {
        // An engine whose limit fits two of three ~1 KiB
        // documents, so a collection pass cuts the collection.
        let store = Arc::new(crate::store::LocalStore::open_in_memory().unwrap());
        let options = FirebaseOptions::new("test-key", "p");
        let write = crate::write::WriteStream::new(
            options.clone(),
            Arc::new(StubTokens),
            Arc::new(tokio::sync::OnceCell::new()),
            "https://firestore.googleapis.com".to_string(),
        );
        let engine = Arc::new(
            PersistenceEngine::new(store, write, PREFIX.to_string())
                .with_cache_size_limit(Some(2500)),
        );
        let big = |name: &str| {
            let mut fields = Fields::new();
            fields.insert("blob".to_string(), Value::String("x".repeat(1000)));
            Document {
                name: format!("{PREFIX}/{name}"),
                fields,
                create_time: None,
                update_time: Some(Timestamp::new(10, 0)),
            }
        };
        engine.store_remote_document(&big("notes/a")).unwrap();
        engine.store_remote_document(&big("notes/b")).unwrap();
        engine.store_remote_document(&big("notes/c")).unwrap();
        engine.collect_garbage_if_needed();
        assert!(
            !engine.collection_known("", "notes").unwrap(),
            "eviction marked the collection incomplete"
        );
        // A filtered watch syncs only its own result set: the
        // collection-wide mark must survive it.
        let filtered = StoreWatch::new(
            engine.clone(),
            WatchSource::Query {
                parent_relative: String::new(),
                query: Query::new("notes").filter(crate::query::Filter::eq("n", Value::Integer(2))),
            },
            43,
        );
        filtered.sync_membership(std::slice::from_ref(&big("notes/b")), true);
        assert!(
            !engine.collection_known("", "notes").unwrap(),
            "a filtered sync must not clear the mark"
        );
        // An unfiltered, unbounded watch sync does clear it.
        let full = StoreWatch::new(
            engine.clone(),
            WatchSource::Query {
                parent_relative: String::new(),
                query: Query::new("notes"),
            },
            44,
        );
        full.sync_membership(&[big("notes/b"), big("notes/c")], true);
        assert!(engine.collection_known("", "notes").unwrap());
    }

    fn stored_doc(name: &str, n: i64) -> Document {
        let mut fields = Fields::new();
        fields.insert("n".to_string(), Value::Integer(n));
        Document {
            name: format!("{PREFIX}/{name}"),
            fields,
            create_time: None,
            update_time: Some(Timestamp::new(10, 0)),
        }
    }

    #[tokio::test]
    async fn store_delivery_composes_from_the_store() {
        let (engine, store_watch) = test_store_watch();
        engine
            .store_remote_document(&stored_doc("notes/a", 1))
            .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
        let mut delivery = StoreDelivery::new();

        // The first emit is the cache snapshot.
        assert!(delivery.emit(&store_watch, &sender, true).await);
        let snapshot = receiver.recv().await.unwrap().unwrap();
        assert_eq!(snapshot.documents.len(), 1);
        assert_eq!(snapshot.changes.len(), 1);
        assert_eq!(snapshot.changes[0].kind, DocumentChangeKind::Added);
        // The cache snapshot: from cache, nothing pending.
        assert!(snapshot.metadata.is_from_cache);
        assert!(!snapshot.metadata.has_pending_writes);

        // Nothing changed: no further snapshot.
        assert!(delivery.emit(&store_watch, &sender, true).await);
        assert!(receiver.try_recv().is_err());

        // A queued local write appears in the composed view as
        // soon as it is queued (latency compensation), without
        // any stream traffic.
        engine
            .store()
            .enqueue_batch(&[crate::mutation::Mutation::Set {
                path: "notes/b".to_string(),
                fields: {
                    let mut fields = Fields::new();
                    fields.insert("n".to_string(), Value::Integer(2));
                    fields
                },
                merge_paths: None,
                exists: None,
                transforms: Vec::new(),
            }])
            .unwrap();
        assert!(delivery.emit(&store_watch, &sender, true).await);
        let snapshot = receiver.recv().await.unwrap().unwrap();
        assert_eq!(snapshot.documents.len(), 2);
        assert_eq!(snapshot.changes.len(), 1);
        assert_eq!(snapshot.changes[0].kind, DocumentChangeKind::Added);
        assert_eq!(
            snapshot.changes[0].document.name,
            format!("{PREFIX}/notes/b")
        );
        // The latency-compensated snapshot is cache state and
        // reports the queued write as pending.
        assert!(snapshot.metadata.is_from_cache);
        assert!(snapshot.metadata.has_pending_writes);
    }

    #[tokio::test]
    async fn store_delivery_classifies_server_snapshots_and_pending_deletes() {
        let (engine, store_watch) = test_store_watch();
        engine
            .store_remote_document(&stored_doc("notes/a", 1))
            .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
        let mut delivery = StoreDelivery::new();

        // A pending delete of a collection member that is not
        // in the composed view still counts as pending writes
        // for the watch.
        engine
            .store()
            .enqueue_batch(&[crate::mutation::Mutation::Delete {
                path: "notes/ghost".to_string(),
            }])
            .unwrap();
        // The cache snapshot reports the pending delete too.
        assert!(delivery.emit(&store_watch, &sender, true).await);
        let snapshot = receiver.recv().await.unwrap().unwrap();
        assert!(snapshot.metadata.is_from_cache);
        assert!(snapshot.metadata.has_pending_writes);

        // A server snapshot over the unchanged view is still
        // delivered: the metadata flip is itself the event
        // (it marks the initial sync).
        assert!(delivery.emit(&store_watch, &sender, false).await);
        let snapshot = receiver.recv().await.unwrap().unwrap();
        assert_eq!(snapshot.documents.len(), 1);
        assert!(!snapshot.metadata.is_from_cache);
        assert!(snapshot.metadata.has_pending_writes);

        // Nothing changed since: no further snapshot.
        assert!(delivery.emit(&store_watch, &sender, false).await);
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn store_delivery_reports_a_pending_delete_on_a_document_watch() {
        let (engine, _) = test_store_watch();
        let watch = StoreWatch::new(
            engine.clone(),
            WatchSource::Document {
                relative_path: "notes/a".to_string(),
            },
            43,
        );
        engine
            .store_remote_document(&stored_doc("notes/a", 1))
            .unwrap();
        engine
            .store()
            .enqueue_batch(&[crate::mutation::Mutation::Delete {
                path: "notes/a".to_string(),
            }])
            .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
        let mut delivery = StoreDelivery::new();
        assert!(delivery.emit(&watch, &sender, true).await);
        let snapshot = receiver.recv().await.unwrap().unwrap();
        // The composed view is empty (the delete is already
        // applied locally), but the watch still reports the
        // unacknowledged write.
        assert!(snapshot.documents.is_empty());
        assert!(snapshot.metadata.has_pending_writes);
    }

    struct OfflineTokens;

    #[async_trait::async_trait]
    impl firebase_native_core::TokenProvider for OfflineTokens {
        async fn id_token(&self, _force_refresh: bool) -> Result<String> {
            Err(firebase_native_core::Error::Http("offline".to_string()))
        }
    }

    /// A store-backed listener keeps receiving local view
    /// changes while the driver is stuck in reconnect backoff
    /// (token acquisition failing transiently): the initial
    /// cache snapshot arrives, and a queued local write is
    /// delivered without any stream ever opening.
    #[tokio::test]
    async fn store_backed_watch_delivers_local_changes_while_offline() {
        let dir = std::env::temp_dir().join(format!(
            "firebase-native-offline-watch-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store_path = dir.join("store.db");
        let _ = std::fs::remove_file(&store_path);

        let app = firebase_native_core::App::new(FirebaseOptions::new("test-key", "demo-project"));
        let firestore = crate::Firestore::new(&app, Arc::new(OfflineTokens))
            .with_endpoint("http://127.0.0.1:9")
            .enable_local_store(&store_path)
            .unwrap();
        let mut listener = firestore.watch_document("notes/a").await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), listener.next())
            .await
            .expect("cache snapshot timed out")
            .expect("watch ended")
            .expect("cache snapshot failed");
        assert!(first.documents.is_empty());

        let mut fields = Fields::new();
        fields.insert("n".to_string(), Value::Integer(7));
        firestore.set_document("notes/a", fields).await.unwrap();
        let second = tokio::time::timeout(Duration::from_secs(10), listener.next())
            .await
            .expect("offline delivery timed out")
            .expect("watch ended")
            .expect("update snapshot failed");
        assert_eq!(second.documents.len(), 1);
        assert_eq!(
            second.documents[0].fields.get("n"),
            Some(&Value::Integer(7))
        );
        drop(listener);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn store_watch_seed_restores_the_persisted_token() {
        let (engine, store_watch) = test_store_watch();
        engine
            .store_remote_document(&stored_doc("notes/a", 1))
            .unwrap();
        // A previous watch persisted its token at a consistent
        // point.
        let mut previous = watch();
        previous.note_resume_token(b"token-1");
        store_watch.persist_target(&previous, Some(Timestamp::new(50, 0)));
        let persisted = engine.store().target_state(42).unwrap().unwrap();
        assert_eq!(persisted.resume_token, b"token-1");
        assert_eq!(persisted.snapshot_version, Some(Timestamp::new(50, 0)));

        // A later watch of the same target resumes with it and
        // seeds its view from the store, without synthesized
        // deletes (a resumed add receives deltas only).
        let mut state = watch();
        let seeded = store_watch.seed(&mut state);
        assert_eq!(state.resume_token(), Some(b"token-1".as_slice()));
        assert_eq!(seeded.len(), 1);
        assert_eq!(seeded[0].name, format!("{PREFIX}/notes/a"));
    }
}
