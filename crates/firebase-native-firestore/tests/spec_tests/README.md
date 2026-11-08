# Firestore spec tests (vendored)

The JSON suites in `json/` are the official Firestore spec tests,
vendored verbatim from
[firebase/firebase-ios-sdk](https://github.com/firebase/firebase-ios-sdk)
at commit `c06a1d7199e9a8362e4f102629793c937ce0f7cd`
(`Firestore/Example/Tests/SpecTests/json/`), where they are shared
by all official client implementations to pin down sync-engine
behavior. They are licensed under the Apache License 2.0; see
`LICENSE-upstream.txt` in this directory and `json/README-upstream.md`
for the upstream format description.

18 suites, 303 cases in total. `bundle_spec_test.json` is not a
step-driven suite (it describes named-query bundles) and is
vendored for completeness only.

## The runner and its coverage policy

The runner lives in the crate as a `#[cfg(test)]` module
(`src/spec_runner.rs`); `cargo test` executes it. The spec DSL
drives a complete sync engine, while this crate is built in
layers, so the runner covers the **watch layer** today:

- Remote-event steps (`watchEntity`, `watchCurrent`,
  `watchSnapshot`, `watchRemove`, `watchAck`) are translated into
  real `ListenResponse` protocol messages and fed to the crate's
  actual `WatchState`; `expectedSnapshotEvents` are compared in
  full — the document set rebuilt from the events'
  added/modified/removed deltas, field values, and change
  categorization.
- Around it, the harness models the local store the watch layer
  sits on: a versioned remote-document cache (with delete
  tombstones, so documents cannot revert), per-target resume
  tracking, initial views computed from the cache through the
  query's path and filters (`Query::matches`), and synthesized
  deletes for documents a freshly added target never
  re-confirms (`WatchState::seed_view`).
- Client lifecycle steps are modeled directly: `restart` and
  `shutdown` drop streams, listeners and session bookkeeping
  while persisted state (the local store, the document cache,
  target resume tokens) survives; `clearPersistence` wipes it.
  Mutation queues are per-user: `changeUser` switches which
  queued batches are visible to listeners and settled by
  acknowledgements. Resume-token persistence follows the
  reference rules: a target-specific (CURRENT) token persists
  at the next barrier, while a global snapshot's token
  persists only once it is at least five minutes ahead of the
  last persisted snapshot version.
- Cases needing machinery that does not exist yet are reported
  as **skipped with a reason**, never silently passed: multiple
  clients/tabs (including primary election), multiple
  concurrent targets, query limits, bloom existence filters
  and mismatch limbo resolution, and snapshots-in-sync
  listeners.

Current results: **listen** (66 cases) 29 passed / 37 skipped /
0 failed — skips are multi-client / multi-tab scenarios,
snapshots-in-sync listeners, multiple concurrent targets,
limbo existence filters and a query limit case.
**Persistence** (13 cases) 8 passed / 5 skipped / 0 failed —
the skips are all multi-client primary-lease scenarios; the
single-client cases cover persisted mutations re-sent after
restart, per-user mutation queues across uid switches and
restarts, persisted mutations visible to listeners, and
persisted remote documents. **Write** (59 cases) 41 passed /
18 skipped / 0 failed — the skips are all multi-client
scenarios. Field transforms are modeled end to end: the
mutation model carries transforms (increment, server
timestamp, array union/remove), the wire form emits them as
`update_transforms`, and the acknowledgement fold installs
the server's transform results (carried on the Write
stream's committed-batch acknowledgement). Decimal128
values — the reference core's exact decimal number kind,
held here as canonical strings — lose precision through
`f64` in local increments and recover exactly from the
transform result, as the two Decimal128 cases pin down: a
queued transform's transformed fields show the estimate
computed at enqueue time, over any base movement and past
the fold, until the watch confirms the write.
**Recovery** (25 cases) 15 passed / 10 skipped / 0 failed —
the skips are multi-client scenarios (7), limbo documents (2)
and multiple concurrent targets (1). `failDatabase` arms
named local-store operations to fail once at their next
invocation; the harness models the reference outcomes:
target allocation failures fail the listen with UNAVAILABLE,
locally-write failures reject the batch unsurfaced, release
failures are swallowed, remote-event failures discard the
event and deactivate the target until the async-queue retry
re-listens it, user-change transaction failures terminate
the client (queued changes then apply one per retry), and
acknowledgement/rejection persistence failures are retried
by the async queue — with a listener attached the
acknowledgement failure is fatal to the session instead:
the fold is applied in memory, its callback is withheld
until the restart delivers it, and the listener re-raises
its view from cache. `notifyLocalViewChanges` failures are
consumed without effect, because this harness applies store
changes before notifying listeners, so a failed notification
cannot roll anything back — the behavior the cases pin down.
**Offline** (10 cases) 9 passed / 1 skipped / 0 failed: the
harness models the reference online-state tracker (a fresh
client is Unknown and its queries wait; a successful watch
message makes it Online; from Unknown a single stream failure
goes Offline, from Online a first failure arms the
online-state timeout and a second failure — or the timeout
firing via `runTimer` — goes Offline; removing the last
listener resets the tracker to Unknown). Going Offline
resolves the active listen from cache, and a listen registered
while Offline (or with the network disabled) resolves
immediately. The skipped case needs limbo documents.
**Resume token** (2 cases) 2 passed / 0 skipped. **Existence
filter** (26 cases) 2 passed / 24 skipped / 0 failed: the
matching-count cases pass through `WatchState`'s filter
handling, while bloom-filter cases and count mismatches are
skipped — resolving either needs the sync engine's limbo /
target re-run orchestration (the reference client verifies
possibly-deleted documents on a separate limbo target before
removing them), which this crate does not have yet. In the real
protocol a mismatch restarts the watch without its resume
token, which `WatchState` already signals; the spec harness's
stepwise limbo resolution is what is not modeled.

Two verification properties are worth noting. Resume tokens are
held per target, synced from the watch state after every
response, restored into new listens, and compared exactly
against `expectedState.activeTargets[*].resumeToken`; at each
(re)add the harness also builds the request through the
driver's real `add_target_request` and checks the token it
would send. And whether an existence filter is a supported
matching-count case is decided from the harness's own view
(emitted documents plus the pending overlay), independently of
the implementation's restart signal — a restart on a matching
filter *fails* the case. Both properties were validated by
fault injection: making `WatchState` discard received tokens
fails both resume-token cases, and making it restart on every
filter fails both matching-count cases (skips unchanged).

## The write-mode harness

Cases with user writes run in "write mode": the harness drives
the real `PersistenceEngine` on an in-memory store. Streamed
watch documents are buffered and applied to the store at the
next accepted consistent point — never through a local
operation interleaved before the barrier — so a confirmation
whose fields are unchanged (which raises no watch snapshot)
still lands, and a barrier's read time confirms committed
writes at or below it. User writes become real
mutation batches in the store's queue, `writeAck` folds the
oldest batch through the engine's acknowledgement path, and
`failWrite` drops it (retryable error codes keep it queued, as
the write stream would retry). Listener events are *composed*
from the store — remote base overlaid with queued mutations,
resolved through `PersistenceEngine::local_document` — and
compared in full: document sets, field values, change
categorization, `fromCache` / `hasPendingWrites`, per-document
metadata-only changes, drained `userCallbacks`,
`numOutstandingWrites` (the spec model's write stream
pipelines at most 10 batches in flight), and
`waitForPendingWrites` resolutions.

The semantics this pins down include: `hasPendingWrites`
counts documents in the view with unacknowledged *or*
acknowledged-but-watch-unconfirmed writes; an acknowledgement
raises an event only when the watch had already delivered the
write's effect (otherwise the flip surfaces, absorbed, with a
later event); a watch delivery must not resurrect a document
whose acknowledged delete/patch folded it away at a newer
version; and the engine's fold must not regress a base the
watch has already advanced past the commit (a bug the suite
caught in `PersistenceEngine::fold_acknowledgement`, fixed
with a regression test). Deliberately not compared: the
per-document `options`/`version` fields inside metadata
entries (keys are compared).
