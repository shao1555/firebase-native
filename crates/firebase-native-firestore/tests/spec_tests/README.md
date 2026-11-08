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
- **Limbo resolution is modeled**: documents a target cannot
  account for — a document-remove message, an existence
  filter's failing bloom set, a target reset, a re-run's
  unconfirmed remainder, or unconfirmed seeded documents of a
  fresh target — stay in the view as limbo candidates and are
  verified on document targets (at most two resolutions run
  concurrently; batches enqueued by a target reset start only
  half of them, rounded up). Existence filters are *decided*
  against the view as of receipt but *applied* at the next
  consistent point, mirroring the reference client's
  "handled at global snapshot" behavior; a mismatch re-runs
  the target (its unconfirmed documents become candidates at
  the re-run's consistent point), and a failed resolution
  (its target removed) settles immediately. The reported
  resume token follows the reference rules: while a target
  has outstanding limbo documents it reads the token the
  target was added with; a clean consistent point promotes
  the live token.
- Cases needing machinery that does not exist yet are reported
  as **skipped with a reason**, never silently passed: multiple
  clients/tabs (including primary election), multiple
  concurrent targets, query limits, and snapshots-in-sync
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
**Recovery** (25 cases) 17 passed / 8 skipped / 0 failed —
the skips are multi-client scenarios (7) and multiple
concurrent targets (1); the two limbo cases (a limbo
acknowledgement / rejection whose persistence fails, which
terminates the client and restarts its resolution after the
async-queue retry) now pass. `failDatabase` arms
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
**Offline** (10 cases) 10 passed / 0 skipped / 0 failed: the
harness models the reference online-state tracker (a fresh
client is Unknown and its queries wait; a successful watch
message makes it Online; from Unknown a single stream failure
goes Offline, from Online a first failure arms the
online-state timeout and a second failure — or the timeout
firing via `runTimer` — goes Offline; removing the last
listener resets the tracker to Unknown). Going Offline
resolves the active listen from cache, and a listen registered
while Offline (or with the network disabled) resolves
immediately; a listen with outstanding limbo documents does
not resolve from cache until they settle.
**Resume token** (2 cases) 2 passed / 0 skipped. **Existence
filter** (26 cases) 24 passed / 2 skipped / 0 failed: bloom
filters (the crate's own MD5-based `BloomFilter` probes them),
count mismatches with their target re-runs, filters applied
at global snapshots, and limbo resolutions of the failing
documents all run; the skips are the very-large-batch case
(fifty concurrent limbo resolutions; the modeled batch
throttling caps a batch at two) and a multiple-concurrent-
targets case. **Limbo** (26 cases) 11 passed / 15 skipped /
0 failed — the single-client, single-target cases cover
document-remove limbo, existence-filter and reset limbo,
the throttling batches, and failed resolutions; the skips
are multiple concurrent targets (8), multiple clients (5)
and query limits (2). The production watch layer gained the
matching pieces: existence filters are decided at receipt
and applied at consistent points, a restart keeps the view
and marks its documents restart-unconfirmed (deleted at the
re-run's consistent point by default, or reported as limbo
candidates under the caller's policy), and seeded
unconfirmed documents follow the same policy.

Two verification properties are worth noting. Resume tokens are
held per target, synced from the watch state after every
response, restored into new listens, and compared exactly
against `expectedState.activeTargets[*].resumeToken`; at each
(re)add the harness also builds the request through the
driver's real `add_target_request` and checks the token it
would send. And whether an existence filter matches is
decided from the harness's own view (emitted documents plus
the pending overlay), independently of the implementation's
filter outcome. Fault injection backs the wiring: making the
deferred-filter application drop its restart signal fails 10
existence-filter cases and 2 limbo cases.

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
