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
- Snapshot metadata owned by layers above the watch state
  (`fromCache`, `hasPendingWrites`) is **not** compared yet.
- Cases needing machinery that does not exist yet are reported
  as **skipped with a reason**, never silently passed: local
  mutations and write acknowledgements (the write suite's
  harness), multiple clients/tabs, multiple concurrent targets,
  query limits, existence filters, snapshots-in-sync listeners,
  and restart/shutdown/primary-election behavior.

Current results (listen suite, 66 cases): **20 passed, 46
skipped, 0 failed**. Skip reasons break down as: multi-client /
multi-tab scenarios (28), local mutations (7),
snapshots-in-sync listeners (4), restart (3), multiple
concurrent targets (3), query limit (1).

Driving the remaining suites (existence filter and resume token
first — they exercise `WatchState` directly), the write suite
against the persistence engine, and store-backed watches are
follow-up work, as is comparing snapshot metadata.
