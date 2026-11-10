# Design

The invariants the implementation is built around. These
are the properties the tests — including the vendored
spec-test corpus — exist to defend.

## Tokens are per call, never captured

Service clients are constructed with a `TokenProvider`,
never with a token string. Every request asks the provider
for a token; the provider (normally `Auth`) refreshes
proactively inside the token's last five minutes, and a
call that still comes back `UNAUTHENTICATED` / 401 forces
exactly one refresh and exactly one retry. Anything more
would mask real credential failures; anything less would
surface routine expiry to callers.

## The public model never leaks wire types

Firestore's public API is a hand-written model (`Value`,
`Document`, `Query`, `Snapshot`); the generated `prost`
types stay inside the crate. The same applies to the REST
services' JSON shapes. This is what makes the semantics
portable across the sync engine, local query evaluation and
the spec-test harness, which all operate on the public
model.

## Session transitions are generation-guarded

`Auth` keeps its session under a lifecycle lock with a
monotonic generation. Any asynchronous operation that loads
state, does network I/O unlocked, and then installs a
result (restore, refresh) snapshots the generation first
and re-checks it — plus the continued existence of the
stored entry it belongs to — before persisting or
installing anything. A delayed operation therefore cannot
resurrect a signed-out session or overwrite a newer
sign-in, races the test suite reproduces with delayed
stub endpoints.

Destruction is deliberately *not* guarded: sign-out and
the kill switch are fully local, unconditional, and
available without any credential, because their safety
comes from being device-local and fail-closed.

## Persistence is delegated, fail-closed

The library owns no credential storage and no opinion
about where an app keeps its local databases. Shells
provide a `CredentialStore` for session blobs; Firestore
stores are explicit file paths. If persistence fails, the
operation fails — there is no silent fallback to weaker
storage.

## The local view is the composed truth

In persistent mode, reads, query evaluation and watches all
operate on one composed view: server state in the SQLite
store, overlaid with the pending mutation queue in FIFO
order. Remote changes enter the store only at accepted
consistent points of the Listen stream (never mid-batch),
which is what lets latency compensation, limbo resolution
and resume-token bookkeeping agree with each other. The
existence-filter and limbo logic follows the reference
implementation's model, verified against its own test
corpus (below).

## Spec tests

Large parts of Firestore's behavior are pinned by the
official `firebase-ios-sdk` spec-test suites, vendored
under `crates/firebase-native-firestore/tests/spec_tests/`
(18 files, 303 cases from a pinned upstream commit) and run
by an in-repo harness: listen, write, persistence, offline,
recovery, existence filter, resume token and limbo suites
all pass with zero failures. Skips are explicit and
categorical (multiple clients/tabs, concurrent targets,
snapshots-in-sync listeners, watch-layer limit semantics) —
never silent. A pass turning into a skip is treated as a
regression.

Correct handling of a fresh-stream quirk is also pinned by
tests: a brand-new Listen/Write stream can be rejected once
with a database-name-mismatch `InvalidArgument`; the
clients classify that one message as retryable rather than
terminal.

## MSRV and dependencies

- MSRV is **Rust 1.75**, declared in the workspace
  `Cargo.toml` and exercised by CI.
- Firestore protobufs are vendored from a pinned
  `googleapis` commit and generated with
  `protoc-bin-vendored`, so builds need no system `protoc`.
- Cryptographic needs (token generation, SHA-256) use
  `aws-lc-rs`, already in the tree via `rustls`.
- CI runs `cargo fmt --check`, `cargo clippy -- -D warnings`
  and the full test suite on every pull request, with
  third-party actions pinned by commit SHA.
