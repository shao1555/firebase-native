# Documentation

Reference guides for `firebase-native`, a pure-Rust Firebase
client for native applications.

- [Getting started](getting-started.md) — add the crates, build
  an `App`, sign in, and make your first Firestore and Storage
  calls.
- [Auth and sessions](auth-and-sessions.md) — sign-in methods,
  token handling, and the multi-user session model: offline
  tokens, offline activation, session facts, targeted sign-out
  and the wipe-all kill switch.
- [Firestore](firestore.md) — documents and values, queries,
  realtime watches and snapshot metadata, the `FieldValue`
  sentinel API, offline persistence and latency compensation,
  and per-user local stores.
- [Storage](storage.md) — references, uploads (simple and
  resumable), downloads, metadata and listing.
- [Design](design.md) — the invariants the implementation is
  built around: per-call tokens, refresh/retry discipline,
  session-generation discipline, the spec-test strategy, and
  versioning/MSRV policy.

For the API reference itself, run `cargo doc --workspace
--open` — every public item is documented, and the crates
build with `#![warn(missing_docs)]` so coverage stays
complete. Runnable end-to-end programs live in
`crates/firebase-native/examples/` (`smoke`, `watch`,
`persist`, `storage`, `session`); each header comment lists
the environment variables it needs.
