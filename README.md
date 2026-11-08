# firebase-native

A pure-Rust Firebase client for native applications.

`firebase-native` implements the Firebase client protocols directly —
Authentication over the Identity Toolkit REST API, Firestore over gRPC,
and (from P3) Cloud Storage for Firebase over its JSON API — instead of
wrapping the official Firebase C++ SDK. Pure Rust means it builds
natively for every target Rust supports, including Windows on ARM64,
for which the C++ SDK ships no prebuilt libraries (x86/x64 only).

> Status: **P2 complete** — Auth sign-in/refresh, Firestore unary
> CRUD, the Listen stream (reconnect/resume, production limbo
> resolution), the local store with offline persistence and
> latency compensation, the Write stream, local query evaluation
> and the FieldValue sentinel API work against live projects. The
> P2 spec-test suites (listen, write, persistence, offline,
> recovery, existence filter, resume token, limbo) pass against
> the vendored firebase-ios-sdk corpus (see
> `crates/firebase-native-firestore/tests/spec_tests/`). Storage
> lands in P3.

## Crates

| Crate | Contents |
|---|---|
| `firebase-native` | Umbrella crate re-exporting the crates below. |
| `firebase-native-core` | `App`, `FirebaseOptions`, the shared `Error` type, the `TokenProvider` trait, and the `CredentialStore` trait. |
| `firebase-native-auth` | Identity Toolkit REST client: custom token, email/password, anonymous and IdP sign-in; automatic token refresh; session restore via `CredentialStore`. |
| `firebase-native-firestore` | Firestore gRPC client with a hand-written public `Value` model (generated protobuf types are never exposed). |
| `firebase-native-storage` | Cloud Storage for Firebase (placeholder until P3). |

## Design points

- **Tokens per call, never captured.** Firestore and Storage obtain the
  ID token through the `TokenProvider` trait on every request. Auth
  refreshes proactively before expiry; on an `UNAUTHENTICATED` response
  the client forces one refresh and retries the call exactly once.
- **Proxies are honoured.** The Auth REST client uses the system
  proxy configuration via `reqwest`; the Firestore gRPC transport
  tunnels through `grpc_proxy` / `https_proxy` / `all_proxy` with
  `CONNECT` when one is set (`NO_PROXY` respected), like gRPC core.
  Both transports trust the OS certificate store.
- **The library owns no storage.** Session persistence is delegated to
  the application shell through the `CredentialStore` trait: a file for
  development builds, the OS secure store for production, and
  fail-closed (sign in on every launch) when no secure store exists.
- **Protocol fidelity.** The Firestore Listen and Write streams will
  follow the official protocol rather than substituting unary `Commit`
  for the Write stream, because offline write acknowledgement and
  stream resumption semantics are exactly what the official spec tests
  exercise (P2).

## Compatibility strategy

Firestore's core is shared by all official SDKs (in `firebase-ios-sdk`
under `Firestore/core`), and its language-neutral spec tests are JSON
files (18 files, 303 cases: listen, write, limbo, existence filter,
recovery, persistence, offline, and more). From P2, this project ports
those suites to a Rust runner — listen/write first, then
persistence/offline/recovery — and this README will state which suites
pass and how many cases, as the public form of the compatibility
claim. Serializer compatibility is covered separately by hand-written
golden protobuf conversion tests, since the official serializer tests
are C++ unit tests rather than JSON.

## Example

`crates/firebase-native/examples/smoke.rs` signs in with a custom
token from the environment and reads a document path from the
environment:

```sh
FIREBASE_API_KEY=... \
FIREBASE_PROJECT_ID=my-project \
FIREBASE_DATABASE_ID=(default) \
FIREBASE_CUSTOM_TOKEN=... \
FIREBASE_DOCUMENT_PATH=notes/<id> \
cargo run --example smoke
```

## Build

```sh
cargo build
cargo test
```

No system `protoc` is required: the Firestore build script compiles
the vendored googleapis protos (pinned at commit
`bf54c1031efddde9cdfee0f89254315d0116a5f9`, see
`crates/firebase-native-firestore/proto/README.md`) with
`protoc-bin-vendored`.

## Roadmap

- **P0** — Skeleton, Auth, Firestore unary CRUD (online only). ✅
- **P1** — Listen stream (online only), serializer golden tests. ✅
- **P2** — Local store, Write stream, offline persistence and
  recovery; spec-test runner and the listen/write/persistence/offline
  suites; listen-through-store, local query evaluation, the public
  FieldValue sentinel API, and production limbo resolution. ✅
- **P3** — Storage implementation.
- **P4** — Integration into a production native client app.

## License

MIT — see `LICENSE`. The vendored `.proto` files under
`crates/firebase-native-firestore/proto/` are from googleapis and are
licensed under the Apache License, Version 2.0 (see
`proto/LICENSE.googleapis`).
