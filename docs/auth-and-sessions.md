# Auth and sessions

`firebase-native-auth` is a client for the Identity Toolkit
REST API and the Secure Token service. It signs users in,
keeps the ID token fresh, and — when the application shell
provides a `CredentialStore` — persists sessions across
launches, for several users on the same device.

## Sign-in methods

All sign-in methods return `SignedIn`:

| Method | Notes |
|---|---|
| `sign_in_with_custom_token(token)` | For tokens minted by your backend (or the Firebase Admin SDK). |
| `sign_in_with_email_password(email, password)` | |
| `sign_up_with_email_password(email, password)` | Creates the account and signs in. |
| `sign_in_anonymously()` | |
| `sign_in_with_idp_id_token(provider_id, id_token)` | Federated sign-in with an IdP-issued token. |

```rust
pub struct SignedIn {
    pub user: User,               // uid, email, display_name
    pub offline_token: String,    // shown only here — see below
    pub session_key: SessionKey,  // hash of the offline token
}
```

`current_user()` returns the active `User`, if any.

## Token handling

`Auth` implements `TokenProvider`, which is how the service
clients obtain tokens:

- **Per call.** Services ask for a token on every request;
  no token string is captured when a client is constructed.
- **Proactive refresh.** A token inside its last five
  minutes is refreshed before use.
- **Forced refresh on rejection.** If a call still returns
  `UNAUTHENTICATED` (or HTTP 401), the client forces one
  refresh and retries the call exactly once.

The refresh token is the long-lived credential. Firebase
imposes **no session timeout**: ID tokens live exactly one
hour, and refresh tokens end only on events (user deleted
or disabled, password/email change, Admin SDK revocation).
There is no developer-configurable lifetime — which is why
the offline model below is built from facts this crate
exposes, not from a policy baked into it.

## Where sessions live: the `CredentialStore`

The crate owns no storage. The shell implements one trait:

```rust
#[async_trait]
pub trait CredentialStore: Send + Sync {
    async fn load(&self) -> Result<Option<Vec<u8>>>;
    async fn save(&self, blob: &[u8]) -> Result<()>;
    async fn clear(&self) -> Result<()>;
}
```

— an OS keychain on desktop/mobile, a file in tests
(`MemoryCredentialStore` is provided for tests and tools).
The blob is opaque to the shell: a versioned collection of
per-user session entries, each holding the refresh token
and an **ID-token cache** (the last ID token, its expiry,
and the last successful refresh time). `Auth::new` attaches
no store, so sessions are memory-only and no offline token
is usable after the process exits.

Treat a failing store as fatal (fail-closed): if the shell
cannot persist, the crate does not silently fall back to
keeping credentials somewhere else.

## The offline token model

At sign-in the crate issues a random **offline token** and
stores only its SHA-256 hash — the *session key* — as the
entry's key. The token itself is disclosed exactly once, in
`SignedIn`. The shell keeps the token wherever its own
security posture wants it (keychain, a user-held secret, a
device credential); the key is *not* a secret and may live
in a plain account registry so the shell can enumerate and
address sessions.

Why a separate token, instead of restoring "whoever was
signed in last"? Because activation is then an explicit,
capability-bearing act: possessing the token is what
authorizes turning a stored refresh token into an active
session, and several users' sessions can coexist on one
device without any of them being implicitly active.

### Activating a session

```rust
let restored: bool = auth.restore_session(&offline_token).await?;
```

`restore_session` resolves the entry by hashing the token,
then:

1. **Online** — runs the refresh exchange, updates the
   stored entry (including refresh-token rotation and the
   ID-token cache), and installs the session.
2. **Offline** — if the exchange cannot run at all
   (transport failure), the session is instead activated
   from the persisted ID-token cache. The cached token
   serves calls until the network returns and a refresh
   succeeds; the entry is not modified.
3. **Rejected** — if the server refuses the refresh token
   (`Unauthenticated`), the entry is deleted and the call
   returns `false`.

An unknown token returns `false`. A restore that races a
sign-in or a sign-out loses: session transitions carry a
generation, and a restore whose generation went stale — or
whose entry was deleted while its exchange was in flight —
installs and persists nothing.

### Facts for the caller's offline policy

The crate holds no opinion about how long an offline
session may continue. It exposes the facts a caller's guard
needs:

```rust
pub struct SessionFacts {
    pub uid: String,
    pub id_token_expires_at: Option<SystemTime>,
    pub last_refresh_at: Option<SystemTime>,
}

auth.session_facts().await                 // active session
auth.stored_session_facts(&key).await?     // any stored entry
auth.stored_session_keys().await?          // enumerate entries
```

A typical guard: on launch, read the stored facts; if
`last_refresh_at` is older than the deployment's limit,
withhold the offline token (the session simply never
activates) or delete the entry with `sign_out_session`.

## Signing out

Both forms are **fully local** — they work offline, which
guards that force a re-sign-in rely on:

- `sign_out()` — drops the active session and removes **its
  entry**; other users' entries are kept.
- `sign_out_session(&key)` — removes the entry addressed by
  session key, without activating it; if it names the
  active session, that is dropped too. Destruction never
  requires the token preimage: whoever holds the device
  (and the key, which is not secret) can remove a session.

## The kill switch

```rust
auth.wipe_all_sessions().await?;
```

For a lost offline token — or a device being handed over —
`wipe_all_sessions` clears the entire credential blob and
the in-memory session, with no authentication. Deletion is
fail-closed and device-local: it cannot endanger
server-side data, so requiring a capability for it would
only protect an attacker who holds the device.

Pair it with the store-side wipe to remove the user's
cached data as well:

```rust
firebase_native::firestore::wipe_all_user_stores(root)?; // all users
firebase_native::firestore::wipe_user_store(root, uid)?; // one user
```

Store wipes carry a process-lifetime caveat — a started
persistence engine cannot currently be stopped, and the
wipe has no cross-process coordination — so run them
before the owning application opens its stores, or after
it has exited; see [Firestore](firestore.md#per-user-stores).
