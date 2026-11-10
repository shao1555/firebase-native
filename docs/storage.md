# Storage

`firebase-native-storage` is a client for the Cloud Storage
for Firebase JSON API (`firebasestorage.googleapis.com/v0/b/{bucket}/o`).
Like the other services, it takes an ID token per call —
sent as `Authorization: Firebase <token>`, the scheme this
host expects — and force-refreshes and retries exactly once
on a 401.

## References

```rust
use firebase_native::storage::Storage;

let storage = Storage::new(&app, auth.clone());  // default bucket from FirebaseOptions
let reference = storage.reference("users/uid/avatar.png");
let other = storage.reference_in_bucket("another-bucket", "path/to/object");
let child = reference.child("thumbnails/64.png");
```

A `Reference` is a cheap handle: `bucket()`, `path()` and
`name()` read its parts without any network call. The
default bucket comes from
`FirebaseOptions::with_storage_bucket`; a client built
without one can still address buckets explicitly through
`reference_in_bucket`.

## Uploads

```rust
use firebase_native::storage::PutMetadata;

let metadata = reference
    .put_bytes(bytes, Some(PutMetadata {
        content_type: Some("image/png".to_string()),
        cache_control: None,
        content_disposition: None,
        content_encoding: None,
        content_language: None,
        custom_metadata: Default::default(),
    }))
    .await?;
```

- `put_bytes(data, metadata)` — one-shot upload. With no
  metadata it uses the simple media upload; with metadata,
  the multipart form of the `X-Goog-Upload` protocol this
  endpoint speaks.
- `put_bytes_resumable(data, metadata)` — opens a resumable
  session and uploads in chunks (a multiple of the
  protocol's 256 KiB granularity), so large objects survive
  individual request failures better and report the
  server-acknowledged offset as they go.

Both return the created object's `ObjectMetadata`.

## Downloads and metadata

| Method | Behavior |
|---|---|
| `get_bytes()` | Downloads the object's contents (`alt=media`). |
| `get_metadata()` | The object's `ObjectMetadata` (name, bucket, size, content type, `md5Hash`, download tokens, custom metadata, timestamps). |
| `update_metadata(PutMetadata)` | Patches the metadata fields that are set. |
| `download_url()` | A token-bearing download URL suitable for handing to a browser or another client. Treat it as a capability: anyone holding it can read the object. |
| `delete()` | Deletes the object. |

## Listing

```rust
let page = reference.list(Some("/"), Some(100), page_token.as_deref()).await?;
let all = reference.list_all(Some("/")).await?;
```

`list(delimiter, max_results, page_token)` returns one page
as a `ListResult` — `items` and `prefixes` (both
`Vec<Reference>`; prefixes are the "subdirectory" names
when a delimiter such as `"/"` is given) plus
`next_page_token`. `list_all(delimiter)` walks all pages
and merges them into a single `ListResult`.

Note that listing is a separate permission from reading:
Storage Rules commonly grant object read/write without
list (for example, an attachments layout where objects are
addressed by ID from another database). A `PermissionDenied`
from `list` on objects you can otherwise read usually means
exactly that.

## Errors

A missing object surfaces as `Error::NotFound`; rule denials
as `Error::PermissionDenied`. As elsewhere in the
workspace, the token is refreshed and the call retried once
on a 401 before any error reaches the caller.
