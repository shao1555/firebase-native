# Vendored protocol buffer definitions

The `.proto` files in this directory are vendored from
[googleapis/googleapis](https://github.com/googleapis/googleapis) at commit
`bf54c1031efddde9cdfee0f89254315d0116a5f9`, pinned so that consumers of this
crate do not need a network fetch or a `protoc` installation at build time
(the build script uses `protoc-bin-vendored`).

Covered files:

- `google/firestore/v1/*.proto` — the Firestore v1 API.
- `google/api/{annotations,client,field_behavior,http,launch_stage,routing}.proto`
  — annotations imported by the Firestore definitions.
- `google/rpc/status.proto`, `google/type/latlng.proto` — shared types.

The googleapis repository is licensed under the Apache License, Version 2.0;
see `LICENSE.googleapis` in this directory. Well-known types under
`google/protobuf/` are provided by `prost-types` / the vendored `protoc`
and are not copied here.
