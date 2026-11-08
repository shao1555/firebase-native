//! The mutation model: one queued unit of change, applied three
//! ways over its lifetime —
//!
//! 1. to the **local view** immediately (latency compensation),
//! 2. to the **wire** as a `google.firestore.v1.Write` on the Write
//!    stream,
//! 3. to the **server-confirmed state** when the batch is
//!    acknowledged, with the server commit time.
//!
//! A `Patch` whose mask names a field absent from `fields` deletes
//! that field, matching `UpdateDocument` mask semantics. A `Patch`
//! against a document that does not exist fails on the server (its
//! precondition is `exists = true`); locally it leaves the view
//! absent, and the engine drops the batch when the server reports
//! the failure.

use serde::{Deserialize, Serialize};

use crate::google::firestore::v1::write::Operation;
use crate::google::firestore::v1::{DocumentMask, Precondition, Write};
use crate::path::{full_document_name, parse_field_path};
use crate::value::{fields_to_proto, Document, Timestamp, Value};
use crate::Fields;

/// A server-side field transform attached to a mutation: the
/// field's new value is computed by the server from the value
/// it holds, rather than written literally. Transform paths are
/// disjoint from the mutation's literal field paths.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct FieldTransform {
    /// Field path the transform applies to.
    pub(crate) path: String,
    /// The transform operation.
    pub(crate) kind: TransformKind,
}

/// The transform operations, mirroring the wire
/// `FieldTransform` types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum TransformKind {
    /// Set the field to the server's commit time. Locally the
    /// pending value reads as `Null` (the "none" estimate);
    /// the fold installs the commit time.
    ServerTimestamp,
    /// Add the operand to the field's numeric value.
    Increment(Value),
    /// Append the elements that are not already present.
    ArrayUnion(Vec<Value>),
    /// Remove every occurrence of the elements.
    ArrayRemove(Vec<Value>),
}

/// Applies one transform to a document's fields. With server
/// `results` (aligned with the mutation's transform order,
/// indexed by `index`), timestamp and increment fields take
/// the result verbatim — the server computed it exactly. The
/// array transforms are the exception: the server reports a
/// null result for them (the wire contract in `write.proto`),
/// so the fold recomputes the operation on the base instead.
/// Without results the transform is computed locally: numeric
/// addition goes through the operand kinds (integer arithmetic
/// stays integral, anything involving a double stays double,
/// and anything involving a decimal goes through `f64` and
/// loses precision exactly as the reference core's local
/// increment does — the exact value returns with the server's
/// transform result at acknowledgement).
fn apply_transform(
    fields: &mut Fields,
    transform: &FieldTransform,
    base: Option<Value>,
    commit_time: Option<Timestamp>,
    results: Option<&[Value]>,
    index: usize,
) {
    let installs_result = !matches!(
        transform.kind,
        TransformKind::ArrayUnion(_) | TransformKind::ArrayRemove(_)
    );
    if installs_result {
        if let Some(result) = results.and_then(|results| results.get(index)) {
            set_path(fields, &transform.path, result.clone());
            return;
        }
    }
    let current = base;
    match &transform.kind {
        TransformKind::ServerTimestamp => {
            let value = commit_time.map(Value::Timestamp).unwrap_or(Value::Null);
            set_path(fields, &transform.path, value);
        }
        TransformKind::Increment(operand) => {
            let value = match (current, operand) {
                (Some(base), _) if base.is_number() && operand.is_number() => {
                    if matches!(base, Value::Decimal128(_))
                        || matches!(operand, Value::Decimal128(_))
                    {
                        let sum = base.as_f64().unwrap_or(0.0) + operand.as_f64().unwrap_or(0.0);
                        Value::Decimal128(format_decimal(sum))
                    } else if let (Value::Integer(a), Value::Integer(b)) = (&base, operand) {
                        // Integer overflow saturates to the
                        // range end (the wire contract); it does
                        // not promote the value to a double.
                        Value::Integer(a.saturating_add(*b))
                    } else {
                        Value::Double(
                            base.as_f64().unwrap_or(0.0) + operand.as_f64().unwrap_or(0.0),
                        )
                    }
                }
                // Incrementing a missing or non-numeric field
                // sets it to the operand.
                _ => operand.clone(),
            };
            set_path(fields, &transform.path, value);
        }
        TransformKind::ArrayUnion(elements) => {
            let mut array = match current {
                Some(Value::Array(existing)) => existing,
                _ => Vec::new(),
            };
            for element in elements {
                if !array.iter().any(|existing| values_equal(existing, element)) {
                    array.push(element.clone());
                }
            }
            set_path(fields, &transform.path, Value::Array(array));
        }
        TransformKind::ArrayRemove(elements) => {
            // Removing from a missing or non-array field leaves
            // an empty array (the wire contract), not the old
            // value or an absent field.
            let kept: Vec<Value> = match current {
                Some(Value::Array(existing)) => existing
                    .into_iter()
                    .filter(|element| !elements.iter().any(|target| values_equal(target, element)))
                    .collect(),
                _ => Vec::new(),
            };
            set_path(fields, &transform.path, Value::Array(kept));
        }
    }
}

/// Firestore element equivalence for the array transforms:
/// two elements are equivalent when they compare equal in
/// Firestore's total order — numerically across integer,
/// double and decimal (`1` equals `1.0`), `NaN` equals `NaN`,
/// and nested arrays and maps element-wise.
fn values_equal(a: &Value, b: &Value) -> bool {
    a.compare(b) == std::cmp::Ordering::Equal
}

/// Formats an `f64` as a canonical decimal string: integral
/// values without a fraction (the reference core's decimal
/// string for a whole number is `"2"`, not `"2.0"`).
fn format_decimal(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

/// One document mutation. Paths are relative document paths
/// (`notes/abc`), the same form the public API takes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum Mutation {
    /// Creates or replaces a document. With `merge_paths`, only
    /// those field paths are written (values taken from `fields`);
    /// without, the document is overwritten wholesale.
    Set {
        /// Relative document path.
        path: String,
        /// Field values (top-level literal names to values).
        fields: Fields,
        /// Field paths to merge, or `None` for a full overwrite.
        merge_paths: Option<Vec<String>>,
        /// `Some(false)` requires the document to not exist yet
        /// (create), `Some(true)` requires it to exist, `None` is
        /// unconditional.
        exists: Option<bool>,
        /// Server-side field transforms applied with the write.
        #[serde(default)]
        transforms: Vec<FieldTransform>,
    },
    /// Patches selected field paths of an existing document.
    Patch {
        /// Relative document path.
        path: String,
        /// Field values; mask paths absent here delete the field.
        fields: Fields,
        /// Field paths the patch applies to.
        mask: Vec<String>,
        /// Existence precondition (updates use `Some(true)`).
        exists: Option<bool>,
        /// Server-side field transforms applied with the write.
        #[serde(default)]
        transforms: Vec<FieldTransform>,
    },
    /// Deletes a document.
    Delete {
        /// Relative document path.
        path: String,
    },
}

impl Mutation {
    /// The relative document path this mutation touches.
    pub(crate) fn path(&self) -> &str {
        match self {
            Mutation::Set { path, .. }
            | Mutation::Patch { path, .. }
            | Mutation::Delete { path } => path,
        }
    }

    /// Applies the mutation to a local-view document (`None` =
    /// absent), returning the new view. Server timestamps inside the
    /// mutation are not a thing in this model yet (transforms arrive
    /// with the write spec suite), so local and remote application
    /// share the field logic; they differ only in time bookkeeping.
    /// The mutation's field transforms (empty for deletes).
    /// Part of the mutation model's surface; the offline
    /// engine's callers reach transforms through the batch
    /// fold today.
    #[allow(dead_code)]
    pub(crate) fn transforms(&self) -> &[FieldTransform] {
        match self {
            Mutation::Set { transforms, .. } | Mutation::Patch { transforms, .. } => transforms,
            Mutation::Delete { .. } => &[],
        }
    }

    pub(crate) fn apply_to_local(&self, current: Option<Document>) -> Option<Document> {
        self.apply_with(current, None, None)
    }

    /// Applies the mutation to the server-confirmed state after the
    /// batch was acknowledged with `commit_time`.
    pub(crate) fn apply_to_remote(
        &self,
        current: Option<Document>,
        commit_time: Timestamp,
    ) -> Option<Document> {
        self.apply_with(current, Some(commit_time), None)
    }

    /// The acknowledgement fold with the server's transform
    /// results for this mutation (in transform order):
    /// timestamp and increment fields take their result
    /// verbatim; array transforms recompute on the base because
    /// the server reports a null result for them.
    pub(crate) fn apply_to_remote_with_results(
        &self,
        current: Option<Document>,
        commit_time: Timestamp,
        results: &[Value],
    ) -> Option<Document> {
        self.apply_with(current, Some(commit_time), Some(results))
    }

    fn apply_with(
        &self,
        current: Option<Document>,
        commit_time: Option<Timestamp>,
        results: Option<&[Value]>,
    ) -> Option<Document> {
        let pre_fields = current.as_ref().map(|document| document.fields.clone());
        match self {
            Mutation::Delete { .. } => None,
            Mutation::Set {
                fields,
                merge_paths,
                transforms,
                ..
            } => {
                let mut document = match (merge_paths, current) {
                    (Some(_), Some(existing)) => existing,
                    (Some(_), None) => Document {
                        name: String::new(),
                        fields: Fields::new(),
                        create_time: None,
                        update_time: None,
                    },
                    (None, existing) => Document {
                        name: existing.map(|d| d.name).unwrap_or_default(),
                        fields: Fields::new(),
                        create_time: None,
                        update_time: None,
                    },
                };
                match merge_paths {
                    Some(paths) => {
                        for path in paths {
                            if let Some(value) = lookup_path(fields, path) {
                                set_path(&mut document.fields, path, value.clone());
                            }
                        }
                    }
                    None => {
                        document.fields = fields.clone();
                        // An overwrite resets creation time to the
                        // commit time when acknowledged.
                        document.create_time = commit_time.or(document.create_time);
                    }
                }
                for (index, transform) in transforms.iter().enumerate() {
                    // A transform in an overwrite reads its base
                    // from the pre-image (the server computes it
                    // from the stored document); a merge reads
                    // it from the merged result (the paths are
                    // disjoint from the literal fields).
                    let base = if merge_paths.is_some() {
                        lookup_path(&document.fields, &transform.path).cloned()
                    } else {
                        pre_fields
                            .as_ref()
                            .and_then(|fields| lookup_path(fields, &transform.path).cloned())
                    };
                    apply_transform(
                        &mut document.fields,
                        transform,
                        base,
                        commit_time,
                        results,
                        index,
                    );
                }
                if let Some(commit_time) = commit_time {
                    document.update_time = Some(commit_time);
                    if document.create_time.is_none() {
                        document.create_time = Some(commit_time);
                    }
                }
                Some(document)
            }
            Mutation::Patch {
                fields,
                mask,
                transforms,
                ..
            } => {
                let mut document = current?;
                for path in mask {
                    match lookup_path(fields, path) {
                        Some(value) => set_path(&mut document.fields, path, value.clone()),
                        None => delete_path(&mut document.fields, path),
                    }
                }
                for (index, transform) in transforms.iter().enumerate() {
                    let base = lookup_path(&document.fields, &transform.path).cloned();
                    apply_transform(
                        &mut document.fields,
                        transform,
                        base,
                        commit_time,
                        results,
                        index,
                    );
                }
                if let Some(commit_time) = commit_time {
                    document.update_time = Some(commit_time);
                }
                Some(document)
            }
        }
    }

    /// Converts to the wire representation for the Write stream.
    pub(crate) fn to_proto(&self, documents_prefix: &str) -> Write {
        match self {
            Mutation::Set {
                path,
                fields,
                merge_paths,
                exists,
                transforms,
            } => Write {
                operation: Some(Operation::Update(crate::google::firestore::v1::Document {
                    name: full_document_name(documents_prefix, path),
                    fields: fields_to_proto(fields),
                    create_time: None,
                    update_time: None,
                })),
                update_mask: merge_paths.as_ref().map(|paths| DocumentMask {
                    field_paths: paths.clone(),
                }),
                update_transforms: transforms.iter().map(transform_to_proto).collect(),
                current_document: precondition(*exists),
            },
            Mutation::Patch {
                path,
                fields,
                mask,
                exists,
                transforms,
            } => {
                // The update document carries only the masked fields
                // that are being set; masked fields absent from
                // `fields` are deletions expressed by the mask alone.
                let mut update_fields = Fields::new();
                for path in mask {
                    if let Some(value) = lookup_path(fields, path) {
                        set_path(&mut update_fields, path, value.clone());
                    }
                }
                Write {
                    operation: Some(Operation::Update(crate::google::firestore::v1::Document {
                        name: full_document_name(documents_prefix, path),
                        fields: fields_to_proto(&update_fields),
                        create_time: None,
                        update_time: None,
                    })),
                    update_mask: Some(DocumentMask {
                        field_paths: mask.clone(),
                    }),
                    update_transforms: transforms.iter().map(transform_to_proto).collect(),
                    current_document: precondition(*exists),
                }
            }
            Mutation::Delete { path } => Write {
                operation: Some(Operation::Delete(full_document_name(
                    documents_prefix,
                    path,
                ))),
                update_mask: None,
                update_transforms: Vec::new(),
                current_document: None,
            },
        }
    }
}

/// Converts a transform to its wire form.
fn transform_to_proto(
    transform: &FieldTransform,
) -> crate::google::firestore::v1::document_transform::FieldTransform {
    use crate::google::firestore::v1::document_transform::field_transform::TransformType;
    let transform_type = match &transform.kind {
        TransformKind::ServerTimestamp => {
            TransformType::SetToServerValue(
                crate::google::firestore::v1::document_transform::field_transform::ServerValue::RequestTime as i32,
            )
        }
        TransformKind::Increment(operand) => {
            TransformType::Increment(operand.to_proto())
        }
        TransformKind::ArrayUnion(elements) => {
            TransformType::AppendMissingElements(crate::google::firestore::v1::ArrayValue {
                values: elements.iter().map(Value::to_proto).collect(),
            })
        }
        TransformKind::ArrayRemove(elements) => {
            TransformType::RemoveAllFromArray(crate::google::firestore::v1::ArrayValue {
                values: elements.iter().map(Value::to_proto).collect(),
            })
        }
    };
    crate::google::firestore::v1::document_transform::FieldTransform {
        field_path: transform.path.clone(),
        transform_type: Some(transform_type),
    }
}

fn precondition(exists: Option<bool>) -> Option<Precondition> {
    exists.map(|exists| Precondition {
        condition_type: Some(
            crate::google::firestore::v1::precondition::ConditionType::Exists(exists),
        ),
    })
}

/// Looks up a field path in a fields map (first segment is a literal
/// top-level key, later segments traverse nested maps).
pub(crate) fn lookup_path<'a>(fields: &'a Fields, path: &str) -> Option<&'a Value> {
    let segments = parse_field_path(path);
    let mut segments = segments.iter();
    let mut current = fields.get(segments.next()?)?;
    for segment in segments {
        match current {
            Value::Map(map) => current = map.get(segment)?,
            _ => return None,
        }
    }
    Some(current)
}

/// Sets a field path in a fields map, creating intermediate maps.
pub(crate) fn set_path(fields: &mut Fields, path: &str, value: Value) {
    let segments = parse_field_path(path);
    set_segments(fields, &segments, value);
}

fn set_segments(fields: &mut Fields, segments: &[String], value: Value) {
    let Some((first, rest)) = segments.split_first() else {
        return;
    };
    if rest.is_empty() {
        fields.insert(first.clone(), value);
        return;
    }
    let entry = fields
        .entry(first.clone())
        .or_insert_with(|| Value::Map(Fields::new()));
    if !matches!(entry, Value::Map(_)) {
        *entry = Value::Map(Fields::new());
    }
    let Value::Map(inner) = entry else {
        unreachable!()
    };
    set_segments(inner, rest, value);
}

/// Deletes a field path from a fields map (a no-op when absent).
pub(crate) fn delete_path(fields: &mut Fields, path: &str) {
    let segments = parse_field_path(path);
    delete_segments(fields, &segments);
}

fn delete_segments(fields: &mut Fields, segments: &[String]) {
    let Some((first, rest)) = segments.split_first() else {
        return;
    };
    if rest.is_empty() {
        fields.remove(first);
        return;
    }
    if let Some(Value::Map(inner)) = fields.get_mut(first) {
        delete_segments(inner, rest);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(fields: &[(&str, Value)]) -> Document {
        Document {
            name: "projects/p/databases/d/documents/notes/a".to_string(),
            fields: fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            create_time: None,
            update_time: None,
        }
    }

    fn fields(pairs: &[(&str, Value)]) -> Fields {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn set_overwrite_and_merge() {
        let overwrite = Mutation::Set {
            path: "notes/a".to_string(),
            fields: fields(&[("x", Value::Integer(1))]),
            merge_paths: None,
            exists: None,
            transforms: Vec::new(),
        };
        let view = overwrite.apply_to_local(Some(doc(&[("y", Value::Integer(2))])));
        assert_eq!(view.unwrap().fields, fields(&[("x", Value::Integer(1))]));

        let merge = Mutation::Set {
            path: "notes/a".to_string(),
            fields: fields(&[("meta", {
                let mut m = Fields::new();
                m.insert("rank".to_string(), Value::Integer(5));
                Value::Map(m)
            })]),
            merge_paths: Some(vec!["meta.rank".to_string()]),
            exists: None,
            transforms: Vec::new(),
        };
        let base = doc(&[("meta", {
            let mut m = Fields::new();
            m.insert("keep".to_string(), Value::Bool(true));
            Value::Map(m)
        })]);
        let view = merge.apply_to_local(Some(base)).unwrap();
        let meta = lookup_path(&view.fields, "meta.rank").unwrap();
        assert_eq!(*meta, Value::Integer(5));
        assert_eq!(
            *lookup_path(&view.fields, "meta.keep").unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn patch_sets_and_deletes() {
        let patch = Mutation::Patch {
            path: "notes/a".to_string(),
            fields: fields(&[("n", Value::Integer(3))]),
            mask: vec!["n".to_string(), "gone".to_string()],
            exists: Some(true),
            transforms: Vec::new(),
        };
        let view = patch.apply_to_local(Some(doc(&[
            ("n", Value::Integer(1)),
            ("gone", Value::String("x".to_string())),
        ])));
        let view = view.unwrap();
        assert_eq!(view.fields, fields(&[("n", Value::Integer(3))]));
        // Patch against an absent document stays absent locally.
        assert!(patch.apply_to_local(None).is_none());
    }

    #[test]
    fn remote_apply_stamps_commit_time() {
        let set = Mutation::Set {
            path: "notes/a".to_string(),
            fields: fields(&[("n", Value::Integer(1))]),
            merge_paths: None,
            exists: None,
            transforms: Vec::new(),
        };
        let commit = Timestamp::new(100, 0);
        let view = set.apply_to_remote(None, commit).unwrap();
        assert_eq!(view.update_time, Some(commit));
        assert_eq!(view.create_time, Some(commit));
    }

    #[test]
    fn proto_conversion_shapes() {
        let prefix = "projects/p/databases/d/documents";
        let patch = Mutation::Patch {
            path: "notes/a".to_string(),
            fields: fields(&[("n", Value::Integer(3))]),
            mask: vec!["n".to_string(), "gone".to_string()],
            exists: Some(true),
            transforms: Vec::new(),
        };
        let write = patch.to_proto(prefix);
        assert_eq!(
            write.update_mask.unwrap().field_paths,
            vec!["n".to_string(), "gone".to_string()]
        );
        assert!(write.current_document.is_some());
        let delete = Mutation::Delete {
            path: "notes/a".to_string(),
        };
        match delete.to_proto(prefix).operation.unwrap() {
            Operation::Delete(name) => {
                assert_eq!(name, "projects/p/databases/d/documents/notes/a")
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    fn patch_with(transforms: Vec<FieldTransform>) -> Mutation {
        Mutation::Patch {
            path: "notes/a".to_string(),
            fields: Fields::new(),
            mask: Vec::new(),
            exists: Some(true),
            transforms,
        }
    }

    fn transform(path: &str, kind: TransformKind) -> FieldTransform {
        FieldTransform {
            path: path.to_string(),
            kind,
        }
    }

    #[test]
    fn increment_coerces_like_the_reference() {
        let increment =
            |operand: Value| patch_with(vec![transform("n", TransformKind::Increment(operand))]);
        // Integer + integer stays integral.
        let view =
            increment(Value::Integer(2)).apply_to_local(Some(doc(&[("n", Value::Integer(3))])));
        assert_eq!(view.unwrap().fields["n"], Value::Integer(5));
        // A double operand makes a double.
        let view =
            increment(Value::Double(0.5)).apply_to_local(Some(doc(&[("n", Value::Integer(3))])));
        assert_eq!(view.unwrap().fields["n"], Value::Double(3.5));
        // A missing field takes the operand.
        let view = increment(Value::Integer(7)).apply_to_local(Some(doc(&[])));
        assert_eq!(view.unwrap().fields["n"], Value::Integer(7));
        // A decimal base goes through f64 and loses precision,
        // exactly like the reference core's local increment.
        let view = increment(Value::Integer(1)).apply_to_local(Some(doc(&[(
            "n",
            Value::Decimal128("1.000000000000000000000000000000001".to_string()),
        )])));
        assert_eq!(
            view.unwrap().fields["n"],
            Value::Decimal128("2".to_string())
        );
    }

    #[test]
    fn fold_installs_transform_results_verbatim() {
        let patch = patch_with(vec![transform(
            "n",
            TransformKind::Increment(Value::Integer(1)),
        )]);
        let exact = Value::Decimal128("2.000000000000000000000000000000001".to_string());
        let view = patch.apply_to_remote_with_results(
            Some(doc(&[(
                "n",
                Value::Decimal128("1.000000000000000000000000000000001".to_string()),
            )])),
            Timestamp::new(10, 0),
            std::slice::from_ref(&exact),
        );
        assert_eq!(view.unwrap().fields["n"], exact);
    }

    #[test]
    fn server_timestamp_estimates_null_and_folds_to_commit_time() {
        let patch = patch_with(vec![transform("ts", TransformKind::ServerTimestamp)]);
        let view = patch.apply_to_local(Some(doc(&[])));
        assert_eq!(view.unwrap().fields["ts"], Value::Null);
        let commit = Timestamp::new(42, 0);
        let view = patch.apply_to_remote(Some(doc(&[])), commit);
        assert_eq!(view.unwrap().fields["ts"], Value::Timestamp(commit));
    }

    #[test]
    fn array_union_and_remove() {
        let patch = patch_with(vec![
            transform(
                "tags",
                TransformKind::ArrayUnion(vec![Value::String("b".to_string())]),
            ),
            transform(
                "tags",
                TransformKind::ArrayRemove(vec![Value::String("a".to_string())]),
            ),
        ]);
        // Two transforms on one path apply in order in this
        // synthetic case (the SDKs keep them disjoint).
        let view = patch.apply_to_local(Some(doc(&[(
            "tags",
            Value::Array(vec![Value::String("a".to_string())]),
        )])));
        assert_eq!(
            view.unwrap().fields["tags"],
            Value::Array(vec![Value::String("b".to_string())])
        );
        // Union creates a missing field; remove sets a missing
        // or non-array field to an empty array (the wire
        // contract), it does not leave it absent or unchanged.
        let union_only = patch_with(vec![transform(
            "tags",
            TransformKind::ArrayUnion(vec![Value::Integer(1)]),
        )]);
        let view = union_only.apply_to_local(Some(doc(&[])));
        assert_eq!(
            view.unwrap().fields["tags"],
            Value::Array(vec![Value::Integer(1)])
        );
        let remove_only = patch_with(vec![transform(
            "tags",
            TransformKind::ArrayRemove(vec![Value::Integer(1)]),
        )]);
        let view = remove_only.apply_to_local(Some(doc(&[])));
        assert_eq!(view.unwrap().fields["tags"], Value::Array(Vec::new()));
        let view = remove_only.apply_to_local(Some(doc(&[("tags", Value::Integer(7))])));
        assert_eq!(view.unwrap().fields["tags"], Value::Array(Vec::new()));
    }

    #[test]
    fn fold_recomputes_array_transforms_ignoring_null_result() {
        // The server reports a null transform result for the
        // array transforms: the fold must apply the operation
        // to the base, not install the null.
        let union = patch_with(vec![transform(
            "tags",
            TransformKind::ArrayUnion(vec![Value::Integer(2)]),
        )]);
        let view = union.apply_to_remote_with_results(
            Some(doc(&[("tags", Value::Array(vec![Value::Integer(1)]))])),
            Timestamp::new(10, 0),
            &[Value::Null],
        );
        assert_eq!(
            view.unwrap().fields["tags"],
            Value::Array(vec![Value::Integer(1), Value::Integer(2)])
        );
        let remove = patch_with(vec![transform(
            "tags",
            TransformKind::ArrayRemove(vec![Value::Integer(2)]),
        )]);
        let view = remove.apply_to_remote_with_results(
            Some(doc(&[(
                "tags",
                Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
            )])),
            Timestamp::new(10, 0),
            &[Value::Null],
        );
        assert_eq!(
            view.unwrap().fields["tags"],
            Value::Array(vec![Value::Integer(1)])
        );
    }

    #[test]
    fn array_transforms_use_firestore_equivalence() {
        // Numerically equal elements of different kinds are
        // already present / removed: no duplicates by type.
        let union = patch_with(vec![transform(
            "tags",
            TransformKind::ArrayUnion(vec![Value::Double(1.0)]),
        )]);
        let view = union.apply_to_local(Some(doc(&[(
            "tags",
            Value::Array(vec![Value::Integer(1)]),
        )])));
        assert_eq!(
            view.unwrap().fields["tags"],
            Value::Array(vec![Value::Integer(1)])
        );
        let remove = patch_with(vec![transform(
            "tags",
            TransformKind::ArrayRemove(vec![Value::Double(2.0)]),
        )]);
        let view = remove.apply_to_local(Some(doc(&[(
            "tags",
            Value::Array(vec![Value::Integer(2), Value::Integer(3)]),
        )])));
        assert_eq!(
            view.unwrap().fields["tags"],
            Value::Array(vec![Value::Integer(3)])
        );
        // NaN equals NaN, and the equivalence is recursive.
        let union_nan = patch_with(vec![transform(
            "tags",
            TransformKind::ArrayUnion(vec![Value::Double(f64::NAN)]),
        )]);
        let view = union_nan.apply_to_local(Some(doc(&[(
            "tags",
            Value::Array(vec![Value::Double(f64::NAN)]),
        )])));
        match &view.unwrap().fields["tags"] {
            Value::Array(items) => {
                assert_eq!(items.len(), 1);
                assert!(matches!(items[0], Value::Double(d) if d.is_nan()));
            }
            other => panic!("unexpected: {other:?}"),
        }
        let union_nested = patch_with(vec![transform(
            "tags",
            TransformKind::ArrayUnion(vec![Value::Array(vec![Value::Integer(1)])]),
        )]);
        let view = union_nested.apply_to_local(Some(doc(&[(
            "tags",
            Value::Array(vec![Value::Array(vec![Value::Double(1.0)])]),
        )])));
        assert_eq!(
            view.unwrap().fields["tags"],
            Value::Array(vec![Value::Array(vec![Value::Double(1.0)])])
        );
    }

    #[test]
    fn increment_saturates_on_integer_overflow() {
        let increment =
            |operand: Value| patch_with(vec![transform("n", TransformKind::Increment(operand))]);
        let view = increment(Value::Integer(1))
            .apply_to_local(Some(doc(&[("n", Value::Integer(i64::MAX))])));
        assert_eq!(view.unwrap().fields["n"], Value::Integer(i64::MAX));
        let view = increment(Value::Integer(-1))
            .apply_to_local(Some(doc(&[("n", Value::Integer(i64::MIN))])));
        assert_eq!(view.unwrap().fields["n"], Value::Integer(i64::MIN));
    }

    #[test]
    fn transforms_reach_the_wire() {
        let prefix = "projects/p/databases/d/documents";
        let patch = patch_with(vec![
            transform("ts", TransformKind::ServerTimestamp),
            transform("n", TransformKind::Increment(Value::Integer(1))),
        ]);
        let write = patch.to_proto(prefix);
        assert_eq!(write.update_transforms.len(), 2);
        assert_eq!(write.update_transforms[0].field_path, "ts");
        assert!(matches!(
            write.update_transforms[0].transform_type,
            Some(
                crate::google::firestore::v1::document_transform::field_transform::TransformType::SetToServerValue(1)
            )
        ));
        assert_eq!(write.update_transforms[1].field_path, "n");
        assert!(matches!(
            write.update_transforms[1].transform_type,
            Some(
                crate::google::firestore::v1::document_transform::field_transform::TransformType::Increment(_)
            )
        ));
    }
}
