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
    pub(crate) fn apply_to_local(&self, current: Option<Document>) -> Option<Document> {
        self.apply(current, None)
    }

    /// Applies the mutation to the server-confirmed state after the
    /// batch was acknowledged with `commit_time`.
    pub(crate) fn apply_to_remote(
        &self,
        current: Option<Document>,
        commit_time: Timestamp,
    ) -> Option<Document> {
        self.apply(current, Some(commit_time))
    }

    fn apply(&self, current: Option<Document>, commit_time: Option<Timestamp>) -> Option<Document> {
        match self {
            Mutation::Delete { .. } => None,
            Mutation::Set {
                fields,
                merge_paths,
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
                if let Some(commit_time) = commit_time {
                    document.update_time = Some(commit_time);
                    if document.create_time.is_none() {
                        document.create_time = Some(commit_time);
                    }
                }
                Some(document)
            }
            Mutation::Patch { fields, mask, .. } => {
                let mut document = current?;
                for path in mask {
                    match lookup_path(fields, path) {
                        Some(value) => set_path(&mut document.fields, path, value.clone()),
                        None => delete_path(&mut document.fields, path),
                    }
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
                update_transforms: Vec::new(),
                current_document: precondition(*exists),
            },
            Mutation::Patch {
                path,
                fields,
                mask,
                exists,
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
                    update_transforms: Vec::new(),
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
}
