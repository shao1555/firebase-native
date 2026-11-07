//! Document and collection path helpers.
//!
//! Paths in the public API are relative to the database's documents
//! prefix: a document path has an even number of segments
//! (`notes/abc`, `notes/abc/items/def`), a collection path an odd
//! number (`notes`, `notes/abc/items`). Full resource names prefix the
//! path with `projects/{p}/databases/{d}/documents/`.

use firebase_native_core::{Error, Result};

/// Returns the relative path portion of a full document resource name,
/// or `None` when the name has no `/documents/` marker.
pub fn relative_path(full_name: &str) -> Option<&str> {
    full_name.split_once("/documents/").map(|(_, rest)| rest)
}

/// Whether the relative path addresses a document (even segment count,
/// at least 2 segments).
pub fn is_document_path(path: &str) -> bool {
    let segments = segment_count(path);
    segments >= 2 && segments % 2 == 0
}

/// Whether the relative path addresses a collection (odd segment count).
pub fn is_collection_path(path: &str) -> bool {
    segment_count(path) % 2 == 1
}

/// The document ID (last segment) of a document path or full name.
pub fn document_id_of(path: &str) -> Option<&str> {
    let relative = relative_path(path).unwrap_or(path);
    relative.rsplit('/').next().filter(|id| !id.is_empty())
}

/// The collection path containing the given document path:
/// `notes/abc/items/def` → `notes/abc/items`.
pub fn collection_path_of(document_path: &str) -> Option<&str> {
    let relative = relative_path(document_path).unwrap_or(document_path);
    relative.rsplit_once('/').map(|(parent, _)| parent)
}

fn segment_count(path: &str) -> usize {
    let relative = relative_path(path).unwrap_or(path).trim_matches('/');
    if relative.is_empty() {
        0
    } else {
        relative.split('/').count()
    }
}

pub(crate) fn validate_document_path(path: &str) -> Result<()> {
    validate_segments(path)?;
    if !is_document_path(path) {
        return Err(Error::InvalidArgument(format!(
            "expected a document path (even number of segments), got `{path}`"
        )));
    }
    Ok(())
}

pub(crate) fn validate_collection_path(path: &str) -> Result<()> {
    validate_segments(path)?;
    if !is_collection_path(path) {
        return Err(Error::InvalidArgument(format!(
            "expected a collection path (odd number of segments), got `{path}`"
        )));
    }
    Ok(())
}

fn validate_segments(path: &str) -> Result<()> {
    let relative = relative_path(path).unwrap_or(path);
    if relative.is_empty() || relative.starts_with('/') || relative.ends_with('/') {
        return Err(Error::InvalidArgument(format!(
            "path must be non-empty without leading/trailing slashes, got `{path}`"
        )));
    }
    if relative.split('/').any(|segment| segment.is_empty()) {
        return Err(Error::InvalidArgument(format!(
            "path contains an empty segment: `{path}`"
        )));
    }
    Ok(())
}

/// Builds the full resource name for a relative document path under the
/// given documents prefix. Accepts a full name unchanged.
pub(crate) fn full_document_name(documents_prefix: &str, path: &str) -> String {
    if path.starts_with("projects/") {
        path.to_string()
    } else {
        format!("{documents_prefix}/{path}")
    }
}

/// Splits a collection path into (parent full name, collection ID) for
/// `CreateDocument` / `ListDocuments`. The parent of a root collection
/// is the documents prefix itself.
pub(crate) fn split_collection(documents_prefix: &str, collection_path: &str) -> (String, String) {
    match collection_path.rsplit_once('/') {
        Some((parent_path, collection_id)) => (
            format!("{documents_prefix}/{parent_path}"),
            collection_id.to_string(),
        ),
        None => (documents_prefix.to_string(), collection_path.to_string()),
    }
}

/// Encodes one literal field name as a single Firestore field-path
/// segment, for use in document masks. Bare names that match the
/// simple identifier grammar are kept as-is; anything else (dots,
/// spaces, leading digits, ...) is backtick-quoted with `\` and
/// backtick escaped, so the segment always denotes the literal
/// top-level field and never a nested path.
pub(crate) fn encode_field_path_segment(name: &str) -> String {
    let mut chars = name.chars();
    let bare = match chars.next() {
        Some(first) => {
            (first.is_ascii_alphabetic() || first == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        None => false,
    };
    if bare {
        return name.to_string();
    }
    let mut out = String::with_capacity(name.len() + 2);
    out.push('`');
    for c in name.chars() {
        if c == '\\' || c == '`' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('`');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_path_segment_encoding() {
        assert_eq!(encode_field_path_segment("title"), "title");
        assert_eq!(encode_field_path_segment("_a1"), "_a1");
        // A dot must not become a nested path.
        assert_eq!(encode_field_path_segment("a.b"), "`a.b`");
        assert_eq!(encode_field_path_segment("a b"), "`a b`");
        assert_eq!(encode_field_path_segment("1st"), "`1st`");
        assert_eq!(encode_field_path_segment("a`b"), "`a\\`b`");
        assert_eq!(encode_field_path_segment("a\\b"), "`a\\\\b`");
    }

    #[test]
    fn path_kinds() {
        assert!(is_collection_path("notes"));
        assert!(is_document_path("notes/abc"));
        assert!(is_collection_path("notes/abc/items"));
        assert!(is_document_path("notes/abc/items/def"));
        assert!(!is_document_path("notes"));
        assert!(!is_collection_path("notes/abc"));
    }

    #[test]
    fn relative_and_ids() {
        let full = "projects/p/databases/mydb/documents/spaces/personal/notes/n1";
        assert_eq!(relative_path(full), Some("spaces/personal/notes/n1"));
        assert_eq!(document_id_of(full), Some("n1"));
        assert_eq!(document_id_of("notes/n1"), Some("n1"));
        assert_eq!(
            collection_path_of("notes/abc/items/def"),
            Some("notes/abc/items")
        );
    }

    #[test]
    fn validation_rejects_bad_paths() {
        assert!(validate_document_path("notes/abc").is_ok());
        assert!(validate_document_path("notes").is_err());
        assert!(validate_document_path("").is_err());
        assert!(validate_document_path("/notes/abc").is_err());
        assert!(validate_document_path("notes//abc").is_err());
        assert!(validate_collection_path("notes").is_ok());
        assert!(validate_collection_path("notes/abc").is_err());
    }

    #[test]
    fn full_name_and_split() {
        let prefix = "projects/p/databases/d/documents";
        assert_eq!(
            full_document_name(prefix, "notes/n1"),
            "projects/p/databases/d/documents/notes/n1"
        );
        assert_eq!(
            full_document_name(prefix, "projects/p/databases/d/documents/notes/n1"),
            "projects/p/databases/d/documents/notes/n1"
        );
        assert_eq!(
            split_collection(prefix, "notes"),
            (prefix.to_string(), "notes".to_string())
        );
        assert_eq!(
            split_collection(prefix, "notes/n1/items"),
            (
                "projects/p/databases/d/documents/notes/n1".to_string(),
                "items".to_string()
            )
        );
    }
}
