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

/// Validates the syntax of a field path a *write* may target
/// and returns its parsed segments. Each segment must be a
/// simple name (a letter or underscore, then letters, digits
/// and underscores — the form [`encode_field_path_segment`]
/// leaves unquoted) or a backtick-quoted string whose closing
/// backtick ends the segment, with non-empty content and no
/// unescaped backtick inside; reserved names (`__…__`, e.g.
/// `__name__`) are rejected. [`parse_field_path`] alone is a
/// permissive lookup parser: it accepts empty paths, empty
/// segments and malformed quoting, so a write API must not
/// enqueue a path it has not run through this first — the
/// malformed write would otherwise succeed locally and be
/// rejected by the server after queueing.
pub(crate) fn validate_field_path(path: &str) -> Result<Vec<String>> {
    let invalid =
        |reason: &str| Error::InvalidArgument(format!("invalid field path `{path}`: {reason}"));
    if path.is_empty() {
        return Err(invalid("the path is empty"));
    }
    // Split into raw (still-quoted) segments, respecting
    // backtick quoting and backslash escapes inside it.
    let mut raw: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut chars = path.chars();
    loop {
        match chars.next() {
            None => {
                raw.push(current);
                break;
            }
            Some('`') if !quoted && current.is_empty() => {
                quoted = true;
                current.push('`');
            }
            Some('`') if quoted => {
                quoted = false;
                current.push('`');
            }
            Some('\\') if quoted => {
                current.push('\\');
                match chars.next() {
                    Some(escaped) => current.push(escaped),
                    None => return Err(invalid("a dangling escape ends the path")),
                }
            }
            Some('.') if !quoted => {
                raw.push(std::mem::take(&mut current));
            }
            Some(c) => current.push(c),
        }
    }
    if quoted {
        return Err(invalid("a backtick quote is not closed"));
    }
    for segment in &raw {
        if segment.is_empty() {
            return Err(invalid("a segment is empty"));
        }
        if !valid_field_segment(segment) {
            return Err(invalid("a segment is malformed"));
        }
    }
    let segments = parse_field_path(path);
    for segment in &segments {
        if segment.len() > 3 && segment.starts_with("__") && segment.ends_with("__") {
            return Err(invalid("a segment uses a reserved name"));
        }
    }
    Ok(segments)
}

/// Whether one raw (still-quoted, if quoted) field-path
/// segment is well-formed; see [`validate_field_path`].
fn valid_field_segment(segment: &str) -> bool {
    if let Some(rest) = segment.strip_prefix('`') {
        let Some(inner) = rest.strip_suffix('`') else {
            return false;
        };
        if inner.is_empty() {
            return false;
        }
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            match c {
                '`' => return false,
                '\\' => {
                    chars.next();
                }
                _ => {}
            }
        }
        true
    } else {
        let mut chars = segment.chars();
        match chars.next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
            _ => return false,
        }
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }
}

/// Splits a Firestore field path into its segments. Segments are
/// dot-separated; a segment wrapped in backticks may itself contain
/// dots, and inside backticks a backslash escapes the next character
/// (for a literal backtick or backslash). This is the inverse of
/// [`encode_field_path_segment`].
pub(crate) fn parse_field_path(path: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut chars = path.chars();
    loop {
        match chars.next() {
            None => {
                segments.push(current);
                return segments;
            }
            Some('`') if !quoted && current.is_empty() => {
                quoted = true;
            }
            Some('`') if quoted => {
                quoted = false;
            }
            Some('\\') if quoted => {
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                }
            }
            Some('.') if !quoted => {
                segments.push(std::mem::take(&mut current));
            }
            Some(c) => current.push(c),
        }
    }
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
    fn field_path_parsing() {
        assert_eq!(parse_field_path("title"), vec!["title"]);
        assert_eq!(parse_field_path("a.b.c"), vec!["a", "b", "c"]);
        assert_eq!(parse_field_path("`a.b`.c"), vec!["a.b", "c"]);
        assert_eq!(parse_field_path("`a\\`b`"), vec!["a`b"]);
        assert_eq!(parse_field_path("`a\\\\b`"), vec!["a\\b"]);
        // Round trip with the encoder for names that need quoting.
        for name in ["a.b", "a b", "a`b", "1st"] {
            let encoded = encode_field_path_segment(name);
            assert_eq!(parse_field_path(&encoded), vec![name], "{name}");
        }
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
