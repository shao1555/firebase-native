//! A basic structured query builder for `run_query` (P0 scope).
//!
//! Mirrors a subset of the Firestore `StructuredQuery` proto: a single
//! collection, field/unary filters combined with AND, ordering, limit
//! and offset. Composite (AND/OR) filters, cursors and collection-group
//! queries arrive with the fuller query work in later phases.

use crate::google::firestore::v1::structured_query::Direction;
use crate::google::firestore::v1::{
    structured_query::{
        CompositeFilter, FieldFilter as ProtoFieldFilter, Filter as ProtoFilter,
        Order as ProtoOrder, UnaryFilter,
    },
    StructuredQuery,
};
use crate::Value;

/// A field comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldOp {
    /// `<`
    LessThan,
    /// `<=`
    LessThanOrEqual,
    /// `>`
    GreaterThan,
    /// `>=`
    GreaterThanOrEqual,
    /// `==`
    Equal,
    /// `!=`
    NotEqual,
    /// `array-contains`
    ArrayContains,
    /// `in`
    In,
    /// `array-contains-any`
    ArrayContainsAny,
    /// `not-in`
    NotIn,
}

impl FieldOp {
    fn to_proto(self) -> ProtoFieldFilterOperator {
        use ProtoFieldFilterOperator as Op;
        match self {
            FieldOp::LessThan => Op::LessThan,
            FieldOp::LessThanOrEqual => Op::LessThanOrEqual,
            FieldOp::GreaterThan => Op::GreaterThan,
            FieldOp::GreaterThanOrEqual => Op::GreaterThanOrEqual,
            FieldOp::Equal => Op::Equal,
            FieldOp::NotEqual => Op::NotEqual,
            FieldOp::ArrayContains => Op::ArrayContains,
            FieldOp::In => Op::In,
            FieldOp::ArrayContainsAny => Op::ArrayContainsAny,
            FieldOp::NotIn => Op::NotIn,
        }
    }
}

type ProtoFieldFilterOperator =
    crate::google::firestore::v1::structured_query::field_filter::Operator;

/// A unary test on a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// The field is `NaN`.
    IsNan,
    /// The field is `NULL`.
    IsNull,
    /// The field is not `NaN`.
    IsNotNan,
    /// The field is not `NULL`.
    IsNotNull,
}

impl UnaryOp {
    fn to_proto(self) -> crate::google::firestore::v1::structured_query::unary_filter::Operator {
        use crate::google::firestore::v1::structured_query::unary_filter::Operator as Op;
        match self {
            UnaryOp::IsNan => Op::IsNan,
            UnaryOp::IsNull => Op::IsNull,
            UnaryOp::IsNotNan => Op::IsNotNan,
            UnaryOp::IsNotNull => Op::IsNotNull,
        }
    }
}

/// One query filter.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// Compare a field against a value.
    Field {
        /// Field path, e.g. `title` or `meta.done`.
        field: String,
        /// Comparison operator.
        op: FieldOp,
        /// Value to compare against.
        value: Value,
    },
    /// Test a field for `NaN` / `NULL`.
    Unary {
        /// Field path.
        field: String,
        /// Unary operator.
        op: UnaryOp,
    },
}

impl Filter {
    /// Builds an equality filter (`field == value`).
    pub fn eq(field: impl Into<String>, value: Value) -> Self {
        Filter::Field {
            field: field.into(),
            op: FieldOp::Equal,
            value,
        }
    }

    fn to_proto(&self) -> ProtoFilter {
        match self {
            Filter::Field { field, op, value } => ProtoFilter {
                filter_type: Some(
                    crate::google::firestore::v1::structured_query::filter::FilterType::FieldFilter(
                        ProtoFieldFilter {
                            field: Some(field_reference(field)),
                            op: op.to_proto() as i32,
                            value: Some(value.to_proto()),
                        },
                    ),
                ),
            },
            Filter::Unary { field, op } => ProtoFilter {
                filter_type: Some(
                    crate::google::firestore::v1::structured_query::filter::FilterType::UnaryFilter(
                        UnaryFilter {
                            op: op.to_proto() as i32,
                            operand_type: Some(
                                crate::google::firestore::v1::structured_query::unary_filter::OperandType::Field(
                                    field_reference(field),
                                ),
                            ),
                        },
                    ),
                ),
            },
        }
    }
}

/// One ordering clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    /// Field path to order by.
    pub field: String,
    /// Whether to sort descending (ascending by default).
    pub descending: bool,
}

/// The value a document sorts by for one ordering field:
/// `__name__` is the document reference; any other field is a
/// field path whose segments traverse nested maps. A path that
/// cannot be fully resolved sorts as `Null`.
pub(crate) fn order_value(document: &crate::value::Document, field: &str) -> Value {
    if field == "__name__" {
        return Value::Reference(document.name.clone());
    }
    crate::mutation::lookup_path(&document.fields, field)
        .cloned()
        .unwrap_or(Value::Null)
}

/// Compares two documents under an ordering: each clause in
/// turn under the Firestore total order, then the implicit
/// `__name__` tiebreak whose direction follows the last clause
/// (ascending when there are none), per the server semantics.
/// This is the one ordering implementation: the watch layer
/// sorts its snapshots with it, and local query evaluation
/// sorts store candidates with it, so the two can never drift.
pub(crate) fn compare_by_orders(
    orders: &[Order],
    a: &crate::value::Document,
    b: &crate::value::Document,
) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    for order in orders {
        let cmp = order_value(a, &order.field).compare(&order_value(b, &order.field));
        let cmp = if order.descending { cmp.reverse() } else { cmp };
        if cmp != Equal {
            return cmp;
        }
    }
    let name_cmp = compare_names(&a.name, &b.name);
    if orders.last().is_some_and(|order| order.descending) {
        name_cmp.reverse()
    } else {
        name_cmp
    }
}

/// Compares full document resource names segment by segment.
fn compare_names(a: &str, b: &str) -> std::cmp::Ordering {
    let mut left = a.split('/');
    let mut right = b.split('/');
    loop {
        match (left.next(), right.next()) {
            (None, None) => return std::cmp::Ordering::Equal,
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
            (Some(l), Some(r)) => match l.cmp(r) {
                std::cmp::Ordering::Equal => continue,
                ordering => return ordering,
            },
        }
    }
}

impl Order {
    /// Ascending order on a field.
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            descending: false,
        }
    }

    /// Descending order on a field.
    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            descending: true,
        }
    }
}

fn field_reference(path: &str) -> crate::google::firestore::v1::structured_query::FieldReference {
    crate::google::firestore::v1::structured_query::FieldReference {
        field_path: path.to_string(),
    }
}

/// A structured query against a single collection.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    collection_id: String,
    filters: Vec<Filter>,
    orders: Vec<Order>,
    limit: Option<i32>,
    offset: Option<i32>,
}

impl Query {
    /// Whether a document satisfies this query's filters, under
    /// local evaluation. Collection / document path matching is
    /// the caller's job. Semantics mirror the server's:
    /// comparisons use the Firestore total order, and a missing
    /// field matches nothing except the unary tests that
    /// explicitly accept it (none do — `IsNull` requires an
    /// explicit null, as on the server).
    pub(crate) fn matches(&self, document: &crate::value::Document) -> bool {
        self.filters.iter().all(|filter| match filter {
            Filter::Field { field, op, value } => {
                // `__name__` is the document reference: it lives
                // in the document's name, not its fields (the
                // shared ordering code resolves it the same way).
                let name_reference;
                let actual = if field == "__name__" {
                    name_reference = Value::Reference(document.name.clone());
                    Some(&name_reference)
                } else {
                    crate::mutation::lookup_path(&document.fields, field)
                };
                let Some(actual) = actual else {
                    return false;
                };
                use std::cmp::Ordering::*;
                match op {
                    FieldOp::Equal => actual.compare(value) == Equal,
                    FieldOp::NotEqual => actual.compare(value) != Equal,
                    FieldOp::LessThan => actual.compare(value) == Less,
                    FieldOp::LessThanOrEqual => actual.compare(value) != Greater,
                    FieldOp::GreaterThan => actual.compare(value) == Greater,
                    FieldOp::GreaterThanOrEqual => actual.compare(value) != Less,
                    FieldOp::In => match value {
                        Value::Array(options) => {
                            options.iter().any(|option| actual.compare(option) == Equal)
                        }
                        _ => false,
                    },
                    FieldOp::NotIn => match value {
                        Value::Array(options) => {
                            *actual != Value::Null
                                && options
                                    .iter()
                                    .all(|option| actual.compare(option) != Equal)
                        }
                        _ => false,
                    },
                    FieldOp::ArrayContains => match actual {
                        Value::Array(items) => {
                            items.iter().any(|item| item.compare(value) == Equal)
                        }
                        _ => false,
                    },
                    FieldOp::ArrayContainsAny => match (actual, value) {
                        (Value::Array(items), Value::Array(options)) => items
                            .iter()
                            .any(|item| options.iter().any(|o| item.compare(o) == Equal)),
                        _ => false,
                    },
                }
            }
            Filter::Unary { field, op } => {
                let name_reference;
                let actual = if field == "__name__" {
                    name_reference = Value::Reference(document.name.clone());
                    Some(&name_reference)
                } else {
                    crate::mutation::lookup_path(&document.fields, field)
                };
                match op {
                    UnaryOp::IsNull => matches!(actual, Some(Value::Null)),
                    UnaryOp::IsNotNull => matches!(actual, Some(value) if *value != Value::Null),
                    UnaryOp::IsNan => {
                        matches!(actual, Some(Value::Double(v)) if v.is_nan())
                    }
                    UnaryOp::IsNotNan => {
                        matches!(actual, Some(value) if !matches!(value, Value::Double(v) if v.is_nan()))
                    }
                }
            }
        })
    }

    /// The collection this query targets.
    pub(crate) fn collection_id(&self) -> &str {
        &self.collection_id
    }

    /// A copy of this query without limit or offset, for
    /// fetching the full candidate set that a local evaluation
    /// then paginates exactly once.
    /// Whether this query covers its entire collection:
    /// no filters, no pagination window, and no ordering by
    /// an ordinary field — ordering bounds the set too,
    /// because evaluation drops documents that lack the
    /// ordering field (only `__name__` exists on every
    /// document). Only a sync of such a query may
    /// re-establish the collection's completeness after
    /// garbage collection marked it partial: a partial sync
    /// legitimately never sees the rest.
    pub(crate) fn covers_entire_collection(&self) -> bool {
        self.filters.is_empty()
            && self.limit.is_none()
            && self.offset.is_none()
            && self.orders.iter().all(|order| order.field == "__name__")
    }

    pub(crate) fn without_pagination(&self) -> Query {
        let mut query = self.clone();
        query.limit = None;
        query.offset = None;
        query
    }

    /// A clone with the offset removed. For reconciling a
    /// watch's view locally: the view is the server's already
    /// paginated window and does not contain the documents the
    /// offset skipped, so re-applying the offset to it would
    /// skip valid results a second time. The limit still
    /// applies — the view's documents compete for its slots.
    pub(crate) fn without_offset(&self) -> Query {
        let mut query = self.clone();
        query.offset = None;
        query
    }

    /// Evaluates the query locally over candidate documents
    /// (the caller supplies every document in the target
    /// collection, e.g. the local store's views): filters by
    /// [`Query::matches`], drops documents missing an ordering
    /// field (the server only returns documents that have every
    /// field named by the effective ordering), sorts with the
    /// shared ordering implementation, then applies offset and
    /// limit.
    pub(crate) fn evaluate(
        &self,
        candidates: Vec<crate::value::Document>,
    ) -> Vec<crate::value::Document> {
        let orders = self.effective_orders();
        let mut documents: Vec<crate::value::Document> = candidates
            .into_iter()
            .filter(|document| self.matches(document))
            .filter(|document| {
                orders.iter().all(|order| {
                    order.field == "__name__"
                        || crate::mutation::lookup_path(&document.fields, &order.field).is_some()
                })
            })
            .collect();
        documents.sort_by(|a, b| compare_by_orders(&orders, a, b));
        if let Some(offset) = self.offset {
            let offset = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
            documents = documents.into_iter().skip(offset).collect();
        }
        if let Some(limit) = self.limit {
            let limit = usize::try_from(limit.max(0)).unwrap_or(0);
            documents.truncate(limit);
        }
        documents
    }

    /// Starts a query on a collection (its ID, not a full path:
    /// `notes`, not `spaces/personal/notes` — for subcollections pass
    /// the parent document to `run_query`).
    pub fn new(collection_id: impl Into<String>) -> Self {
        Self {
            collection_id: collection_id.into(),
            filters: Vec::new(),
            orders: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    /// Adds a filter (filters combine with AND).
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Adds an ordering clause.
    pub fn order_by(mut self, order: Order) -> Self {
        self.orders.push(order);
        self
    }

    /// Limits the number of documents returned.
    pub fn limit(mut self, limit: i32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Skips the first `offset` documents.
    pub fn offset(mut self, offset: i32) -> Self {
        self.offset = Some(offset);
        self
    }

    /// The effective ordering the server applies, following the
    /// Standard-edition rules in the `StructuredQuery` documentation:
    /// every field used with an inequality filter must be ordered;
    /// missing ones are appended in lexicographical field order with
    /// the direction of the last specified order (ascending when
    /// there is none). `__name__` is not listed here; it is the
    /// implicit final tiebreak applied by the snapshot sorter.
    pub(crate) fn effective_orders(&self) -> Vec<Order> {
        let mut orders = self.orders.clone();
        let mut missing: Vec<&str> = Vec::new();
        for filter in &self.filters {
            // Inequality filters: field comparisons other than
            // equality/containment, plus the unary IS_NOT_NULL /
            // IS_NOT_NAN tests (the proto documentation explicitly
            // requires those fields first in the ordering too).
            let inequality_field = match filter {
                Filter::Field { field, op, .. } => match op {
                    FieldOp::LessThan
                    | FieldOp::LessThanOrEqual
                    | FieldOp::GreaterThan
                    | FieldOp::GreaterThanOrEqual
                    | FieldOp::NotEqual
                    | FieldOp::NotIn => Some(field.as_str()),
                    _ => None,
                },
                Filter::Unary { field, op } => match op {
                    UnaryOp::IsNotNull | UnaryOp::IsNotNan => Some(field.as_str()),
                    _ => None,
                },
            };
            if let Some(field) = inequality_field {
                if field != "__name__"
                    && !orders.iter().any(|order| order.field == field)
                    && !missing.contains(&field)
                {
                    missing.push(field);
                }
            }
        }
        missing.sort_unstable();
        let descending = orders.last().is_some_and(|order| order.descending);
        for field in missing {
            orders.push(Order {
                field: field.to_string(),
                descending,
            });
        }
        orders
    }

    pub(crate) fn to_proto(&self) -> StructuredQuery {
        let filter = match self.filters.len() {
            0 => None,
            1 => Some(self.filters[0].to_proto()),
            _ => Some(ProtoFilter {
                filter_type: Some(
                    crate::google::firestore::v1::structured_query::filter::FilterType::CompositeFilter(
                        CompositeFilter {
                            op: crate::google::firestore::v1::structured_query::composite_filter::Operator::And as i32,
                            filters: self.filters.iter().map(Filter::to_proto).collect(),
                        },
                    ),
                ),
            }),
        };
        StructuredQuery {
            select: None,
            from: vec![
                crate::google::firestore::v1::structured_query::CollectionSelector {
                    collection_id: self.collection_id.clone(),
                    all_descendants: false,
                },
            ],
            r#where: filter,
            order_by: self
                .orders
                .iter()
                .map(|order| ProtoOrder {
                    field: Some(field_reference(&order.field)),
                    direction: if order.descending {
                        Direction::Descending as i32
                    } else {
                        Direction::Ascending as i32
                    },
                })
                .collect(),
            start_at: None,
            end_at: None,
            offset: self.offset.unwrap_or(0),
            find_nearest: None,
            limit: self.limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_structured_query_proto() {
        let query = Query::new("notes")
            .filter(Filter::eq("spaceId", Value::String("personal".to_string())))
            .filter(Filter::Field {
                field: "order".to_string(),
                op: FieldOp::GreaterThan,
                value: Value::Integer(3),
            })
            .order_by(Order::desc("updatedAt"))
            .limit(20);
        let proto = query.to_proto();
        assert_eq!(proto.from.len(), 1);
        assert_eq!(proto.from[0].collection_id, "notes");
        assert!(!proto.from[0].all_descendants);
        assert_eq!(proto.limit, Some(20));
        assert_eq!(proto.order_by.len(), 1);
        // Two filters combine into an AND composite filter.
        match proto.r#where.unwrap().filter_type.unwrap() {
            crate::google::firestore::v1::structured_query::filter::FilterType::CompositeFilter(
                composite,
            ) => assert_eq!(composite.filters.len(), 2),
            other => panic!("unexpected filter type: {other:?}"),
        }
    }

    #[test]
    fn effective_orders_append_inequality_fields() {
        // WHERE rank > 0 with no explicit order: implicit rank ASC.
        let query = Query::new("notes").filter(Filter::Field {
            field: "rank".to_string(),
            op: FieldOp::GreaterThan,
            value: Value::Integer(0),
        });
        let orders = query.effective_orders();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].field, "rank");
        assert!(!orders[0].descending);

        // An explicit ordering on another field inherits its
        // direction for the appended inequality field.
        let query = Query::new("notes")
            .filter(Filter::Field {
                field: "rank".to_string(),
                op: FieldOp::GreaterThan,
                value: Value::Integer(0),
            })
            .order_by(Order::desc("updatedAt"));
        let orders = query.effective_orders();
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[1].field, "rank");
        assert!(orders[1].descending);

        // Already ordered by the inequality field: unchanged. An
        // equality filter adds nothing.
        let query = Query::new("notes")
            .filter(Filter::eq("spaceId", Value::String("x".to_string())))
            .filter(Filter::Field {
                field: "rank".to_string(),
                op: FieldOp::LessThan,
                value: Value::Integer(5),
            })
            .order_by(Order::asc("rank"));
        assert_eq!(query.effective_orders().len(), 1);
    }

    #[test]
    fn effective_orders_include_unary_inequalities() {
        for op in [UnaryOp::IsNotNull, UnaryOp::IsNotNan] {
            let query = Query::new("notes").filter(Filter::Unary {
                field: "rank".to_string(),
                op,
            });
            let orders = query.effective_orders();
            assert_eq!(orders.len(), 1, "{op:?}");
            assert_eq!(orders[0].field, "rank");
            assert!(!orders[0].descending);
        }
        // The equality-like unary tests add no ordering.
        for op in [UnaryOp::IsNull, UnaryOp::IsNan] {
            let query = Query::new("notes").filter(Filter::Unary {
                field: "rank".to_string(),
                op,
            });
            assert!(query.effective_orders().is_empty(), "{op:?}");
        }
    }

    fn document(name: &str, fields: &[(&str, Value)]) -> crate::value::Document {
        crate::value::Document {
            name: name.to_string(),
            fields: fields
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            create_time: None,
            update_time: None,
        }
    }

    fn names(documents: &[crate::value::Document]) -> Vec<String> {
        documents
            .iter()
            .map(|document| document.name.clone())
            .collect()
    }

    #[test]
    fn evaluate_orders_limits_and_offsets() {
        let prefix = "projects/p/databases/d/documents/notes/";
        let candidates = vec![
            document(&format!("{prefix}c"), &[("rank", Value::Integer(3))]),
            document(&format!("{prefix}a"), &[("rank", Value::Integer(2))]),
            document(&format!("{prefix}b"), &[("rank", Value::Integer(1))]),
        ];
        let query = Query::new("notes").order_by(Order::asc("rank"));
        assert_eq!(
            names(&query.evaluate(candidates.clone())),
            [
                format!("{prefix}b"),
                format!("{prefix}a"),
                format!("{prefix}c")
            ]
        );
        // Descending reverses the values and the name tiebreak.
        let query = Query::new("notes").order_by(Order::desc("rank")).limit(2);
        assert_eq!(
            names(&query.evaluate(candidates.clone())),
            [format!("{prefix}c"), format!("{prefix}a")]
        );
        let query = Query::new("notes").order_by(Order::asc("rank")).offset(1);
        assert_eq!(
            names(&query.evaluate(candidates)),
            [format!("{prefix}a"), format!("{prefix}c")]
        );
    }

    #[test]
    fn evaluate_matches_name_filters() {
        let prefix = "projects/p/databases/d/documents/notes/";
        let candidates = vec![
            document(&format!("{prefix}a"), &[("rank", Value::Integer(1))]),
            document(&format!("{prefix}b"), &[("rank", Value::Integer(2))]),
        ];
        // Equality on the document reference: `__name__` lives
        // in the document's name, not its fields.
        let query = Query::new("notes").filter(Filter::Field {
            field: "__name__".to_string(),
            op: FieldOp::Equal,
            value: Value::Reference(format!("{prefix}a")),
        });
        assert_eq!(
            names(&query.evaluate(candidates.clone())),
            [format!("{prefix}a")]
        );
        // A range over names.
        let query = Query::new("notes").filter(Filter::Field {
            field: "__name__".to_string(),
            op: FieldOp::GreaterThan,
            value: Value::Reference(format!("{prefix}a")),
        });
        assert_eq!(
            names(&query.evaluate(candidates.clone())),
            [format!("{prefix}b")]
        );
        // Membership in a list of references.
        let query = Query::new("notes").filter(Filter::Field {
            field: "__name__".to_string(),
            op: FieldOp::In,
            value: Value::Array(vec![Value::Reference(format!("{prefix}b"))]),
        });
        assert_eq!(names(&query.evaluate(candidates)), [format!("{prefix}b")]);
        // The unary tests see the reference too: a name is
        // never null or NaN.
        let query = Query::new("notes").filter(Filter::Unary {
            field: "__name__".to_string(),
            op: UnaryOp::IsNotNull,
        });
        assert_eq!(query.evaluate(vec![]).len(), 0);
        let candidates = vec![document(&format!("{prefix}a"), &[])];
        assert_eq!(query.evaluate(candidates).len(), 1);
    }

    #[test]
    fn covers_entire_collection_requires_no_filters_and_no_window() {
        assert!(Query::new("notes").covers_entire_collection());
        assert!(Query::new("notes")
            .order_by(Order::asc("__name__"))
            .covers_entire_collection());
        assert!(
            !Query::new("notes")
                .order_by(Order::asc("n"))
                .covers_entire_collection(),
            "ordering by an ordinary field drops documents lacking it"
        );
        assert!(!Query::new("notes")
            .filter(Filter::eq("n", Value::Integer(1)))
            .covers_entire_collection());
        assert!(!Query::new("notes").limit(1).covers_entire_collection());
        assert!(!Query::new("notes").offset(1).covers_entire_collection());
    }

    #[test]
    fn without_pagination_keeps_filters_and_orders() {
        let query = Query::new("notes")
            .filter(Filter::eq("spaceId", Value::String("x".to_string())))
            .order_by(Order::desc("rank"))
            .limit(5)
            .offset(2);
        let unpaginated = query.without_pagination();
        assert_eq!(unpaginated.limit, None);
        assert_eq!(unpaginated.offset, None);
        let proto = unpaginated.to_proto();
        assert!(proto.r#where.is_some());
        assert_eq!(proto.order_by.len(), 1);
        assert_eq!(proto.limit, None);
        assert_eq!(proto.offset, 0);
    }

    #[test]
    fn evaluate_excludes_documents_missing_the_order_field() {
        let prefix = "projects/p/databases/d/documents/notes/";
        let candidates = vec![
            document(&format!("{prefix}a"), &[("rank", Value::Integer(1))]),
            document(
                &format!("{prefix}b"),
                &[("title", Value::String("x".into()))],
            ),
        ];
        // Ordered: b has no rank and the server would not return it.
        let query = Query::new("notes").order_by(Order::asc("rank"));
        assert_eq!(
            names(&query.evaluate(candidates.clone())),
            [format!("{prefix}a")]
        );
        // Unordered: both are candidates, in name order.
        let query = Query::new("notes");
        assert_eq!(
            names(&query.evaluate(candidates)),
            [format!("{prefix}a"), format!("{prefix}b")]
        );
    }

    #[test]
    fn evaluate_applies_filters_with_ordering() {
        let prefix = "projects/p/databases/d/documents/notes/";
        let candidates = vec![
            document(&format!("{prefix}a"), &[("rank", Value::Integer(1))]),
            document(&format!("{prefix}b"), &[("rank", Value::Integer(5))]),
            document(&format!("{prefix}c"), &[("rank", Value::Integer(3))]),
        ];
        let query = Query::new("notes")
            .filter(Filter::Field {
                field: "rank".to_string(),
                op: FieldOp::GreaterThan,
                value: Value::Integer(1),
            })
            .order_by(Order::asc("rank"));
        assert_eq!(
            names(&query.evaluate(candidates)),
            [format!("{prefix}c"), format!("{prefix}b")]
        );
    }

    #[test]
    fn single_filter_is_not_wrapped() {
        let query = Query::new("notes").filter(Filter::Unary {
            field: "deletedAt".to_string(),
            op: UnaryOp::IsNull,
        });
        let proto = query.to_proto();
        match proto.r#where.unwrap().filter_type.unwrap() {
            crate::google::firestore::v1::structured_query::filter::FilterType::UnaryFilter(_) => {}
            other => panic!("unexpected filter type: {other:?}"),
        }
    }
}
