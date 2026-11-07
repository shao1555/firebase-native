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
