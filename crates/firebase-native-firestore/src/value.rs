//! The public value model and its conversion to/from the generated
//! protobuf types. The generated types never appear in the public API.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use firebase_native_core::{Error, Result};

use crate::google::firestore::v1::{value::ValueType, MapValue};
use crate::Fields;

type ProtoValue = crate::google::firestore::v1::Value;

/// A Firestore timestamp: seconds and nanoseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    /// Seconds since the Unix epoch.
    pub seconds: i64,
    /// Nanoseconds within the second (0..=999,999,999).
    pub nanos: i32,
}

impl Timestamp {
    /// Creates a timestamp from its parts.
    pub fn new(seconds: i64, nanos: i32) -> Self {
        Self { seconds, nanos }
    }

    /// Converts a [`SystemTime`] to a timestamp.
    pub fn from_system_time(time: SystemTime) -> Self {
        match time.duration_since(UNIX_EPOCH) {
            Ok(duration) => Self {
                seconds: duration.as_secs() as i64,
                nanos: duration.subsec_nanos() as i32,
            },
            Err(err) => {
                // Before the epoch: duration() is the distance to it.
                // e.g. epoch - 1.5s is represented as -2s + 500,000,000ns.
                let duration = err.duration();
                let subsec = duration.subsec_nanos() as i64;
                if subsec == 0 {
                    Self {
                        seconds: -(duration.as_secs() as i64),
                        nanos: 0,
                    }
                } else {
                    Self {
                        seconds: -(duration.as_secs() as i64) - 1,
                        nanos: (1_000_000_000 - subsec) as i32,
                    }
                }
            }
        }
    }

    /// Converts the timestamp back to a [`SystemTime`].
    pub fn to_system_time(&self) -> SystemTime {
        if self.seconds >= 0 {
            UNIX_EPOCH + Duration::new(self.seconds as u64, self.nanos.max(0) as u32)
        } else {
            UNIX_EPOCH - Duration::new((-self.seconds) as u64, 0)
                + Duration::from_nanos(self.nanos.max(0) as u64)
        }
    }
}

impl From<SystemTime> for Timestamp {
    fn from(time: SystemTime) -> Self {
        Self::from_system_time(time)
    }
}

impl From<Timestamp> for prost_types::Timestamp {
    fn from(ts: Timestamp) -> Self {
        prost_types::Timestamp {
            seconds: ts.seconds,
            nanos: ts.nanos,
        }
    }
}

impl From<prost_types::Timestamp> for Timestamp {
    fn from(ts: prost_types::Timestamp) -> Self {
        Timestamp {
            seconds: ts.seconds,
            nanos: ts.nanos,
        }
    }
}

/// A geographic point (latitude / longitude in degrees).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoPoint {
    /// Latitude in degrees.
    pub latitude: f64,
    /// Longitude in degrees.
    pub longitude: f64,
}

/// A Firestore value. Mirrors the `Value` oneof of the Firestore API
/// without exposing the generated protobuf type.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// `NULL`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A 64-bit integer.
    Integer(i64),
    /// A 64-bit floating point number.
    Double(f64),
    /// A UTF-8 string.
    String(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// A timestamp.
    Timestamp(Timestamp),
    /// A geographic point.
    GeoPoint(GeoPoint),
    /// A reference to a document, as a full resource name
    /// (`projects/.../databases/.../documents/...`).
    Reference(String),
    /// An array of values.
    Array(Vec<Value>),
    /// A map of string keys to values.
    Map(Fields),
}

impl Value {
    /// Converts to the protobuf representation.
    pub(crate) fn to_proto(&self) -> ProtoValue {
        let value_type = match self {
            Value::Null => ValueType::NullValue(0),
            Value::Bool(v) => ValueType::BooleanValue(*v),
            Value::Integer(v) => ValueType::IntegerValue(*v),
            Value::Double(v) => ValueType::DoubleValue(*v),
            Value::String(v) => ValueType::StringValue(v.clone()),
            Value::Bytes(v) => ValueType::BytesValue(v.clone()),
            Value::Timestamp(ts) => ValueType::TimestampValue(prost_types::Timestamp::from(*ts)),
            Value::GeoPoint(point) => ValueType::GeoPointValue(crate::google::r#type::LatLng {
                latitude: point.latitude,
                longitude: point.longitude,
            }),
            Value::Reference(name) => ValueType::ReferenceValue(name.clone()),
            Value::Array(values) => {
                ValueType::ArrayValue(crate::google::firestore::v1::ArrayValue {
                    values: values.iter().map(Value::to_proto).collect(),
                })
            }
            Value::Map(fields) => ValueType::MapValue(MapValue {
                fields: fields_to_proto(fields),
            }),
        };
        ProtoValue {
            value_type: Some(value_type),
        }
    }

    /// Converts from the protobuf representation.
    pub(crate) fn from_proto(proto: ProtoValue) -> Result<Value> {
        let value_type = proto
            .value_type
            .ok_or_else(|| Error::Other("protobuf Value without a value_type".to_string()))?;
        Ok(match value_type {
            ValueType::NullValue(_) => Value::Null,
            ValueType::BooleanValue(v) => Value::Bool(v),
            ValueType::IntegerValue(v) => Value::Integer(v),
            ValueType::DoubleValue(v) => Value::Double(v),
            ValueType::StringValue(v) => Value::String(v),
            ValueType::BytesValue(v) => Value::Bytes(v),
            ValueType::TimestampValue(ts) => Value::Timestamp(Timestamp::from(ts)),
            ValueType::GeoPointValue(point) => Value::GeoPoint(GeoPoint {
                latitude: point.latitude,
                longitude: point.longitude,
            }),
            ValueType::ReferenceValue(name) => Value::Reference(name),
            ValueType::ArrayValue(array) => Value::Array(
                array
                    .values
                    .into_iter()
                    .map(Value::from_proto)
                    .collect::<Result<Vec<_>>>()?,
            ),
            ValueType::MapValue(map) => Value::Map(fields_from_proto(map.fields)?),
            // Pipeline-only value kinds (field/variable references,
            // function calls, sub-pipelines) are not storable document
            // values and have no counterpart in the public model.
            other => {
                return Err(Error::Other(format!(
                    "unsupported pipeline-only protobuf Value kind: {other:?}"
                )))
            }
        })
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(v as i64)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Double(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}

/// A Firestore document in the public model.
#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    /// Full resource name:
    /// `projects/{project}/databases/{db}/documents/{path}`.
    pub name: String,
    /// Document fields.
    pub fields: Fields,
    /// Server creation time, when reported.
    pub create_time: Option<Timestamp>,
    /// Server last-update time, when reported.
    pub update_time: Option<Timestamp>,
}

impl Document {
    /// The document's path relative to the documents prefix
    /// (e.g. `notes/abc123`).
    pub fn path(&self) -> &str {
        crate::path::relative_path(&self.name).unwrap_or(&self.name)
    }

    /// The document ID (last path segment).
    pub fn id(&self) -> &str {
        crate::path::document_id_of(&self.name).unwrap_or("")
    }

    pub(crate) fn from_proto(proto: crate::google::firestore::v1::Document) -> Result<Document> {
        Ok(Document {
            name: proto.name,
            fields: fields_from_proto(proto.fields)?,
            create_time: proto.create_time.map(Timestamp::from),
            update_time: proto.update_time.map(Timestamp::from),
        })
    }

    // Used by the conversion round-trip tests; the client builds proto
    // documents inline where it also needs request-specific fields.
    #[allow(dead_code)]
    pub(crate) fn to_proto(&self) -> crate::google::firestore::v1::Document {
        crate::google::firestore::v1::Document {
            name: self.name.clone(),
            fields: fields_to_proto(&self.fields),
            create_time: self.create_time.map(prost_types::Timestamp::from),
            update_time: self.update_time.map(prost_types::Timestamp::from),
        }
    }
}

pub(crate) fn fields_to_proto(fields: &Fields) -> HashMap<String, ProtoValue> {
    fields
        .iter()
        .map(|(key, value)| (key.clone(), value.to_proto()))
        .collect()
}

pub(crate) fn fields_from_proto(fields: HashMap<String, ProtoValue>) -> Result<Fields> {
    fields
        .into_iter()
        .map(|(key, value)| Ok((key, Value::from_proto(value)?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: Value) {
        let proto = value.to_proto();
        let back = Value::from_proto(proto).unwrap();
        assert_eq!(value, back);
    }

    #[test]
    fn scalar_round_trips() {
        round_trip(Value::Null);
        round_trip(Value::Bool(true));
        round_trip(Value::Integer(-42));
        round_trip(Value::Double(3.5));
        round_trip(Value::String("hello".to_string()));
        round_trip(Value::Bytes(vec![0, 1, 255]));
        round_trip(Value::Timestamp(Timestamp::new(1_700_000_000, 123_000_000)));
        round_trip(Value::GeoPoint(GeoPoint {
            latitude: 35.8617,
            longitude: 139.6455,
        }));
        round_trip(Value::Reference(
            "projects/p/databases/d/documents/notes/n1".to_string(),
        ));
    }

    #[test]
    fn nested_round_trip() {
        let mut map = Fields::new();
        map.insert("title".to_string(), Value::String("note".to_string()));
        map.insert(
            "items".to_string(),
            Value::Array(vec![Value::Integer(1), Value::Null]),
        );
        let mut inner = Fields::new();
        inner.insert("done".to_string(), Value::Bool(false));
        map.insert("meta".to_string(), Value::Map(inner));
        round_trip(Value::Map(map));
    }

    #[test]
    fn document_round_trip_through_proto() {
        let mut fields = Fields::new();
        fields.insert("count".to_string(), Value::Integer(7));
        let doc = Document {
            name: "projects/p/databases/d/documents/notes/n1".to_string(),
            fields,
            create_time: Some(Timestamp::new(10, 20)),
            update_time: None,
        };
        let back = Document::from_proto(doc.to_proto()).unwrap();
        assert_eq!(doc, back);
        assert_eq!(doc.id(), "n1");
        assert_eq!(doc.path(), "notes/n1");
    }

    #[test]
    fn timestamp_system_time_round_trip() {
        let time = UNIX_EPOCH + Duration::new(1_700_000_000, 42_000_000);
        let ts = Timestamp::from_system_time(time);
        assert_eq!(ts.seconds, 1_700_000_000);
        assert_eq!(ts.nanos, 42_000_000);
        assert_eq!(ts.to_system_time(), time);
    }
}
