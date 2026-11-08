//! The public value model and its conversion to/from the generated
//! protobuf types. The generated types never appear in the public API.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use firebase_native_core::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::google::firestore::v1::{value::ValueType, MapValue};
use crate::Fields;

type ProtoValue = crate::google::firestore::v1::Value;

/// A Firestore timestamp: seconds and nanoseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
/// Lossless serde encoding for `f64` fields: JSON (the mutation
/// queue's encoding) has no representation for NaN or the
/// infinities — serde_json silently emits `null` for them and the
/// value cannot be read back. Finite values encode as plain JSON
/// numbers; non-finite values encode as the strings "NaN",
/// "Infinity" and "-Infinity". Firestore fully supports all three
/// as double values, so losing them would corrupt queued writes.
pub(crate) mod lossless_f64 {
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serializes an `f64` losslessly.
    pub fn serialize<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
        if value.is_nan() {
            serializer.serialize_str("NaN")
        } else if *value == f64::INFINITY {
            serializer.serialize_str("Infinity")
        } else if *value == f64::NEG_INFINITY {
            serializer.serialize_str("-Infinity")
        } else {
            serializer.serialize_f64(*value)
        }
    }

    /// Deserializes an `f64` written by the matching serializer.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Number(f64),
            Text(String),
        }
        match Repr::deserialize(deserializer)? {
            Repr::Number(value) => Ok(value),
            Repr::Text(text) => match text.as_str() {
                "NaN" => Ok(f64::NAN),
                "Infinity" => Ok(f64::INFINITY),
                "-Infinity" => Ok(f64::NEG_INFINITY),
                other => Err(serde::de::Error::custom(format!(
                    "invalid non-finite double encoding: {other}"
                ))),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GeoPoint {
    /// Latitude in degrees.
    #[serde(with = "lossless_f64")]
    pub latitude: f64,
    /// Longitude in degrees.
    #[serde(with = "lossless_f64")]
    pub longitude: f64,
}

/// A Firestore value. Mirrors the `Value` oneof of the Firestore API
/// without exposing the generated protobuf type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// `NULL`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A 64-bit integer.
    Integer(i64),
    /// A 64-bit floating point number.
    Double(#[serde(with = "lossless_f64")] f64),
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
    /// A 128-bit decimal number, held as its canonical string
    /// form so no precision is lost. This mirrors the reference
    /// core's decimal number kind: it arises from increment
    /// transform operands and server transform results, where
    /// exact decimal precision matters beyond what `Double`
    /// holds. It has no distinct wire type — on the wire it is
    /// sent as its `Double` approximation, and values read from
    /// the server are never decimals. Local arithmetic on a
    /// decimal goes through `Double`, losing precision exactly
    /// as the reference core's local increment does; the exact
    /// value is restored from the server's transform result
    /// when the write is acknowledged.
    Decimal128(String),
}

impl Value {
    /// The `f64` approximation of a numeric value, if it is one.
    pub(crate) fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Integer(v) => Some(*v as f64),
            Value::Double(v) => Some(*v),
            Value::Decimal128(v) => v.parse().ok(),
            _ => None,
        }
    }

    /// Whether this value is one of the numeric kinds.
    pub(crate) fn is_number(&self) -> bool {
        matches!(
            self,
            Value::Integer(_) | Value::Double(_) | Value::Decimal128(_)
        )
    }
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
            Value::Decimal128(v) => ValueType::DoubleValue(v.parse().unwrap_or(f64::NAN)),
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

impl Value {
    /// Compares two values using Firestore's total ordering, the one
    /// the server applies for `orderBy` and range filters:
    ///
    /// `Null < Bool < Number < Timestamp < String < Bytes <
    /// Reference < GeoPoint < Array < Map`, with numbers compared
    /// numerically across integers and doubles (`NaN` before all
    /// other numbers). Used to sort watch snapshots locally exactly
    /// as the server would.
    pub fn compare(&self, other: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        let rank = |value: &Value| match value {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Integer(_) | Value::Double(_) | Value::Decimal128(_) => 2,
            Value::Timestamp(_) => 3,
            Value::String(_) => 4,
            Value::Bytes(_) => 5,
            Value::Reference(_) => 6,
            Value::GeoPoint(_) => 7,
            Value::Array(_) => 8,
            Value::Map(_) => 9,
        };
        match rank(self).cmp(&rank(other)) {
            Equal => {}
            ordering => return ordering,
        }
        // Numbers compare numerically across all three kinds,
        // exactly (see `compare_numbers`).
        if self.is_number() {
            return compare_numbers(self, other);
        }
        match (self, other) {
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => {
                a.seconds.cmp(&b.seconds).then(a.nanos.cmp(&b.nanos))
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (Value::Reference(a), Value::Reference(b)) => compare_references(a, b),
            (Value::GeoPoint(a), Value::GeoPoint(b)) => a
                .latitude
                .total_cmp(&b.latitude)
                .then(a.longitude.total_cmp(&b.longitude)),
            (Value::Array(a), Value::Array(b)) => {
                for (left, right) in a.iter().zip(b.iter()) {
                    match left.compare(right) {
                        Equal => continue,
                        ordering => return ordering,
                    }
                }
                a.len().cmp(&b.len())
            }
            (Value::Map(a), Value::Map(b)) => {
                // Maps compare as their (key, value) pair sequence in
                // key order: first differing key decides, then the
                // value under the shared key, then length.
                let mut left = a.iter();
                let mut right = b.iter();
                loop {
                    match (left.next(), right.next()) {
                        (None, None) => return Equal,
                        (None, Some(_)) => return Less,
                        (Some(_), None) => return Greater,
                        (Some((lk, lv)), Some((rk, rv))) => match lk.cmp(rk) {
                            Equal => match lv.compare(rv) {
                                Equal => continue,
                                ordering => return ordering,
                            },
                            ordering => return ordering,
                        },
                    }
                }
            }
            _ => Equal,
        }
    }
}

/// Orders doubles with `NaN` before every other number (Firestore
/// semantics), otherwise by value.
fn compare_doubles(a: f64, b: f64) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Equal,
        (true, false) => Less,
        (false, true) => Greater,
        (false, false) => a.partial_cmp(&b).unwrap_or(Equal),
    }
}

/// Compares two numbers exactly, across the integer, double
/// and decimal kinds. `NaN` sorts before every other number
/// (Firestore semantics); infinities sort beyond every finite
/// value. Finite values are compared through their exact
/// decimal forms, so two decimals that round to the same `f64`
/// still order by their true value, and a decimal compares
/// against an integer or a double by the double's exact binary
/// value — never by a rounded approximation.
fn compare_numbers(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    #[derive(PartialEq, Clone, Copy)]
    enum Class {
        Nan,
        NegInf,
        Finite,
        PosInf,
    }
    let class = |value: &Value| match value {
        Value::Double(double) => {
            if double.is_nan() {
                Class::Nan
            } else if *double == f64::INFINITY {
                Class::PosInf
            } else if *double == f64::NEG_INFINITY {
                Class::NegInf
            } else {
                Class::Finite
            }
        }
        Value::Decimal128(text) => {
            let lower = text.trim().to_ascii_lowercase();
            if lower.contains("nan") {
                Class::Nan
            } else if lower.contains("inf") {
                if lower.starts_with('-') {
                    Class::NegInf
                } else {
                    Class::PosInf
                }
            } else {
                Class::Finite
            }
        }
        _ => Class::Finite,
    };
    match (class(a), class(b)) {
        (Class::Nan, Class::Nan) => return Equal,
        (Class::Nan, _) => return Less,
        (_, Class::Nan) => return Greater,
        (Class::PosInf, Class::PosInf) | (Class::NegInf, Class::NegInf) => return Equal,
        (Class::PosInf, _) | (_, Class::NegInf) => return Greater,
        (Class::NegInf, _) | (_, Class::PosInf) => return Less,
        (Class::Finite, Class::Finite) => {}
    }
    match (ExactDecimal::of(a), ExactDecimal::of(b)) {
        (Some(left), Some(right)) => left.compare(&right),
        // A decimal string the exact parser cannot read: keep
        // the historical `f64` ordering rather than failing.
        _ => compare_doubles(
            a.as_f64().unwrap_or(f64::NAN),
            b.as_f64().unwrap_or(f64::NAN),
        ),
    }
}

/// An exact decimal number: `(-1 if negative) * digits *
/// 10^exponent`, where `digits` is a decimal integer string
/// with no leading zeros (`"0"` for zero). Two values in this
/// form compare by adjusted exponent, then digit by digit.
struct ExactDecimal {
    negative: bool,
    digits: String,
    exponent: i64,
}

impl ExactDecimal {
    fn of(value: &Value) -> Option<Self> {
        match value {
            Value::Integer(int) => Some(Self {
                negative: *int < 0,
                digits: int.unsigned_abs().to_string(),
                exponent: 0,
            }),
            Value::Double(double) => Self::from_f64(*double),
            Value::Decimal128(text) => Self::parse(text),
            _ => None,
        }
    }

    /// Parses a decimal string (`[sign] digits [. digits]
    /// [(e|E) [sign] digits]`).
    fn parse(text: &str) -> Option<Self> {
        let text = text.trim();
        let (negative, rest) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text.strip_prefix('+').unwrap_or(text)),
        };
        let (mantissa, exp) = match rest.find(['e', 'E']) {
            Some(index) => (&rest[..index], rest[index + 1..].parse::<i64>().ok()?),
            None => (rest, 0),
        };
        let (int_part, frac_part) = match mantissa.split_once('.') {
            Some((int, frac)) => (int, frac),
            None => (mantissa, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        if !int_part.bytes().all(|byte| byte.is_ascii_digit())
            || !frac_part.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
        let digits = format!("{int_part}{frac_part}");
        let digits = digits.trim_start_matches('0');
        if digits.is_empty() {
            return Some(Self {
                negative: false,
                digits: "0".to_string(),
                exponent: 0,
            });
        }
        Some(Self {
            negative,
            digits: digits.to_string(),
            exponent: exp - frac_part.len() as i64,
        })
    }

    /// The exact decimal form of a finite `f64`: every binary
    /// float is `mantissa * 2^e2`, which has a finite decimal
    /// expansion (`mantissa * 5^(-e2)` shifted by `10^e2` when
    /// `e2` is negative).
    fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        if value == 0.0 {
            return Some(Self {
                negative: false,
                digits: "0".to_string(),
                exponent: 0,
            });
        }
        let bits = value.to_bits();
        let biased = ((bits >> 52) & 0x7ff) as i64;
        let fraction = bits & ((1u64 << 52) - 1);
        let (mantissa, e2) = if biased == 0 {
            (fraction, -1074) // Subnormal: no implicit leading bit.
        } else {
            (fraction | (1u64 << 52), biased - 1075)
        };
        // Decimal digits of the mantissa, least significant first.
        let mut digits: Vec<u8> = Vec::new();
        let mut rest = mantissa;
        while rest > 0 {
            digits.push((rest % 10) as u8);
            rest /= 10;
        }
        let multiply = |digits: &mut Vec<u8>, factor: u8| {
            let mut carry = 0u8;
            for digit in digits.iter_mut() {
                let product = *digit * factor + carry;
                *digit = product % 10;
                carry = product / 10;
            }
            while carry > 0 {
                digits.push(carry % 10);
                carry /= 10;
            }
        };
        let mut exponent = 0i64;
        if e2 >= 0 {
            for _ in 0..e2 {
                multiply(&mut digits, 2);
            }
        } else {
            for _ in 0..-e2 {
                multiply(&mut digits, 5);
            }
            exponent = e2;
        }
        let digits: String = digits
            .iter()
            .rev()
            .map(|digit| (b'0' + digit) as char)
            .collect();
        Some(Self {
            negative: bits >> 63 != 0,
            digits,
            exponent,
        })
    }

    fn compare(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        match (self.digits == "0", other.digits == "0") {
            (true, true) => return Equal,
            (true, false) => return if other.negative { Greater } else { Less },
            (false, true) => return if self.negative { Less } else { Greater },
            (false, false) => {}
        }
        if self.negative != other.negative {
            return if self.negative { Less } else { Greater };
        }
        // Magnitude: the adjusted exponent (position of the
        // leading digit) decides first, then the digit strings
        // with the shorter one reading as zero-padded.
        let magnitude = (self.exponent + self.digits.len() as i64)
            .cmp(&(other.exponent + other.digits.len() as i64))
            .then_with(|| {
                let (left, right) = (self.digits.as_bytes(), other.digits.as_bytes());
                for index in 0..left.len().max(right.len()) {
                    let x = left.get(index).copied().unwrap_or(b'0');
                    let y = right.get(index).copied().unwrap_or(b'0');
                    match x.cmp(&y) {
                        Equal => continue,
                        ordering => return ordering,
                    }
                }
                Equal
            });
        if self.negative {
            magnitude.reverse()
        } else {
            magnitude
        }
    }
}

/// References order by path segment, not by raw string: segments
/// compare individually and a strict prefix sorts first.
fn compare_references(a: &str, b: &str) -> std::cmp::Ordering {
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

    #[test]
    fn ordering_across_types() {
        use std::cmp::Ordering::*;
        let ordered: Vec<Value> = vec![
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            Value::Double(f64::NAN),
            Value::Integer(-1),
            Value::Double(-0.5),
            Value::Integer(0),
            Value::Double(0.5),
            Value::Integer(2),
            Value::Timestamp(Timestamp::new(0, 0)),
            Value::String("a".to_string()),
            Value::Bytes(vec![0]),
            Value::Reference("projects/p/databases/d/documents/a/b".to_string()),
            Value::GeoPoint(GeoPoint {
                latitude: 0.0,
                longitude: 0.0,
            }),
            Value::Array(vec![Value::Integer(1)]),
            Value::Map(Fields::new()),
        ];
        for pair in ordered.windows(2) {
            assert_eq!(pair[0].compare(&pair[1]), Less, "{pair:?}");
            assert_eq!(pair[1].compare(&pair[0]), Greater, "{pair:?}");
        }
    }

    #[test]
    fn ordering_numbers_mixed() {
        use std::cmp::Ordering::*;
        // Integral doubles equal the integer.
        assert_eq!(Value::Integer(2).compare(&Value::Double(2.0)), Equal);
        assert_eq!(Value::Integer(2).compare(&Value::Double(2.5)), Less);
        assert_eq!(Value::Integer(-2).compare(&Value::Double(-2.5)), Greater);
        assert_eq!(Value::Double(-2.5).compare(&Value::Integer(-2)), Less);
        // Beyond-i64 doubles.
        assert_eq!(
            Value::Integer(i64::MAX).compare(&Value::Double(9.223372036854776e18)),
            Less
        );
        // NaN before -inf, -0.0 equals 0.
        assert_eq!(
            Value::Double(f64::NAN).compare(&Value::Double(f64::NEG_INFINITY)),
            Less
        );
        assert_eq!(Value::Double(-0.0).compare(&Value::Integer(0)), Equal);
    }

    #[test]
    fn ordering_decimals_exact() {
        use std::cmp::Ordering::*;
        let decimal = |text: &str| Value::Decimal128(text.to_string());
        // Decimals that round to the same f64 still order by
        // their true value, not lexicographically.
        let close_low = decimal("9.999999999999999999999999999999999");
        let close_high = decimal("10.00000000000000000000000000000001");
        assert_eq!(close_low.compare(&close_high), Less);
        assert_eq!(close_high.compare(&close_low), Greater);
        // Mixed comparisons agree: the exact value of the
        // double 10.0 is 10, between the two decimals.
        assert_eq!(close_low.compare(&Value::Double(10.0)), Less);
        assert_eq!(close_high.compare(&Value::Double(10.0)), Greater);
        assert_eq!(Value::Double(10.0).compare(&close_low), Greater);
        // Precision an f64 cannot represent: 2^53 + 1.
        assert_eq!(
            decimal("9007199254740993").compare(&Value::Integer(9007199254740993)),
            Equal
        );
        assert_eq!(
            decimal("9007199254740993").compare(&Value::Double(9007199254740992.0)),
            Greater
        );
        // The exact value of the double 0.1 is slightly above
        // the decimal 0.1.
        assert_eq!(decimal("0.1").compare(&Value::Double(0.1)), Less);
        // Numerically equal forms are Equal, whatever the
        // spelling; exponent forms parse too.
        assert_eq!(decimal("1.0").compare(&decimal("1.00")), Equal);
        assert_eq!(decimal("1E+2").compare(&Value::Integer(100)), Equal);
        assert_eq!(decimal("5e-1").compare(&Value::Double(0.5)), Equal);
        assert_eq!(decimal("-0.0").compare(&Value::Integer(0)), Equal);
        // Negatives order by reversed magnitude.
        assert_eq!(decimal("-2.5").compare(&decimal("-2.4")), Less);
        assert_eq!(decimal("-2.5").compare(&Value::Double(-2.4)), Less);
        // Decimal NaN sorts before every other number, like
        // double NaN.
        assert_eq!(decimal("NaN").compare(&Value::Integer(i64::MIN)), Less);
        assert_eq!(decimal("NaN").compare(&Value::Double(f64::NAN)), Equal);
    }

    #[test]
    fn ordering_composites() {
        use std::cmp::Ordering::*;
        let a1 = Value::Array(vec![Value::Integer(1), Value::Integer(9)]);
        let a2 = Value::Array(vec![Value::Integer(1)]);
        assert_eq!(a1.compare(&a2), Greater);
        let mut m1 = Fields::new();
        m1.insert("a".to_string(), Value::Integer(1));
        let mut m2 = Fields::new();
        m2.insert("a".to_string(), Value::Integer(2));
        let mut m3 = Fields::new();
        m3.insert("b".to_string(), Value::Integer(0));
        assert_eq!(Value::Map(m1.clone()).compare(&Value::Map(m2)), Less);
        assert_eq!(Value::Map(m1).compare(&Value::Map(m3)), Less);
        // References compare segment-wise: "a/b" < "a/b/c" and segment
        // boundaries matter ("a2" is one segment).
        assert_eq!(
            Value::Reference("x/a/b".to_string()).compare(&Value::Reference("x/a/b/c".to_string())),
            Less
        );
        assert_eq!(
            Value::Reference("x/a2".to_string()).compare(&Value::Reference("x/a/b".to_string())),
            Greater
        );
    }
}
