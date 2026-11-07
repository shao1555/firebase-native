//! Serializer golden tests.
//!
//! The Firestore spec tests do not cover the serializer (in the
//! official repos those are C++ unit tests), so compatibility of the
//! `Value`/`Document` <-> protobuf conversion is pinned here instead:
//! every vector below is a hand-derived canonical protobuf encoding
//! (field numbers and wire types from `document.proto`, encoded
//! independently of this crate), and each case asserts both
//! directions — our encoder must reproduce the bytes exactly, and
//! our decoder must read them back to the same public value.
//!
//! Note: multi-entry maps are intentionally absent. The protobuf
//! map encoding of several entries has no defined order, so only
//! single-entry maps can have a byte-level golden vector; larger
//! structures are covered by round-trip tests in `value.rs`.

use prost::Message;

use crate::google::firestore::v1::{Document as ProtoDocument, Value as ProtoValue};
use crate::value::{Document, GeoPoint, Timestamp, Value};
use crate::Fields;

fn unhex(text: &str) -> Vec<u8> {
    assert!(text.len() % 2 == 0);
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn assert_value_golden(value: &Value, golden_hex: &str) {
    let encoded = value.to_proto().encode_to_vec();
    assert_eq!(hex(&encoded), golden_hex, "encode mismatch for {value:?}");
    let decoded = ProtoValue::decode(unhex(golden_hex).as_slice()).unwrap();
    let round_tripped = Value::from_proto(decoded).unwrap();
    assert_eq!(&round_tripped, value, "decode mismatch for {value:?}");
}

#[test]
fn scalar_values() {
    assert_value_golden(&Value::Null, "5800");
    assert_value_golden(&Value::Bool(true), "0801");
    assert_value_golden(&Value::Bool(false), "0800");
    assert_value_golden(&Value::Integer(300), "10ac02");
    assert_value_golden(&Value::Integer(-1), "10ffffffffffffffffff01");
    assert_value_golden(&Value::Double(1.5), "19000000000000f83f");
    assert_value_golden(&Value::String("hi".to_string()), "8a01026869");
    assert_value_golden(&Value::Bytes(vec![1, 2]), "9201020102");
}

#[test]
fn nan_double() {
    // NaN never equals itself, so this case cannot use the shared
    // helper; the encoding is the canonical quiet-NaN bit pattern.
    let value = Value::Double(f64::NAN);
    assert_eq!(hex(&value.to_proto().encode_to_vec()), "19000000000000f87f");
    let decoded = ProtoValue::decode(unhex("19000000000000f87f").as_slice()).unwrap();
    match Value::from_proto(decoded).unwrap() {
        Value::Double(d) => assert!(d.is_nan()),
        other => panic!("expected Double(NaN), got {other:?}"),
    }
}

#[test]
fn timestamp_value() {
    assert_value_golden(
        &Value::Timestamp(Timestamp::new(1_700_000_000, 123_456_789)),
        "520b0880e2cfaa0610959aef3a",
    );
}

#[test]
fn reference_and_geopoint_values() {
    assert_value_golden(
        &Value::Reference("projects/p/databases/d/documents/a/b".to_string()),
        "2a2470726f6a656374732f702f6461746162617365732f642f646f63756d656e74732f612f62",
    );
    assert_value_golden(
        &Value::GeoPoint(GeoPoint {
            latitude: 1.0,
            longitude: 2.0,
        }),
        "421209000000000000f03f110000000000000040",
    );
}

#[test]
fn composite_values() {
    assert_value_golden(
        &Value::Array(vec![Value::Integer(1), Value::String("a".to_string())]),
        "4a0a0a0210010a048a010161",
    );
    let mut fields = Fields::new();
    fields.insert("a".to_string(), Value::Integer(1));
    assert_value_golden(&Value::Map(fields), "32090a070a016112021001");
}

#[test]
fn document_golden() {
    let mut fields = Fields::new();
    fields.insert("n".to_string(), Value::Integer(7));
    let document = Document {
        name: "projects/p/databases/d/documents/c/x".to_string(),
        fields,
        create_time: Some(Timestamp::new(2, 500_000_000)),
        update_time: Some(Timestamp::new(1, 0)),
    };
    let golden = "0a2470726f6a656374732f702f6461746162617365732f642f646f63756d656e74732f632f7812070a016e120210071a0808021080cab5ee0122020801";
    let encoded = document.to_proto().encode_to_vec();
    assert_eq!(hex(&encoded), golden, "document encode mismatch");
    let decoded = ProtoDocument::decode(unhex(golden).as_slice()).unwrap();
    let round_tripped = Document::from_proto(decoded).unwrap();
    assert_eq!(round_tripped, document, "document decode mismatch");
}
