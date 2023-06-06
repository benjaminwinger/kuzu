use crate::ffi::ffi;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::convert::TryFrom;
use std::fmt;

// From types.h
#[allow(non_camel_case_types)]
#[derive(FromPrimitive)]
enum LogicalTypeID {
    ANY = 0,
    NODE = 10,
    REL = 11,
    RECURSIVE_REL = 12,
    // SERIAL is a special data type that is used to represent a sequence of INT64 values that are
    // incremented by 1 starting from 0.
    SERIAL = 13,

    // fixed size types
    BOOL = 22,
    INT64 = 23,
    INT32 = 24,
    INT16 = 25,
    DOUBLE = 26,
    FLOAT = 27,
    DATE = 28,
    TIMESTAMP = 29,
    INTERVAL = 30,
    FIXED_LIST = 31,

    INTERNAL_ID = 40,

    // variable size types
    STRING = 50,
    VAR_LIST = 52,
    STRUCT = 53,
}

pub enum ConversionError {
    /// Kuzu's internal date as the number of days since 1970-01-01
    Date(i32),
    /// Kuzu's internal timestamp as the number of microseconds since 1970-01-01
    Timestamp(i64),
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::fmt::Debug for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ConversionError::*;
        match self {
            Date(days) => write!(f, "Could not convert Kuzu date offset of UNIX_EPOCH + {days} days to time::Date"),
            Timestamp(us) => write!(f, "Could not convert Kuzu timestamp offset of UNIX_EPOCH + {us} microseconds to time::OffsetDateTime"),
        }
    }
}

impl std::error::Error for ConversionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

trait CppValue {
    fn get_cpp_value(self) -> ffi::Value;
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodeValue {
    id_val: Value,
    label_val: Value,
    properties: Vec<(String, Value)>,
}

impl NodeValue {
    pub fn get_node_id_val(&self) -> &Value {
        &self.id_val
    }
    pub fn get_label_val(&self) -> &Value {
        &self.label_val
    }

    pub fn get_label_name(&self) -> &String {
        if let Value::String(value) = &self.label_val {
            value
        } else {
            // Is this unreachable?
            unreachable!()
        }
    }

    pub fn add_property(&mut self, key: String, value: Value) {
        self.properties.push((key, value));
    }

    pub fn get_properties(&self) -> &Vec<(String, Value)> {
        &self.properties
    }
}

/*
impl std::fmt::Display for NodeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            ffi::node_value_to_string(self.value.get_ref().unwrap())
        )
    }
}*/

#[derive(Clone, Debug, PartialEq)]
pub struct RelValue {}

// FIXME: should this be entirely private? The C++ api at least defines operators
#[derive(Clone, Debug, PartialEq)]
pub struct InternalID {
    offset: u64,
    table: u64,
}

/// Data types supported by Kùzu
///
/// Also see <https://kuzudb.com/docs/cypher/data-types/overview.html>
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Bool(bool),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Double(f64),
    Float(f32),
    /// Stored internally as the number of days since 1970-01-01 as a 32-bit signed integer, which
    /// allows for a wider range of dates to be stored than can be represented by time::Date
    ///
    /// <https://kuzudb.com/docs/cypher/data-types/date.html>
    Date(time::Date),
    /// May be signed or unsigned.
    ///
    /// Nanosecond precision of time::Duration (if available) will not be preserved when passed to
    /// queries, and results will always have at most microsecond precision.
    ///
    /// <https://kuzudb.com/docs/cypher/data-types/interval.html>
    Interval(time::Duration),
    /// Stored internally as the number of microseconds since 1970-01-01
    /// Nanosecond precision of SystemTime (if available) will not be preserved when used.
    ///
    /// <https://kuzudb.com/docs/cypher/data-types/timestamp.html>
    Timestamp(time::OffsetDateTime),
    InternalID(InternalID),
    /// <https://kuzudb.com/docs/cypher/data-types/string.html>
    String(String),
    /// <https://kuzudb.com/docs/cypher/data-types/list.html>
    List(Box<Vec<Value>>),
    Node(Box<NodeValue>),
    Rel(Box<RelValue>),
}

/*
 * Independent implementation with conversion generally seems easier, and allows the use of a more rust-like
 * interface with proper enums.
 */
// TODO: Test that builtin Display matches c++ value to-string.
impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(true) => write!(f, "True"),
            Value::Bool(false) => write!(f, "False"),
            x => write!(f, "{x}"),
        }
    }
}

/*
impl Value for Value {
    fn get_cpp_value(self) -> ffi::Value {
        use Self::*;
        match self {
            Bool(x) => ffi::bool_value(x),
            Int64(x),
            Int32(x),
            Int16(x),
            Double(x),
            Float(x),
            Date,
            Timestamp,
            InternalID,
            String(x),
            Nested(x) | Node(x) | Rel(x) => x.get_cpp_value(),
        }
    }
}
*/

impl TryFrom<&ffi::Value> for Value {
    type Error = ConversionError;

    fn try_from(value: &ffi::Value) -> Result<Self, Self::Error> {
        use LogicalTypeID::*;
        match FromPrimitive::from_u8(ffi::value_get_data_type_id(value)) {
            Some(BOOL) => Ok(Value::Bool(ffi::value_get_bool(value))),
            Some(INT16) => Ok(Value::Int16(ffi::value_get_int16(value))),
            Some(INT32) => Ok(Value::Int32(ffi::value_get_int32(value))),
            Some(INT64) => Ok(Value::Int64(ffi::value_get_int64(value))),
            Some(FLOAT) => Ok(Value::Float(ffi::value_get_float(value))),
            Some(DOUBLE) => Ok(Value::Double(ffi::value_get_double(value))),
            Some(STRING) => Ok(Value::String(ffi::value_get_string(value))),
            Some(INTERVAL) => Ok(Value::Interval(time::Duration::new(
                ffi::value_get_interval_secs(value),
                // Duration is constructed using nanoseconds, but kuzu stores microseconds
                ffi::value_get_interval_micros(value) * 1000,
            ))),
            Some(DATE) => {
                let days = ffi::value_get_date_days(value);
                time::Date::from_calendar_date(1970, time::Month::January, 01)
                    .unwrap()
                    .checked_add(time::Duration::days(days as i64))
                    .map(Value::Date)
                    .ok_or(ConversionError::Date(days))
            }
            Some(Timestamp) => {
                let us = ffi::value_get_timestamp_micros(value);
                time::OffsetDateTime::UNIX_EPOCH
                    .clone()
                    .checked_add(time::Duration::microseconds(us))
                    .map(Value::Timestamp)
                    .ok_or(ConversionError::Timestamp(us))
            }
            // Maybe an "internal" error instead for better readability when new types are
            // added on the C++ side.
            None => unreachable!(),
            _ => unimplemented!(),
        }
    }
}

// TODO: Test that intervals, dates, etc. have the expected values
