use crate::ffi::ffi;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;

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
    pub(crate) offset: u64,
    pub(crate) table: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LogicalType {
    Any,
    Bool,
    Int64,
    Int32,
    Int16,
    Double,
    Float,
    Date,
    Interval,
    Timestamp,
    InternalID,
    String,
    VarList {
        child_type: Box<LogicalType>,
    },
    FixedList {
        child_type: Box<LogicalType>,
        num_elements: u64,
    },
    Struct {
        fields: Vec<(String, LogicalType)>,
    },
    Node,
    Rel,
}

impl From<&ffi::Value> for LogicalType {
    fn from(value: &ffi::Value) -> Self {
        use ffi::LogicalTypeID;
        match ffi::value_get_data_type_id(value) {
            LogicalTypeID::ANY => LogicalType::Any,
            LogicalTypeID::BOOL => LogicalType::Bool,
            LogicalTypeID::INT16 => LogicalType::Int16,
            LogicalTypeID::INT32 => LogicalType::Int32,
            LogicalTypeID::INT64 => LogicalType::Int64,
            LogicalTypeID::FLOAT => LogicalType::Float,
            LogicalTypeID::DOUBLE => LogicalType::Double,
            LogicalTypeID::STRING => LogicalType::String,
            LogicalTypeID::INTERVAL => LogicalType::Interval,
            LogicalTypeID::DATE => LogicalType::Date,
            LogicalTypeID::TIMESTAMP => LogicalType::Timestamp,
            LogicalTypeID::INTERNAL_ID => LogicalType::InternalID,
            LogicalTypeID::VAR_LIST
            | LogicalTypeID::FIXED_LIST
            | LogicalTypeID::STRUCT
            | LogicalTypeID::NODE
            | LogicalTypeID::REL => unimplemented!(),
            // Should be unreachable, as cxx will check that the LogicalTypeID enum matches the one
            // on the C++ side.
            x => panic!("Unsupported type {:?}", x),
        }
    }
}

impl LogicalType {
    pub(crate) fn id(&self) -> ffi::LogicalTypeID {
        use ffi::LogicalTypeID;
        match self {
            LogicalType::Any => LogicalTypeID::ANY,
            LogicalType::Bool => LogicalTypeID::BOOL,
            LogicalType::Int16 => LogicalTypeID::INT16,
            LogicalType::Int32 => LogicalTypeID::INT32,
            LogicalType::Int64 => LogicalTypeID::INT64,
            LogicalType::Float => LogicalTypeID::FLOAT,
            LogicalType::Double => LogicalTypeID::DOUBLE,
            LogicalType::String => LogicalTypeID::STRING,
            LogicalType::Interval => LogicalTypeID::INTERVAL,
            LogicalType::Date => LogicalTypeID::DATE,
            LogicalType::Timestamp => LogicalTypeID::TIMESTAMP,
            LogicalType::InternalID => LogicalTypeID::INTERNAL_ID,
            LogicalType::VarList { .. }
            | LogicalType::FixedList { .. }
            | LogicalType::Struct { .. }
            | LogicalType::Node
            | LogicalType::Rel => unimplemented!(),
        }
    }
}

/// Data types supported by Kùzu
///
/// Also see <https://kuzudb.com/docs/cypher/data-types/overview.html>
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null(LogicalType),
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
    // TODO: Enforce type of contents
    /// These must contain elements which are all the same type.
    /// <https://kuzudb.com/docs/cypher/data-types/list.html>
    VarList(Vec<Value>),
    /// These must contain elements which are all the same type.
    /// <https://kuzudb.com/docs/cypher/data-types/list.html>
    FixedList(Vec<Value>),
    /// <https://kuzudb.com/docs/cypher/data-types/struct.html>
    Struct(HashMap<String, Value>),
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

impl TryFrom<&ffi::Value> for Value {
    type Error = ConversionError;

    fn try_from(value: &ffi::Value) -> Result<Self, Self::Error> {
        use ffi::LogicalTypeID;
        if value.isNull() {
            return Ok(Value::Null(value.into()));
        }
        match ffi::value_get_data_type_id(value) {
            LogicalTypeID::ANY => unimplemented!(),
            LogicalTypeID::BOOL => Ok(Value::Bool(value.get_value_bool())),
            LogicalTypeID::INT16 => Ok(Value::Int16(value.get_value_i16())),
            LogicalTypeID::INT32 => Ok(Value::Int32(value.get_value_i32())),
            LogicalTypeID::INT64 => Ok(Value::Int64(value.get_value_i64())),
            LogicalTypeID::FLOAT => Ok(Value::Float(value.get_value_float())),
            LogicalTypeID::DOUBLE => Ok(Value::Double(value.get_value_double())),
            LogicalTypeID::STRING => Ok(Value::String(ffi::value_get_string(value))),
            LogicalTypeID::INTERVAL => Ok(Value::Interval(time::Duration::new(
                ffi::value_get_interval_secs(value),
                // Duration is constructed using nanoseconds, but kuzu stores microseconds
                ffi::value_get_interval_micros(value) * 1000,
            ))),
            LogicalTypeID::DATE => {
                let days = ffi::value_get_date_days(value);
                time::Date::from_calendar_date(1970, time::Month::January, 1)
                    .unwrap()
                    .checked_add(time::Duration::days(days as i64))
                    .map(Value::Date)
                    .ok_or(ConversionError::Date(days))
            }
            LogicalTypeID::TIMESTAMP => {
                let us = ffi::value_get_timestamp_micros(value);
                time::OffsetDateTime::UNIX_EPOCH
                    .checked_add(time::Duration::microseconds(us))
                    .map(Value::Timestamp)
                    .ok_or(ConversionError::Timestamp(us))
            }
            LogicalTypeID::VAR_LIST => {
                let list = ffi::value_get_list(value);
                let mut result = vec![];
                for index in 0..list.size() {
                    let value: Value = list.get(index).as_ref().unwrap().try_into()?;
                    result.push(value);
                }
                Ok(Value::VarList(result))
            }
            LogicalTypeID::FIXED_LIST => {
                let list = ffi::value_get_list(value);
                let mut result = vec![];
                for index in 0..list.size() {
                    let value: Value = list.get(index).as_ref().unwrap().try_into()?;
                    result.push(value);
                }
                Ok(Value::FixedList(result))
            }
            LogicalTypeID::STRUCT => {
                // Data is a list of field values in the value itself (same as list),
                // with the field names stored in the DataType
                let field_names = ffi::value_get_struct_names(value);
                let list = ffi::value_get_list(value);
                let mut result = HashMap::new();
                for index in 0..list.size() {
                    let value: Value = list.get(index).as_ref().unwrap().try_into()?;
                    result.insert(
                        field_names
                            .as_ref()
                            .unwrap()
                            .get(index as usize)
                            // Presumably an internal error if this panics?
                            .unwrap()
                            .to_string(),
                        value,
                    );
                }
                Ok(Value::Struct(result))
            }
            LogicalTypeID::NODE => unimplemented!(),
            LogicalTypeID::REL => unimplemented!(),
            LogicalTypeID::INTERNAL_ID => {
                let internal_id = ffi::value_get_internal_id(value);
                let (offset, table) = (internal_id[0], internal_id[1]);
                Ok(Value::InternalID(InternalID { offset, table }))
            }
            // Should be unreachable, as cxx will check that the LogicalTypeID enum matches the one
            // on the C++ side.
            x => panic!("Unsupported type {:?}", x),
        }
    }
}

impl From<i16> for Value {
    fn from(item: i16) -> Self {
        Value::Int16(item)
    }
}

impl From<i32> for Value {
    fn from(item: i32) -> Self {
        Value::Int32(item)
    }
}

impl From<i64> for Value {
    fn from(item: i64) -> Self {
        Value::Int64(item)
    }
}

impl From<f32> for Value {
    fn from(item: f32) -> Self {
        Value::Float(item)
    }
}

impl From<f64> for Value {
    fn from(item: f64) -> Self {
        Value::Double(item)
    }
}

impl From<String> for Value {
    fn from(item: String) -> Self {
        Value::String(item)
    }
}

impl From<&str> for Value {
    fn from(item: &str) -> Self {
        Value::String(item.to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        connection::Connection,
        database::Database,
        value::{LogicalType, Value},
    };
    use anyhow::Result;
    use std::collections::HashSet;
    use std::iter::FromIterator;
    use time::macros::{date, datetime};

    // Note: Cargo runs tests in parallel by default, however kuzu does not support
    // working with multiple databases in parallel.
    // Tests can be run serially with `cargo test -- --test-threads=1` to work around this.

    #[test]
    /// Tests that the list value is correctly constructed
    fn test_var_list_get() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let mut db = Database::new(temp_dir.path(), 0)?;
        let mut conn = Connection::new(&mut db)?;
        for result in conn.query("RETURN [\"Alice\", \"Bob\"] AS l;")? {
            assert_eq!(result.len(), 1);
            assert_eq!(
                result[0],
                Value::VarList(vec!["Alice".into(), "Bob".into(),])
            );
        }
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    /// Test that the timestamp round-trips through kuzu's internal timestamp
    fn test_timestamp() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let mut db = Database::new(temp_dir.path(), 0)?;
        let mut conn = Connection::new(&mut db)?;
        conn.query(
            "CREATE NODE TABLE Person(name STRING, registerTime TIMESTAMP, PRIMARY KEY(name));",
        )?;
        conn.query(
            "CREATE (:Person {name: \"Alice\", registerTime: timestamp(\"2011-08-20 11:25:30\")});",
        )?;
        let mut add_person =
            conn.prepare("CREATE (:Person {name: $name, registerTime: $time});")?;
        let timestamp = datetime!(2011-08-20 11:25:30 UTC);
        conn.execute(
            &mut add_person,
            &[
                ("name", "Bob".into()),
                ("time", Value::Timestamp(timestamp)),
            ],
        )?;
        let result: HashSet<String> = conn
            .query(
                "MATCH (a:Person) WHERE a.registerTime = timestamp(\"2011-08-20 11:25:30\") RETURN a.name;",
            )?
            .map(|x| match &x[0] {
                Value::String(x) => x.clone(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(
            result,
            HashSet::from_iter(vec!["Alice".to_string(), "Bob".to_string()])
        );
        let mut result =
            conn.query("MATCH (a:Person) WHERE a.name = \"Bob\" RETURN a.registerTime;")?;
        let result: time::OffsetDateTime =
            if let Value::Timestamp(timestamp) = result.next().unwrap()[0] {
                timestamp
            } else {
                panic!("Wrong type returned!")
            };
        assert_eq!(result, timestamp);
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    /// Test that the date round-trips through kuzu's internal date
    fn test_date() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let mut db = Database::new(temp_dir.path(), 0)?;
        let mut conn = Connection::new(&mut db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, registerDate DATE, PRIMARY KEY(name));")?;
        let mut add_person =
            conn.prepare("CREATE (:Person {name: $name, registerDate: $date});")?;
        let date = date!(2011 - 08 - 20);
        conn.execute(
            &mut add_person,
            &[("name", "Bob".into()), ("date", Value::Date(date))],
        )?;
        let mut result =
            conn.query("MATCH (a:Person) WHERE a.name = \"Bob\" RETURN a.registerDate;")?;
        let result: time::Date = if let Value::Date(date) = result.next().unwrap()[0] {
            date
        } else {
            panic!("Expected a Date Value")
        };
        assert_eq!(result, date);
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    /// Test that the date round-trips through kuzu's internal date
    fn test_interval() -> Result<()> {
        use time::Duration;
        let temp_dir = tempdir::TempDir::new("example")?;
        let mut db = Database::new(temp_dir.path(), 0)?;
        let mut conn = Connection::new(&mut db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, interval INTERVAL, PRIMARY KEY(name));")?;
        let mut add_person =
            conn.prepare("CREATE (:Person {name: $name, interval: $interval});")?;
        let interval = Duration::weeks(3);
        conn.execute(
            &mut add_person,
            &[
                ("name", "Bob".into()),
                ("interval", Value::Interval(interval)),
            ],
        )?;
        let mut result =
            conn.query("MATCH (a:Person) WHERE a.name = \"Bob\" RETURN a.interval;")?;
        let result: Duration = if let Value::Interval(interval) = result.next().unwrap()[0] {
            interval
        } else {
            panic!("Expected an Interval Value")
        };
        assert_eq!(result, interval);
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    /// Test that the date round-trips through kuzu's internal date
    fn test_null() -> Result<()> {
        use time::Duration;
        let temp_dir = tempdir::TempDir::new("example")?;
        let mut db = Database::new(temp_dir.path(), 0)?;
        let mut conn = Connection::new(&mut db)?;
        let result = conn.query("RETURN null")?.next();
        let result = &result.unwrap()[0];
        assert_eq!(result, &Value::Null(LogicalType::String));
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    /// Test that the date round-trips through kuzu's internal date
    fn test_null_data() -> Result<()> {
        use time::Duration;
        let temp_dir = tempdir::TempDir::new("example")?;
        let mut db = Database::new(temp_dir.path(), 0)?;
        let mut conn = Connection::new(&mut db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
        let mut add_person = conn.prepare("CREATE (:Person {name: $name, age: $age});")?;

        conn.execute(
            &mut add_person,
            &[
                ("name", "Bob".into()),
                ("age", Value::Null(LogicalType::Int64)),
            ],
        )?;

        let result = conn
            .query("MATCH (a:Person) WHERE a.name = \"Bob\" RETURN a.age")?
            .next();
        let result = &result.unwrap()[0];
        assert_eq!(result, &Value::Null(LogicalType::Int64));
        temp_dir.close()?;
        Ok(())
    }
}
