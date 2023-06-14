use crate::ffi::ffi;
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
        ffi::value_get_data_type(value).as_ref().unwrap().into()
    }
}

impl From<&ffi::LogicalType> for LogicalType {
    fn from(logical_type: &ffi::LogicalType) -> Self {
        use ffi::LogicalTypeID;
        match logical_type.getLogicalTypeID() {
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
            LogicalTypeID::VAR_LIST => LogicalType::VarList {
                child_type: Box::new(
                    ffi::logical_type_get_var_list_child_type(logical_type).into(),
                ),
            },
            LogicalTypeID::FIXED_LIST => LogicalType::FixedList {
                child_type: Box::new(
                    ffi::logical_type_get_fixed_list_child_type(logical_type).into(),
                ),
                num_elements: ffi::logical_type_get_fixed_list_num_elements(logical_type),
            },
            LogicalTypeID::STRUCT => {
                let names = ffi::logical_type_get_struct_field_names(logical_type);
                let types = ffi::logical_type_get_struct_field_types(logical_type);
                LogicalType::Struct {
                    fields: names
                        .into_iter()
                        .zip(types.into_iter().map(Into::<LogicalType>::into))
                        .collect(),
                }
            }
            LogicalTypeID::NODE | LogicalTypeID::REL => unimplemented!(),
            // Should be unreachable, as cxx will check that the LogicalTypeID enum matches the one
            // on the C++ side.
            x => panic!("Unsupported type {:?}", x),
        }
    }
}

impl From<&LogicalType> for cxx::UniquePtr<ffi::LogicalType> {
    fn from(typ: &LogicalType) -> Self {
        match typ {
            LogicalType::Any
            | LogicalType::Bool
            | LogicalType::Int64
            | LogicalType::Int32
            | LogicalType::Int16
            | LogicalType::Float
            | LogicalType::Double
            | LogicalType::Date
            | LogicalType::Timestamp
            | LogicalType::Interval
            | LogicalType::InternalID
            | LogicalType::String => ffi::create_logical_type(typ.id()),
            LogicalType::VarList { child_type } => {
                ffi::create_logical_type_var_list(child_type.as_ref().into())
            }
            LogicalType::FixedList {
                child_type,
                num_elements,
            } => ffi::create_logical_type_fixed_list(child_type.as_ref().into(), *num_elements),
            LogicalType::Struct { fields } => {
                let mut builder = ffi::create_type_list();
                let mut names = vec![];
                for (name, typ) in fields {
                    names.push(name.clone());
                    builder.pin_mut().insert(typ.into());
                }
                ffi::create_logical_type_struct(&names, builder)
            }
            LogicalType::Node | LogicalType::Rel => unimplemented!(),
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
            LogicalType::VarList { .. } => LogicalTypeID::VAR_LIST,
            LogicalType::FixedList { .. } => LogicalTypeID::FIXED_LIST,
            LogicalType::Struct { .. } => LogicalTypeID::STRUCT,
            LogicalType::Node => LogicalTypeID::NODE,
            LogicalType::Rel => LogicalTypeID::REL,
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
    // LogicalType is necessary so that we can pass the correct type to the C++ API if the list is empty.
    /// These must contain elements which are all the given type.
    /// <https://kuzudb.com/docs/cypher/data-types/list.html>
    VarList(LogicalType, Vec<Value>),
    /// These must contain elements which are all the same type.
    /// <https://kuzudb.com/docs/cypher/data-types/list.html>
    FixedList(LogicalType, Vec<Value>),
    /// <https://kuzudb.com/docs/cypher/data-types/struct.html>
    Struct(Vec<(String, Value)>),
    Node(Box<NodeValue>),
    Rel(Box<RelValue>),
}

// TODO: Test that builtin Display matches c++ value toString.
impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(true) => write!(f, "True"),
            Value::Bool(false) => write!(f, "False"),
            Value::Int16(x) => write!(f, "{x}"),
            Value::Int32(x) => write!(f, "{x}"),
            Value::Int64(x) => write!(f, "{x}"),
            Value::Date(x) => write!(f, "{x}"),
            Value::String(x) => write!(f, "{x}"),
            Value::Null(_) => write!(f, "null"),
            Value::VarList(_, x) | Value::FixedList(_, x) => {
                write!(f, "[")?;
                for i in 0..x.len() {
                    write!(f, "{}", x[i])?;
                    if i != x.len() - 1 {
                        write!(f, ",")?;
                    }
                }
                write!(f, "]")
            }
            // Note: These don't match kuzu's toString, but we probably don't want them to
            Value::Interval(x) => write!(f, "{x}"),
            Value::Timestamp(x) => write!(f, "{x}"),
            Value::Float(x) => write!(f, "{x}"),
            Value::Double(x) => write!(f, "{x}"),
            _ => unimplemented!(),
        }
    }
}

impl From<&Value> for LogicalType {
    fn from(value: &Value) -> Self {
        match value {
            Value::Bool(_) => LogicalType::Bool,
            Value::Int16(_) => LogicalType::Int16,
            Value::Int32(_) => LogicalType::Int32,
            Value::Int64(_) => LogicalType::Int64,
            Value::Float(_) => LogicalType::Float,
            Value::Double(_) => LogicalType::Double,
            Value::Date(_) => LogicalType::Date,
            Value::Interval(_) => LogicalType::Interval,
            Value::Timestamp(_) => LogicalType::Timestamp,
            Value::String(_) => LogicalType::String,
            Value::Null(x) => x.clone(),
            Value::VarList(x, _) => LogicalType::VarList {
                child_type: Box::new(x.clone()),
            },
            Value::FixedList(x, value) => LogicalType::FixedList {
                child_type: Box::new(x.clone()),
                num_elements: value.len() as u64,
            },
            Value::Struct(values) => LogicalType::Struct {
                fields: values
                    .iter()
                    .map(|(name, x)| {
                        let typ: LogicalType = x.into();
                        (name.clone(), typ)
                    })
                    .collect(),
            },
            Value::InternalID(_) => LogicalType::InternalID,
            Value::Node(_) => LogicalType::Node,
            Value::Rel(_) => LogicalType::Rel,
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
                if let LogicalType::VarList { child_type } = value.into() {
                    Ok(Value::VarList(*child_type, result))
                } else {
                    unreachable!()
                }
            }
            LogicalTypeID::FIXED_LIST => {
                let list = ffi::value_get_list(value);
                let mut result = vec![];
                for index in 0..list.size() {
                    let value: Value = list.get(index).as_ref().unwrap().try_into()?;
                    result.push(value);
                }
                if let LogicalType::FixedList { child_type, .. } = value.into() {
                    Ok(Value::FixedList(*child_type, result))
                } else {
                    unreachable!()
                }
            }
            LogicalTypeID::STRUCT => {
                // Data is a list of field values in the value itself (same as list),
                // with the field names stored in the DataType
                let field_names = ffi::logical_type_get_struct_field_names(
                    ffi::value_get_data_type(value).as_ref().unwrap(),
                );
                let list = ffi::value_get_list(value);
                let mut result = vec![];
                for (name, index) in field_names.into_iter().zip(0..list.size()) {
                    let value: Value = list.get(index).as_ref().unwrap().try_into()?;
                    result.push((name, value));
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

impl TryInto<cxx::UniquePtr<ffi::Value>> for Value {
    // Errors should occur if:
    // - types are heterogeneous in lists
    type Error = crate::error::Error;

    fn try_into(self) -> Result<cxx::UniquePtr<ffi::Value>, Self::Error> {
        match self {
            Value::Null(typ) => Ok(ffi::create_value_null((&typ).into())),
            Value::Bool(value) => Ok(ffi::create_value_bool(value)),
            Value::Int16(value) => Ok(ffi::create_value_i16(value)),
            Value::Int32(value) => Ok(ffi::create_value_i32(value)),
            Value::Int64(value) => Ok(ffi::create_value_i64(value)),
            Value::Float(value) => Ok(ffi::create_value_float(value)),
            Value::Double(value) => Ok(ffi::create_value_double(value)),
            Value::String(value) => Ok(ffi::create_value_string(&value)),
            Value::Timestamp(value) => Ok(ffi::create_value_timestamp(
                // Convert to microseconds since 1970-01-01
                (value.unix_timestamp_nanos() / 1000) as i64,
            )),
            Value::Date(value) => Ok(ffi::create_value_date(
                // Convert to days since 1970-01-01
                (value - time::Date::from_ordinal_date(1970, 1).unwrap()).whole_days(),
            )),
            Value::Interval(value) => {
                use time::Duration;
                let mut interval = value;
                let months = interval.whole_days() / 30;
                interval -= Duration::days(months * 30);
                let days = interval.whole_days();
                interval -= Duration::days(days);
                let micros = interval.whole_microseconds() as i64;
                Ok(ffi::create_value_interval(
                    months as i32,
                    days as i32,
                    micros,
                ))
            }
            Value::VarList(typ, value) => {
                let mut builder = ffi::create_list();
                for elem in value {
                    builder.pin_mut().insert(elem.try_into()?);
                }
                Ok(ffi::get_list_value(
                    (&LogicalType::VarList {
                        child_type: Box::new(typ),
                    })
                        .into(),
                    builder,
                ))
            }
            Value::FixedList(typ, value) => {
                let mut builder = ffi::create_list();
                let len = value.len();
                for elem in value {
                    builder.pin_mut().insert(elem.try_into()?);
                }
                Ok(ffi::get_list_value(
                    (&LogicalType::FixedList {
                        child_type: Box::new(typ),
                        num_elements: len as u64,
                    })
                        .into(),
                    builder,
                ))
            }
            Value::Struct(value) => {
                let typ: LogicalType = LogicalType::Struct {
                    fields: value
                        .iter()
                        .map(|(name, value)| {
                            // Unwrap is safe since we already converted when inserting into the
                            // builder
                            (name.clone(), Into::<LogicalType>::into(value))
                        })
                        .collect(),
                };

                let mut builder = ffi::create_list();
                for (_, elem) in value {
                    //builder.pin_mut().insert(Value::String(name).try_into()?);
                    builder.pin_mut().insert(elem.try_into()?);
                }

                Ok(ffi::get_list_value((&typ).into(), builder))
            }
            _ => unimplemented!(),
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
    use crate::ffi::ffi;
    use crate::{
        connection::Connection,
        database::Database,
        value::{LogicalType, Value},
    };
    use anyhow::Result;
    use std::collections::HashSet;
    use std::convert::TryInto;
    use std::iter::FromIterator;
    use time::macros::{date, datetime};

    // Note: Cargo runs tests in parallel by default, however kuzu does not support
    // working with multiple databases in parallel.
    // Tests can be run serially with `cargo test -- --test-threads=1` to work around this.

    macro_rules! type_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            /// Tests that the values are correctly converted into kuzu::common::Value and back
            fn $name() -> Result<()> {
                let rust_type: LogicalType = $value;
                let typ: cxx::UniquePtr<ffi::LogicalType> = (&rust_type).try_into()?;
                let new_rust_type: LogicalType = typ.as_ref().unwrap().try_into()?;
                assert_eq!(new_rust_type, rust_type);
                Ok(())
            }
        )*
        }
    }

    macro_rules! value_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            /// Tests that the values are correctly converted into kuzu::common::Value and back
            fn $name() -> Result<()> {
                let rust_value: Value = $value;
                let value: cxx::UniquePtr<ffi::Value> = rust_value.clone().try_into()?;
                //assert_eq!(ffi::value_to_string(value.as_ref().unwrap()), format!("{rust_value}"));
                let new_rust_value: Value = value.as_ref().unwrap().try_into()?;
                assert_eq!(new_rust_value, rust_value);
                Ok(())
            }
        )*
        }
    }

    macro_rules! database_tests {
        ($($name:ident: $value:expr, $decl:expr,)*) => {
        $(
            #[test]
            /// Tests that passing the values through the database returns what we put in
            fn $name() -> Result<()> {
                let temp_dir = tempdir::TempDir::new("example")?;
                let db = Database::new(temp_dir.path(), 0)?;
                let conn = Connection::new(&db)?;
                conn.query(&format!(
                    "CREATE NODE TABLE Person(name STRING, item {}, PRIMARY KEY(name));",
                    $decl,
                ))?;

                let mut add_person =
                    conn.prepare("CREATE (:Person {name: $name, item: $item});")?;
                conn.execute(
                    &mut add_person,
                    vec![("name", "Bob".into()), ("item", $value)],
                )?;
                let result = conn
                    .query("MATCH (a:Person) WHERE a.name = \"Bob\" RETURN a.item;")?
                    .next()
                    .unwrap();
                // TODO: Test equivalence to value constructed inside a a query
                assert_eq!(result[0], $value);
                temp_dir.close()?;
                Ok(())
            }
        )*
        }
    }

    type_tests! {
        convert_var_list_type: LogicalType::VarList { child_type: Box::new(LogicalType::String) },
        convert_fixed_list_type: LogicalType::FixedList { child_type: Box::new(LogicalType::Int64), num_elements: 3 },
        convert_int16_type: LogicalType::Int16,
        convert_int32_type: LogicalType::Int32,
        convert_int64_type: LogicalType::Int64,
        convert_float_type: LogicalType::Float,
        convert_double_type: LogicalType::Double,
        convert_timestamp_type: LogicalType::Timestamp,
        convert_date_type: LogicalType::Date,
        convert_interval_type: LogicalType::Interval,
        convert_string_type: LogicalType::String,
        convert_bool_type: LogicalType::Bool,
        convert_struct_type: LogicalType::Struct { fields: vec![("NAME".to_string(), LogicalType::String)]},
    }

    value_tests! {
        convert_var_list: Value::VarList(LogicalType::String, vec!["Alice".into(), "Bob".into()]),
        convert_var_list_empty: Value::VarList(LogicalType::String, vec![]),
        convert_fixed_list: Value::FixedList(LogicalType::String, vec!["Alice".into(), "Bob".into()]),
        convert_int16: Value::Int16(1),
        convert_int32: Value::Int32(2),
        convert_int64: Value::Int64(3),
        convert_float: Value::Float(4.),
        convert_double: Value::Double(5.),
        convert_timestamp: Value::Timestamp(datetime!(2023-06-13 11:25:30 UTC)),
        convert_date: Value::Date(date!(2023-06-13)),
        convert_interval: Value::Interval(time::Duration::weeks(10)),
        convert_string: Value::String("Hello World".to_string()),
        convert_bool: Value::Bool(false),
        convert_null: Value::Null(LogicalType::VarList {
            child_type: Box::new(LogicalType::FixedList { child_type: Box::new(LogicalType::Int16), num_elements: 3 })
        }),
        convert_struct: Value::Struct(vec![("NAME".to_string(), "Alice".into()), ("AGE".to_string(), 25.into())]),
    }

    database_tests! {
        // Passing these values as arguments is not yet implemented in kuzu:
        // db_struct:
        //    Value::Struct(vec![("item".to_string(), "Knife".into()), ("count".to_string(), 1.into())]),
        //    "STRUCT(item STRING, count INT32)",
        // db_fixed_list: Value::FixedList(LogicalType::String, vec!["Alice".into(), "Bob".into()]), "STRING[2]",
        db_var_list_string: Value::VarList(LogicalType::String, vec!["Alice".into(), "Bob".into()]), "STRING[]",
        db_var_list_int: Value::VarList(LogicalType::Int64, vec![0i64.into(), 1i64.into(), 2i64.into()]), "INT64[]",
        db_int16: Value::Int16(1), "INT16",
        db_int32: Value::Int32(2), "INT32",
        db_int64: Value::Int64(3), "INT64",
        db_float: Value::Float(4.), "FLOAT",
        db_double: Value::Double(5.), "DOUBLE",
        db_timestamp: Value::Timestamp(datetime!(2023-06-13 11:25:30 UTC)), "TIMESTAMP",
        db_date: Value::Date(date!(2023-06-13)), "DATE",
        db_interval: Value::Interval(time::Duration::weeks(200)), "INTERVAL",
        db_string: Value::String("Hello World".to_string()), "STRING",
        db_bool: Value::Bool(true), "BOOLEAN",
        // Causes buffer manager failure
        // db_null: Value::Null(LogicalType::VarList {
        //    child_type: Box::new(LogicalType::FixedList { child_type: Box::new(LogicalType::Int16), num_elements: 3 })
        // }), "INT16[3][]",
    }

    #[test]
    /// Tests that the list value is correctly constructed
    fn test_var_list_get() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
        for result in conn.query("RETURN [\"Alice\", \"Bob\"] AS l;")? {
            assert_eq!(result.len(), 1);
            assert_eq!(
                result[0],
                Value::VarList(LogicalType::String, vec!["Alice".into(), "Bob".into(),])
            );
        }
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    /// Test that the timestamp round-trips through kuzu's internal timestamp
    fn test_timestamp() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
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
            vec![
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
    /// Test that null values are read correctly by the API
    fn test_null() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
        let result = conn.query("RETURN null")?.next();
        let result = &result.unwrap()[0];
        assert_eq!(result, &Value::Null(LogicalType::String));
        temp_dir.close()?;
        Ok(())
    }
}
