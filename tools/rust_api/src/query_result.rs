use crate::ffi::ffi;
use crate::logical_type::LogicalType;
use crate::value::Value;
use cxx::UniquePtr;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt;
use std::marker::PhantomData;

pub struct KuzuRow(Vec<Value>);

impl From<Vec<Value>> for KuzuRow {
    fn from(row: Vec<Value>) -> Self {
        KuzuRow(row)
    }
}

impl From<KuzuRow> for Vec<Value> {
    fn from(row: KuzuRow) -> Self {
        row.0
    }
}

impl<T: TryFrom<Value>> TryInto<(T,)> for KuzuRow {
    type Error = T::Error;
    fn try_into(self) -> Result<(T,), Self::Error> {
        let KuzuRow(mut vector) = self;
        if vector.len() == 1 {
            Ok((vector.pop().unwrap().try_into()?,))
        } else {
            panic!("Row has multiple columns, but expected just one!");
        }
    }
}

impl<T1, T2> TryFrom<KuzuRow> for (T1, T2)
where
    T1: TryFrom<Value>,
    T2: TryFrom<Value>,
    <T1 as TryFrom<Value>>::Error: std::error::Error,
    <T1 as TryFrom<Value>>::Error: 'static,
    <T2 as TryFrom<Value>>::Error: std::error::Error,
    <T2 as TryFrom<Value>>::Error: 'static,
{
    type Error = Box<dyn std::error::Error>;
    fn try_from(row: KuzuRow) -> Result<(T1, T2), Self::Error> {
        let KuzuRow(mut vector) = row;
        if vector.len() == 2 {
            let second = vector.pop().unwrap();
            let first = vector.pop().unwrap();
            Ok((
                first
                    .try_into()
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)?,
                second
                    .try_into()
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)?,
            ))
        } else {
            panic!("Expected row to have two columns!");
        }
    }
}

impl<T1, T2, T3> TryFrom<KuzuRow> for (T1, T2, T3)
where
    T1: TryFrom<Value>,
    T2: TryFrom<Value>,
    T3: TryFrom<Value>,
    <T1 as TryFrom<Value>>::Error: std::error::Error,
    <T1 as TryFrom<Value>>::Error: 'static,
    <T2 as TryFrom<Value>>::Error: std::error::Error,
    <T2 as TryFrom<Value>>::Error: 'static,
    <T3 as TryFrom<Value>>::Error: std::error::Error,
    <T3 as TryFrom<Value>>::Error: 'static,
{
    type Error = Box<dyn std::error::Error>;
    fn try_from(row: KuzuRow) -> Result<(T1, T2, T3), Self::Error> {
        let KuzuRow(mut vector) = row;
        if vector.len() == 2 {
            let third = vector.pop().unwrap();
            let second = vector.pop().unwrap();
            let first = vector.pop().unwrap();
            Ok((
                first
                    .try_into()
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)?,
                second
                    .try_into()
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)?,
                third
                    .try_into()
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)?,
            ))
        } else {
            panic!("Expected row to have two columns!");
        }
    }
}

/*
impl<T: TryInto<Value>, E: T as TryInto<Value>>::Error: std::fmt::Debug> TryFrom<Vec<T>> for KuzuRow
{ type Error = ;

    fn try_from(value: Vec<T>) -> Result<Self, Self::Error> {
        value
            .into_iter()
            .map(|x| x.try_into())
            .collect::<Result<Vec<Value>, _>>()?
    }
}

impl<T: TryFrom<Value>> TryFrom<KuzuRow> for Vec<T> {
    type Error = &'static str;

    fn try_from(value: KuzuRow) -> Result<Self, Self::Error> {
        value
            .map(|x| x.try_into::<Value>())
            .collect::<Result<Vec<Value>>, _>()?
    }
}

impl<T: TryInto<Value>> TryFrom<&[T]> for KuzuRow {
    type Error = &'static str;

    fn try_from(value: &[T]) -> Result<Self, Self::Error> {
        value
            .map(|x| x.try_into::<Value>())
            .collect::<Result<Vec<Value>>, _>()?;
    }
}
*/

/// Stores the result of a query execution
pub struct QueryResult<'a, T: TryFrom<KuzuRow> = Vec<Value>> {
    pub(crate) result: UniquePtr<ffi::QueryResult<'a>>,
    pub(crate) _t: PhantomData<T>,
}

impl<T: TryFrom<KuzuRow>> QueryResult<'_, T> {
    pub(crate) fn new(result: UniquePtr<ffi::QueryResult<'_>>) -> Self {
        QueryResult {
            result,
            _t: PhantomData,
        }
    }
}

// Should be safe to move across threads, however access is not synchronized
unsafe impl Send for ffi::QueryResult<'_> {}

/// Options for writing CSV files
pub struct CSVOptions {
    delimiter: char,
    escape_character: char,
    newline: char,
}

impl Default for CSVOptions {
    /// Default CSV options with delimiter `,`, escape character `"` and newline `\n`.
    fn default() -> Self {
        CSVOptions {
            delimiter: ',',
            escape_character: '"',
            newline: '\n',
        }
    }
}

impl CSVOptions {
    /// Sets the field delimiter to use when writing the CSV file. If not specified the default is
    /// `,`
    pub fn delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Sets the escape character to use for text containing special characters.
    /// If not specified the default is `"`
    pub fn escape_character(mut self, escape_character: char) -> Self {
        self.escape_character = escape_character;
        self
    }

    /// Sets the newline character
    /// If not specified the default is `\n`
    pub fn newline(mut self, newline: char) -> Self {
        self.newline = newline;
        self
    }
}

impl<'db, T: TryFrom<KuzuRow>> QueryResult<'db, T> {
    /// Displays the query result as a string
    pub fn display(&mut self) -> String {
        ffi::query_result_to_string(self.result.pin_mut())
    }

    /// Returns the time spent compiling the query in milliseconds
    pub fn get_compiling_time(&self) -> f64 {
        ffi::query_result_get_compiling_time(self.result.as_ref().unwrap())
    }

    /// Returns the time spent executing the query in milliseconds
    pub fn get_execution_time(&self) -> f64 {
        ffi::query_result_get_execution_time(self.result.as_ref().unwrap())
    }

    /// Returns the number of columns in the query result.
    ///
    /// This corresponds to the length of each result vector yielded by the iterator.
    pub fn get_num_columns(&self) -> usize {
        self.result.as_ref().unwrap().getNumColumns()
    }
    /// Returns the number of tuples in the query result.
    ///
    /// This corresponds to the total number of result
    /// vectors that the query result iterator will yield.
    pub fn get_num_tuples(&self) -> u64 {
        self.result.as_ref().unwrap().getNumTuples()
    }

    /// Returns the name of each column in the query result
    pub fn get_column_names(&self) -> Vec<String> {
        ffi::query_result_column_names(self.result.as_ref().unwrap())
    }
    /// Returns the data type of each column in the query result
    pub fn get_column_data_types(&self) -> Vec<LogicalType> {
        ffi::query_result_column_data_types(self.result.as_ref().unwrap())
            .as_ref()
            .unwrap()
            .iter()
            .map(|x| x.into())
            .collect()
    }

    #[cfg(feature = "arrow")]
    /// Produces an iterator over the results as [RecordBatch](arrow::record_batch::RecordBatch)es,
    /// split into chunks of the given size.
    ///
    /// *Requires the `arrow` feature*
    pub fn iter_arrow<'qr>(
        &'qr mut self,
        chunk_size: usize,
    ) -> Result<ArrowIterator<'qr, 'db>, crate::error::Error> {
        let schema = crate::ffi::arrow::ffi_arrow::query_result_get_arrow_schema(
            self.result.as_ref().unwrap(),
        )?
        .0;
        Ok(ArrowIterator {
            chunk_size,
            result: &mut self.result,
            schema,
        })
    }
}

// the underlying C++ type is both data and an iterator (sort-of)
impl<E, T: TryFrom<KuzuRow, Error = E>> Iterator for QueryResult<'_, T> {
    // TODO: Better error type
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.result.as_ref().unwrap().hasNext() {
            let flat_tuple = self.result.pin_mut().getNext();
            let mut result: Vec<Value> = vec![];
            for i in 0..flat_tuple.as_ref().unwrap().len() {
                let value = ffi::flat_tuple_get_value(flat_tuple.as_ref().unwrap(), i);
                // TODO: Return result instead of unwrapping?
                // Unfortunately, as an iterator, this would require producing
                // Vec<Result<Value>>, though it would be possible to turn that into
                // Result<Vec<Value>> instead, but it would lose information when multiple
                // failures occur.
                result.push(value.try_into().unwrap());
            }
            // TODO: This is is ignoring conversion errors and terminating the iterator early if
            // the TryFrom fails
            // Since all rows should have the same type, we should ideally be able to handle most
            // errors when the query result is constructed by asserting that the column types match
            // T
            // However conversion errors for individual rows also need to be handled.
            Some(TryInto::<T>::try_into(KuzuRow(result)))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.result.as_ref().unwrap().getNumTuples() as usize;
        (len, Some(len))
    }
}

#[cfg(feature = "arrow")]
/// Produces an iterator over a QueryResult as [RecordBatch](arrow::record_batch::RecordBatch)es
///
/// The result is split into chunks of a size specified in [iter_arrow](QueryResult::iter_arrow).
///
/// *Requires the `arrow` feature*
pub struct ArrowIterator<'qr, 'db: 'qr> {
    pub(crate) chunk_size: usize,
    pub(crate) result: &'qr mut UniquePtr<ffi::QueryResult<'db>>,
    pub(crate) schema: arrow::ffi::FFI_ArrowSchema,
}

#[cfg(feature = "arrow")]
impl Iterator for ArrowIterator<'_, '_> {
    type Item = arrow::record_batch::RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.result.as_ref().unwrap().hasNext() {
            use crate::ffi::arrow::ffi_arrow;
            // Generally this panic should be unreachable, since the only exceptions produced by
            // arrow_converter are for unsupported types, but those would produce an error when
            // we create the schema.
            let array = ffi_arrow::query_result_get_next_arrow_chunk(
                self.result.pin_mut(),
                self.chunk_size as u64,
            )
            .expect("Failed to get next recordbatch");
            let struct_array: arrow::array::StructArray =
                unsafe { arrow::ffi::from_ffi(array.0, &self.schema) }
                    .expect("Failed to convert ArrowArray from C data")
                    .into();
            Some(struct_array.into())
        } else {
            None
        }
    }
}

impl<T: TryFrom<KuzuRow>> fmt::Debug for QueryResult<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryResult")
            .field(
                "result",
                &"Opaque C++ data which whose toString method requires mutation".to_string(),
            )
            .finish()
    }
}

/* TODO: QueryResult.toString() needs to be const
impl std::fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ffi::query_result_to_string(self.result.as_ref().unwrap()))
    }
}
*/

#[cfg(test)]
mod tests {
    use crate::connection::Connection;
    use crate::database::{Database, SystemConfig};
    use crate::logical_type::LogicalType;

    #[test]
    fn test_query_result_metadata() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db = Database::new(temp_dir.path(), SystemConfig::default())?;
        let connection = Connection::new(&db)?;

        // Create schema.
        connection.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
        // Create nodes.
        connection.query("CREATE (:Person {name: 'Alice', age: 25});")?;
        connection.query("CREATE (:Person {name: 'Bob', age: 30});")?;

        // Execute a simple query.
        let result = connection.query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")?;

        assert!(result.get_compiling_time() > 0.);
        assert!(result.get_execution_time() > 0.);
        assert_eq!(result.get_column_names(), vec!["NAME", "AGE"]);
        assert_eq!(
            result.get_column_data_types(),
            vec![LogicalType::String, LogicalType::Int64]
        );
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_query_result_move() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db = Database::new(temp_dir.path(), SystemConfig::default())?;
        let mut result = {
            let connection = Connection::new(&db)?;

            // Create schema.
            connection
                .query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
            // Create nodes.
            connection.query("CREATE (:Person {name: 'Alice', age: 25});")?;
            connection.query("CREATE (:Person {name: 'Bob', age: 30});")?;

            // Execute a simple query.
            connection.query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")?
        };

        assert_eq!(result.display().to_string(), "NAME|AGE\nAlice|25\nBob|30\n");
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn test_arrow() -> anyhow::Result<()> {
        use arrow::array::{Int64Array, StringArray};
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let db = Database::new(path, SystemConfig::default())?;
        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
        conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
        conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;
        let mut result = conn.query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")?;
        let mut result = result.iter_arrow(1)?;
        let rb = result.next().unwrap();
        assert_eq!(rb.num_rows(), 1);
        let names: &StringArray = rb
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Type of column 0 is not a StringArray!");
        assert_eq!(names.value(0), "Alice");
        let ages: &Int64Array = rb
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Type of column 1 is not a StringArray!");
        assert_eq!(ages.value(0), 25);
        let rb = result.next().unwrap();
        assert_eq!(rb.num_rows(), 1);
        assert_eq!(result.next(), None);
        temp_dir.close()?;
        Ok(())
    }
}
