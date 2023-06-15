use crate::ffi::ffi;
use crate::value::Value;
use cxx::UniquePtr;
use std::convert::TryInto;
use std::fmt;
use crate::logical_type::LogicalType;

/// Stores the result of a query execution
pub struct QueryResult {
    pub(crate) result: UniquePtr<ffi::QueryResult>,
}

// Should be safe to move across threads, however access is not synchronized
unsafe impl Send for ffi::QueryResult {}

pub struct CSVOptions {
    delimiter: char,
    escape_character: char,
    newline: char,
}

impl Default for CSVOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl CSVOptions {
    pub fn new() -> Self {
        CSVOptions {
            delimiter: ',',
            escape_character: '"',
            newline: '\n',
        }
    }
    pub fn delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = delimiter;
        self
    }

    pub fn escape_character(mut self, escape_character: char) -> Self {
        self.escape_character = escape_character;
        self
    }

    pub fn newline(mut self, newline: char) -> Self {
        self.newline = newline;
        self
    }
}

// TODO: getNext will throw an exception if the query result is a failure result
// Is there any need for a QueryResult at all in that case, instead of some form of error?
// If so, then we need to make sure you can't call next when the query failed.
// Otherwise, it shouldn't matter and getNext should be safe.
//
// getNumColumns, getColumnDataTypes, getColumnNames, getNumTuples can be called when it's an error
//
// Even if that data is needed, we could use a custom error type that retains it
impl QueryResult {
    pub fn display(&mut self) -> String {
        ffi::query_result_to_string(self.result.pin_mut())
    }

    pub fn get_compiling_time(&self) -> f64 {
        ffi::query_result_get_compiling_time(self.result.as_ref().unwrap())
    }

    pub fn get_execution_time(&self) -> f64 {
        ffi::query_result_get_execution_time(self.result.as_ref().unwrap())
    }

    pub fn get_num_columns(&self) -> usize {
        self.result.as_ref().unwrap().getNumColumns()
    }
    pub fn get_num_tuples(&self) -> u64 {
        self.result.as_ref().unwrap().getNumTuples()
    }

    pub fn get_column_names(&self) -> Vec<String> {
        ffi::query_result_column_names(self.result.as_ref().unwrap())
    }
    pub fn get_column_data_types(&self) -> Vec<LogicalType> {
        ffi::query_result_column_data_types(self.result.as_ref().unwrap())
            .as_ref()
            .unwrap()
            .iter()
            .map(|x| x.into())
            .collect()
    }

    pub fn write_to_csv<P: AsRef<std::path::Path>>(
        &mut self,
        path: P,
        options: CSVOptions,
    ) -> Result<(), crate::error::Error> {
        Ok(ffi::query_result_write_to_csv(
            self.result.pin_mut(),
            &path.as_ref().display().to_string(),
            options.delimiter as i8,
            options.escape_character as i8,
            options.newline as i8,
        )?)
    }
}

// the underlying C++ type is both data and an iterator (sort-of)
impl Iterator for QueryResult {
    // we will be counting with usize
    type Item = Vec<Value>;

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        if self.result.as_ref().unwrap().hasNext() {
            let flat_tuple = self.result.pin_mut().getNext();
            let mut result = vec![];
            for i in 0..flat_tuple.as_ref().unwrap().len() {
                let value = ffi::flat_tuple_get_value(flat_tuple.as_ref().unwrap(), i);
                // TODO: Return result instead of unwrapping?
                // Unfortunately, as an iterator, this would require producing
                // Vec<Result<Value>>, though it would be possible to turn that into
                // Result<Vec<Value>> instead, but it would lose information when multiple failures
                // occur.
                result.push(value.try_into().unwrap());
            }
            Some(result)
        } else {
            None
        }
    }
}

impl fmt::Debug for QueryResult {
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
    use crate::database::Database;
    use crate::logical_type::LogicalType;
    #[test]
    fn test_query_result_metadata() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let db = Database::new(temp_dir.path(), 0)?;
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
}
