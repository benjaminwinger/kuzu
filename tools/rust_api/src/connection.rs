use crate::database::Database;
use crate::error::Error;
use crate::ffi::ffi;
use crate::query_result::QueryResult;
use cxx::{let_cxx_string, UniquePtr};

pub struct PreparedStatement {
    statement: UniquePtr<ffi::PreparedStatement>,
}

pub struct Connection {
    conn: UniquePtr<ffi::Connection>,
}

impl Connection {
    pub fn new(database: &mut Database) -> Result<Self, Error> {
        Ok(Connection {
            conn: ffi::database_connect(database.db.pin_mut())?,
        })
    }

    pub fn set_max_num_threads_for_exec(&mut self, num_threads: u64) {
        ffi::connection_set_max_num_threads_for_exec(self.conn.pin_mut(), num_threads);
    }

    pub fn get_max_num_threads_for_exec(&mut self) -> u64 {
        ffi::connection_get_max_num_threads_for_exec(self.conn.pin_mut())
    }

    pub fn prepare(&mut self, query: &str) -> Result<PreparedStatement, Error> {
        let_cxx_string!(query = query);
        Ok(PreparedStatement {
            statement: ffi::connection_prepare(self.conn.pin_mut(), &query)?,
        })
    }

    pub fn query(&mut self, query: &str) -> Result<QueryResult, Error> {
        let mut statement = self.prepare(query)?;
        self.execute(&mut statement)
    }

    pub fn execute(
        &mut self,
        // TODO: C++ API takes a mutable pointer
        // Maybe consume it instead? Is there a benefit to keeping the statement?
        query: &mut PreparedStatement,
        //params: std::collections::HashMap<String, Value>,
    ) -> Result<QueryResult, Error> {
        /*
        let keys = vec![];
        let values = vec![];
        for (key, value) in params.drain() {
            keys.push(key);
            values.push(value);
        }*/
        let result = ffi::connection_execute(
            self.conn.pin_mut(),
            query.statement.pin_mut(),
            /*keys,
            values,*/
        )?;
        if !ffi::query_result_is_success(&result) {
            Err(Error::FailedQuery(ffi::query_result_get_error_message(
                &result,
            )))
        } else {
            Ok(QueryResult { result })
        }
    }

    pub fn begin_read_only_transaction(&mut self) -> Result<(), Error> {
        Ok(ffi::connection_begin_read_only_transaction(
            self.conn.pin_mut(),
        )?)
    }

    pub fn begin_write_transaction(&mut self) -> Result<(), Error> {
        Ok(ffi::connection_begin_write_transaction(
            self.conn.pin_mut(),
        )?)
    }
    pub fn commit(&mut self) -> Result<(), Error> {
        Ok(ffi::connection_commit(self.conn.pin_mut())?)
    }
    pub fn rollback(&mut self) -> Result<(), Error> {
        Ok(ffi::connection_rollback(self.conn.pin_mut())?)
    }
    pub fn interrupt(&mut self) -> Result<(), Error> {
        Ok(ffi::connection_interrupt(self.conn.pin_mut())?)
    }

    /* TODO (bmwinger)
    /// Mutable due to internal locking
    pub fn get_node_table_names(&mut self) -> String {}
    /// Mutable due to internal locking
    pub fn get_rel_table_names(&mut self) -> String {}
    /// Mutable due to internal locking
    pub fn get_node_property_names(&mut self) -> String {}
    /// Mutable due to internal locking
    pub fn get_rel_property_names(&mut self) -> String {}
    pub fn set_query_timeout(&mut self, timeout_ms: u64) {}
    */
}

#[cfg(test)]
mod tests {
    use crate::{connection::Connection, database::Database, value::Value};
    use anyhow::{Error, Result};
    // Note: Cargo runs tests in parallel by default, however kuzu does not support
    // working with multiple databases in parallel.
    // Tests can be run serially with `cargo test -- --test-threads=1` to work around this.

    #[test]
    fn test_connection_threads() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example1")?;
        let mut db = Database::new(temp_dir.path(), 0)?;
        let mut conn = Connection::new(&mut db)?;
        conn.set_max_num_threads_for_exec(5);
        assert_eq!(conn.get_max_num_threads_for_exec(), 5);
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_invalid_query() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example2")?;
        let mut db = Database::new(temp_dir.path(), 0)?;
        let mut conn = Connection::new(&mut db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
        conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
        conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;

        let result: Error = conn
            .query("MATCH (a:Person RETURN a.name AS NAME, a.age AS AGE;")
            .expect_err("Invalid syntax in query should produce an error")
            .into();
        assert_eq!(
            result.to_string(),
            "Query execution failed: Parser exception: \
Invalid input <MATCH (a:Person RETURN>: expected rule oC_SingleQuery (line: 1, offset: 16)
\"MATCH (a:Person RETURN a.name AS NAME, a.age AS AGE;\"
                 ^^^^^^"
        );
        Ok(())
    }

    #[test]
    fn test_query_result() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example3")?;
        let mut db = Database::new(temp_dir.path(), 0)?;
        let mut conn = Connection::new(&mut db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
        conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;

        let mut statement =
            conn.prepare("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")?;
        for result in conn.execute(&mut statement)? {
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], Value::String("Alice".to_string()));
            assert_eq!(result[1], Value::Int64(25));
        }
        temp_dir.close()?;
        Ok(())
    }
}
