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

/// Connections are used to interact with a Database instance.
///
/// Each connection is thread-safe, and multiple connections can connect to the same Database
/// instance in a multithreaded environment.
///
/// ## Committing
/// If the connection is in AUTO_COMMIT mode any query over the connection will be wrapped around
/// a transaction and committed (even if the query is READ_ONLY).
/// If the connection is in MANUAL transaction mode, which happens only if an application
/// manually begins a transaction (see below), then an application has to manually commit or
/// rollback the transaction by calling commit() or rollback().
///
/// AUTO_COMMIT is the default mode when a Connection is created. If an application calls
/// begin[ReadOnly/Write]Transaction at any point, the mode switches to MANUAL. This creates
/// an "active transaction" in the connection. When a connection is in MANUAL mode and the
/// active transaction is rolled back or committed, then the active transaction is removed (so
/// the connection no longer has an active transaction) and the mode automatically switches
/// back to AUTO_COMMIT.
/// Note: When a Connection object is deconstructed, if the connection has an active (manual)
/// transaction, then the active transaction is rolled back.
///
/// ```
/// use kuzu::{Database, Connection};
/// # use anyhow::Error;
///
/// # fn main() -> Result<(), Error> {
/// # let temp_dir = tempdir::TempDir::new("example")?;
/// # let path = temp_dir.path();
/// let mut db = Database::new(path, 0)?;
/// let mut conn = Connection::new(&mut db)?;
/// /// AUTO_COMMIT mode
/// conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
/// conn.begin_write_transaction()?;
/// /// MANUAL mode (write)
/// conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
/// conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;
/// /// Queries committed and mode goes back to AUTO_COMMIT
/// conn.commit()?;
/// let result = conn.query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")?;
/// assert!(result.count() == 2);
/// # temp_dir.close()?;
/// # Ok(())
/// # }
/// ```
///
/// ```
/// use kuzu::{Database, Connection};
/// # use anyhow::Error;
///
/// # fn main() -> Result<(), Error> {
/// # let temp_dir = tempdir::TempDir::new("example")?;
/// # let path = temp_dir.path();
/// let mut db = Database::new(path, 0)?;
/// let mut conn = Connection::new(&mut db)?;
/// /// AUTO_COMMIT mode
/// conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
/// conn.begin_write_transaction()?;
/// /// MANUAL mode (write)
/// conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
/// conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;
/// /// Queries rolled back and mode goes back to AUTO_COMMIT
/// conn.rollback()?;
/// let result = conn.query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")?;
/// assert!(result.count() == 0);
/// # temp_dir.close()?;
/// # Ok(())
/// # }
/// ```
impl Connection {
    /// Creates a connection to the database.
    ///
    /// # Arguments
    /// * `database`: A reference to the database instance to which this connection will be connected.
    pub fn new(database: &mut Database) -> Result<Self, Error> {
        Ok(Connection {
            conn: ffi::database_connect(database.db.pin_mut())?,
        })
    }

    /// Sets the maximum number of threads to use for execution in the current connection
    ///
    /// # Arguments
    /// * `num_threads`: The maximum number of threads to use for execution in the current connection
    pub fn set_max_num_threads_for_exec(&mut self, num_threads: u64) {
        ffi::connection_set_max_num_threads_for_exec(self.conn.pin_mut(), num_threads);
    }

    /// Returns the maximum number of threads used for execution in the current connection
    pub fn get_max_num_threads_for_exec(&mut self) -> u64 {
        ffi::connection_get_max_num_threads_for_exec(self.conn.pin_mut())
    }

    /// Prepares the given query and returns the prepared statement.
    ///
    /// # Arguments
    /// * `query`: The query to prepare.
    ///            See <https://kuzudb.com/docs/cypher> for details on the query format
    pub fn prepare(&mut self, query: &str) -> Result<PreparedStatement, Error> {
        let_cxx_string!(query = query);
        Ok(PreparedStatement {
            statement: ffi::connection_prepare(self.conn.pin_mut(), &query)?,
        })
    }

    /// Executes the given query and returns the result.
    ///
    /// # Arguments
    /// * `query`: The query to execute.
    ///            See <https://kuzudb.com/docs/cypher> for details on the query format
    pub fn query(&mut self, query: &str) -> Result<QueryResult, Error> {
        let mut statement = self.prepare(query)?;
        self.execute(&mut statement)
    }

    /// Executes the given prepared statement with args and returns the result.
    ///
    /// # Arguments
    /// * `prepared_statement`: The prepared statement to execute
    pub fn execute(
        &mut self,
        // TODO: C++ API takes a mutable pointer
        // Maybe consume it instead? Is there a benefit to keeping the statement?
        prepared_statement: &mut PreparedStatement,
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
            prepared_statement.statement.pin_mut(),
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

    /// Manually starts a new read-only transaction in the current connection
    pub fn begin_read_only_transaction(&mut self) -> Result<(), Error> {
        Ok(ffi::connection_begin_read_only_transaction(
            self.conn.pin_mut(),
        )?)
    }

    /// Manually starts a new write transaction in the current connection
    pub fn begin_write_transaction(&mut self) -> Result<(), Error> {
        Ok(ffi::connection_begin_write_transaction(
            self.conn.pin_mut(),
        )?)
    }

    /// Manually commits the current transaction
    pub fn commit(&mut self) -> Result<(), Error> {
        Ok(ffi::connection_commit(self.conn.pin_mut())?)
    }

    /// Manually rolls back the current transaction
    pub fn rollback(&mut self) -> Result<(), Error> {
        Ok(ffi::connection_rollback(self.conn.pin_mut())?)
    }

    /// Interrupts all queries currenttly executing within this connection
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
