#[cxx::bridge]
pub(crate) mod ffi {
    #[namespace = "kuzu::main"]
    unsafe extern "C++" {
        type PreparedStatement;
        fn isSuccess(&self) -> bool;

        #[namespace = "kuzu_rs"]
        fn prepared_statement_error_message(statement: &PreparedStatement) -> String;
    }

    #[namespace = "kuzu_rs"]
    unsafe extern "C++" {
        include!("kuzu/include/kuzu_rs.h");

        #[namespace = "kuzu::main"]
        type Database;
        #[namespace = "kuzu::main"]
        type Connection;
        #[namespace = "kuzu::main"]
        type QueryResult;
        #[namespace = "kuzu::processor"]
        type FlatTuple;

        #[namespace = "kuzu::common"]
        type Value;
        #[namespace = "kuzu::common"]
        type NodeVal;
        #[namespace = "kuzu::common"]
        type internalID_t;

        fn new_database(
            databasePath: &CxxString,
            bufferPoolSize: u64,
        ) -> Result<UniquePtr<Database>>;

        fn database_set_logging_level(database: Pin<&mut Database>, level: &CxxString);

        fn database_connect(database: Pin<&mut Database>) -> Result<UniquePtr<Connection>>;

        fn connection_prepare(
            connection: Pin<&mut Connection>,
            query: &CxxString,
        ) -> Result<UniquePtr<PreparedStatement>>;

        fn connection_execute(
            connection: Pin<&mut Connection>,
            query: Pin<&mut PreparedStatement>,
            /*
             TODO: Will need to just implement a value constructor for each C++ value,
             convert them on the Rust side, and pass a Vec<UniquePtr<Value>> to C++ for the values.
            param_args: &Vec<String>,
            param_values: &Vec<Value>,
            */
        ) -> Result<UniquePtr<QueryResult>>;

        fn connection_get_max_num_threads_for_exec(connection: Pin<&mut Connection>) -> u64;
        fn connection_set_max_num_threads_for_exec(
            connection: Pin<&mut Connection>,
            num_threads: u64,
        );
        fn connection_begin_read_only_transaction(connection: Pin<&mut Connection>) -> Result<()>;
        fn connection_begin_write_transaction(connection: Pin<&mut Connection>) -> Result<()>;
        fn connection_commit(connection: Pin<&mut Connection>) -> Result<()>;
        fn connection_rollback(connection: Pin<&mut Connection>) -> Result<()>;
        fn connection_interrupt(connection: Pin<&mut Connection>) -> Result<()>;

        fn query_result_to_string(query_result: Pin<&mut QueryResult>) -> String;
        fn query_result_is_success(query_result: &QueryResult) -> bool;
        fn query_result_get_error_message(query_result: &QueryResult) -> String;
        fn query_result_has_next(result: &QueryResult) -> bool;
        fn query_result_get_next(result: Pin<&mut QueryResult>) -> SharedPtr<FlatTuple>;

        fn node_value_to_string(node_value: &NodeVal) -> String;
        fn value_to_string(node_value: &Value) -> String;

        fn flat_tuple_len(tuple: &FlatTuple) -> u32;
        fn flat_tuple_get_value(tuple: &FlatTuple, index: u32) -> &Value;
        fn value_get_bool(value: &Value) -> bool;
        fn value_get_int16(value: &Value) -> i16;
        fn value_get_int32(value: &Value) -> i32;
        fn value_get_int64(value: &Value) -> i64;
        fn value_get_float(value: &Value) -> f32;
        fn value_get_double(value: &Value) -> f64;
        fn value_get_string(value: &Value) -> String;
        fn value_get_interval_secs(value: &Value) -> i64;
        fn value_get_interval_micros(value: &Value) -> i32;
        fn value_get_timestamp_micros(value: &Value) -> i64;
        fn value_get_date_days(value: &Value) -> i32;
        fn value_get_data_type_id(value: &Value) -> u8;
    }
}
