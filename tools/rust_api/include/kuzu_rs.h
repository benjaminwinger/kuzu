#pragma once
#include <memory>

#include "main/kuzu.h"
// Need to explicitly import some types.
// The generated C++ wrapper code needs to be able to call sizeof on PreparedStatement,
// which it can't do when it only sees forward declarations of its components.
#include "rust/cxx.h"
#include <binder/bound_statement.h>
#include <main/prepared_statement.h>
#include <planner/logical_plan/logical_plan.h>

namespace kuzu_rs {

std::unique_ptr<kuzu::main::Database> new_database(
    const std::string& databasePath, const long unsigned int bufferPoolSize);

void database_set_logging_level(kuzu::main::Database& database, const std::string& level);

std::unique_ptr<kuzu::main::Connection> database_connect(kuzu::main::Database& database);

std::unique_ptr<kuzu::main::PreparedStatement> connection_prepare(
    kuzu::main::Connection& connection, const std::string& query);

std::unique_ptr<kuzu::main::QueryResult> connection_execute(kuzu::main::Connection& connection,
    kuzu::main::PreparedStatement&
        query /*, const rust::Vec<rust::String> &, const rust::Vec<kuzu_rs::Value> &*/);

uint64_t connection_get_max_num_threads_for_exec(kuzu::main::Connection& connection);
void connection_set_max_num_threads_for_exec(
    kuzu::main::Connection& connection, uint64_t numThreads);

void connection_begin_read_only_transaction(kuzu::main::Connection& connection);
void connection_begin_write_transaction(kuzu::main::Connection& connection);
void connection_commit(kuzu::main::Connection& connection);
void connection_rollback(kuzu::main::Connection& connection);
void connection_interrupt(kuzu::main::Connection& connection);

rust::String prepared_statement_error_message(const kuzu::main::PreparedStatement& statement);

rust::String query_result_to_string(kuzu::main::QueryResult& result);
bool query_result_is_success(const kuzu::main::QueryResult& result);
rust::String query_result_get_error_message(const kuzu::main::QueryResult& result);
bool query_result_has_next(const kuzu::main::QueryResult& result);
std::shared_ptr<kuzu::processor::FlatTuple> query_result_get_next(kuzu::main::QueryResult& result);

rust::String node_value_to_string(const kuzu::common::NodeVal& val);
rust::String value_to_string(const kuzu::common::Value& val);

uint32_t flat_tuple_len(const kuzu::processor::FlatTuple& flatTuple);
const kuzu::common::Value& flat_tuple_get_value(
    const kuzu::processor::FlatTuple& flatTuple, uint32_t index);
bool value_get_bool(const kuzu::common::Value& value);
int16_t value_get_int16(const kuzu::common::Value& value);
int32_t value_get_int32(const kuzu::common::Value& value);
int64_t value_get_int64(const kuzu::common::Value& value);
float value_get_float(const kuzu::common::Value& value);
double value_get_double(const kuzu::common::Value& value);
rust::String value_get_string(const kuzu::common::Value& value);
int64_t value_get_interval_secs(const kuzu::common::Value& value);
int32_t value_get_interval_micros(const kuzu::common::Value& value);
int32_t value_get_date_days(const kuzu::common::Value& value);
int64_t value_get_timestamp_micros(const kuzu::common::Value& value);
uint8_t value_get_data_type_id(const kuzu::common::Value& value);

} // namespace kuzu_rs
