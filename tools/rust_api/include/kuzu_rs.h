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

struct QueryParams {
    std::unordered_map<std::string, std::shared_ptr<kuzu::common::Value>> inputParams;

    void insert(const rust::Str key, std::unique_ptr<kuzu::common::Value> value) {
        inputParams.insert(std::make_pair(key, std::move(value)));
    }
};

std::unique_ptr<QueryParams> new_params();

std::unique_ptr<kuzu::common::LogicalType> create_logical_type(kuzu::common::LogicalTypeID id);
std::unique_ptr<kuzu::common::LogicalType> create_logical_type_var_list(
    std::unique_ptr<kuzu::common::LogicalType> childType);
std::unique_ptr<kuzu::common::LogicalType> create_logical_type_fixed_list(
    std::unique_ptr<kuzu::common::LogicalType> childType, uint64_t numElements);

const kuzu::common::LogicalType& logical_type_get_var_list_child_type(
    const kuzu::common::LogicalType& logicalType);
const kuzu::common::LogicalType& logical_type_get_fixed_list_child_type(
    const kuzu::common::LogicalType& logicalType);
uint64_t logical_type_get_fixed_list_num_elements(const kuzu::common::LogicalType& logicalType);

// Simple wrapper for vector of unique_ptr since cxx doesn't support functions returning a vector of
// unique_ptr
struct ValueList {
    ValueList(const std::vector<std::unique_ptr<kuzu::common::Value>>& values) : values(values) {}
    const std::vector<std::unique_ptr<kuzu::common::Value>>& values;
    uint64_t size() const { return values.size(); }
    const std::unique_ptr<kuzu::common::Value>& get(uint64_t index) const { return values[index]; }
};

/* Database */
std::unique_ptr<kuzu::main::Database> new_database(
    const std::string& databasePath, const long unsigned int bufferPoolSize);

void database_set_logging_level(kuzu::main::Database& database, const std::string& level);

/* Connection */
std::unique_ptr<kuzu::main::Connection> database_connect(kuzu::main::Database& database);
std::unique_ptr<kuzu::main::QueryResult> connection_execute(kuzu::main::Connection& connection,
    kuzu::main::PreparedStatement& query, std::unique_ptr<QueryParams> params);

/* PreparedStatement */
rust::String prepared_statement_error_message(const kuzu::main::PreparedStatement& statement);

/* QueryResult */
rust::String query_result_to_string(kuzu::main::QueryResult& result);
rust::String query_result_get_error_message(const kuzu::main::QueryResult& result);

/* NodeVal */
rust::String node_value_to_string(const kuzu::common::NodeVal& val);

/* FlatTuple */
const kuzu::common::Value& flat_tuple_get_value(
    const kuzu::processor::FlatTuple& flatTuple, uint32_t index);

/* Value */
rust::String value_get_string(const kuzu::common::Value& value);
int64_t value_get_interval_secs(const kuzu::common::Value& value);
int32_t value_get_interval_micros(const kuzu::common::Value& value);
int32_t value_get_date_days(const kuzu::common::Value& value);
int64_t value_get_timestamp_micros(const kuzu::common::Value& value);
std::array<uint64_t, 2> value_get_internal_id(const kuzu::common::Value& value);
std::unique_ptr<ValueList> value_get_list(const kuzu::common::Value& value);
std::unique_ptr<std::vector<std::string>> value_get_struct_names(const kuzu::common::Value& value);
kuzu::common::LogicalTypeID value_get_data_type_id(const kuzu::common::Value& value);
std::unique_ptr<kuzu::common::LogicalType> value_get_data_type(const kuzu::common::Value& value);
rust::String value_to_string(const kuzu::common::Value& val);

std::unique_ptr<kuzu::common::Value> create_value_string(const rust::String& value);
std::unique_ptr<kuzu::common::Value> create_value_timestamp(const int64_t timestamp);
std::unique_ptr<kuzu::common::Value> create_value_date(const int64_t date);
std::unique_ptr<kuzu::common::Value> create_value_interval(
    const int32_t months, const int32_t days, const int64_t micros);
// TODO: Should take a DataType, not a DataTypeID. This won't work for compound types
std::unique_ptr<kuzu::common::Value> create_value_null(kuzu::common::LogicalTypeID typ);

template<typename T>
std::unique_ptr<kuzu::common::Value> create_value(const T value) {
    return std::make_unique<kuzu::common::Value>(value);
}

struct ValueListBuilder {
    std::vector<std::unique_ptr<kuzu::common::Value>> values;

    void insert(std::unique_ptr<kuzu::common::Value> value) { values.push_back(std::move(value)); }
};

std::unique_ptr<kuzu::common::Value> get_list_value(
    std::unique_ptr<kuzu::common::LogicalType> typ, std::unique_ptr<ValueListBuilder> value);
std::unique_ptr<ValueListBuilder> create_list();

} // namespace kuzu_rs
