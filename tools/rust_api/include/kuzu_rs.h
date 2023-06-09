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

    template<typename T>
    void insert(const rust::Str key, const T value) {
        inputParams.insert(std::make_pair(key, std::make_shared<kuzu::common::Value>(value)));
    }
    void insert_string(const rust::Str key, const rust::String& value) {
        inputParams.insert(
            std::make_pair(key, std::make_shared<kuzu::common::Value>(std::string(value))));
    }

    void insert_timestamp(const rust::Str key, const int64_t timestamp) {
        inputParams.insert(std::make_pair(
            key, std::make_shared<kuzu::common::Value>(kuzu::common::timestamp_t(timestamp))));
    }
    void insert_date(const rust::Str key, const int64_t date) {
        inputParams.insert(
            std::make_pair(key, std::make_shared<kuzu::common::Value>(kuzu::common::date_t(date))));
    }

    void insert_interval(
        const rust::Str key, const int32_t months, const int32_t days, const int64_t micros) {
        inputParams.insert(std::make_pair(key,
            std::make_shared<kuzu::common::Value>(kuzu::common::interval_t(months, days, micros))));
    }
};

std::unique_ptr<QueryParams> new_params();

// Simple wrapper for vector of unique_ptr since cxx doesn't support functions returning a vector of
// unique_ptr
struct ValueList {
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
rust::String value_to_string(const kuzu::common::Value& val);

} // namespace kuzu_rs
