#include "kuzu_rs.h"

using kuzu::common::Interval;
using kuzu::main::Connection;
using kuzu::main::Database;
using kuzu::main::SystemConfig;

namespace kuzu_rs {

std::unique_ptr<Database> new_database(
    const std::string& databasePath, const long unsigned int bufferPoolSize) {
    auto systemConfig = SystemConfig();
    if (bufferPoolSize > 0) {
        systemConfig.bufferPoolSize = bufferPoolSize;
    }
    return std::make_unique<Database>(databasePath, systemConfig);
}

void database_set_logging_level(Database& database, const std::string& level) {
    database.setLoggingLevel(level);
}

std::unique_ptr<kuzu::main::Connection> database_connect(kuzu::main::Database& database) {
    return std::make_unique<Connection>(&database);
}

std::unique_ptr<kuzu::main::QueryResult> connection_execute(kuzu::main::Connection& connection, kuzu::main::PreparedStatement&
                                                                                                    query /*, const rust::Vec<rust::String> &paramKeys, const rust::Vec<kuzu::common::Value> &paramValues*/) {
    std::unordered_map<std::string, std::shared_ptr<kuzu::common::Value>> params;
    /*
    for (auto i = 0u; i < paramKeys.size(); i++) {
        params[paramKeys[i]()] = std::make_shared<kuzu::common::Value>(paramValues[i]);
    }*/
    return connection.executeWithParams(&query, params);
}

rust::String prepared_statement_error_message(const kuzu::main::PreparedStatement& statement) {
    return rust::String(statement.getErrorMessage());
}

rust::String query_result_to_string(kuzu::main::QueryResult& result) {
    return rust::String(result.toString());
}

rust::String query_result_get_error_message(const kuzu::main::QueryResult& result) {
    return rust::String(result.getErrorMessage());
}

rust::String node_value_to_string(const kuzu::common::NodeVal& val) {
    return rust::String(val.toString());
}
rust::String value_to_string(const kuzu::common::Value& val) {
    return rust::String(val.toString());
}

uint32_t flat_tuple_len(const kuzu::processor::FlatTuple& flatTuple) {
    return flatTuple.len();
}
const kuzu::common::Value& flat_tuple_get_value(
    const kuzu::processor::FlatTuple& flatTuple, uint32_t index) {
    return *flatTuple.getValue(index);
}

rust::String value_get_string(const kuzu::common::Value& value) {
    return rust::String(value.getValue<std::string>());
}
int64_t value_get_interval_secs(const kuzu::common::Value& value) {
    auto interval = value.getValue<kuzu::common::interval_t>();
    return (interval.months * Interval::DAYS_PER_MONTH + interval.days) * Interval::HOURS_PER_DAY *
               Interval::MINS_PER_HOUR * Interval::SECS_PER_MINUTE
           // Include extra microseconds with the seconds
           + interval.micros / Interval::MICROS_PER_SEC;
}
int32_t value_get_interval_micros(const kuzu::common::Value& value) {
    auto interval = value.getValue<kuzu::common::interval_t>();
    return interval.micros % Interval::MICROS_PER_SEC;
}
int32_t value_get_date_days(const kuzu::common::Value& value) {
    return value.getValue<kuzu::common::date_t>().days;
}
int64_t value_get_timestamp_micros(const kuzu::common::Value& value) {
    return value.getValue<kuzu::common::timestamp_t>().value;
}
kuzu::common::LogicalTypeID value_get_data_type_id(const kuzu::common::Value& value) {
    return value.getDataType().getLogicalTypeID();
}

} // namespace kuzu_rs
