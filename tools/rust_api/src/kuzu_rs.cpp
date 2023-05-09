#include "kuzu_rs.h"

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

std::unique_ptr<kuzu::main::PreparedStatement> connection_prepare(
    kuzu::main::Connection& connection, const std::string& query) {
    return connection.prepare(query);
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

uint64_t connection_get_max_num_threads_for_exec(kuzu::main::Connection& connection) {
    return connection.getMaxNumThreadForExec();
}

void connection_set_max_num_threads_for_exec(
    kuzu::main::Connection& connection, uint64_t numThreads) {
    connection.setMaxNumThreadForExec(numThreads);
}

void connection_begin_read_only_transaction(kuzu::main::Connection& connection) {
    connection.beginReadOnlyTransaction();
}
void connection_begin_write_transaction(kuzu::main::Connection& connection) {
    connection.beginWriteTransaction();
}
void connection_commit(kuzu::main::Connection& connection) {
    connection.commit();
}
void connection_rollback(kuzu::main::Connection& connection) {
    connection.rollback();
}
void connection_interrupt(kuzu::main::Connection& connection) {
    connection.interrupt();
}

rust::String query_result_to_string(kuzu::main::QueryResult& result) {
    return rust::String(result.toString());
}

bool query_result_is_success(const kuzu::main::QueryResult& result) {
    return result.isSuccess();
}

rust::String query_result_get_error_message(const kuzu::main::QueryResult& result) {
    return rust::String(result.getErrorMessage());
}

bool query_result_has_next(const kuzu::main::QueryResult& result) {
    return result.hasNext();
}

std::shared_ptr<kuzu::processor::FlatTuple> query_result_get_next(kuzu::main::QueryResult& result) {
    return result.getNext();
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
bool value_get_bool(const kuzu::common::Value& value) {
    return value.getValue<bool>();
}
int16_t value_get_int16(const kuzu::common::Value& value) {
    return value.getValue<int16_t>();
}
int32_t value_get_int32(const kuzu::common::Value& value) {
    return value.getValue<int32_t>();
}
int64_t value_get_int64(const kuzu::common::Value& value) {
    return value.getValue<int64_t>();
}
float value_get_float(const kuzu::common::Value& value) {
    return value.getValue<float>();
}
double value_get_double(const kuzu::common::Value& value) {
    return value.getValue<double>();
}
rust::String value_get_string(const kuzu::common::Value& value) {
    return rust::String(value.getValue<std::string>());
}
uint8_t value_get_data_type_id(const kuzu::common::Value& value) {
    auto typ = value.getDataType();
    return (uint8_t)typ.getLogicalTypeID();
}

} // namespace kuzu_rs
