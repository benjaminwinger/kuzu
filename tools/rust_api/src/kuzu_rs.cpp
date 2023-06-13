#include "kuzu_rs.h"

using kuzu::common::FixedListTypeInfo;
using kuzu::common::Interval;
using kuzu::common::LogicalType;
using kuzu::common::LogicalTypeID;
using kuzu::common::StructField;
using kuzu::common::VarListTypeInfo;
using kuzu::main::Connection;
using kuzu::main::Database;
using kuzu::main::SystemConfig;

namespace kuzu_rs {

std::unique_ptr<QueryParams> new_params() {
    return std::make_unique<QueryParams>();
}

std::unique_ptr<LogicalType> create_logical_type(kuzu::common::LogicalTypeID id) {
    return std::make_unique<LogicalType>(id);
}
std::unique_ptr<LogicalType> create_logical_type_var_list(std::unique_ptr<LogicalType> childType) {
    return std::make_unique<LogicalType>(
        LogicalTypeID::VAR_LIST, std::make_unique<VarListTypeInfo>(std::move(childType)));
}

std::unique_ptr<LogicalType> create_logical_type_fixed_list(
    std::unique_ptr<LogicalType> childType, uint64_t numElements) {
    return std::make_unique<LogicalType>(LogicalTypeID::FIXED_LIST,
        std::make_unique<FixedListTypeInfo>(std::move(childType), numElements));
}

std::unique_ptr<kuzu::common::LogicalType> create_logical_type_struct(
    const rust::Vec<rust::String>& fieldNames, std::unique_ptr<TypeListBuilder> fieldTypes) {
    std::vector<std::unique_ptr<StructField>> fields;
    for (auto i = 0; i < fieldNames.size(); i++) {
        fields.push_back(std::make_unique<StructField>(
            std::string(fieldNames[i]), std::move(fieldTypes->types[i])));
    }
    return std::make_unique<LogicalType>(
        LogicalTypeID::STRUCT, std::make_unique<kuzu::common::StructTypeInfo>(std::move(fields)));
}

const LogicalType& logical_type_get_var_list_child_type(const LogicalType& logicalType) {
    return *kuzu::common::VarListType::getChildType(&logicalType);
}
const LogicalType& logical_type_get_fixed_list_child_type(const LogicalType& logicalType) {
    return *kuzu::common::FixedListType::getChildType(&logicalType);
}
uint64_t logical_type_get_fixed_list_num_elements(const LogicalType& logicalType) {
    return kuzu::common::FixedListType::getNumElementsInList(&logicalType);
}

rust::Vec<rust::String> logical_type_get_struct_field_names(
    const kuzu::common::LogicalType& value) {
    rust::Vec<rust::String> names;
    for (auto name : kuzu::common::StructType::getFieldNames(&value)) {
        names.push_back(name);
    }
    return names;
}

std::unique_ptr<std::vector<kuzu::common::LogicalType>> logical_type_get_struct_field_types(const kuzu::common::LogicalType& value) {
    std::vector<kuzu::common::LogicalType> result;
    for (auto type : kuzu::common::StructType::getFieldTypes(&value)) {
        result.push_back(*type);
    }
    return std::make_unique<std::vector<LogicalType>>(result);
}

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

std::unique_ptr<kuzu::main::QueryResult> connection_execute(kuzu::main::Connection& connection,
    kuzu::main::PreparedStatement& query, std::unique_ptr<QueryParams> params) {
    return connection.executeWithParams(&query, params->inputParams);
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
std::array<uint64_t, 2> value_get_internal_id(const kuzu::common::Value& value) {
    auto internalID = value.getValue<kuzu::common::internalID_t>();
    return std::array{internalID.offset, internalID.tableID};
}

std::unique_ptr<ValueList> value_get_list(const kuzu::common::Value& value) {
    return std::make_unique<ValueList>(value.getListValReference());
}
kuzu::common::LogicalTypeID value_get_data_type_id(const kuzu::common::Value& value) {
    return value.getDataType().getLogicalTypeID();
}
std::unique_ptr<LogicalType> value_get_data_type(const kuzu::common::Value& value) {
    return std::make_unique<LogicalType>(value.getDataType());
}

std::unique_ptr<kuzu::common::Value> create_value_string(const rust::String& value) {
    return std::make_unique<kuzu::common::Value>(std::string(value));
}
std::unique_ptr<kuzu::common::Value> create_value_timestamp(const int64_t timestamp) {
    return std::make_unique<kuzu::common::Value>(kuzu::common::timestamp_t(timestamp));
}
std::unique_ptr<kuzu::common::Value> create_value_date(const int64_t date) {
    return std::make_unique<kuzu::common::Value>(kuzu::common::date_t(date));
}
std::unique_ptr<kuzu::common::Value> create_value_interval(
    const int32_t months, const int32_t days, const int64_t micros) {
    return std::make_unique<kuzu::common::Value>(kuzu::common::interval_t(months, days, micros));
}
std::unique_ptr<kuzu::common::Value> create_value_null(std::unique_ptr<kuzu::common::LogicalType> typ) {
    return std::make_unique<kuzu::common::Value>(
        kuzu::common::Value::createNullValue(kuzu::common::LogicalType(*typ)));
}

std::unique_ptr<kuzu::common::Value> get_list_value(
    std::unique_ptr<kuzu::common::LogicalType> typ, std::unique_ptr<ValueListBuilder> value) {
    return std::make_unique<kuzu::common::Value>(std::move(*typ.get()), std::move(value->values));
}

std::unique_ptr<ValueListBuilder> create_list() {
    return std::make_unique<ValueListBuilder>();
}

std::unique_ptr<TypeListBuilder> create_type_list() {
    return std::make_unique<TypeListBuilder>();
}

} // namespace kuzu_rs
