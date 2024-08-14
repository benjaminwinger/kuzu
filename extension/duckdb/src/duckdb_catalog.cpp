#include "duckdb_catalog.h"

#include "binder/bound_attach_info.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "duckdb_scan.h"
#include "duckdb_storage.h"
#include "duckdb_table_catalog_entry.h"
#include "duckdb_type_converter.h"

namespace kuzu {
namespace duckdb_extension {

DuckDBCatalog::DuckDBCatalog(std::string dbPath, std::string catalogName,
    std::string defaultSchemaName, main::ClientContext* context, const DuckDBConnector& connector,
    const binder::AttachOption& attachOption)
    : CatalogExtension{}, dbPath{std::move(dbPath)}, catalogName{std::move(catalogName)},
      defaultSchemaName{std::move(defaultSchemaName)},
      tableNamesVector{common::LogicalType::STRING(), context->getMemoryManager()},
      connector{connector} {
    skipUnsupportedTable = DuckDBStorageExtension::SKIP_UNSUPPORTED_TABLE_DEFAULT_VAL;
    auto& options = attachOption.options;
    if (options.contains(DuckDBStorageExtension::SKIP_UNSUPPORTED_TABLE_KEY)) {
        auto val = options.at(DuckDBStorageExtension::SKIP_UNSUPPORTED_TABLE_KEY);
        if (val.getDataType().getLogicalTypeID() != common::LogicalTypeID::BOOL) {
            throw common::RuntimeException{common::stringFormat("Invalid option value for {}",
                DuckDBStorageExtension::SKIP_UNSUPPORTED_TABLE_KEY)};
        }
        skipUnsupportedTable = val.getValue<bool>();
    }
}

void DuckDBCatalog::init() {
    auto query = common::stringFormat(
        "select table_name from information_schema.tables where table_catalog = '{}' and "
        "table_schema = '{}';",
        catalogName, defaultSchemaName);
    auto result = connector.executeQuery(query);
    std::unique_ptr<duckdb::DataChunk> resultChunk;
    try {
        resultChunk = result->Fetch();
    } catch (std::exception& e) {
        throw common::BinderException(e.what());
    }
    if (resultChunk == nullptr || resultChunk->size() == 0) {
        return;
    }
    duckdb_conversion_func_t conversionFunc;
    getDuckDBVectorConversionFunc(common::PhysicalTypeID::STRING, conversionFunc);
    conversionFunc(resultChunk->data[0], tableNamesVector, resultChunk->size());
    for (auto i = 0u; i < resultChunk->size(); i++) {
        auto tableName = tableNamesVector.getValue<common::ku_string_t>(i).getAsString();
        createForeignTable(tableName);
    }
}

static std::string getQuery(const binder::BoundCreateTableInfo& info) {
    auto extraInfo = info.extraInfo->constPtrCast<BoundExtraCreateDuckDBTableInfo>();
    return common::stringFormat("SELECT * FROM \"{}\".{}.{}", extraInfo->catalogName,
        extraInfo->schemaName, info.tableName);
}

void DuckDBCatalog::createForeignTable(const std::string& tableName) {
    auto tableID = tables->assignNextOID();
    auto info = bindCreateTableInfo(tableName);
    if (info == nullptr) {
        return;
    }
    auto extraInfo = common::ku_dynamic_cast<binder::BoundExtraCreateCatalogEntryInfo*,
        BoundExtraCreateDuckDBTableInfo*>(info->extraInfo.get());
    std::vector<common::LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    for (auto& definition : extraInfo->propertyDefinitions) {
        columnNames.push_back(definition.getName());
        columnTypes.push_back(definition.getType().copy());
    }
    DuckDBScanBindData bindData(getQuery(*info), std::move(columnTypes), std::move(columnNames),
        connector);
    auto tableEntry = std::make_unique<catalog::DuckDBTableCatalogEntry>(tables.get(),
        info->tableName, tableID, getScanFunction(std::move(bindData)));
    for (auto& definition : extraInfo->propertyDefinitions) {
        tableEntry->addProperty(definition);
    }
    tables->createEntry(&transaction::DUMMY_TRANSACTION, std::move(tableEntry));
}

static bool getTableInfo(const DuckDBConnector& connector, const std::string& tableName,
    const std::string& schemaName, const std::string& catalogName,
    std::vector<common::LogicalType>& columnTypes, std::vector<std::string>& columnNames,
    bool skipUnsupportedTable) {
    auto query =
        common::stringFormat("select data_type,column_name from information_schema.columns where "
                             "table_name = '{}' and table_schema = '{}' and table_catalog = '{}';",
            tableName, schemaName, catalogName);
    auto result = connector.executeQuery(query);
    if (result->RowCount() == 0) {
        return false;
    }
    columnTypes.reserve(result->RowCount());
    columnNames.reserve(result->RowCount());
    for (auto i = 0u; i < result->RowCount(); i++) {
        try {
            columnTypes.push_back(DuckDBTypeConverter::convertDuckDBType(
                result->GetValue(0, i).GetValue<std::string>()));
        } catch (common::BinderException& e) {
            if (skipUnsupportedTable) {
                return false;
            }
            throw;
        }
        columnNames.push_back(result->GetValue(1, i).GetValue<std::string>());
    }
    return true;
}

bool DuckDBCatalog::bindPropertyDefinitions(const std::string& tableName,
    std::vector<binder::PropertyDefinition>& propertyDefinitions) {
    std::vector<common::LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    if (!getTableInfo(connector, tableName, defaultSchemaName, catalogName, columnTypes,
            columnNames, skipUnsupportedTable)) {
        return false;
    }
    for (auto i = 0u; i < columnNames.size(); i++) {
        auto columnDefinition = binder::ColumnDefinition(columnNames[i], columnTypes[i].copy());
        auto propertyDefinition = binder::PropertyDefinition(std::move(columnDefinition));
        propertyDefinitions.push_back(std::move(propertyDefinition));
    }
    return true;
}

std::unique_ptr<binder::BoundCreateTableInfo> DuckDBCatalog::bindCreateTableInfo(
    const std::string& tableName) {
    std::vector<binder::PropertyDefinition> propertyDefinitions;
    if (!bindPropertyDefinitions(tableName, propertyDefinitions)) {
        return nullptr;
    }
    return std::make_unique<binder::BoundCreateTableInfo>(common::TableType::FOREIGN, tableName,
        common::ConflictAction::ON_CONFLICT_THROW,
        std::make_unique<duckdb_extension::BoundExtraCreateDuckDBTableInfo>(catalogName,
            defaultSchemaName, std::move(propertyDefinitions)));
}

} // namespace duckdb_extension
} // namespace kuzu
