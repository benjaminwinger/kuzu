#pragma once

#include <atomic>
#include <mutex>

#include "catalog/table_schema.h"
#include "common/ser_deser.h"
#include "spdlog/spdlog.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

using lock_t = std::unique_lock<std::mutex>;
using atomic_uint64_vec_t = std::vector<std::atomic<uint64_t>>;

// TODO: hasNull should default to false
// When does hasNull get set to true? Whenever modifications are made to nullColumnChunk/NodeColumn?
// If those modifications go straight to the disk, presumably we also need to update the
// propertystatistics, but do we actually have the requisite information?
class PropertyStatistics {
public:
    PropertyStatistics() = default;
    PropertyStatistics(bool hasNull) : hasNull(hasNull) {}

    inline bool hasNullValues() const { return hasNull; }

    void serialize(common::FileInfo* fileInfo, uint64_t& offset) {
        common::SerDeser::serializeValue(hasNull, fileInfo, offset);
    }
    static PropertyStatistics deserialize(common::FileInfo* fileInfo, uint64_t& offset) {
        bool hasNull;
        common::SerDeser::deserializeValue<bool>(hasNull, fileInfo, offset);
        return PropertyStatistics(hasNull);
    }

private:
    bool hasNull = true;
};

using TablePropertyStats = std::unordered_map<common::property_id_t, PropertyStatistics>;

class TableStatistics {
    // TODO(bmwinger): Remove
    friend class TablesStatistics;

public:
    virtual ~TableStatistics() = default;

    explicit TableStatistics(const catalog::TableSchema &schema) {
        for (auto property : schema.getProperties()) {
            propertyStatistics[property->getPropertyID()] = PropertyStatistics();
        }
    }

    explicit TableStatistics(uint64_t numTuples, TablePropertyStats&& propertyStatistics)
        : numTuples{numTuples}, propertyStatistics{std::move(propertyStatistics)} {
        assert(numTuples != UINT64_MAX);
    }

    explicit TableStatistics(const TableStatistics&) = default;

    inline bool isEmpty() const { return numTuples == 0; }

    inline uint64_t getNumTuples() const { return numTuples; }

    virtual inline void setNumTuples(uint64_t numTuples_) {
        assert(numTuples_ != UINT64_MAX);
        numTuples = numTuples_;
    }

    inline PropertyStatistics& getPropertyStatistics(common::property_id_t propertyID) {
        auto result = propertyStatistics.find(propertyID);
        if (result == propertyStatistics.end()) {
            throw std::runtime_error(
                "Could not find property with id " + std::to_string(propertyID));
        }
        return result->second;
    }

    inline void setPropertyStatistics(
        common::property_id_t propertyID, PropertyStatistics newStats) {
        propertyStatistics[propertyID] = newStats;
    }

private:
    uint64_t numTuples;
    TablePropertyStats propertyStatistics;
};

struct TablesStatisticsContent {
    TablesStatisticsContent() = default;
    std::unordered_map<common::table_id_t, std::unique_ptr<TableStatistics>> tableStatisticPerTable;
};

class TablesStatistics {

public:
    TablesStatistics();

    virtual ~TablesStatistics() = default;

    virtual void setNumTuplesForTable(common::table_id_t tableID, uint64_t numTuples) = 0;

    inline void writeTablesStatisticsFileForWALRecord(const std::string& directory) {
        saveToFile(directory, common::DBFileType::WAL_VERSION, transaction::TransactionType::WRITE);
    }

    inline bool hasUpdates() { return tablesStatisticsContentForWriteTrx != nullptr; }

    inline void checkpointInMemoryIfNecessary() {
        lock_t lck{mtx};
        tablesStatisticsContentForReadOnlyTrx = std::move(tablesStatisticsContentForWriteTrx);
    }

    inline TablesStatisticsContent* getReadOnlyVersion() const {
        return tablesStatisticsContentForReadOnlyTrx.get();
    }

    inline void addTableStatistic(catalog::TableSchema* tableSchema) {
        initTableStatisticPerTableForWriteTrxIfNecessary();
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableSchema->tableID] =
            constructTableStatistic(tableSchema);
    }
    inline void removeTableStatistic(common::table_id_t tableID) {
        tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.erase(tableID);
    }

    inline uint64_t getNumTuplesForTable(common::table_id_t tableID) {
        return tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable[tableID]
            ->getNumTuples();
    }

    inline PropertyStatistics& getPropertyStatisticsForTable(
        common::table_id_t tableID, common::property_id_t propertyID) const {
        // FIXME: This is naive and will not work once we start updating the propertystatistics
        auto result = tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.find(tableID);
        if (result != tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.end()) {
            return result->second->getPropertyStatistics(propertyID);
        }
        return tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableID]
            ->getPropertyStatistics(propertyID);
    }

protected:
    virtual inline std::string getTableTypeForPrinting() const = 0;

    virtual inline std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableSchema* tableSchema) = 0;

    virtual inline std::unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) = 0;

    virtual inline std::string getTableStatisticsFilePath(
        const std::string& directory, common::DBFileType dbFileType) = 0;

    virtual std::unique_ptr<TableStatistics> deserializeTableStatistics(uint64_t numTuples,
        TablePropertyStats&& propertyStats, uint64_t& offset, common::FileInfo* fileInfo,
        uint64_t tableID) = 0;

    virtual void serializeTableStatistics(
        TableStatistics* tableStatistics, uint64_t& offset, common::FileInfo* fileInfo) = 0;

    void readFromFile(const std::string& directory);

    void saveToFile(const std::string& directory, common::DBFileType dbFileType,
        transaction::TransactionType transactionType);

    void initTableStatisticPerTableForWriteTrxIfNecessary();

protected:
    std::shared_ptr<spdlog::logger> logger;
    std::unique_ptr<TablesStatisticsContent> tablesStatisticsContentForReadOnlyTrx;
    std::unique_ptr<TablesStatisticsContent> tablesStatisticsContentForWriteTrx;
    std::mutex mtx;
};

} // namespace storage
} // namespace kuzu
