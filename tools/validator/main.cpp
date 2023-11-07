#include <iostream>
#include <unordered_set>

#include "catalog/catalog_content.h"
#include "main/kuzu.h"
#include "storage/storage_manager.h"
#include "storage/storage_structure/disk_array.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::storage;

namespace kuzu::validator {
void validateDatabase(std::string databasePath) {
    kuzu::catalog::CatalogContent catalog(databasePath);

    Database database(databasePath);
    auto stats = database.storageManager->getNodesStore().getNodesStatisticsAndDeletedIDs();

    auto& transaction = kuzu::transaction::DUMMY_READ_TRANSACTION;

    std::unordered_set<page_idx_t> pages;

    for (auto schema : catalog.getTableSchemas()) {
        for (auto property : schema->getProperties()) {
            auto metaDAHeaderInfo = stats->getMetadataDAHInfo(
                &transaction, schema->getTableID(), property->getPropertyID());
            InMemDiskArray<ColumnChunkMetadata> metadataDA(
                *database.storageManager->getMetadataFH(), DBFileID::newMetadataFileID(),
                metaDAHeaderInfo->dataDAHPageIdx, database.bufferManager.get(), database.wal.get(),
                &transaction);
            for (auto i = 0; i < metadataDA.getNumElements(); i++) {
                auto metadata = metadataDA.get(i, transaction.getType());
                for (auto i = 0; i < metadata.numPages; i++) {
                    auto page = metadata.pageIdx + i;
                    if (pages.contains(page)) {
                        throw std::runtime_error("Page " + std::to_string(page) +
                                                 " is claimed by another chunk's metadata!");
                    }
                    pages.insert(page);
                }
            }
        }
    }
}
} // namespace kuzu::validator

int main(int argc, const char** argv) {
    if (argc != 2) {
        std::cout << "Usage: kuzu_validator <database>" << std::endl;
    }

    kuzu::validator::validateDatabase(std::string(argv[1]));
}
