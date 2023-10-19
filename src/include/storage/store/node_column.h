#pragma once

#include "catalog/catalog.h"
#include "storage/stats/metadata_dah_info.h"
#include "storage/stats/property_statistics.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

struct CompressionMetadata;

using read_values_to_vector_func_t = std::function<void(uint8_t* frame,
    PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
    uint32_t numValuesToRead, const CompressionMetadata& metadata)>;
using write_values_from_vector_func_t = std::function<void(uint8_t* frame, uint16_t posInFrame,
    common::ValueVector* vector, uint32_t posInVector, const CompressionMetadata& metadata)>;
using write_values_func_t = std::function<void(uint8_t* frame, uint16_t posInFrame,
    const uint8_t* data, common::offset_t dataOffset, common::offset_t numValues,
    const CompressionMetadata& metadata)>;

using read_values_to_page_func_t =
    std::function<void(uint8_t* frame, PageElementCursor& pageCursor, uint8_t* result,
        uint32_t posInResult, uint64_t numValues, const CompressionMetadata& metadata)>;
// This is a special usage for the `batchLookup` interface.
using batch_lookup_func_t = read_values_to_page_func_t;

class NullNodeColumn;
class StructNodeColumn;
// TODO(Guodong): This is intentionally duplicated with `Column`, as for now, we don't change rel
// tables. `Column` is used for rel tables only. Eventually, we should remove `Column`.
class BaseNodeColumn {
public:
    BaseNodeColumn(common::LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats PropertyStatistics,
        bool enableCompression, bool requireNullColumn = true);
    virtual ~BaseNodeColumn() = default;

    virtual void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx);

    virtual void setNull(common::offset_t nodeOffset);

    inline const common::LogicalType& getDataType() const { return dataType; }
    inline uint32_t getNumBytesPerValue() const { return numBytesPerFixedSizedValue; }
    inline uint64_t getNumNodeGroups(transaction::Transaction* transaction) const {
        return metadataDA->getNumElements(transaction->getType());
    }

    virtual void checkpointInMemory();
    virtual void rollbackInMemory();

    void populateWithDefaultVal(const catalog::Property& property, NodeColumn* nodeColumn,
        common::ValueVector* defaultValueVector, uint64_t numNodeGroups) const;

    inline ColumnChunkMetadata getMetadata(
        common::node_group_idx_t nodeGroupIdx, transaction::TransactionType transaction) const {
        return metadataDA->get(nodeGroupIdx, transaction);
    }

    virtual void scan(common::node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk);
    virtual void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup, uint8_t* result);

protected:
    void scanUnfiltered(transaction::Transaction* transaction, PageElementCursor& pageCursor,
        uint64_t numValuesToScan, common::ValueVector* resultVector,
        const CompressionMetadata& compMeta, uint64_t startPosInVector = 0);
    void scanFiltered(transaction::Transaction* transaction, PageElementCursor& pageCursor,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector,
        const CompressionMetadata& compMeta);

    void readFromPage(transaction::Transaction* transaction, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);

    // Produces a page cursor for the offset relative to the given node group
    PageElementCursor getPageCursorForOffsetInGroup(transaction::TransactionType transactionType,
        common::offset_t nodeOffset, common::node_group_idx_t nodeGroupIdx);

protected:
    StorageStructureID storageStructureID;
    common::LogicalType dataType;
    // TODO(bmwinger): Remove. Only used by var_list_column_chunk for something which should be
    // rewritten
    uint32_t numBytesPerFixedSizedValue;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    BufferManager* bufferManager;
    WAL* wal;
    std::unique_ptr<InMemDiskArray<ColumnChunkMetadata>> metadataDA;
    std::unique_ptr<NodeColumn> nullColumn;
    read_values_to_vector_func_t readToVectorFunc;
    write_values_from_vector_func_t writeFromVectorFunc;
    write_values_func_t writeFunc;
    read_values_to_page_func_t readToPageFunc;
    batch_lookup_func_t batchLookupFunc;
    RWPropertyStats propertyStatistics;
    bool enableCompression;
};

// NodeColumn where we assume it the underlying storage always stores NodeGroupSize values
// Data is indexed using a global offset (which is internally used to find the node group via the
// node group size)
class NodeColumn : public BaseNodeColumn {
public:
    NodeColumn(common::LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
        bool enableCompression, bool requireNullColumn = true)
        : BaseNodeColumn{std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager,
              wal, transaction, propertyStatistics, enableCompression, requireNullColumn} {}

    // Expose for feature store
    virtual void batchLookup(transaction::Transaction* transaction,
        const common::offset_t* nodeOffsets, size_t size, uint8_t* result);

    virtual void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);
    virtual void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector);
    inline void scan(common::node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) override {
        BaseNodeColumn::scan(nodeGroupIdx, columnChunk);
    }
    virtual void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    virtual void write(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom);

protected:
    virtual void writeValue(const ColumnChunkMetadata& chunkMeta, common::offset_t nodeOffset,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom);
    virtual void writeValue(
        const ColumnChunkMetadata& chunkMeta, common::offset_t nodeOffset, const uint8_t* data);

    virtual void scanInternal(transaction::Transaction* transaction,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void lookupInternal(transaction::Transaction* transaction,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void lookupValue(transaction::Transaction* transaction, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t posInVector);

    // TODO(Guodong): This is mostly duplicated with
    // StorageStructure::createWALVersionOfPageIfNecessaryForElement(). Should be cleared later.
    WALPageIdxPosInPageAndFrame createWALVersionOfPageForValue(common::offset_t nodeOffset);

    // Produces a page cursor for the absolute node offset
    PageElementCursor getPageCursorForOffset(
        transaction::TransactionType transactionType, common::offset_t nodeOffset);
    // Produces a page cursor for the absolute node offset, given a node group
    PageElementCursor getPageCursorForOffsetAndGroup(transaction::TransactionType transactionType,
        common::offset_t nodeOffset, common::node_group_idx_t nodeGroupIdx);
};

// NodeColumn for data adjacent to a NodeGroup
// Data is indexed using the node group identifier and the offset within the node group
class AuxiliaryNodeColumn : public BaseNodeColumn {
public:
    AuxiliaryNodeColumn(common::LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
        bool enableCompression, bool requireNullColumn = true)
        : BaseNodeColumn{std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager,
              wal, transaction, propertyStatistics, enableCompression, requireNullColumn} {}

    // Append values to the end of the node group, resizing it if necessary
    common::offset_t appendValues(
        common::node_group_idx_t nodeGroupIdx, const uint8_t* data, common::offset_t numValues);

protected:
};

struct NodeColumnFactory {
    static std::unique_ptr<NodeColumn> createNodeColumn(const common::LogicalType& dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression);
};

} // namespace storage
} // namespace kuzu
