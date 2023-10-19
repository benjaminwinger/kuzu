#include "storage/store/string_node_column.h"

#include "storage/store/string_column_chunk.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StringNodeColumn::StringNodeColumn(LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, RWPropertyStats stats)
    : NodeColumn{std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, stats, false /* enableCompression */, true /* requireNullColumn */} {
    // TODO(bmwinger): detecting if string child columns must be re-compressed when updating is not
    // yet supported
    auto enableCompression = false;
    dataColumn = std::make_unique<AuxiliaryNodeColumn>(LogicalType(LogicalTypeID::UINT8),
        *metaDAHeaderInfo.childrenInfos[0], dataFH, metadataFH, bufferManager, wal, transaction,
        stats, enableCompression, false /*requireNullColumn*/);
    offsetColumn = std::make_unique<AuxiliaryNodeColumn>(LogicalType(LogicalTypeID::UINT64),
        *metaDAHeaderInfo.childrenInfos[1], dataFH, metadataFH, bufferManager, wal, transaction,
        stats, enableCompression, false /*requireNullColumn*/);
}

void StringNodeColumn::scanOffsets(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    string_offset_t* offsets, uint64_t index) {
    // We either need to read the next value, or store the maximum string offset at the end.
    // Otherwise we won't know what the length of the last string is.
    auto numValues = offsetColumn->getMetadata(nodeGroupIdx, transaction->getType()).numValues;
    if (index < numValues - 1) {
        offsetColumn->scan(transaction, nodeGroupIdx, index, index + 2, (uint8_t*)offsets);
    } else {
        offsetColumn->scan(transaction, nodeGroupIdx, index, index + 1, (uint8_t*)offsets);
        offsets[1] = dataColumn->getMetadata(nodeGroupIdx, transaction->getType()).numValues;
    }
}

void StringNodeColumn::scanValueToVector(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    string_offset_t startOffset, string_offset_t endOffset, ValueVector* resultVector,
    uint64_t offsetInVector) {
    assert(endOffset >= startOffset);
    // TODO: Add string to vector first and read directly instead of using a temporary buffer
    std::unique_ptr<char[]> stringRead = std::make_unique<char[]>(endOffset - startOffset);
    dataColumn->scan(transaction, nodeGroupIdx, startOffset, endOffset, (uint8_t*)stringRead.get());
    StringVector::addString(
        resultVector, offsetInVector, stringRead.get(), endOffset - startOffset);
}

void StringNodeColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);

    // scan offsets into temporary buffer
    // TODO: Would it be more efficient to reuse a small stack allocated buffer multiple times
    // instead of a large heap-allocated buffer?
    auto indices = std::make_unique<string_index_t[]>(endOffsetInGroup - startOffsetInGroup);
    BaseNodeColumn::scan(
        transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, (uint8_t*)indices.get());

    auto numValuesToRead = endOffsetInGroup - startOffsetInGroup;
    for (auto i = 0u; i < numValuesToRead; i++) {
        if (resultVector->isNull(offsetInVector + i)) {
            continue;
        }
        // scan string data from the dataColumn into a temporary string
        string_offset_t offsets[2];
        scanOffsets(transaction, nodeGroupIdx, offsets, indices[i]);
        scanValueToVector(
            transaction, nodeGroupIdx, offsets[0], offsets[1], resultVector, offsetInVector + i);
    }
}

void StringNodeColumn::scan(node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) {
    NodeColumn::scan(nodeGroupIdx, columnChunk);
    auto stringColumnChunk = reinterpret_cast<StringColumnChunk*>(columnChunk);
    dataColumn->scan(nodeGroupIdx, stringColumnChunk->getDataChunk());
    offsetColumn->scan(nodeGroupIdx, stringColumnChunk->getOffsetChunk());
}

void StringNodeColumn::append(ColumnChunk* columnChunk, node_group_idx_t nodeGroupIdx) {
    NodeColumn::append(columnChunk, nodeGroupIdx);
    auto stringColumnChunk = reinterpret_cast<StringColumnChunk*>(columnChunk);
    dataColumn->append(stringColumnChunk->getDataChunk(), nodeGroupIdx);
    offsetColumn->append(stringColumnChunk->getOffsetChunk(), nodeGroupIdx);
}

void StringNodeColumn::writeValue(const ColumnChunkMetadata& chunkMeta, offset_t nodeOffset,
    ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto& kuStr = vectorToWriteFrom->getValue<ku_string_t>(posInVectorToWriteFrom);
    // Write string data to end of dataColumn
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto startOffset =
        dataColumn->appendValues(nodeGroupIdx, (const uint8_t*)kuStr.getData(), kuStr.len);

    // Write offset
    string_index_t index =
        offsetColumn->appendValues(nodeGroupIdx, (const uint8_t*)&startOffset, 1);

    // Write index to main column
    NodeColumn::writeValue(chunkMeta, nodeOffset, (uint8_t*)&index);
}

void StringNodeColumn::checkpointInMemory() {
    BaseNodeColumn::checkpointInMemory();
    dataColumn->checkpointInMemory();
    offsetColumn->checkpointInMemory();
}

void StringNodeColumn::rollbackInMemory() {
    BaseNodeColumn::rollbackInMemory();
    dataColumn->rollbackInMemory();
    offsetColumn->rollbackInMemory();
}

void StringNodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    assert(resultVector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    assert(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    // TODO: Following NodeColumn::scanInternal, we need to take into account if the vector is
    // filtered
    // NodeColumn::scanInternal(transaction, nodeIDVector, resultVector);
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (resultVector->isNull(pos)) {
            // Ignore positions which were scanned as null
            continue;
        }
        auto offsetInGroup =
            startNodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx) + pos;
        string_index_t index;
        BaseNodeColumn::scan(
            transaction, nodeGroupIdx, offsetInGroup, offsetInGroup + 1, (uint8_t*)&index);
        string_offset_t offsets[2];
        scanOffsets(transaction, nodeGroupIdx, offsets, index);
        scanValueToVector(transaction, nodeGroupIdx, offsets[0], offsets[1], resultVector, pos);
    }
}

void StringNodeColumn::lookupInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    assert(dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        if (resultVector->isNull(pos)) {
            // Ignore positions which were scanned as null
            continue;
        }
        string_offset_t offsets[2];
        auto offsetInGroup =
            startNodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx) + pos;
        string_index_t index;
        BaseNodeColumn::scan(
            transaction, nodeGroupIdx, offsetInGroup, offsetInGroup + 1, (uint8_t*)&index);
        scanOffsets(transaction, nodeGroupIdx, offsets, index);
        scanValueToVector(transaction, nodeGroupIdx, offsets[0], offsets[1], resultVector, pos);
    }
}

} // namespace storage
} // namespace kuzu
