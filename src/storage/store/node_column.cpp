#include "storage/store/node_column.h"

#include <memory>

#include "storage/copier/column_chunk.h"
#include "storage/copier/compression.h"
#include "storage/storage_structure/storage_structure.h"
#include "storage/store/string_node_column.h"
#include "storage/store/struct_node_column.h"
#include "storage/store/var_list_node_column.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeColumn::NodeColumn(std::unique_ptr<PhysicalMapping> physicalMapping,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
    bool requireNullColumn)
    : storageStructureID{StorageStructureID::newDataID()}, dataType{physicalMapping->logicalType()},
      dataFH{dataFH}, metadataFH{metadataFH}, bufferManager{bufferManager}, wal{wal},
      physicalMapping{std::move(physicalMapping)} {
    metadataDA = std::make_unique<InMemDiskArray<ColumnChunkMetadata>>(*metadataFH,
        StorageStructureID::newMetadataID(), metaDAHeaderInfo.dataDAHPageIdx, bufferManager, wal,
        transaction);
    numBytesPerFixedSizedValue = getDataTypeSizeInChunk(this->dataType);
    assert(numBytesPerFixedSizedValue <= BufferPoolConstants::PAGE_4KB_SIZE);
    if (requireNullColumn) {
        nullColumn = std::make_unique<NullNodeColumn>(
            metaDAHeaderInfo.nullDAHPageIdx, dataFH, metadataFH, bufferManager, wal, transaction);
    }
}

void NodeColumn::batchLookup(
    Transaction* transaction, const offset_t* nodeOffsets, size_t size, uint8_t* result) {
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
        auto cursor = PageUtils::getPageElementCursorForPos(nodeOffset, chunkMeta.numValuesPerPage);
        cursor.pageIdx += chunkMeta.pageIdx;
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            physicalMapping->getValue(frame, cursor.elemPosInPage,
                result + i * numBytesPerFixedSizedValue, numBytesPerFixedSizedValue);
        });
    }
}

void BoolNodeColumn::batchLookup(
    Transaction* transaction, const offset_t* nodeOffsets, size_t size, uint8_t* result) {
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
        auto cursor = PageUtils::getPageElementCursorForPos(nodeOffset, chunkMeta.numValuesPerPage);
        cursor.pageIdx += chunkMeta.pageIdx;
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            // De-compress bitpacked bools
            result[i] = NullMask::isNull((uint64_t*)frame, cursor.elemPosInPage);
        });
    }
}

void NodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    nullColumn->scan(transaction, nodeIDVector, resultVector);
    scanInternal(transaction, nodeIDVector, resultVector);
}

void NodeColumn::scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    if (nullColumn) {
        nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup,
            resultVector, offsetInVector);
    }
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    auto pageCursor =
        PageUtils::getPageElementCursorForPos(startOffsetInGroup, chunkMeta.numValuesPerPage);
    pageCursor.pageIdx += chunkMeta.pageIdx;
    auto numValuesToScan = endOffsetInGroup - startOffsetInGroup;
    scanUnfiltered(transaction, pageCursor, numValuesToScan, resultVector,
        chunkMeta.numValuesPerPage, offsetInVector);
}

void NodeColumn::scan(node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) {
    if (nullColumn) {
        nullColumn->scan(nodeGroupIdx, columnChunk->getNullChunk());
    }
    auto chunkMetadata = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
    FileUtils::readFromFile(dataFH->getFileInfo(), columnChunk->getData(),
        columnChunk->getNumBytes(), chunkMetadata.pageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    columnChunk->setNumValues(
        metadataDA->get(nodeGroupIdx, transaction::TransactionType::READ_ONLY).numValues);
}

void NodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    assert(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    auto offsetInNodeGroup =
        startNodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    auto pageCursor =
        PageUtils::getPageElementCursorForPos(offsetInNodeGroup, chunkMeta.numValuesPerPage);
    pageCursor.pageIdx += chunkMeta.pageIdx;
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, pageCursor, nodeIDVector->state->selVector->selectedSize,
            resultVector, chunkMeta.numValuesPerPage);
    } else {
        scanFiltered(
            transaction, pageCursor, nodeIDVector, resultVector, chunkMeta.numValuesPerPage);
    }
}

void NodeColumn::scanUnfiltered(Transaction* transaction, PageElementCursor& pageCursor,
    uint64_t numValuesToScan, ValueVector* resultVector, uint64_t numValuesPerPage,
    uint64_t startPosInVector) {
    uint64_t numValuesScanned = 0;
    while (numValuesScanned < numValuesToScan) {
        // This won't work with compressed data. We need a new way of accessing the values
        // Really all we need is to have readValuesFromPage return the number of values found in
        // that page.
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
            physicalMapping->readValuesFromPage(frame, pageCursor, resultVector,
                numValuesScanned + startPosInVector, numValuesToScanInPage);
        });
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
    }
}

void NodeColumn::scanFiltered(Transaction* transaction, PageElementCursor& pageCursor,
    ValueVector* nodeIDVector, ValueVector* resultVector, uint64_t numValuesPerPage) {
    auto numValuesToScan = nodeIDVector->state->getOriginalSize();
    auto numValuesScanned = 0u;
    auto posInSelVector = 0u;
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        if (StorageStructure::isInRange(
                nodeIDVector->state->selVector->selectedPositions[posInSelVector], numValuesScanned,
                numValuesScanned + numValuesToScanInPage)) {
            readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
                physicalMapping->readValuesFromPage(
                    frame, pageCursor, resultVector, numValuesScanned, numValuesToScanInPage);
            });
        }
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
        while (
            posInSelVector < nodeIDVector->state->selVector->selectedSize &&
            nodeIDVector->state->selVector->selectedPositions[posInSelVector] < numValuesScanned) {
            posInSelVector++;
        }
    }
}

void NodeColumn::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    nullColumn->lookup(transaction, nodeIDVector, resultVector);
    lookupInternal(transaction, nodeIDVector, resultVector);
}

void NodeColumn::lookupInternal(
    transaction::Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (nodeIDVector->isNull(pos)) {
            continue;
        }
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        lookupValue(transaction, nodeOffset, resultVector, pos);
    }
}

void NodeColumn::lookupValue(transaction::Transaction* transaction, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    auto pageCursor = PageUtils::getPageElementCursorForPos(nodeOffset, chunkMeta.numValuesPerPage);
    pageCursor.pageIdx += chunkMeta.pageIdx;
    readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
        physicalMapping->readValuesFromPage(
            frame, pageCursor, resultVector, posInVector, 1 /* numValuesToRead */);
    });
}

void NodeColumn::readFromPage(
    Transaction* transaction, page_idx_t pageIdx, const std::function<void(uint8_t*)>& func) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *dataFH, pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
}

page_idx_t NodeColumn::append(
    ColumnChunk* columnChunk, page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    // Main column chunk.
    page_idx_t numPagesFlushed = 0;
    auto metadata = columnChunk->flushBuffer(dataFH, startPageIdx);
    metadataDA->resize(nodeGroupIdx + 1);
    metadataDA->update(nodeGroupIdx, metadata);
    numPagesFlushed += metadata.numPages;
    startPageIdx += metadata.numPages;
    // Null column chunk.
    auto numPagesForNullChunk =
        nullColumn->append(columnChunk->getNullChunk(), startPageIdx, nodeGroupIdx);
    numPagesFlushed += numPagesForNullChunk;
    startPageIdx += numPagesForNullChunk;
    // Children column chunks.
    assert(childrenColumns.size() == columnChunk->getNumChildren());
    for (auto i = 0u; i < childrenColumns.size(); i++) {
        auto numPagesForChild =
            childrenColumns[i]->append(columnChunk->getChild(i), startPageIdx, nodeGroupIdx);
        numPagesFlushed += numPagesForChild;
        startPageIdx += numPagesForChild;
    }
    return numPagesFlushed;
}

void NodeColumn::write(ValueVector* nodeIDVector, ValueVector* vectorToWriteFrom) {
    if (nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset =
            nodeIDVector->readNodeOffset(nodeIDVector->state->selVector->selectedPositions[0]);
        writeInternal(nodeOffset, vectorToWriteFrom,
            vectorToWriteFrom->state->selVector->selectedPositions[0]);
    } else if (nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset =
            nodeIDVector->readNodeOffset(nodeIDVector->state->selVector->selectedPositions[0]);
        auto lastPos = vectorToWriteFrom->state->selVector->selectedSize - 1;
        writeInternal(nodeOffset, vectorToWriteFrom, lastPos);
    } else if (!nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto nodeOffset =
                nodeIDVector->readNodeOffset(nodeIDVector->state->selVector->selectedPositions[i]);
            writeInternal(nodeOffset, vectorToWriteFrom,
                vectorToWriteFrom->state->selVector->selectedPositions[0]);
        }
    } else if (!nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto nodeOffset = nodeIDVector->readNodeOffset(pos);
            writeInternal(nodeOffset, vectorToWriteFrom, pos);
        }
    }
}

void NodeColumn::writeInternal(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    nullColumn->writeInternal(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    bool isNull = vectorToWriteFrom->isNull(posInVectorToWriteFrom);
    if (isNull) {
        return;
    }
    writeValue(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
}

void NodeColumn::writeValue(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto walPageInfo = createWALVersionOfPageForValue(nodeOffset);
    try {
        physicalMapping->writeValueToPage(
            walPageInfo.frame, walPageInfo.posInPage, vectorToWriteFrom, posInVectorToWriteFrom);
    } catch (Exception& e) {
        bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
        dataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
        throw;
    }
    bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
    dataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
}

WALPageIdxPosInPageAndFrame NodeColumn::createWALVersionOfPageForValue(offset_t nodeOffset) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
    auto originalPageCursor = PageUtils::getPageElementCursorForPos(
        nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx),
        chunkMeta.numValuesPerPage);
    originalPageCursor.pageIdx += chunkMeta.pageIdx;
    bool insertingNewPage = false;
    if (originalPageCursor.pageIdx >= dataFH->getNumPages()) {
        assert(originalPageCursor.pageIdx == dataFH->getNumPages());
        StorageStructureUtils::insertNewPage(*dataFH, storageStructureID, *bufferManager, *wal);
        insertingNewPage = true;
    }
    auto walPageIdxAndFrame =
        StorageStructureUtils::createWALVersionIfNecessaryAndPinPage(originalPageCursor.pageIdx,
            insertingNewPage, *dataFH, storageStructureID, *bufferManager, *wal);
    return {walPageIdxAndFrame, originalPageCursor.elemPosInPage};
}

void NodeColumn::setNull(offset_t nodeOffset) {
    if (nullColumn) {
        nullColumn->setNull(nodeOffset);
    }
}

void NodeColumn::checkpointInMemory() {
    metadataDA->checkpointInMemoryIfNecessary();
    for (auto& child : childrenColumns) {
        child->checkpointInMemory();
    }
    if (nullColumn) {
        nullColumn->checkpointInMemory();
    }
}

void NodeColumn::rollbackInMemory() {
    metadataDA->rollbackInMemoryIfNecessary();
    for (auto& child : childrenColumns) {
        child->rollbackInMemory();
    }
    if (nullColumn) {
        nullColumn->rollbackInMemory();
    }
}

void NodeColumn::populateWithDefaultVal(const catalog::Property& property,
    kuzu::storage::NodeColumn* nodeColumn, common::ValueVector* defaultValueVector,
    uint64_t numNodeGroups) {
    auto columnChunk = ColumnChunkFactory::createColumnChunk(
        *property.getDataType(), nullptr /* copyDescription */);
    columnChunk->populateWithDefaultVal(defaultValueVector);
    for (auto i = 0u; i < numNodeGroups; i++) {
        auto numPages = columnChunk->getNumPages();
        auto startPageIdx = dataFH->addNewPages(numPages);
        nodeColumn->append(columnChunk.get(), startPageIdx, i);
    }
}

// Page size must be aligned to 8 byte chunks for the 64-bit NullMask algorithms to work
// without the possibility of memory errors from reading/writing off the end of a page.
static_assert(PageUtils::getNumElementsInAPage(1, false /*requireNullColumn*/) % 8 == 0);

BoolNodeColumn::BoolNodeColumn(const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
    BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction,
    bool requireNullColumn)
    : NodeColumn{std::make_unique<CompressedMapping>(std::make_unique<BoolCompression>()),
          metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction,
          requireNullColumn} {}

NullNodeColumn::NullNodeColumn(page_idx_t metaDAHPageIdx, BMFileHandle* dataFH,
    BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction)
    : NodeColumn{std::make_unique<NullMapping>(), MetadataDAHInfo{metaDAHPageIdx}, dataFH,
          metadataFH, bufferManager, wal, transaction, false /*requireNullColumn*/} {}

void NullNodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    scanInternal(transaction, nodeIDVector, resultVector);
}

void NullNodeColumn::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    lookupInternal(transaction, nodeIDVector, resultVector);
}

page_idx_t NullNodeColumn::append(
    ColumnChunk* columnChunk, page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    auto metadata = columnChunk->flushBuffer(dataFH, startPageIdx);
    metadataDA->resize(nodeGroupIdx + 1);
    metadataDA->update(nodeGroupIdx, metadata);
    return metadata.numPages;
}

void NullNodeColumn::setNull(offset_t nodeOffset) {
    auto walPageInfo = createWALVersionOfPageForValue(nodeOffset);
    try {
        NullMask::setNull((uint64_t*)walPageInfo.frame, walPageInfo.posInPage, true);
    } catch (Exception& e) {
        bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
        dataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
        throw;
    }
    bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
    dataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
}

void NullNodeColumn::writeInternal(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    writeValue(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
}

SerialNodeColumn::SerialNodeColumn(const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
    BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction)
    : NodeColumn{std::make_unique<FixedValueMapping>(LogicalType(LogicalTypeID::SERIAL)),
          metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction, false} {}

void SerialNodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    // Serial column cannot contain null values.
    for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset = nodeIDVector->readNodeOffset(pos);
        assert(!resultVector->isNull(pos));
        resultVector->setValue<offset_t>(pos, offset);
    }
}

void SerialNodeColumn::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    // Serial column cannot contain null values.
    for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset = nodeIDVector->readNodeOffset(pos);
        resultVector->setValue<offset_t>(pos, offset);
    }
}

page_idx_t SerialNodeColumn::append(
    ColumnChunk* columnChunk, page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    metadataDA->resize(nodeGroupIdx + 1);
    return 0;
}

std::unique_ptr<NodeColumn> NodeColumnFactory::createNodeColumn(const LogicalType& dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        return std::make_unique<BoolNodeColumn>(
            metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
    }
    case LogicalTypeID::INT64: {
        return std::make_unique<NodeColumn>(
            std::make_unique<CompressedMapping>(
                std::make_unique<IntegerBitpacking<int64_t, uint64_t>>()),
            metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
    }
    case LogicalTypeID::INT32: {
        return std::make_unique<NodeColumn>(
            std::make_unique<CompressedMapping>(
                std::make_unique<IntegerBitpacking<int32_t, uint32_t>>()),
            metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
    }
    case LogicalTypeID::INT16: /* {
         return std::make_unique<NodeColumn>(
             std::make_unique<CompressedMapping>(
                 std::make_unique<IntegerBitpacking<int16_t, uint16_t>>()),
             metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
     }*/
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::FIXED_LIST: {
        return std::make_unique<NodeColumn>(std::make_unique<FixedValueMapping>(dataType),
            metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
    }
    case LogicalTypeID::INTERNAL_ID: {
        return std::make_unique<NodeColumn>(std::make_unique<InternalIDMapping>(), metaDAHeaderInfo,
            dataFH, metadataFH, bufferManager, wal, transaction);
    }
    case LogicalTypeID::BLOB: {
        return std::make_unique<StringNodeColumn>(std::make_unique<FixedValueMapping>(dataType),
            metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
    }
    case LogicalTypeID::STRING: {
        return std::make_unique<StringNodeColumn>(std::make_unique<CopyString>(), metaDAHeaderInfo,
            dataFH, metadataFH, bufferManager, wal, transaction);
    }
    case LogicalTypeID::MAP:
    case LogicalTypeID::VAR_LIST: {
        return std::make_unique<VarListNodeColumn>(std::make_unique<FixedValueMapping>(dataType),
            metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
    }
    case LogicalTypeID::UNION:
    case LogicalTypeID::STRUCT: {
        return std::make_unique<StructNodeColumn>(std::make_unique<FixedValueMapping>(dataType),
            metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
    }
    case LogicalTypeID::SERIAL: {
        return std::make_unique<SerialNodeColumn>(
            metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
    }
    default: {
        throw NotImplementedException("NodeColumnFactory::createNodeColumn");
    }
    }
}

NodeColumn::~NodeColumn() = default;

} // namespace storage
} // namespace kuzu
