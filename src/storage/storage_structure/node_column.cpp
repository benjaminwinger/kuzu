#include "storage/storage_structure/node_column.h"

#include "storage/storage_structure/storage_structure.h"
#include "storage/storage_structure/struct_node_column.h"
#include "storage/storage_structure/var_sized_node_column.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void FixedSizedNodeColumnFunc::readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
    ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead) {
    auto numBytesPerValue = resultVector->getNumBytesPerValue();
    memcpy(resultVector->getData() + posInVector * numBytesPerValue,
        frame + pageCursor.elemPosInPage * numBytesPerValue, numValuesToRead * numBytesPerValue);
}

void FixedSizedNodeColumnFunc::writeValuesToPage(
    uint8_t* frame, uint16_t posInFrame, ValueVector* vector, uint32_t posInVector) {
    auto numBytesPerValue = vector->getNumBytesPerValue();
    memcpy(frame + posInFrame * numBytesPerValue,
        vector->getData() + posInVector * numBytesPerValue, numBytesPerValue);
}

void FixedSizedNodeColumnFunc::readInternalIDValuesFromPage(uint8_t* frame,
    PageElementCursor& pageCursor, ValueVector* resultVector, uint32_t posInVector,
    uint32_t numValuesToRead) {
    auto resultData = (internalID_t*)resultVector->getData();
    for (auto i = 0u; i < numValuesToRead; i++) {
        auto posInFrame = pageCursor.elemPosInPage + i;
        resultData[posInVector + i].offset = *(offset_t*)(frame + (posInFrame * sizeof(offset_t)));
    }
}

void FixedSizedNodeColumnFunc::writeInternalIDValuesToPage(
    uint8_t* frame, uint16_t posInFrame, ValueVector* vector, uint32_t posInVector) {
    auto relID = vector->getValue<relID_t>(posInVector);
    memcpy(frame + posInFrame * sizeof(offset_t), &relID.offset, sizeof(offset_t));
}

void NullNodeColumnFunc::readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
    ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead) {
    // Read bit-packed null flags from the frame into the result vector
    // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
    // Otherwise it could read off the end of the page.
    resultVector->setNullFromBits(
        (uint64_t*)frame, pageCursor.elemPosInPage, posInVector, numValuesToRead);
}

void NullNodeColumnFunc::writeValuesToPage(
    uint8_t* frame, uint16_t posInFrame, ValueVector* vector, uint32_t posInVector) {
    // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
    // Otherwise it could read off the end of the page.
    // TODO: Is this efficient for copying only a single bit?
    NullMask::copyNullMask(vector->getNullMaskData(), posInVector, (uint64_t*)frame, posInFrame, 1);
}

NodeColumn::NodeColumn(const Property& property, BMFileHandle* nodeGroupsDataFH,
    BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager, WAL* wal, bool requireNullColumn)
    : NodeColumn{property.dataType, property.metaDiskArrayHeaderInfo, nodeGroupsDataFH,
          nodeGroupsMetaFH, bufferManager, wal, requireNullColumn} {}

NodeColumn::NodeColumn(LogicalType dataType, const MetaDiskArrayHeaderInfo& metaDAHeaderInfo,
    BMFileHandle* nodeGroupsDataFH, BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager,
    WAL* wal, bool requireNullColumn)
    : storageStructureID{StorageStructureID::newNodeGroupsDataID()}, dataType{std::move(dataType)},
      nodeGroupsDataFH{nodeGroupsDataFH}, bufferManager{bufferManager}, wal{wal} {
    columnChunksMetaDA = std::make_unique<InMemDiskArray<ColumnChunkMetadata>>(*nodeGroupsMetaFH,
        StorageStructureID::newNodeGroupsMetaID(), metaDAHeaderInfo.mainHeaderPageIdx,
        bufferManager, wal);
    numBytesPerFixedSizedValue = ColumnChunk::getDataTypeSizeInChunk(this->dataType) / 8;
    assert(numBytesPerFixedSizedValue <= BufferPoolConstants::PAGE_4KB_SIZE);
    numValuesPerPage =
        PageUtils::getNumElementsInAPage(numBytesPerFixedSizedValue, false /* hasNull */);
    readNodeColumnFunc = this->dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID ?
                             FixedSizedNodeColumnFunc::readInternalIDValuesFromPage :
                             FixedSizedNodeColumnFunc::readValuesFromPage;
    writeNodeColumnFunc = this->dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID ?
                              FixedSizedNodeColumnFunc::writeInternalIDValuesToPage :
                              FixedSizedNodeColumnFunc::writeValuesToPage;
    if (requireNullColumn) {
        nullColumn = std::make_unique<NullNodeColumn>(metaDAHeaderInfo.nullHeaderPageIdx,
            nodeGroupsDataFH, nodeGroupsMetaFH, bufferManager, wal);
    }
}

void NodeColumn::batchLookup(const offset_t* nodeOffsets, size_t size, uint8_t* result) {
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(nodeOffset);
        auto cursor = PageUtils::getPageElementCursorForPos(nodeOffset, numValuesPerPage);
        auto dummyReadOnlyTransaction = Transaction::getDummyReadOnlyTrx();
        cursor.pageIdx +=
            columnChunksMetaDA->get(nodeGroupIdx, dummyReadOnlyTransaction->getType()).pageIdx;
        // TODO: this won't work for bitpacked bool columns
        // However changing it would probably violate assumptions in the calling function
        // Figure out how this is used and document it
        readFromPage(dummyReadOnlyTransaction.get(), cursor.pageIdx, [&](uint8_t* frame) -> void {
            memcpy(result + i * numBytesPerFixedSizedValue,
                frame + (cursor.elemPosInPage * numBytesPerFixedSizedValue),
                numBytesPerFixedSizedValue);
        });
    }
}

void NodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    nullColumn->scan(transaction, nodeIDVector, resultVector);
    scanInternal(transaction, nodeIDVector, resultVector);
}

void NodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    assert(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(startNodeOffset);
    auto offsetInNodeGroup =
        startNodeOffset - (nodeGroupIdx << StorageConstants::NODE_GROUP_SIZE_LOG2);
    auto pageCursor = PageUtils::getPageElementCursorForPos(offsetInNodeGroup, numValuesPerPage);
    auto chunkMeta = columnChunksMetaDA->get(nodeGroupIdx, transaction->getType());
    pageCursor.pageIdx += chunkMeta.pageIdx;
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, pageCursor, nodeIDVector, resultVector);
    } else {
        scanFiltered(transaction, pageCursor, nodeIDVector, resultVector);
    }
}

void NodeColumn::scanUnfiltered(Transaction* transaction, PageElementCursor& pageCursor,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto numValuesToScan = nodeIDVector->state->originalSize;
    auto numValuesScanned = 0u;
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
            readNodeColumnFunc(
                frame, pageCursor, resultVector, numValuesScanned, numValuesToScanInPage);
        });
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
    }
}

void NodeColumn::scanFiltered(Transaction* transaction, PageElementCursor& pageCursor,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto numValuesToScan = nodeIDVector->state->originalSize;
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
                readNodeColumnFunc(
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
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[0];
        if (nodeIDVector->isNull(pos)) {
            return;
        }
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        lookupSingleValue(transaction, nodeOffset, resultVector, pos);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            if (nodeIDVector->isNull(pos)) {
                continue;
            }
            auto nodeOffset = nodeIDVector->readNodeOffset(pos);
            lookupSingleValue(transaction, nodeOffset, resultVector, pos);
        }
    }
}

void NodeColumn::lookupSingleValue(Transaction* transaction, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(nodeOffset);
    auto pageCursor = PageUtils::getPageElementCursorForPos(nodeOffset, numValuesPerPage);
    pageCursor.pageIdx += columnChunksMetaDA->get(nodeGroupIdx, transaction->getType()).pageIdx;
    readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
        readNodeColumnFunc(frame, pageCursor, resultVector, posInVector, 1 /* numValuesToRead */);
    });
}

void NodeColumn::readFromPage(
    Transaction* transaction, page_idx_t pageIdx, const std::function<void(uint8_t*)>& func) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *nodeGroupsDataFH, pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
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

page_idx_t NodeColumn::appendColumnChunk(
    ColumnChunk* columnChunk, page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    // Main column chunk.
    page_idx_t numPagesFlushed = 0;
    auto numPagesForChunk = columnChunk->flushBuffer(nodeGroupsDataFH, startPageIdx);
    columnChunksMetaDA->resize(nodeGroupIdx + 1);
    columnChunksMetaDA->update(nodeGroupIdx, ColumnChunkMetadata{startPageIdx, numPagesForChunk});
    numPagesFlushed += numPagesForChunk;
    startPageIdx += numPagesForChunk;
    // Null column chunk.
    auto numPagesForNullChunk =
        nullColumn->appendColumnChunk(columnChunk->getNullChunk(), startPageIdx, nodeGroupIdx);
    numPagesFlushed += numPagesForNullChunk;
    startPageIdx += numPagesForNullChunk;
    // Children column chunks.
    assert(childrenColumns.size() == columnChunk->getNumChildren());
    for (auto i = 0u; i < childrenColumns.size(); i++) {
        auto numPagesForChild = childrenColumns[i]->appendColumnChunk(
            columnChunk->getChild(i), startPageIdx, nodeGroupIdx);
        numPagesFlushed += numPagesForChild;
        startPageIdx += numPagesForChild;
    }
    return numPagesFlushed;
}

void NodeColumn::writeInternal(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    static_cast<NodeColumn*>(nullColumn.get())
        ->writeInternal(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    bool isNull = vectorToWriteFrom->isNull(posInVectorToWriteFrom);
    if (isNull) {
        return;
    }
    writeSingleValue(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
}

void NodeColumn::writeSingleValue(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto walPageInfo = createWALVersionOfPageForValue(nodeOffset);
    try {
        writeNodeColumnFunc(
            walPageInfo.frame, walPageInfo.posInPage, vectorToWriteFrom, posInVectorToWriteFrom);
    } catch (Exception& e) {
        bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
        nodeGroupsDataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
        throw;
    }
    bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
    nodeGroupsDataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
}

void NodeColumn::addNewPageToNodeGroupsDataFH() {
    auto pageIdxInOriginalFile = nodeGroupsDataFH->addNewPage();
    auto pageIdxInWAL = wal->logPageInsertRecord(storageStructureID, pageIdxInOriginalFile);
    bufferManager->pin(
        *wal->fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::DONT_READ_PAGE);
    nodeGroupsDataFH->addWALPageIdxGroupIfNecessary(pageIdxInOriginalFile);
    nodeGroupsDataFH->setWALPageIdx(pageIdxInOriginalFile, pageIdxInWAL);
    wal->fileHandle->setLockedPageDirty(pageIdxInWAL);
    bufferManager->unpin(*wal->fileHandle, pageIdxInWAL);
}

WALPageIdxPosInPageAndFrame NodeColumn::createWALVersionOfPageForValue(offset_t nodeOffset) {
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(nodeOffset);
    auto originalPageCursor = PageUtils::getPageElementCursorForPos(nodeOffset, numValuesPerPage);
    originalPageCursor.pageIdx +=
        columnChunksMetaDA->get(nodeGroupIdx, TransactionType::WRITE).pageIdx;
    bool insertingNewPage = false;
    if (originalPageCursor.pageIdx >= nodeGroupsDataFH->getNumPages()) {
        assert(originalPageCursor.pageIdx == nodeGroupsDataFH->getNumPages());
        addNewPageToNodeGroupsDataFH();
        insertingNewPage = true;
    }
    auto walPageIdxAndFrame =
        StorageStructureUtils::createWALVersionIfNecessaryAndPinPage(originalPageCursor.pageIdx,
            insertingNewPage, *nodeGroupsDataFH, storageStructureID, *bufferManager, *wal);
    return {walPageIdxAndFrame, originalPageCursor.elemPosInPage};
}

void NodeColumn::setNull(offset_t nodeOffset) {
    if (nullColumn) {
        nullColumn->setNull(nodeOffset);
    }
}

void NodeColumn::checkpointInMemory() {
    columnChunksMetaDA->checkpointInMemoryIfNecessary();
    for (auto& child : childrenColumns) {
        child->checkpointInMemory();
    }
    if (nullColumn) {
        nullColumn->checkpointInMemory();
    }
}

void NodeColumn::rollbackInMemory() {
    columnChunksMetaDA->rollbackInMemoryIfNecessary();
    for (auto& child : childrenColumns) {
        child->rollbackInMemory();
    }
    if (nullColumn) {
        nullColumn->rollbackInMemory();
    }
}

BoolNodeColumn::BoolNodeColumn(const catalog::MetaDiskArrayHeaderInfo& metaDAHeaderInfo,
    BMFileHandle* nodeGroupsDataFH, BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager,
    WAL* wal, bool requireNullColumn)
    : NodeColumn{LogicalType(LogicalTypeID::BOOL), metaDAHeaderInfo, nodeGroupsDataFH,
          nodeGroupsMetaFH, bufferManager, wal, requireNullColumn} {
    readNodeColumnFunc = NullNodeColumnFunc::readValuesFromPage;
    writeNodeColumnFunc = NullNodeColumnFunc::writeValuesToPage;
    // 8 values per byte
    numValuesPerPage = PageUtils::getNumElementsInAPage(1, false) * 8;
    // Page size must be aligned to 8 byte chunks for the 64-bit NullMask algorithms to work
    // without the possibility of memory errors from reading/writing off the end of a page.
    assert(PageUtils::getNumElementsInAPage(1, false) % 8 == 0);
}

NullNodeColumn::NullNodeColumn(page_idx_t metaDAHeaderPageIdx, BMFileHandle* nodeGroupsDataFH,
    BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager, WAL* wal)
    : BoolNodeColumn{MetaDiskArrayHeaderInfo{metaDAHeaderPageIdx}, nodeGroupsDataFH,
          nodeGroupsMetaFH, bufferManager, wal, false /*requireNullColumn*/} {}

void NullNodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    scanInternal(transaction, nodeIDVector, resultVector);
}

void NullNodeColumn::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    lookupInternal(transaction, nodeIDVector, resultVector);
}

page_idx_t BoolNodeColumn::appendColumnChunk(
    ColumnChunk* columnChunk, page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    auto numPagesFlushed = columnChunk->flushBuffer(nodeGroupsDataFH, startPageIdx);
    columnChunksMetaDA->resize(nodeGroupIdx + 1);
    columnChunksMetaDA->update(nodeGroupIdx, ColumnChunkMetadata{startPageIdx, numPagesFlushed});
    return numPagesFlushed;
}

void NullNodeColumn::setNull(common::offset_t nodeOffset) {
    auto walPageInfo = createWALVersionOfPageForValue(nodeOffset);
    try {
        using common::NullMask;
        NullMask::copyNullMask(
            &NullMask::ALL_NULL_ENTRY, 0, (uint64_t*)walPageInfo.frame, walPageInfo.posInPage, 1);
    } catch (Exception& e) {
        bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
        nodeGroupsDataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
        throw;
    }
    bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
    nodeGroupsDataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
}

void NullNodeColumn::writeInternal(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    writeSingleValue(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
}

SerialNodeColumn::SerialNodeColumn(const catalog::MetaDiskArrayHeaderInfo& metaDAHeaderInfo,
    BMFileHandle* nodeGroupsDataFH, BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager,
    WAL* wal)
    : NodeColumn{LogicalType(LogicalTypeID::SERIAL), metaDAHeaderInfo, nodeGroupsDataFH,
          nodeGroupsMetaFH, bufferManager, wal, false} {}

void SerialNodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    // Serial column cannot contain null values.
    for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset = nodeIDVector->readNodeOffset(pos);
        resultVector->setValue<offset_t>(pos, offset);
    }
}

void SerialNodeColumn::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (nodeIDVector->state->isFlat()) {
        // Serial column cannot contain null values.
        auto pos = nodeIDVector->state->selVector->selectedPositions[0];
        auto offset = nodeIDVector->readNodeOffset(pos);
        resultVector->setValue<offset_t>(pos, offset);
    } else {
        // Serial column cannot contain null values.
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            resultVector->setValue<offset_t>(pos, offset);
        }
    }
}

page_idx_t SerialNodeColumn::appendColumnChunk(
    ColumnChunk* columnChunk, page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    // DO NOTHING.
    return 0;
}

std::unique_ptr<NodeColumn> NodeColumnFactory::createNodeColumn(const LogicalType& dataType,
    const catalog::MetaDiskArrayHeaderInfo& metaDAHeaderInfo, BMFileHandle* nodeGroupsDataFH,
    BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager, WAL* wal) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return std::make_unique<BoolNodeColumn>(
            metaDAHeaderInfo, nodeGroupsDataFH, nodeGroupsMetaFH, bufferManager, wal, true);
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::INTERNAL_ID:
    case LogicalTypeID::FIXED_LIST: {
        return std::make_unique<NodeColumn>(dataType, metaDAHeaderInfo, nodeGroupsDataFH,
            nodeGroupsMetaFH, bufferManager, wal, true);
    }
        // TODO: Add a special case for FIXED_LIST, which should read without assuming 2^n per val.
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING:
    case LogicalTypeID::VAR_LIST: {
        return std::make_unique<VarSizedNodeColumn>(
            dataType, metaDAHeaderInfo, nodeGroupsDataFH, nodeGroupsMetaFH, bufferManager, wal);
    }
    case LogicalTypeID::STRUCT: {
        return std::make_unique<StructNodeColumn>(
            dataType, metaDAHeaderInfo, nodeGroupsDataFH, nodeGroupsMetaFH, bufferManager, wal);
    }
    case LogicalTypeID::SERIAL: {
        return std::make_unique<SerialNodeColumn>(
            metaDAHeaderInfo, nodeGroupsDataFH, nodeGroupsMetaFH, bufferManager, wal);
    }
    default: {
        throw NotImplementedException("NodeColumnFactory::createNodeColumn");
    }
    }
}

} // namespace storage
} // namespace kuzu
