#include "storage/store/struct_chunk_data.h"

#include "common/cast.h"
#include "common/copy_constructors.h"
#include "common/data_chunk/sel_vector.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/store/column.h"
#include "storage/store/column_chunk.h"
#include "storage/store/column_chunk_data.h"
#include "storage/store/struct_column.h"

using namespace kuzu::common;

namespace kuzu {
using transaction::Transaction;
namespace storage {

StructChunk::StructChunk(MemoryManager& mm, LogicalType dataType, uint64_t capacity,
    bool enableCompression, ResidencyState residencyState)
    : ColumnChunk{std::move(dataType), enableCompression, residencyState} {
    const auto fieldTypes = StructType::getFieldTypes(this->dataType);
    childChunks.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childChunks[i] = std::make_unique<ColumnChunk>(mm, fieldTypes[i]->copy(), capacity,
            enableCompression, residencyState);
    }
}

StructChunk::StructChunk(MemoryManager& mm, LogicalType dataType, bool enableCompression,
    const ColumnChunkMetadata& metadata)
    : ColumnChunk{mm, std::move(dataType), enableCompression, metadata} {
    const auto fieldTypes = StructType::getFieldTypes(this->dataType);
    childChunks.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childChunks[i] = std::make_unique<ColumnChunk>(mm, fieldTypes[i]->copy(), 0,
            enableCompression, ResidencyState::IN_MEMORY);
    }
}

void StructChunk::finalize() {
    for (const auto& childChunk : childChunks) {
        childChunk->finalize();
    }
}

uint64_t StructChunk::getEstimatedMemoryUsage() const {
    auto estimatedMemoryUsage = ColumnChunk::getEstimatedMemoryUsage();
    for (auto& childChunk : childChunks) {
        estimatedMemoryUsage += childChunk->getEstimatedMemoryUsage();
    }
    return estimatedMemoryUsage;
}

void StructChunk::serialize(Serializer& serializer) const {
    ColumnChunk::serialize(serializer);
    serializer.writeDebuggingInfo("struct_children");
    serializer.serializeVectorOfPtrs<ColumnChunk>(childChunks);
}

void StructChunk::deserialize(Deserializer& deSer, ColumnChunkData& chunkData) {
    std::string key;
    deSer.validateDebuggingInfo(key, "struct_children");
    deSer.deserializeVectorOfPtrs<ColumnChunk>(chunkData.cast<StructChunk>().childChunks,
        [&](Deserializer& deser) {
            return ColumnChunk::deserialize(chunkData.getMemoryManager(), deser);
        });
}

void StructChunk::flush(FileHandle& dataFH) {
    ColumnChunk::flush(dataFH);
    for (const auto& childChunk : childChunks) {
        childChunk->flush(dataFH);
    }
}

void StructChunk::reclaimStorage(FileHandle& dataFH) {
    ColumnChunk::reclaimStorage(dataFH);
    for (const auto& childChunk : childChunks) {
        childChunk->reclaimStorage(dataFH);
    }
}

void StructChunk::append(const ColumnChunk* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    KU_ASSERT(other->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    const auto& otherStructChunk = ku_dynamic_cast<const StructChunk*>(other);
    KU_ASSERT(childChunks.size() == otherStructChunk->childChunks.size());
    // nullData->append(other->getNullData(), startPosInOtherChunk, numValuesToAppend);
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->append(otherStructChunk->childChunks[i].get(), startPosInOtherChunk,
            numValuesToAppend);
    }
}

void StructChunk::append(ValueVector* vector, const SelectionView& selView) {
    const auto numFields = StructType::getNumFields(dataType);
    for (auto i = 0u; i < numFields; i++) {
        childChunks[i]->append(StructVector::getFieldVector(vector, i).get(), selView);
    }
    /*
    for (auto i = 0u; i < selView.getSelSize(); i++) {
        nullData->setNull(numValues + i, vector->isNull(selView[i]));
    }
    */
}

void StructChunk::scan(const Transaction* transaction, const ChunkState& state, ValueVector& output,
    offset_t offsetInChunk, length_t length) const {
    const auto numFields = StructType::getNumFields(dataType);
    for (auto i = 0u; i < numFields; i++) {
        childChunks[i]->scan(transaction, state.getChildState(i),
            *StructVector::getFieldVector(&output, i), offsetInChunk, length);
    }
}

void StructChunk::lookup(const Transaction* transaction, const ChunkState& state,
    common::offset_t rowInChunk, common::ValueVector& output,
    common::sel_t posInOutputVector) const {
    const auto numFields = StructType::getNumFields(dataType);
    // TODO: nulls
    // output.setNull(posInOutputVector, nullData->isNull(offsetInChunk));
    for (auto i = 0u; i < numFields; i++) {
        childChunks[i]->lookup(transaction, state, rowInChunk,
            *StructVector::getFieldVector(&output, i).get(), posInOutputVector);
    }
}

void StructChunk::initializeScanState(ChunkState& state, const Column* column) const {
    ColumnChunk::initializeScanState(state, column);
    auto* structColumn = ku_dynamic_cast<const StructColumn*>(column);
    state.childrenStates.resize(childChunks.size());
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->initializeScanState(state.childrenStates[i], structColumn->getChild(i));
    }
}

/*

void StructChunkData::write(const ValueVector* vector, offset_t offsetInVector,
    offset_t offsetInChunk) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    nullData->setNull(offsetInChunk, vector->isNull(offsetInVector));
    const auto fields = StructVector::getFieldVectors(vector);
    for (auto i = 0u; i < fields.size(); i++) {
        childChunks[i]->write(fields[i].get(), offsetInVector, offsetInChunk);
    }
    if (offsetInChunk >= numValues) {
        numValues = offsetInChunk + 1;
    }
}

void StructChunkData::write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets,
    RelMultiplicity multiplicity) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        const auto offsetInChunk = dstOffsets->getValue<offset_t>(i);
        KU_ASSERT(offsetInChunk < capacity);
        nullData->setNull(offsetInChunk, chunk->getNullData()->isNull(i));
        numValues = offsetInChunk >= numValues ? offsetInChunk + 1 : numValues;
    }
    auto& structChunk = chunk->cast<StructChunkData>();
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->write(structChunk.getChild(i), dstOffsets, multiplicity);
    }
}

void StructChunkData::write(const ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    const auto& srcStructChunk = srcChunk->cast<StructChunkData>();
    KU_ASSERT(childChunks.size() == srcStructChunk.childChunks.size());
    nullData->write(srcChunk->getNullData(), srcOffsetInChunk, dstOffsetInChunk, numValuesToCopy);
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->write(srcStructChunk.childChunks[i].get(), srcOffsetInChunk,
            dstOffsetInChunk, numValuesToCopy);
    }
}
*/

StructChunk::StructChunk(bool enableCompression, std::vector<std::unique_ptr<ColumnChunk>> children)
    : ColumnChunk(enableCompression, std::vector<std::unique_ptr<ColumnChunkData>>{}),
      childChunks{std::move(children)} {}

std::unique_ptr<ColumnChunk> StructChunk::flushAsNewColumnChunk(FileHandle& dataFH) const {
    std::vector<std::unique_ptr<ColumnChunk>> children;
    for (auto& child : childChunks) {
        children.push_back(child->flushAsNewColumnChunk(dataFH));
    }
    return std::make_unique<StructChunk>(isCompressionEnabled(), std::move(children));
}

void StructChunk::checkpoint(Column& column,
    std::vector<ChunkCheckpointState>&& chunkCheckpointStates) {

    auto& structColumn = column.cast<StructColumn>();
    for (size_t i = 0; i < childChunks.size(); i++) {
        // TODO(bmwinger): The checkpoint state needs to be able to store the child columns
        // childChunks[i]->checkpoint(*structColumn.getChild(i), chunkCheckpointStates);
    }
}

} // namespace storage
} // namespace kuzu
