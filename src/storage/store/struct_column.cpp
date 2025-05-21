#include "storage/store/struct_column.h"

#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/store/column_chunk.h"
#include "storage/store/null_column.h"
#include "storage/store/struct_chunk_data.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StructColumn::StructColumn(std::string name, LogicalType dataType, FileHandle* dataFH,
    MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression)
    : Column{std::move(name), std::move(dataType), dataFH, mm, shadowFile, enableCompression,
          true /* requireNullColumn */} {
    const auto fieldTypes = StructType::getFieldTypes(this->dataType);
    childColumns.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        const auto childColName = StorageUtils::getColumnName(this->name,
            StorageUtils::ColumnType::STRUCT_CHILD, std::to_string(i));
        childColumns[i] = ColumnFactory::createColumn(childColName, fieldTypes[i]->copy(), dataFH,
            mm, shadowFile, enableCompression);
    }
}

void StructColumn::scan(const Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) const {
    nullColumn->scan(transaction, *state.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->scan(transaction, state.childrenStates[i], startOffsetInGroup,
            endOffsetInGroup, fieldVector, offsetInVector);
    }
}

void StructColumn::scanInternal(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInChunk, row_idx_t numValuesToScan, ValueVector* resultVector) const {
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->scan(transaction, state.childrenStates[i], startOffsetInChunk,
            numValuesToScan, fieldVector);
    }
}

void StructColumn::lookupInternal(const Transaction* transaction, const ChunkState& state,
    offset_t nodeOffset, ValueVector* resultVector, uint32_t posInVector) const {
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->lookupValue(transaction, state.childrenStates[i], nodeOffset, fieldVector,
            posInVector);
    }
}

} // namespace storage
} // namespace kuzu
