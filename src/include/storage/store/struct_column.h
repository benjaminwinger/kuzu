#pragma once

#include "common/assert.h"
#include "storage/store/column.h"

namespace kuzu {
namespace storage {
class MemoryManager;

class StructColumn final : public Column {
public:
    StructColumn(std::string name, common::LogicalType dataType, FileHandle* dataFH,
        MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression);

    static std::unique_ptr<ColumnChunkData> flushChunkData(const ColumnChunkData& chunk,
        FileHandle& dataFH);

    void scan(const transaction::Transaction*, const ChunkState&, ColumnChunkData*,
        common::offset_t = 0, common::offset_t = common::INVALID_OFFSET) const override {
        KU_UNREACHABLE;
    }
    void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) const override;

    Column* getChild(common::idx_t childIdx) const {
        KU_ASSERT(childIdx < childColumns.size());
        return childColumns[childIdx].get();
    }

protected:
    void scanInternal(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::row_idx_t numValuesToScan,
        common::ValueVector* resultVector) const override;

    void lookupInternal(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* resultVector,
        uint32_t posInVector) const override;

private:
    std::vector<std::unique_ptr<Column>> childColumns;
};

} // namespace storage
} // namespace kuzu
