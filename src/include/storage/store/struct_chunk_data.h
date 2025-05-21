#pragma once

#include "common/assert.h"
#include "common/data_chunk/sel_vector.h"
#include "common/types/types.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {
class MemoryManager;

class StructChunk final : public ColumnChunk {
public:
    StructChunk(MemoryManager& mm, common::LogicalType dataType, uint64_t capacity,
        bool enableCompression, ResidencyState residencyState);
    StructChunk(MemoryManager& mm, common::LogicalType dataType, bool enableCompression,
        const ColumnChunkMetadata& metadata);
    StructChunk(bool enableCompression, std::vector<std::unique_ptr<ColumnChunk>> children);

    ColumnChunk* getChild(common::idx_t childIdx) {
        KU_ASSERT(childIdx < childChunks.size());
        return childChunks[childIdx].get();
    }
    const ColumnChunk* getChild(common::idx_t childIdx) const {
        KU_ASSERT(childIdx < childChunks.size());
        return childChunks[childIdx].get();
    }
    std::unique_ptr<ColumnChunk> moveChild(common::idx_t childIdx) {
        KU_ASSERT(childIdx < childChunks.size());
        return std::move(childChunks[childIdx]);
    }

    void finalize() override;

    uint64_t getEstimatedMemoryUsage() const override;

    void serialize(common::Serializer& serializer) const override;
    static void deserialize(common::Deserializer& deSer, ColumnChunkData& chunkData);

    common::idx_t getNumChildren() const { return childChunks.size(); }
    /*
    const ColumnChunkData& getChild(common::idx_t childIdx) const {
        KU_ASSERT(childIdx < childChunks.size());
        return *childChunks[childIdx];
    }
    void setChild(common::idx_t childIdx, std::unique_ptr<ColumnChunkData> childChunk) {
        KU_ASSERT(childIdx < childChunks.size());
        childChunks[childIdx] = std::move(childChunk);
    }
    */

    void flush(FileHandle& dataFH) override;
    void reclaimStorage(FileHandle& dataFH) override;

    void lookup(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t rowInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const override;

    void write(Column&, ChunkState&, common::offset_t, const ColumnChunkData&, common::offset_t,
        common::length_t) override {
        KU_UNREACHABLE;
    }

    std::unique_ptr<ColumnChunk> flushAsNewColumnChunk(FileHandle& dataFH) const override;

    void checkpoint(Column& column, std::vector<ChunkCheckpointState>&& chunkCheckpointStates);

protected:
    void append(const ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;
    void append(common::ValueVector* vector, const common::SelectionView& selView) override;

    void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::ValueVector& output, common::offset_t offsetInChunk,
        common::length_t length) const override;
    void initializeScanState(ChunkState& state, const Column* column) const override;

    void setNumValues(uint64_t numValues) override {
        ColumnChunk::setNumValues(numValues);
        for (auto& childChunk : childChunks) {
            childChunk->setNumValues(numValues);
        }
    }

private:
    std::vector<std::unique_ptr<ColumnChunk>> childChunks;
};

} // namespace storage
} // namespace kuzu
