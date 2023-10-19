#pragma once

#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class StringColumnChunk : public ColumnChunk {
    using string_offset_t = uint64_t;
    using string_index_t = uint32_t;

public:
    explicit StringColumnChunk(common::LogicalType dataType);

    void resetToEmpty() final;
    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;

    void update(common::ValueVector* vector, common::vector_idx_t vectorIdx) override;

    template<typename T>
    T getValue(common::offset_t /*pos*/) const {
        throw common::NotImplementedException("StringColumnChunk::getValue");
    }

    uint64_t getStringLength(common::offset_t pos) const {
        auto index = ColumnChunk::getValue<string_index_t>(pos);
        if (index + 1 < numValues) {
            return offsetChunk->getValue<string_offset_t>(index + 1) -
                   offsetChunk->getValue<string_offset_t>(index);
        }
        return stringDataChunk->getNumValues() - offsetChunk->getValue<string_offset_t>(index);
    }

    ColumnChunk* getDataChunk() { return stringDataChunk.get(); }
    ColumnChunk* getOffsetChunk() { return offsetChunk.get(); }

private:
    void appendStringColumnChunk(StringColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend);

    void write(const common::Value& val, uint64_t posToWrite) override;

    void setValueFromString(const char* value, uint64_t length, uint64_t pos);

private:
    // String data is stored as a UINT8 chunk, using the numValues in the chunk to track the number
    // of characters stored.
    std::unique_ptr<ColumnChunk> stringDataChunk;
    std::unique_ptr<ColumnChunk> offsetChunk;
};

// STRING
template<>
std::string StringColumnChunk::getValue<std::string>(common::offset_t pos) const;
template<>
std::string_view StringColumnChunk::getValue<std::string_view>(common::offset_t pos) const;

} // namespace storage
} // namespace kuzu
