#include "storage/store/string_column_chunk.h"

#include "common/exception/not_implemented.h"
#include "common/types/value/value.h"
#include "storage/store/table_copy_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StringColumnChunk::StringColumnChunk(LogicalType dataType)
    : ColumnChunk{std::move(dataType), false /*enableCompression*/} {
    // Bitpacking might save 1 bit per value with regular ascii compared to UTF-8
    // Detecting when we need to re-compress the child chunks is not currently supported.
    bool enableCompression = false;
    stringDataChunk = std::make_unique<ColumnChunk>(
        LogicalType(LogicalTypeID::UINT8), enableCompression, false /*hasNullChunk*/);
    // The offset chunk is able to grow beyond the node group size.
    // We rely on appending to the dictionary when updating, however if the chunk is full,
    // there will be no space for in-place updates.
    // The data chunk doubles in size on use, but out of place updates will never need the offset
    // chunk to be greater than the node group size since they remove unused entries.
    // So the chunk is initialized with a size equal to 3/4 the node group size, making sure there
    // is always extra space for updates.
    offsetChunk = std::make_unique<ColumnChunk>(LogicalType(LogicalTypeID::UINT64),
        enableCompression, false /*hasNullChunk*/, StorageConstants::NODE_GROUP_SIZE * 0.75);
}

void StringColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    stringDataChunk->resetToEmpty();
    offsetChunk->resetToEmpty();
}

void StringColumnChunk::append(ValueVector* vector, offset_t startPosInChunk) {
    assert(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        // index is stored in main chunk, data is stored in the data chunk
        auto pos = vector->state->selVector->selectedPositions[i];
        nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
        if (vector->isNull(pos)) {
            numValues++;
            continue;
        }
        auto kuString = vector->getValue<ku_string_t>(pos);
        setValueFromString(kuString.getAsString().c_str(), kuString.len, startPosInChunk + i);
    }
}

void StringColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto otherChunk = reinterpret_cast<StringColumnChunk*>(other);
    nullChunk->append(
        otherChunk->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        appendStringColumnChunk(
            otherChunk, startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    } break;
    default: {
        throw NotImplementedException("StringColumnChunk::append");
    }
    }
}

void StringColumnChunk::update(ValueVector* vector, vector_idx_t vectorIdx) {
    auto startOffsetInChunk = vectorIdx << DEFAULT_VECTOR_CAPACITY_LOG_2;
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        auto offsetInChunk = startOffsetInChunk + pos;
        nullChunk->setNull(offsetInChunk, vector->isNull(pos));
        if (!vector->isNull(pos)) {
            auto kuStr = vector->getValue<ku_string_t>(pos);
            setValueFromString(kuStr.getAsString().c_str(), kuStr.len, offsetInChunk);
        }
    }
}

void StringColumnChunk::appendStringColumnChunk(StringColumnChunk* other,
    offset_t startPosInOtherChunk, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto indices = reinterpret_cast<string_index_t*>(buffer.get());
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto posInChunk = i + startPosInChunk;
        auto posInOtherChunk = i + startPosInOtherChunk;
        if (nullChunk->isNull(posInChunk)) {
            indices[posInChunk] = 0;
            continue;
        }
        auto stringInOtherChunk = other->getValue<std::string_view>(posInOtherChunk);
        setValueFromString(stringInOtherChunk.data(), stringInOtherChunk.size(), posInChunk);
    }
}

void StringColumnChunk::write(const Value& val, uint64_t posToWrite) {
    assert(val.getDataType()->getPhysicalType() == PhysicalTypeID::STRING);
    nullChunk->setNull(posToWrite, val.isNull());
    if (posToWrite >= numValues) {
        numValues = posToWrite + 1;
    }
    if (val.isNull()) {
        return;
    }
    auto strVal = val.getValue<std::string>();
    setValueFromString(strVal.c_str(), strVal.length(), posToWrite);
}

void StringColumnChunk::setValueFromString(const char* value, uint64_t length, uint64_t pos) {
    auto space = stringDataChunk->getCapacity() - stringDataChunk->getNumValues();
    if (length > space) {
        stringDataChunk->resize(std::bit_ceil(stringDataChunk->getCapacity() + length));
    }
    if (pos >= numValues) {
        numValues = pos + 1;
    }
    auto startOffset = stringDataChunk->getNumValues();
    memcpy(stringDataChunk->getData() + startOffset, value, length);
    stringDataChunk->setNumValues(startOffset + length);
    auto index = offsetChunk->getNumValues();

    if (index >= offsetChunk->getCapacity()) {
        offsetChunk->resize(offsetChunk->getCapacity() * 2);
    }
    offsetChunk->setValue<string_offset_t>(startOffset, index);
    offsetChunk->setNumValues(index + 1);
    ColumnChunk::setValue<string_index_t>(index, pos);
}

// STRING
template<>
std::string_view StringColumnChunk::getValue<std::string_view>(offset_t pos) const {
    auto index = ColumnChunk::getValue<string_index_t>(pos);
    auto offset = offsetChunk->getValue<string_offset_t>(index);
    return std::string_view((const char*)stringDataChunk->getData() + offset, getStringLength(pos));
}
template<>
std::string StringColumnChunk::getValue<std::string>(offset_t pos) const {
    return std::string(getValue<std::string_view>(pos));
}

} // namespace storage
} // namespace kuzu
