#include "storage/copier/compression.h"

#include "arrow/array.h"
#include "common/null_mask.h"
#include "common/vector/value_vector.h"
#include "fastpfor/bitpackinghelpers.h"

using namespace kuzu::common;
namespace arrow {
class Array;
}

namespace kuzu {
namespace storage {

const LogicalType BoolCompression::LOGICAL_TYPE = LogicalType(LogicalTypeID::BOOL);
const LogicalType InternalIDMapping::LOGICAL_TYPE = LogicalType(LogicalTypeID::INTERNAL_ID);

template<typename T, typename U>
void IntegerBitpacking<T, U>::setValueFromUncompressed(
    uint8_t* srcBuffer, common::offset_t posInSrc, uint8_t* dstBuffer, common::offset_t posInDst) {
    auto bitWidth = dstBuffer[0];
    auto byteIndex = posInDst * bitWidth / 32;
    auto posInTemp = posInDst * bitWidth % 32;

    U tmp[32];
    FastPForLib::fastunpack((const uint32_t*)dstBuffer + byteIndex, tmp, bitWidth);
    tmp[posInTemp] = ((U*)srcBuffer)[posInSrc];
    FastPForLib::fastpack(tmp, (uint32_t*)dstBuffer + byteIndex, bitWidth);
}

template<typename T, typename U>
void IntegerBitpacking<T, U>::compress(const uint8_t* srcBuffer, uint64_t numValues,
    uint8_t* dstBuffer, uint64_t dstBufferSize) {
    auto bitWidth = (dstBufferSize - 1) / sizeof(T);
    assert(bitWidth == getBitWidth(srcBuffer, numValues));
    dstBuffer[0] = bitWidth;
    for (auto i = 0ull; i < numValues; i += 32) {
        FastPForLib::fastpack((const U*)srcBuffer + i, (uint32_t*)dstBuffer, bitWidth);
        // fastpack packs 32 values at a time, i.e. 4 bytes per bit of width.
        dstBuffer += bitWidth * 4;
    }
}

template<typename T, typename U>
void IntegerBitpacking<T, U>::decompress(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues) {
    auto bitWidth = srcBuffer[0];
    auto chunkSize = 32;
    for (auto i = 0ull; i < numValues; i += chunkSize) {
        FastPForLib::fastunpack((const uint32_t*)srcBuffer, (U*)dstBuffer + i, bitWidth);
        duckdb::SignExtend(dstBuffer + i, bitWidth, chunkSize);
        srcBuffer += bitWidth;
    }
}

// Uses unsigned types since the storage is unsigned
// TODO: Doesn't currently support int16; there is no wrapper for it
// template class IntegerBitpacking<uint16_t>;
template class IntegerBitpacking<int32_t, uint32_t>;
template class IntegerBitpacking<int64_t, uint64_t>;

} // namespace storage
} // namespace kuzu
