#include "storage/copier/compression.h"

#include "arrow/array.h"
#include "common/null_mask.h"
#include "common/vector/value_vector.h"
#include "fastpfor/bitpackinghelpers.h"
#include "oroch/bitpck.h"

using namespace kuzu::common;
namespace arrow {
class Array;
}

namespace kuzu {
namespace storage {

const LogicalType BoolCompression::LOGICAL_TYPE = LogicalType(LogicalTypeID::BOOL);
const LogicalType InternalIDMapping::LOGICAL_TYPE = LogicalType(LogicalTypeID::INTERNAL_ID);
const LogicalType NullMapping::LOGICAL_TYPE = LogicalType(LogicalTypeID::BOOL);
template<>
const LogicalType IntegerBitpacking<int32_t, uint32_t>::LOGICAL_TYPE = LogicalType(
    LogicalTypeID::INT32);
template<>
const LogicalType IntegerBitpacking<int64_t, uint64_t>::LOGICAL_TYPE = LogicalType(
    LogicalTypeID::INT64);

// Six bits are needed for the bit width (fewer for smaller types, but the header byte is the same
// for simplicity) One bit (the eighth) is needed to indicate if there are negative values The
// seventh bit is unused
struct BitpackHeader {
    uint8_t bitWidth;
    bool hasNegative;

    uint8_t getHeaderByte() const {
        uint8_t result = bitWidth;
        result |= hasNegative << 7;
        return result;
    }

    static BitpackHeader readHeaderByte(uint8_t headerByte) {
        BitpackHeader header;
        header.bitWidth = headerByte & 0b1111111;
        header.hasNegative = headerByte & 0b10000000;
        return header;
    }
};

template<typename T, typename U>
void IntegerBitpacking<T, U>::setValueFromUncompressed(
    uint8_t* srcBuffer, common::offset_t posInSrc, uint8_t* dstBuffer, common::offset_t posInDst) {
    auto header = BitpackHeader::readHeaderByte(dstBuffer++[0]);
    // This is a fairly naive implementation which uses fastunpack/fastpack
    // to modify the data by decompressing/compressing a single chunk of values.
    //
    // TODO(bmwinger): optimize by calculating the start/end bit positions,
    // and then use copyNullBits (which should probably get renamed at some point)
    // to modify a smaller chunk of data
    // (max 2 64-bit values, compared to as many as 64 32-bit values).
    //
    // Data can be considered to be stored in aligned chunks of 32 values
    // with a size of 32 * bitWidth bits,
    // or bitWidth 32-bit values (we cast the buffer to a uint32_t* later).
    auto chunkIndex = posInDst / 32;
    auto posInChunk = posInDst % 32;
    auto value = ((T*)srcBuffer)[posInSrc];
    // If there are negatives, the effective bit width is smaller
    auto valueSize = std::bit_width((U)std::abs(value));
    if (!header.hasNegative && value < 0) {
        throw NotImplementedException("Setting negative values to a chunk stored without negatives is not implemented yet");
    }
    if (header.hasNegative && valueSize > header.bitWidth - 1 ||
        !header.hasNegative && valueSize > header.bitWidth) {
        throw NotImplementedException("Setting values larger than the bit width is not implemented yet");
    }

    U chunk[32];
    FastPForLib::fastunpack(
        (const uint32_t*)dstBuffer + chunkIndex * header.bitWidth, chunk, header.bitWidth);
    chunk[posInChunk] = (U)value;
    FastPForLib::fastpack(
        chunk, (uint32_t*)dstBuffer + chunkIndex * header.bitWidth, header.bitWidth);
}

template<typename T, typename U>
void IntegerBitpacking<T, U>::getValue(
    const uint8_t* buffer, common::offset_t pos, uint8_t* dst) const {
    auto header = BitpackHeader::readHeaderByte(buffer++[0]);
    // TODO(bmwinger): optimize as in setValueFromUncompressed
    auto chunkIndex = pos / 32;
    auto posInChunk = pos % 32;

    U chunk[32];
    FastPForLib::fastunpack(
        (const uint32_t*)buffer + chunkIndex * header.bitWidth, chunk, header.bitWidth);
    duckdb::SignExtend<T>((uint8_t*)&chunk[posInChunk], header.bitWidth, 1);
    memcpy(dst, &chunk[posInChunk], sizeof(T));
}

template<typename T, typename U>
void IntegerBitpacking<T, U>::compress(
    const uint8_t* srcBuffer, uint64_t numValues, uint8_t* dstBuffer, uint64_t dstBufferSize) {
    // uint8_t bitWidth = (dstBufferSize - 1) / sizeof(T);
    // assert(bitWidth == ;
    auto result = getBitWidth(srcBuffer, numValues);
    uint8_t bitWidth = result.first;
    bool hasNegative = result.second;

    // Six bits are needed for the bit width (fewer for smaller types, but the header byte is the
    // same for simplicity) One bit (the eighth) is needed to indicate if there are negative values
    // The seventh bit is unused
    dstBuffer[0] = bitWidth;
    dstBuffer[0] |= hasNegative << 7;
    dstBuffer++;
    // FIXME(bmwinger): will overflow data with fewer than 32 values
    assert(numValues >= 32);
    for (auto i = 0ull; i < numValues; i += 32) {
        FastPForLib::fastpack((const U*)srcBuffer + i, (uint32_t*)dstBuffer, bitWidth);
        // fastpack packs 32 values at a time, i.e. 4 bytes per bit of width.
        dstBuffer += bitWidth * 4;
    }
}

template<typename T, typename U>
void IntegerBitpacking<T, U>::decompress(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues) {
    auto header = BitpackHeader::readHeaderByte(srcBuffer++[0]);
    auto chunkSize = 32;
    assert(numValues >= chunkSize);
    // FIXME(bmwinger): will overflow data with fewer than 32 values
    for (auto i = 0ull; i < numValues; i += chunkSize) {
        FastPForLib::fastunpack((const uint32_t*)srcBuffer, (U*)dstBuffer + i, header.bitWidth);
        if (header.hasNegative) {
            duckdb::SignExtend<T>(dstBuffer + i, header.bitWidth, chunkSize);
        }
        srcBuffer += header.bitWidth;
    }
}

// Uses unsigned types since the storage is unsigned
// TODO: Doesn't currently support int16
// template class IntegerBitpacking<uint16_t>;
template class IntegerBitpacking<int32_t, uint32_t>;
template class IntegerBitpacking<int64_t, uint64_t>;

} // namespace storage
} // namespace kuzu
