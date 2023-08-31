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
const LogicalType IntegerBitpacking<int32_t, uint32_t>::LOGICAL_TYPE = LogicalType(LogicalTypeID::INT32);
template<>
const LogicalType IntegerBitpacking<int64_t, uint64_t>::LOGICAL_TYPE = LogicalType(LogicalTypeID::INT64);

template<>
const LogicalType IntegerZigZagBitpacking<int16_t, uint16_t>::LOGICAL_TYPE = LogicalType(LogicalTypeID::INT16);
template<>
const LogicalType IntegerZigZagBitpacking<int32_t, uint32_t>::LOGICAL_TYPE = LogicalType(LogicalTypeID::INT32);
template<>
const LogicalType IntegerZigZagBitpacking<int64_t, uint64_t>::LOGICAL_TYPE = LogicalType(LogicalTypeID::INT64);

template<typename T, typename U>
void IntegerBitpacking<T, U>::setValueFromUncompressed(
    uint8_t* srcBuffer, common::offset_t posInSrc, uint8_t* dstBuffer, common::offset_t posInDst) {
    uint8_t bitWidth = dstBuffer[0] & 0b1111111;
    bool hasNegative = srcBuffer[0] & 0b10000000;
    auto byteIndex = posInDst * bitWidth / 32;
    auto posInTemp = posInDst * bitWidth % 32;
    auto value = ((T*)srcBuffer)[posInSrc];
    // If there are negatives, the effective bit width is smaller
    if (hasNegative) {
        bitWidth -= 1;
    }
    if (std::bit_width((U)std::abs(value)) > bitWidth) {
        throw NotImplementedException("Setting values larger than the bit width is not supported");
    }

    U tmp[32];
    FastPForLib::fastunpack((const uint32_t*)dstBuffer + byteIndex, tmp, bitWidth);
    tmp[posInTemp] = (U)value;
    FastPForLib::fastpack(tmp, (uint32_t*)dstBuffer + byteIndex, bitWidth);
}

template<typename T, typename U>
void IntegerBitpacking<T, U>::compress(const uint8_t* srcBuffer, uint64_t numValues,
    uint8_t* dstBuffer, uint64_t dstBufferSize) {
    // uint8_t bitWidth = (dstBufferSize - 1) / sizeof(T);
    // assert(bitWidth == ;
    auto result = getBitWidth(srcBuffer, numValues);
    uint8_t bitWidth = result.first;
    bool hasNegative = result.second;

    // Six bits are needed for the bit width (fewer for smaller types, but the header byte is the same for simplicity)
    // One bit (the eighth) is needed to indicate if there are negative values
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


// Six bits are needed for the bit width (fewer for smaller types, but the header byte is the same for simplicity)
// One bit (the eighth) is needed to indicate if there are negative values
// The seventh bit is unused
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
void IntegerBitpacking<T, U>::decompress(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues) {
    auto header = BitpackHeader::readHeaderByte(srcBuffer[0]);
    srcBuffer++;
    auto chunkSize = 32;
    assert(numValues >= chunkSize);
    // FIXME(bmwinger): will overflow data with fewer than 32 values
    for (auto i = 0ull; i < numValues; i += chunkSize) {
        FastPForLib::fastunpack((const uint32_t*)srcBuffer, (U*)dstBuffer + i, header.bitWidth);
        if (header.hasNegative) {
            duckdb::SignExtend(dstBuffer + i, header.bitWidth, chunkSize);
        }
        srcBuffer += header.bitWidth;
    }
}

template<typename T, typename U>
void IntegerZigZagBitpacking<T, U>::setValueFromUncompressed(
    uint8_t* srcBuffer, common::offset_t posInSrc, uint8_t* dstBuffer, common::offset_t posInDst) {
    auto header = BitpackHeader::readHeaderByte(dstBuffer[0]);
    auto maxBits = sizeof(T) * 8;
    auto byteIndex = posInDst * header.bitWidth / maxBits;
    auto posInTemp = posInDst * header.bitWidth % maxBits;
    auto value = ((T*)srcBuffer)[posInSrc];
    // If there are negatives, the effective bit width is smaller
    if (header.hasNegative) {
        header.bitWidth -= 1;
    }
    if (std::bit_width((U)std::abs(value)) > header.bitWidth) {
        throw NotImplementedException("Setting values larger than the bit width is not supported");
    }

    U tmp[maxBits];
    const uint8_t *src = dstBuffer + byteIndex * sizeof(T);
    if (header.hasNegative) {
        oroch::bitpck_codec<T>::decode(&tmp[0], tmp + maxBits, src, header.bitWidth);
    } else {
        oroch::bitpck_codec<U>::decode(&tmp[0], tmp + maxBits, src, header.bitWidth);
    }
    tmp[posInTemp] = (U)value;
    uint8_t *dst = dstBuffer + byteIndex * sizeof(T);
    if (header.hasNegative) {
        oroch::bitpck_codec<T>::encode(dst, tmp, tmp + maxBits, header.bitWidth);
    } else {
        oroch::bitpck_codec<U>::encode(dst, tmp, tmp + maxBits, header.bitWidth);
    }
}

template<typename T, typename U>
void IntegerZigZagBitpacking<T, U>::compress(const uint8_t* srcBuffer, uint64_t numValues,
    uint8_t* dstBuffer, uint64_t dstBufferSize) {
    // uint8_t bitWidth = (dstBufferSize - 1) / sizeof(T);
    // assert(bitWidth == ;
    // TODO: return BitpackHeader
    auto result = getBitWidth(srcBuffer, numValues);
    uint8_t bitWidth = result.first;
    bool hasNegative = result.second;

    dstBuffer[0] = BitpackHeader(bitWidth, hasNegative).getHeaderByte();
    dstBuffer++;

    if (hasNegative) {
        oroch::bitpck_codec<T>::encode(dstBuffer, (T*)srcBuffer, (T*)srcBuffer + numValues, bitWidth);
    } else {
        oroch::bitpck_codec<U>::encode(dstBuffer, (U*)srcBuffer, (U*)srcBuffer + numValues, bitWidth);
    }
}

template<typename T, typename U>
void IntegerZigZagBitpacking<T, U>::decompress(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues) {
    auto header = BitpackHeader::readHeaderByte(srcBuffer[0]);
    srcBuffer++;
    if (header.hasNegative) {
        oroch::bitpck_codec<T>::decode((T*)dstBuffer, (T*)dstBuffer + numValues, srcBuffer, header.bitWidth);
    } else {
        oroch::bitpck_codec<U>::decode((U*)dstBuffer, (U*)dstBuffer + numValues, srcBuffer, header.bitWidth);
    }
}

// Uses unsigned types since the storage is unsigned
// TODO: Doesn't currently support int16; there is no wrapper for it
// template class IntegerBitpacking<uint16_t>;
template class IntegerBitpacking<int32_t, uint32_t>;
template class IntegerBitpacking<int64_t, uint64_t>;

template class IntegerZigZagBitpacking<int16_t, uint16_t>;
template class IntegerZigZagBitpacking<int32_t, uint32_t>;
template class IntegerZigZagBitpacking<int64_t, uint64_t>;

} // namespace storage
} // namespace kuzu
