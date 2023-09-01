#pragma once

#include <math.h>

#include <cstdint>

#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include <bit>
// TODO(bmwinger): Move to cpp
#include "common/null_mask.h"

namespace kuzu {
namespace storage {

namespace duckdb {
// From DDB
// Modified to implement SignExtend on an arbitrary Datatype
using data_ptr_t = uint8_t*;
using const_data_ptr_t = const uint8_t*;
template<typename T>
void Store(const T& val, data_ptr_t ptr) {
    memcpy(ptr, (void*)&val, sizeof(val));
}

template<typename T>
const T Load(const_data_ptr_t ptr) {
    T ret;
    memcpy(&ret, ptr, sizeof(ret));
    return ret;
}

// Sign bit extension
template<typename Datatype>
static void SignExtend(data_ptr_t dst, int width, size_t len) {
    Datatype const mask = 1 << (width - 1);
    for (int i = 0; i < len; ++i) {
        Datatype value = Load<Datatype>(dst + i * sizeof(Datatype));
        value = value & ((1 << width) - 1);
        Datatype result = (value ^ mask) - mask;
        Store(result, dst + i * sizeof(Datatype));
    }
}
} // namespace duckdb

// Returns the size of the data type in bytes
// TODO(bmwinger): Replace with functions in CompressionAlg
inline uint32_t getDataTypeSizeInChunk(const common::LogicalType& dataType) {
    using namespace common;
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::STRUCT: {
        return 0;
    }
    case LogicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case LogicalTypeID::VAR_LIST: {
        return sizeof(offset_t);
    }
    case LogicalTypeID::INTERNAL_ID: {
        return sizeof(offset_t);
    }
    case LogicalTypeID::SERIAL: {
        return sizeof(int64_t);
    }
    default: {
        return StorageUtils::getDataTypeSize(dataType);
    }
    }
}

// TODO it doesn't make much sense to allow the CompressionAlg to keep state if it doesn't also own
// its buffer But keeping state might be helpful in some cases (but not yet anyway)
class CompressionAlg {
public:
    virtual ~CompressionAlg() = default;
    virtual inline const common::LogicalType& logicalType() const = 0;

    // Takes a single uncompressed value from the srcBuffer and compresses it into the dstBuffer
    // Offsets refer to value offsets, not byte offsets
    virtual void setValueFromUncompressed(uint8_t* srcBuffer, common::offset_t posInSrc,
        uint8_t* dstBuffer, common::offset_t posInDst) = 0;

    // Reads a value from the buffer at the given position and stores it at the given memory address
    // dst should point to an uncompressed value
    virtual inline void getValue(
        const uint8_t* buffer, common::offset_t pos, uint8_t* dst) const = 0;

    // TODO: this should probably be scoped. E.g. by having a separate class for handling
    // compression which is returned by the compress function. Called when compression starts. Will
    // always be called before compressNextPage
    // Returns the number of values per page (currently this must be constant)
    virtual uint64_t startCompression(
        const uint8_t* srcBuffer, uint64_t numValues, uint64_t pageSize) = 0;

    // Takes uncompressed data from the srcBuffer and compresses it into the dstBuffer
    //
    // stores only as much data in dstBuffer as will fit, and advances the srcBuffer pointer
    // to the beginning of the next value to store.
    // (This means that we can't start the next page on an unaligned value.
    // Maybe instead we could use value offsets, but the compression algorithms
    // usually work on aligned chunks anyway)
    //
    // dstBufferSize is the size in bytes
    // numValuesRemaining is the number of values remaining in the srcBuffer to be compressed.
    //      compressNextPage must store the least of either the number of values per page
    //      (as returned by startCompression), or the remaining number of values.
    //
    // returns the size in bytes of the compressed data within the page (rounded up to the nearest
    // byte)
    virtual uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize) = 0;

    // Takes compressed data from the srcBuffer and decompresses it into the dstBuffer
    // Offsets refer to value offsets, not byte offsets
    // srcBuffer points to the beginning of a page
    virtual void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
        uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues) = 0;

    // virtual uint64_t numBytesForValues(common::offset_t numValues) const = 0;
    virtual uint64_t numBytesForCompression(
        const uint8_t* srcBuffer, common::offset_t numValues) const = 0;
};

// Compression alg which does not compress values and instead just copies them.
class CopyCompression : public CompressionAlg {
public:
    CopyCompression(const common::LogicalType& logicalType) : mLogicalType{logicalType} {}
    const common::LogicalType& logicalType() const override { return mLogicalType; }

    void setValueFromUncompressed(uint8_t* srcBuffer, common::offset_t posInSrc, uint8_t* dstBuffer,
        common::offset_t posInDst) final {
        auto numBytesPerValue = getDataTypeSizeInChunk(logicalType());
        memcpy(dstBuffer + posInDst * numBytesPerValue, srcBuffer + posInSrc * numBytesPerValue,
            numBytesPerValue);
    }

    inline void getValue(const uint8_t* buffer, common::offset_t pos, uint8_t* dst) const override {
        auto numBytesPerValue = getDataTypeSizeInChunk(logicalType());
        memcpy(dst, buffer + pos * numBytesPerValue, numBytesPerValue);
    }

    uint64_t startCompression(
        const uint8_t* srcBuffer, uint64_t numValues, uint64_t pageSize) override {
        return pageSize / getDataTypeSizeInChunk(logicalType());
    }

    inline uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize) override {
        auto numBytesPerValue = getDataTypeSizeInChunk(logicalType());
        auto numValues = std::min(numValuesRemaining, dstBufferSize / numBytesPerValue);
        auto sizeToCopy = numValues * numBytesPerValue;
        std::memcpy(dstBuffer, srcBuffer, sizeToCopy);
        srcBuffer += sizeToCopy;
        return sizeToCopy;
    }

    inline void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) override {
        auto numBytesPerValue = getDataTypeSizeInChunk(logicalType());
        std::memcpy(dstBuffer + dstOffset * numBytesPerValue,
            srcBuffer + srcOffset * numBytesPerValue, numValues * numBytesPerValue);
    }

    uint64_t numBytesForCompression(
        const uint8_t* srcBuffer, common::offset_t numValues) const final {
        return getDataTypeSizeInChunk(logicalType()) * numValues;
    }

protected:
    common::LogicalType mLogicalType;
};

template<typename T>
constexpr common::LogicalTypeID getLogicalTypeID() {
    if (std::is_same<T, bool>()) {
        return common::LogicalTypeID::BOOL;
    } else if (std::is_same<T, int16_t>()) {
        return common::LogicalTypeID::INT16;
    } else if (std::is_same<T, int32_t>()) {
        return common::LogicalTypeID::INT32;
    } else if (std::is_same<T, int64_t>()) {
        return common::LogicalTypeID::INT64;
    }
    throw std::domain_error("Type does not have a corresponding physical type");
}

// TODO(bmwinger): assert that access is only done on data of at least 32 values
template<typename T, typename U>
class IntegerBitpacking : public CompressionAlg {
    static const common::LogicalType LOGICAL_TYPE;

public:
    const common::LogicalType& logicalType() const override { return LOGICAL_TYPE; }

    void setValueFromUncompressed(uint8_t* srcBuffer, common::offset_t posInSrc, uint8_t* dstBuffer,
        common::offset_t posInDst) final;

    void getValue(const uint8_t* buffer, common::offset_t pos, uint8_t* dst) const final;

    std::pair<uint8_t, bool> getBitWidth(const uint8_t* srcBuffer, uint64_t numValues) const {
        auto max = 0ull;
        auto hasNegative = false;
        for (int i = 0; i < numValues; i++) {
            T value = ((T*)srcBuffer)[i];
            auto abs = std::abs(value);
            if (abs > max) {
                max = abs;
            }
            if (value < 0) {
                hasNegative = true;
            }
        }
        if (hasNegative) {
            // Needs an extra bit for two's complement encoding
            return std::make_pair(std::bit_width(max) + 1, true);
        } else {
            return std::make_pair(std::bit_width(max), false);
        }
    }

    uint64_t numValuesPerPage(uint8_t bitWidth, uint64_t pageSize) {
        return (pageSize - 1) * 8 / bitWidth;
    }

    uint64_t startCompression(
        const uint8_t* srcBuffer, uint64_t numValues, uint64_t pageSize) override {
        auto result = getBitWidth(srcBuffer, numValues);
        bitWidth = result.first;
        hasNegative = result.second;
        return numValuesPerPage(bitWidth, pageSize);
    }

    uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize) final;

    void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) final;

    // TODO(bmwinger): maybe store the calculated bit width and whether or not there are
    // But it feels weird to do this if we don't own the buffer...
    // Maybe we should just have compress produce a buffer. decompress is simpler.
    uint64_t numBytesForCompression(
        const uint8_t* srcBuffer, common::offset_t numValues) const final {
        return 1 /* byte storing bit width */ +
               numValues * getBitWidth(srcBuffer, numValues).first / 8;
    }

protected:
    common::LogicalType mLogicalType;
    uint8_t bitWidth;
    bool hasNegative;
};

class BoolCompression : public CompressionAlg {
    static const common::LogicalType LOGICAL_TYPE;

public:
    inline const common::LogicalType& logicalType() const final { return LOGICAL_TYPE; }
    void setValueFromUncompressed(uint8_t* srcBuffer, common::offset_t posInSrc, uint8_t* dstBuffer,
        common::offset_t posInDst) final {
        auto val = ((bool*)srcBuffer)[posInSrc];
        common::NullMask::setNull((uint64_t*)dstBuffer, posInDst, val);
    }

    inline void getValue(const uint8_t* buffer, common::offset_t pos, uint8_t* dst) const final {
        *dst = common::NullMask::isNull((uint64_t*)buffer, pos);
    }

    uint64_t startCompression(
        const uint8_t* srcBuffer, uint64_t numValues, uint64_t pageSize) override {
        return pageSize * 8;
    }
    uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize) final {
        // TODO(bmwinger): Optimize, e.g. using an integer bitpacking function
        auto numValues = std::min(numValuesRemaining, dstBufferSize * 8);
        for (auto i = 0ull; i < numValues; i++) {
            common::NullMask::setNull((uint64_t*)dstBuffer, i, srcBuffer[i]);
        }
        srcBuffer += numValues / 8;
        // Will be a multiple of 8 except for the last iteration
        return numValues / 8 + (bool)(numValues % 8);
    }

    void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) final {
        // TODO(bmwinger): Optimize, e.g. using an integer bitpacking function
        for (auto i = 0ull; i < numValues; i++) {
            ((bool*)dstBuffer)[dstOffset + i] =
                common::NullMask::isNull((uint64_t*)srcBuffer, srcOffset + i);
        }
    }

    uint64_t numBytesForCompression(
        const uint8_t* srcBuffer, common::offset_t numValues) const final {
        // 8 values per byte, and we need a buffer size which is a multiple of 8 bytes
        return ceil(numValues / 8.0 / 8.0) * 8;
    }
};

// Functions to map logical data in a ValueVector to physical data on disk or in a buffer
class PhysicalMapping {
public:
    virtual inline const common::LogicalType& logicalType() const = 0;

    virtual void readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead) = 0;

    virtual void writeValueToPage(
        uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVector) = 0;

    virtual void getValue(uint8_t* frame, uint16_t posInFrame, uint8_t* result,
        uint32_t numBytesPerFixedSizedValue) = 0;
};

class CompressedMapping : public PhysicalMapping {
public:
    inline const common::LogicalType& logicalType() const final { return alg->logicalType(); }
    explicit CompressedMapping(std::unique_ptr<CompressionAlg> alg) : alg{std::move(alg)} {}

    void readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numValuesToRead) override {
        alg->decompressFromPage(
            frame, pageCursor.elemPosInPage, resultVector->getData(), posInVector, numValuesToRead);
    }
    void writeValueToPage(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector) override {
        alg->setValueFromUncompressed(vector->getData(), posInVector, frame, posInFrame);
    }

    void getValue(uint8_t* frame, uint16_t posInFrame, uint8_t* result,
        uint32_t numBytesPerFixedSizedValue) override {
        alg->getValue(frame, posInFrame, result);
    }

    std::unique_ptr<CompressionAlg> alg;
};

// Mapping for nulls where the values are bitpacked both in-memory and on-disk
class NullMapping : public PhysicalMapping {
    static const common::LogicalType LOGICAL_TYPE;

public:
    inline const common::LogicalType& logicalType() const final { return LOGICAL_TYPE; }

    void readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numValuesToRead) override {
        // Read bit-packed null flags from the frame into the result vector
        // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
        // Otherwise, it could read off the end of the page.
        resultVector->setNullFromBits(
            (uint64_t*)frame, pageCursor.elemPosInPage, posInVector, numValuesToRead);
    }
    void writeValueToPage(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector) override {
        common::NullMask::setNull((uint64_t*)frame, posInFrame,
            common::NullMask::isNull(vector->getNullMaskData(), posInVector));
    }

    void getValue(uint8_t* frame, uint16_t posInFrame, uint8_t* result,
        uint32_t numBytesPerFixedSizedValue) override {
        // Not actually used, since at the time of writing getValue is only used in batchLookup
        throw common::NotImplementedException("NullMapping::getValue");
    }
};

// Compression alg which does not compress values and instead just copies them.
class FixedValueMapping : public PhysicalMapping {
public:
    explicit FixedValueMapping(const common::LogicalType& logicalType)
        : mLogicalType{logicalType} {}

    inline const common::LogicalType& logicalType() const final { return mLogicalType; }

    void writeValueToPage(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector) override {
        auto numBytesPerValue = vector->getNumBytesPerValue();
        memcpy(frame + posInFrame * numBytesPerValue,
            vector->getData() + posInVector * numBytesPerValue, numBytesPerValue);
    }

    void readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numValuesToRead) override {
        auto numBytesPerValue = getDataTypeSizeInChunk(logicalType());
        std::memcpy(resultVector->getData() + posInVector * numBytesPerValue,
            frame + pageCursor.elemPosInPage * numBytesPerValue,
            numValuesToRead * numBytesPerValue);
    }

    void getValue(uint8_t* frame, uint16_t posInFrame, uint8_t* result,
        uint32_t numBytesPerFixedSizedValue) override {
        memcpy(
            result, frame + (posInFrame * numBytesPerFixedSizedValue), numBytesPerFixedSizedValue);
    }

protected:
    common::LogicalType mLogicalType;
};

// Mapping for internal ids since only their offset is stored on disk
class InternalIDMapping : public PhysicalMapping {
    static const common::LogicalType LOGICAL_TYPE;

public:
    inline const common::LogicalType& logicalType() const final { return LOGICAL_TYPE; }
    void readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead) final {
        auto resultData = (common::internalID_t*)resultVector->getData();
        for (auto i = 0u; i < numValuesToRead; i++) {
            auto posInFrame = pageCursor.elemPosInPage + i;
            resultData[posInVector + i].offset =
                *(common::offset_t*)(frame + (posInFrame * sizeof(common::offset_t)));
        }
    }

    void writeValueToPage(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector) final {
        auto relID = vector->getValue<common::relID_t>(posInVector);
        memcpy(
            frame + posInFrame * sizeof(common::offset_t), &relID.offset, sizeof(common::offset_t));
    }

    void getValue(uint8_t* frame, uint16_t posInFrame, uint8_t* result,
        uint32_t numBytesPerFixedSizedValue) override {
        memcpy(
            result, frame + (posInFrame * numBytesPerFixedSizedValue), numBytesPerFixedSizedValue);
    }
};

} // namespace storage
} // namespace kuzu
