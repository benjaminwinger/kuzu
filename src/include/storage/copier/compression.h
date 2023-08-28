#pragma once

#include <math.h>

#include <cstdint>

#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include <bit>
// TODO(bmwinger): Move to cpp
#include "arrow/array.h"
#include "common/null_mask.h"

namespace arrow {
class Array;
}

namespace kuzu {
namespace storage {

namespace duckdb {
using datatype = int32_t;
// From DDB
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
static void SignExtend(data_ptr_t dst, int width, size_t len) {
    datatype const mask = 1 << (width - 1);
    for (int i = 0; i < len; ++i) {
        datatype value = Load<datatype>(dst + i * sizeof(datatype));
        value = value & ((1 << width) - 1);
        datatype result = (value ^ mask) - mask;
        Store(result, dst + i * sizeof(datatype));
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

// TODO: this could probably be split up to better represent how NodeColumn and ColumnChunk use the
// information In one case we're manipulating compressed data, in the other we're dealing with
// storing and retrieving data from disk. they are related, but not the same.
//
// e.g. the Copy variants, particularly string, internalID, where we handle changes to the data
// layout (i.e. turning logical data into physical data) as opposed to the boolcompression, which
// handles the bool physical type.
//
// In fact, it may make sense to merge this back into ColumnChunk (wouldn't allow the
// CompressedMapping to own a CompressionAlg)
//
// templateCopyArrowArray needs a type argument, but read/write from page can't have one for
// copycompression So split them up, which will let CompressionAlg take a type argument

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
    // dst should point to a uncompressed value
    virtual inline void getValue(
        const uint8_t* buffer, common::offset_t pos, uint8_t* dst) const = 0;

    // Takes uncompressed data from the srcBuffer and compresses it into the dstBuffer
    // Offsets refer to value offsets, not byte offsets
    virtual void compress(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) = 0;

    // Takes compressed data from the srcBuffer and decompresses it into the dstBuffer
    // Offsets refer to value offsets, not byte offsets
    virtual void decompress(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) = 0;

    // Copies compressed data from one buffer to another
    // Offsets refer to value offsets, not byte offsets
    /*
    virtual void copyCompressed(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) = 0;
        */

    // Copies arrow data from the source array
    // Null values are copied elsewhere and should be ignored (though the null info can be used to
    // skip copying null values).
    /*
    virtual void compressFromArrowArray(arrow::Array* sourceArray, uint8_t* dstBuffer,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) = 0;
    */

    // virtual uint64_t numBytesForValues(common::offset_t numValues) const = 0;
    virtual uint64_t numBytesForCompression(
        const uint8_t* srcBuffer, common::offset_t numValues) const = 0;
};

// Compression alg which does not compress values and instead just copies them.
class CopyCompression : public CompressionAlg {
public:
    CopyCompression(const common::LogicalType& logicalType) : mLogicalType{logicalType} {}
    const common::LogicalType& logicalType() const override { return mLogicalType; }

    inline void getValue(const uint8_t* buffer, common::offset_t pos, uint8_t* dst) const override {
        auto numBytesPerValue = getDataTypeSizeInChunk(logicalType());
        memcpy(dst, buffer + pos * numBytesPerValue, numBytesPerValue);
    }

    void setValueFromUncompressed(uint8_t* srcBuffer, common::offset_t posInSrc, uint8_t* dstBuffer,
        common::offset_t posInDst) final {
        auto numBytesPerValue = getDataTypeSizeInChunk(logicalType());
        memcpy(dstBuffer + posInDst * numBytesPerValue, srcBuffer + posInSrc * numBytesPerValue,
            numBytesPerValue);
    }

    inline void compress(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) override {
        auto numBytesPerValue = getDataTypeSizeInChunk(logicalType());
        std::memcpy(dstBuffer + dstOffset * numBytesPerValue,
            srcBuffer + srcOffset * numBytesPerValue, numValues * numBytesPerValue);
    }

    inline void decompress(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) override {
        compress(srcBuffer, srcOffset, dstBuffer, dstOffset, numValues);
    }

    /*
    inline void copyCompressed(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) final {
        compress(srcBuffer, srcOffset, dstBuffer, dstOffset, numValues);
    }*/

    /*
    void compressFromArrowArray(arrow::Array* sourceArray, uint8_t* dstBuffer,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final {
        const auto& arrowArray = sourceArray->data();
        auto valuesInChunk = (T*)dstBuffer;
        auto valuesInArray = arrowArray->GetValues<T>(1 \/* value buffer *\/);
        // FIXME(bmwinger): Double check this works, but can probably just memcpy
        // std::memcpy(valuesInChunk + startPosInChunk, valuesInArray, numValuesToAppend);
        if (arrowArray->MayHaveNulls()) {
            for (auto i = 0u; i < numValuesToAppend; i++) {
                auto posInChunk = startPosInChunk + i;
                if (arrowArray->IsNull(i)) {
                    continue;
                }
                valuesInChunk[posInChunk] = valuesInArray[i];
            }
        } else {
            for (auto i = 0u; i < numValuesToAppend; i++) {
                auto posInChunk = startPosInChunk + i;
                valuesInChunk[posInChunk] = valuesInArray[i];
            }
        }
    }
    */

    uint64_t numBytesForCompression(
        const uint8_t* srcBuffer, common::offset_t numValues) const final {
        return getDataTypeSizeInChunk(logicalType()) * numValues;
    }

protected:
    common::LogicalType mLogicalType;
};

template<typename T>
constexpr common::LogicalTypeID getLogicalType() {
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

template<typename T, typename U>
class IntegerBitpacking : public CompressionAlg {
public:
    IntegerBitpacking(const common::LogicalType& logicalType) : mLogicalType{logicalType} {}

    void setValueFromUncompressed(uint8_t* srcBuffer, common::offset_t posInSrc, uint8_t* dstBuffer,
        common::offset_t posInDst) final;

    uint32_t getBitWidth(const uint8_t* srcBuffer, uint64_t numValues) const {
        auto max = 0ull;
        for (int i = 0; i < numValues; i++) {
            auto abs = std::abs((T)srcBuffer[i * sizeof(T)]);
            if (abs > max) {
                max = abs;
            }
        }
        return std::bit_width(max);
    }

    void compress(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) final;

    void decompress(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) final;

    // FIXME: computing this twice is inefficient.
    // Maybe we should store more state.
    uint64_t numBytesForCompression(
        const uint8_t* srcBuffer, common::offset_t numValues) const final {
        return 1 /* byte storing bit width */ + numValues * getBitWidth(srcBuffer, numValues) / 8;
    }

protected:
    common::LogicalType mLogicalType;
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
        // Buffer is rounded up to the nearest 8 bytes so that this cast is safe
        *dst = common::NullMask::isNull((uint64_t*)buffer, pos);
    }

    void compress(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) final {
        // TODO(bmwinger): Optimize, e.g. using an integer bitpacking function
        for (auto i = 0ull; i < numValues; i++) {
            common::NullMask::setNull(
                (uint64_t*)dstBuffer, dstOffset + i, srcBuffer[srcOffset + i]);
        }
    }

    void decompress(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) final {
        // TODO(bmwinger): Optimize, e.g. using an integer bitpacking function
        for (auto i = 0ull; i < numValues; i++) {
            ((bool*)dstBuffer)[dstOffset + i] =
                common::NullMask::isNull((uint64_t*)srcBuffer, srcOffset + i);
        }
    }

    /*
    void copyCompressed(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues) final {
        common::NullMask::copyNullMask(
            (const uint64_t*)srcBuffer, srcOffset, (uint64_t*)dstBuffer, dstOffset, numValues);
    }*/

    /*
    void compressFromArrowArray(arrow::Array* sourceArray, uint8_t* dstBuffer,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final {
        auto* boolArray = (arrow::BooleanArray*)sourceArray;
        auto data = boolArray->data();

        auto arrowBuffer = boolArray->values()->data();
        // Might read off the end with the cast, but copyNullMask should ignore the extra data
        //
        // The arrow BooleanArray offset should be the offset in bits
        // Unfortunately this is not documented.
        common::NullMask::copyNullMask((uint64_t*)arrowBuffer, boolArray->offset(),
            (uint64_t*)dstBuffer, startPosInChunk, numValuesToAppend);
    }
    */

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
};

class CompressedMapping : public PhysicalMapping {
public:
    inline const common::LogicalType& logicalType() const final { return alg->logicalType(); }
    CompressedMapping(std::unique_ptr<CompressionAlg> alg) : alg{std::move(alg)} {}

    void readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numValuesToRead) override {
        alg->decompress(
            frame, pageCursor.elemPosInPage, resultVector->getData(), posInVector, numValuesToRead);
    }
    void writeValueToPage(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector) override {
        alg->setValueFromUncompressed(vector->getData(), posInVector, frame, posInFrame);
    }

    std::unique_ptr<CompressionAlg> alg;
};

// Compression alg which does not compress values and instead just copies them.
class FixedValueMapping : public PhysicalMapping {
public:
    FixedValueMapping(const common::LogicalType& logicalType) : mLogicalType{logicalType} {}

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

protected:
    common::LogicalType mLogicalType;
};

// Compression alg for internal ids since they are read differently by NodeColumn
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
};

} // namespace storage
} // namespace kuzu
