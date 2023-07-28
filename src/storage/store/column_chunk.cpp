#include "storage/store/column_chunk.h"

#include "storage/copier/table_copy_utils.h"
#include "storage/storage_structure/storage_structure_utils.h"
#include "storage/store/struct_column_chunk.h"
#include "storage/store/var_sized_column_chunk.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

ColumnChunk::ColumnChunk(LogicalType dataType, CopyDescription* copyDescription, bool hasNullChunk)
    : ColumnChunk{
          std::move(dataType), StorageConstants::NODE_GROUP_SIZE, copyDescription, hasNullChunk} {}

ColumnChunk::ColumnChunk(
    LogicalType dataType, offset_t numValues, CopyDescription* copyDescription, bool hasNullChunk)
    : dataType{std::move(dataType)}, numBitsPerValue{getDataTypeSizeInChunk(this->dataType)},
      numBytes{numBitsPerValue * numValues / 8}, copyDescription{copyDescription} {
    // TODO(bmwinger): Move logic into NullColumnChunk
    // Maybe we need a BaseColumnChunk which doesn't initialize the buffer, so that each subclass
    // can create a buffer that meets their requirements. Particularly when adding other compression
    // types which will presumably also have different size quirks. But it might be worth making a
    // more complex plan for that later.
    // buffer size needs to be a multiple of 8 bytes so that NullMask can treat it as a 64-bit
    // buffer.
    if (this->dataType.getLogicalTypeID() == LogicalTypeID::BOOL) {
        numBytes = std::ceil(numBytes / 8.0) * 8;
    }
    buffer = std::make_unique<uint8_t[]>(numBytes);
    if (hasNullChunk) {
        nullChunk = std::make_unique<NullColumnChunk>();
    }
}

void ColumnChunk::resetToEmpty() {
    if (nullChunk) {
        nullChunk->resetBuffer();
    }
}

void ColumnChunk::appendVector(
    ValueVector* vector, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    assert(vector->dataType.getLogicalTypeID() == LogicalTypeID::ARROW_COLUMN);
    auto array = ArrowColumnVector::getArrowColumn(vector).get();
    appendArray(array, startPosInChunk, numValuesToAppend);
}

void ColumnChunk::appendColumnChunk(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    if (nullChunk) {
        nullChunk->appendColumnChunk(
            other->nullChunk.get(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    }
    memcpy(buffer.get() + startPosInChunk * numBytesPerValue(),
        other->buffer.get() + startPosInOtherChunk * numBytesPerValue(),
        numValuesToAppend * numBytesPerValue());
}

void ColumnChunk::appendArray(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    switch (array->type_id()) {
    case arrow::Type::BOOL: {
        templateCopyArrowArray<bool>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::INT16: {
        templateCopyArrowArray<int16_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::INT32: {
        templateCopyArrowArray<int32_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::INT64: {
        templateCopyArrowArray<int64_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::DOUBLE: {
        templateCopyArrowArray<double_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::FLOAT: {
        templateCopyArrowArray<float_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::DATE32: {
        templateCopyArrowArray<common::date_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::TIMESTAMP: {
        templateCopyArrowArray<common::timestamp_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::FIXED_SIZE_LIST: {
        templateCopyArrowArray<uint8_t*>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::STRING: {
        switch (dataType.getLogicalTypeID()) {
        case LogicalTypeID::DATE: {
            templateCopyValuesAsString<date_t>(array, startPosInChunk, numValuesToAppend);
        } break;
        case LogicalTypeID::TIMESTAMP: {
            templateCopyValuesAsString<timestamp_t>(array, startPosInChunk, numValuesToAppend);
        } break;
        case LogicalTypeID::INTERVAL: {
            templateCopyValuesAsString<interval_t>(array, startPosInChunk, numValuesToAppend);
        } break;
        case LogicalTypeID::FIXED_LIST: {
            // Fixed list is a fixed-sized blob.
            templateCopyValuesAsString<uint8_t*>(array, startPosInChunk, numValuesToAppend);
        } break;
        default: {
            throw NotImplementedException(
                "Unsupported ColumnChunk::appendVector from arrow STRING");
        }
        }
    } break;
    default: {
        throw NotImplementedException("ColumnChunk::appendVector");
    }
    }
}

template<typename T>
void ColumnChunk::templateCopyArrowArray(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    const auto& arrowArray = array->data();
    auto valuesInChunk = (T*)buffer.get();
    auto valuesInArray = arrowArray->GetValues<T>(1 /* value buffer */);
    if (arrowArray->MayHaveNulls()) {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            if (arrowArray->IsNull(i)) {
                nullChunk->setNull(posInChunk, true);
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

template<>
void ColumnChunk::templateCopyArrowArray<bool>(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto* boolArray = (arrow::BooleanArray*)array;
    auto data = boolArray->data();

    auto arrowBuffer = boolArray->values()->data();
    // FIXME: Double-check that these offsets are correct
    // Might read off the end with the cast, but copyNullMask should ignore the extra data
    //
    // The arrow BooleanArray offset should be the offset in bits
    // Unfortunately this is not documented.
    NullMask::copyNullMask((uint64_t*)arrowBuffer, boolArray->offset(), (uint64_t*)buffer.get(),
        startPosInChunk, numValuesToAppend);

    if (data->MayHaveNulls()) {
        auto arrowNullBitMap = boolArray->null_bitmap_data();

        // Offset should apply to both bool data and nulls
        NullMask::copyNullMask((uint64_t*)arrowNullBitMap, boolArray->offset(),
            (uint64_t*)nullChunk->buffer.get(), startPosInChunk, numValuesToAppend);
    }
}

template<>
void ColumnChunk::templateCopyArrowArray<uint8_t*>(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto fixedSizedListArray = (arrow::FixedSizeListArray*)array;
    auto valuesInList = (uint8_t*)fixedSizedListArray->values()->data()->buffers[1]->data();
    if (fixedSizedListArray->data()->MayHaveNulls()) {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            if (fixedSizedListArray->data()->IsNull(i)) {
                nullChunk->setNull(posInChunk, true);
                continue;
            }
            auto posInList = fixedSizedListArray->offset() + i;
            memcpy(buffer.get() + getOffsetInBuffer(posInChunk),
                valuesInList + posInList * numBytesPerValue(), numBytesPerValue());
        }
    } else {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            auto posInList = fixedSizedListArray->offset() + i;
            memcpy(buffer.get() + getOffsetInBuffer(posInChunk),
                valuesInList + posInList * numBytesPerValue(), numBytesPerValue());
        }
    }
}

template<typename T>
void ColumnChunk::templateCopyValuesAsString(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto stringArray = (arrow::StringArray*)array;
    auto arrayData = stringArray->data();
    if (arrayData->MayHaveNulls()) {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            if (arrayData->IsNull(i)) {
                nullChunk->setNull(posInChunk, true);
                continue;
            }
            auto value = stringArray->GetView(i);
            setValueFromString<T>(value.data(), value.length(), posInChunk);
        }
    } else {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            auto value = stringArray->GetView(i);
            setValueFromString<T>(value.data(), value.length(), posInChunk);
        }
    }
}

common::page_idx_t ColumnChunk::getNumPages() const {
    auto numPagesToFlush = getNumPagesForBuffer();
    if (nullChunk) {
        numPagesToFlush += nullChunk->getNumPages();
    }
    for (auto& child : childrenChunks) {
        numPagesToFlush += child->getNumPages();
    }
    return numPagesToFlush;
}

page_idx_t ColumnChunk::flushBuffer(
    BMFileHandle* nodeGroupsDataFH, common::page_idx_t startPageIdx) {
    // Flush main buffer.
    FileUtils::writeToFile(nodeGroupsDataFH->getFileInfo(), buffer.get(), numBytes,
        startPageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    return getNumPagesForBuffer();
}

uint32_t ColumnChunk::getDataTypeSizeInChunk(common::LogicalType& dataType) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::STRUCT: {
        return 0;
    }
    case LogicalTypeID::STRING: {
        return sizeof(ku_string_t) * 8;
    }
    case LogicalTypeID::VAR_LIST: {
        return sizeof(ku_list_t) * 8;
    }
    case LogicalTypeID::INTERNAL_ID: {
        return sizeof(offset_t) * 8;
    }
    case LogicalTypeID::BOOL: {
        return 1;
    }
    default: {
        return StorageUtils::getDataTypeSize(dataType) * 8;
    }
    }
}

// TODO(bmwinger): Eventually, to support bitpacked bools, all these functions will need to be
// updated to support values sizes of less than one byte.
// But for the moment, this is the only generic ColumnChunk function which is needed by
// NullColumnChunk, and it's invoked directly on the nullColumn, so we don't need dynamic dispatch
void BoolColumnChunk::appendColumnChunk(ColumnChunk* other, common::offset_t startPosInOtherChunk,
    common::offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto otherChunk = static_cast<BoolColumnChunk*>(other);
    NullMask::copyNullMask((uint64_t*)otherChunk->buffer.get(), startPosInOtherChunk,
        (uint64_t*)buffer.get(), startPosInChunk, numValuesToAppend);

    if (nullChunk) {
        nullChunk->appendColumnChunk(
            otherChunk->nullChunk.get(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    }
}

void BoolColumnChunk::setValueFromString(const char* value, uint64_t length, common::offset_t pos) {
    std::istringstream boolStream{std::string(value)};
    bool booleanVal;
    boolStream >> std::boolalpha >> booleanVal;
    setValue(booleanVal, pos);
}

void FixedListColumnChunk::appendColumnChunk(kuzu::storage::ColumnChunk* other,
    common::offset_t startPosInOtherChunk, common::offset_t startPosInChunk,
    uint32_t numValuesToAppend) {
    auto otherChunk = (FixedListColumnChunk*)other;
    if (nullChunk) {
        nullChunk->appendColumnChunk(
            otherChunk->nullChunk.get(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    }
    // TODO: This can be optimized to not copy one by one.
    for (auto i = 0u; i < numValuesToAppend; i++) {
        memcpy(buffer.get() + getOffsetInBuffer(startPosInChunk + i),
            otherChunk->buffer.get() + getOffsetInBuffer(startPosInOtherChunk + i),
            numBytesPerValue());
    }
}

std::unique_ptr<ColumnChunk> ColumnChunkFactory::createColumnChunk(
    const LogicalType& dataType, CopyDescription* copyDescription) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        return std::make_unique<BoolColumnChunk>(copyDescription);
    }
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL: {
        return std::make_unique<ColumnChunk>(dataType, copyDescription);
    }
    case LogicalTypeID::FIXED_LIST: {
        return std::make_unique<FixedListColumnChunk>(dataType, copyDescription);
    }
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING:
    case LogicalTypeID::VAR_LIST: {
        return std::make_unique<VarSizedColumnChunk>(dataType, copyDescription);
    }
    case LogicalTypeID::STRUCT: {
        return std::make_unique<StructColumnChunk>(dataType, copyDescription);
    }
    default: {
        throw NotImplementedException("ColumnChunkFactory::createColumnChunk for data type " +
                                      LogicalTypeUtils::dataTypeToString(dataType) +
                                      " is not supported.");
    }
    }
}

// Fixed list
template<>
void ColumnChunk::setValueFromString<uint8_t*>(const char* value, uint64_t length, uint64_t pos) {
    auto fixedListVal =
        TableCopyUtils::getArrowFixedList(value, 1, length - 2, dataType, *copyDescription);
    memcpy(buffer.get() + pos * numBytesPerValue(), fixedListVal.get(), numBytesPerValue());
}

// Interval
template<>
void ColumnChunk::setValueFromString<interval_t>(const char* value, uint64_t length, uint64_t pos) {
    auto val = Interval::FromCString(value, length);
    setValue(val, pos);
}

// Date
template<>
void ColumnChunk::setValueFromString<date_t>(const char* value, uint64_t length, uint64_t pos) {
    auto val = Date::FromCString(value, length);
    setValue(val, pos);
}

// Timestamp
template<>
void ColumnChunk::setValueFromString<timestamp_t>(
    const char* value, uint64_t length, uint64_t pos) {
    auto val = Timestamp::FromCString(value, length);
    setValue(val, pos);
}

common::offset_t ColumnChunk::getOffsetInBuffer(common::offset_t pos) const {
    auto numElementsInAPage =
        PageUtils::getNumElementsInAPage(numBytesPerValue(), false /* hasNull */);
    auto posCursor =
        PageUtils::getPageByteCursorForPos(pos, numElementsInAPage, numBytesPerValue());
    auto offsetInBuffer =
        posCursor.pageIdx * common::BufferPoolConstants::PAGE_4KB_SIZE + posCursor.offsetInPage;
    assert(offsetInBuffer + numBytesPerValue() <= numBytes);
    return offsetInBuffer;
}

} // namespace storage
} // namespace kuzu
