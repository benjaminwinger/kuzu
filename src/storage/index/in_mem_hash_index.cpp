#include "storage/index/in_mem_hash_index.h"

#include <fcntl.h>

#include <cstring>

#include "common/type_utils.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/file_handle.h"
#include "storage/index/hash_index_header.h"
#include "storage/index/hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/overflow_file.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

// Average slot size to target
static const uint64_t SLOT_SIZE = 16;

template<typename T>
InMemHashIndex<T>::InMemHashIndex(OverflowFileHandle* overflowFileHandle)
    // TODO(bmwinger): Remove temp file and make the builder a completely separate class from the
    // disk array Or remove it entirely (though it might have some benefit as a custom version of
    // std::deque with a reasonable chunk size).
    : overflowFileHandle(overflowFileHandle), indexHeader{TypeUtils::getPhysicalTypeIDForType<T>()},
      slots{1ull << indexHeader.currentLevel} {}

template<typename T>
void InMemHashIndex<T>::allocateSlots(uint32_t newNumSlots) {
    auto numSlotsOfCurrentLevel = 1u << this->indexHeader.currentLevel;
    while ((numSlotsOfCurrentLevel << 1) <= newNumSlots) {
        this->indexHeader.incrementLevel();
        numSlotsOfCurrentLevel <<= 1;
    }
    if (newNumSlots >= numSlotsOfCurrentLevel) {
        this->indexHeader.nextSplitSlotId = newNumSlots - numSlotsOfCurrentLevel;
    }
    auto existingSlots = slots.size();
    if (newNumSlots > existingSlots) {
        slots.resize(newNumSlots);
    }
}

template<typename T>
void InMemHashIndex<T>::reserve(uint32_t numEntries_) {
    slot_id_t numRequiredEntries = HashIndexUtils::getNumRequiredEntries(numEntries_);
    auto numRequiredSlots = (numRequiredEntries + SLOT_SIZE - 1) / SLOT_SIZE;
    if (indexHeader.numEntries == 0) {
        allocateSlots(numRequiredSlots);
    } else {
        while (slots.size() < numRequiredSlots) {
            splitSlot(indexHeader);
        }
    }
}

template<typename T>
void InMemHashIndex<T>::splitSlot(HashIndexHeader& header) {
    // Add new slot
    slots.emplace_back();

    // Rehash the entries in the slot to split
    auto& slotToSplit = slots[header.nextSplitSlotId];
    for (size_t entryPos = 0; entryPos < slotToSplit.size(); entryPos++) {
        auto& entry = slotToSplit[entryPos];
        auto newSlotId = entry.hash & header.higherLevelHashMask;
        if (newSlotId != header.nextSplitSlotId) {
            // Move entry to the new slot
            slots[newSlotId].push_back(std::move(entry));
            // Shift old data to the back and truncate
            if (entryPos != slotToSplit.size() - 1) {
                std::swap(slotToSplit[entryPos], slotToSplit.back());
                // The entry that was moved from the back is processed next
                entryPos--;
            }
            slotToSplit.pop_back();
        }
    }

    header.incrementNextSplitSlotId();
}

template<typename T>
size_t InMemHashIndex<T>::append(const IndexBuffer<BufferKeyType>& buffer) {
    slot_id_t numRequiredEntries =
        HashIndexUtils::getNumRequiredEntries(this->indexHeader.numEntries + buffer.size());
    auto numRequiredSlots = (numRequiredEntries + SLOT_SIZE - 1) / SLOT_SIZE;
    while (slots.size() < numRequiredSlots) {
        this->splitSlot(this->indexHeader);
    }
    // Do both searches after splitting. Returning early if the key already exists isn't a
    // particular concern and doing both after splitting allows the slotID to be reused
    common::hash_t hashes[BUFFER_SIZE];
    for (size_t i = 0; i < buffer.size(); i++) {
        hashes[i] = HashIndexUtils::hash(buffer[i].first);
    }
    for (size_t i = 0; i < buffer.size(); i++) {
        auto& [key, value] = buffer[i];
        if (!appendInternal(key, value, hashes[i])) {
            return i;
        }
    }
    return buffer.size();
}

template<typename T>
bool InMemHashIndex<T>::appendInternal(Key key, common::offset_t value, common::hash_t hash) {
    auto slotID = HashIndexUtils::getPrimarySlotIdForHash(this->indexHeader, hash);
    auto& slot = slots[slotID];
    for (const auto& existing : slot) {
        if (existing.hash == hash && equals(key, existing.key)) {
            return false;
        }
    }
    if constexpr (std::same_as<T, ku_string_t>) {
        slot.emplace_back(hash, overflowFileHandle->writeString(key), value);
    } else {
        slot.emplace_back(hash, key, value);
    }
    this->indexHeader.numEntries++;
    return true;
}

template<typename T>
bool InMemHashIndex<T>::lookup(Key key, offset_t& result) {
    // This needs to be fast if the builder is empty since this function is always tried
    // when looking up in the persistent hash index
    if (this->indexHeader.numEntries == 0) {
        return false;
    }
    auto hashValue = HashIndexUtils::hash(key);
    auto slotId = HashIndexUtils::getPrimarySlotIdForHash(this->indexHeader, hashValue);
    auto& slot = slots[slotId];
    for (auto& entry : slot) {
        if (entry.hash == hashValue && equals(key, entry.key)) {
            // Value already exists
            result = entry.value;
            return true;
        }
    }
    return false;
}

template<>
bool InMemHashIndex<ku_string_t>::equals(std::string_view keyToLookup,
    const ku_string_t& keyInEntry) const {
    // Checks if prefix and len matches first.
    if (!HashIndexUtils::areStringPrefixAndLenEqual(keyToLookup, keyInEntry)) {
        return false;
    }
    if (keyInEntry.len <= ku_string_t::PREFIX_LENGTH) {
        // For strings shorter than PREFIX_LENGTH, the result must be true.
        return true;
    } else if (keyInEntry.len <= ku_string_t::SHORT_STR_LENGTH) {
        // For short strings, whose lengths are larger than PREFIX_LENGTH, check if their actual
        // values are equal.
        return memcmp(keyToLookup.data(), keyInEntry.prefix, keyInEntry.len) == 0;
    } else {
        // For long strings, compare with overflow data
        return overflowFileHandle->equals(transaction::TransactionType::WRITE, keyToLookup,
            keyInEntry);
    }
}

template<typename T>
void InMemHashIndex<T>::createEmptyIndexFiles(uint64_t indexPos, FileHandle& fileHandle) {
    InMemDiskArrayBuilder<HashIndexHeader> headerArray(fileHandle,
        NUM_HEADER_PAGES * indexPos + INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, 0 /*numElements*/);
    HashIndexHeader indexHeader(TypeUtils::getPhysicalTypeIDForType<T>());
    headerArray.resize(1, true /*setToZero=*/);
    headerArray[0] = indexHeader;
    InMemDiskArrayBuilder<Slot<T>> pSlots(fileHandle,
        NUM_HEADER_PAGES * indexPos + P_SLOTS_HEADER_PAGE_IDX, 0 /*numElements */);
    // Reserve a slot for oSlots, which is always skipped, as we treat slot idx 0 as NULL.
    InMemDiskArrayBuilder<Slot<T>> oSlots(fileHandle,
        NUM_HEADER_PAGES * indexPos + O_SLOTS_HEADER_PAGE_IDX, 1 /*numElements */);

    headerArray.saveToDisk();
    pSlots.saveToDisk();
    oSlots.saveToDisk();
}

template class InMemHashIndex<int64_t>;
template class InMemHashIndex<int32_t>;
template class InMemHashIndex<int16_t>;
template class InMemHashIndex<int8_t>;
template class InMemHashIndex<uint64_t>;
template class InMemHashIndex<uint32_t>;
template class InMemHashIndex<uint16_t>;
template class InMemHashIndex<uint8_t>;
template class InMemHashIndex<double>;
template class InMemHashIndex<float>;
template class InMemHashIndex<int128_t>;
template class InMemHashIndex<ku_string_t>;

} // namespace storage
} // namespace kuzu
