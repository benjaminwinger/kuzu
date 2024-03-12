#include "storage/index/hash_index_builder.h"

#include <fcntl.h>

#include <cstring>

#include "common/type_utils.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/index/hash_index_header.h"
#include "storage/index/hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/overflow_file.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

template<IndexHashable T>
HashIndexBuilder<T>::HashIndexBuilder(OverflowFileHandle* overflowFileHandle)
    // Reserve a slot for oSlots, which is always skipped, as we treat slot idx 0 as NULL.
    : overflowFileHandle(overflowFileHandle), pSlots{0}, oSlots{1} {
    this->indexHeader = std::make_unique<HashIndexHeader>(TypeUtils::getPhysicalTypeIDForType<T>());
    allocatePSlots(1u << this->indexHeader->currentLevel);
}

template<IndexHashable T>
void HashIndexBuilder<T>::bulkReserve(uint32_t numEntries_) {
    slot_id_t numRequiredEntries = HashIndexUtils::getNumRequiredEntries(numEntries_);
    // Build from scratch.
    auto numRequiredSlots = (numRequiredEntries + getSlotCapacity<T>() - 1) / getSlotCapacity<T>();
    if (indexHeader->numEntries == 0) {
        auto numSlotsOfCurrentLevel = 1u << this->indexHeader->currentLevel;
        while ((numSlotsOfCurrentLevel << 1) <= numRequiredSlots) {
            this->indexHeader->incrementLevel();
            numSlotsOfCurrentLevel <<= 1;
        }
        if (numRequiredSlots >= numSlotsOfCurrentLevel) {
            this->indexHeader->nextSplitSlotId = numRequiredSlots - numSlotsOfCurrentLevel;
        }
        auto existingSlots = pSlots.size();
        if (numRequiredSlots > existingSlots) {
            allocatePSlots(numRequiredSlots - existingSlots);
        }
    } else {
        while (pSlots.size() < numRequiredSlots) {
            splitSlot(*indexHeader);
        }
    }
}

template<common::IndexHashable T>
void HashIndexBuilder<T>::copy(
    const SlotEntry<T>& oldEntry, slot_id_t newSlotId, uint8_t fingerprint) {
    SlotIterator iter(newSlotId, this);
    do {
        for (auto newEntryPos = 0u; newEntryPos < getSlotCapacity<T>(); newEntryPos++) {
            if (!iter.slot->header.isEntryValid(newEntryPos)) {
                // The original slot was marked as unused, but
                // copying to the original slot is unnecessary and will cause undefined behaviour
                if (&oldEntry != &iter.slot->entries[newEntryPos]) {
                    iter.slot->entries[newEntryPos] = oldEntry;
                }
                iter.slot->header.setEntryValid(newEntryPos, fingerprint);
                return;
            }
        }
    } while (nextChainedSlot(iter));
    // Didn't find an available entry in an existing slot. Insert a new overflow slot
    auto newOvfSlotId = allocateAOSlot();
    iter.slot->header.nextOvfSlotId = newOvfSlotId;
    auto newOvfSlot = getSlot(SlotInfo{newOvfSlotId, SlotType::OVF});
    auto newEntryPos = 0u; // Always insert to the first entry when there is a new slot.
    newOvfSlot->entries[newEntryPos] = oldEntry;
    newOvfSlot->header.setEntryValid(newEntryPos, fingerprint);
}

template<common::IndexHashable T>
void HashIndexBuilder<T>::splitSlot(HashIndexHeader& header) {
    // Add new slot
    allocatePSlots(1);

    // Rehash the entries in the slot to split
    SlotIterator iter(header.nextSplitSlotId, this);
    do {
        // Keep a copy of the header so we know which entries were valid
        SlotHeader slotHeader = iter.slot->header;
        // Reset everything except the next overflow id so we can reuse the overflow slot
        iter.slot->header.reset();
        iter.slot->header.nextOvfSlotId = slotHeader.nextOvfSlotId;
        for (auto entryPos = 0u; entryPos < getSlotCapacity<T>(); entryPos++) {
            if (!slotHeader.isEntryValid(entryPos)) {
                continue; // Skip invalid entries.
            }
            const auto& oldEntry = iter.slot->entries[entryPos];
            hash_t hash = this->hashStored(oldEntry.key);
            auto fingerprint = HashIndexUtils::getFingerprintForHash(hash);
            auto newSlotId = hash & header.higherLevelHashMask;
            copy(oldEntry, newSlotId, fingerprint);
        }
    } while (nextChainedSlot(iter));

    header.incrementNextSplitSlotId();
}

template<IndexHashable T>
size_t HashIndexBuilder<T>::append(const IndexBuffer<BufferKeyType>& buffer) {
    slot_id_t numRequiredEntries =
        HashIndexUtils::getNumRequiredEntries(this->indexHeader->numEntries + buffer.size());
    while (numRequiredEntries > pSlots.size() * getSlotCapacity<T>()) {
        this->splitSlot(*this->indexHeader);
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

template<IndexHashable T>
bool HashIndexBuilder<T>::appendInternal(Key key, common::offset_t value, common::hash_t hash) {
    auto fingerprint = HashIndexUtils::getFingerprintForHash(hash);
    auto slotID = HashIndexUtils::getPrimarySlotIdForHash(*this->indexHeader, hash);
    SlotIterator iter(slotID, this);
    do {
        for (auto entryPos = 0u; entryPos < getSlotCapacity<T>(); entryPos++) {
            if (!iter.slot->header.isEntryValid(entryPos)) {
                // Insert to this position
                // The builder never keeps holes and doesn't support deletions, so this must be the
                // end of the valid entries in this primary slot and the entry does not already
                // exist
                insert(key, iter.slot, entryPos, value, fingerprint);
                this->indexHeader->numEntries++;
                return true;
            } else if (iter.slot->header.fingerprints[entryPos] == fingerprint &&
                       equals(key, iter.slot->entries[entryPos].key)) {
                // Value already exists
                return false;
            }
        }
    } while (nextChainedSlot(iter));
    // Didn't find an available slot. Insert a new one
    insertToNewOvfSlot(key, iter.slot, value, fingerprint);
    this->indexHeader->numEntries++;
    return true;
}

template<IndexHashable T>
bool HashIndexBuilder<T>::lookup(Key key, offset_t& result) {
    // This needs to be fast if the builder is empty since this function is always tried
    // when looking up in the persistent hash index
    if (this->indexHeader->numEntries == 0) {
        return false;
    }
    auto hashValue = HashIndexUtils::hash(key);
    auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
    auto slotId = HashIndexUtils::getPrimarySlotIdForHash(*this->indexHeader, hashValue);
    SlotIterator iter(slotId, this);
    do {
        for (auto entryPos = 0u; entryPos < getSlotCapacity<T>(); entryPos++) {
            if (iter.slot->header.isEntryValid(entryPos) &&
                iter.slot->header.fingerprints[entryPos] == fingerprint &&
                equals(key, iter.slot->entries[entryPos].key)) {
                // Value already exists
                result = iter.slot->entries[entryPos].value;
                return true;
            }
        }
    } while (nextChainedSlot(iter));
    return false;
}

template<IndexHashable T>
uint32_t HashIndexBuilder<T>::allocatePSlots(uint32_t numSlotsToAllocate) {
    auto oldNumSlots = pSlots.size();
    auto newNumSlots = oldNumSlots + numSlotsToAllocate;
    pSlots.resize(newNumSlots);
    return oldNumSlots;
}

template<IndexHashable T>
uint32_t HashIndexBuilder<T>::allocateAOSlot() {
    auto oldNumSlots = oSlots.size();
    auto newNumSlots = oldNumSlots + 1;
    oSlots.resize(newNumSlots);
    return oldNumSlots;
}

template<IndexHashable T>
Slot<T>* HashIndexBuilder<T>::getSlot(const SlotInfo& slotInfo) {
    if (slotInfo.slotType == SlotType::PRIMARY) {
        return &pSlots.operator[](slotInfo.slotId);
    } else {
        return &oSlots.operator[](slotInfo.slotId);
    }
}

template<IndexHashable T>
inline void HashIndexBuilder<T>::insertToNewOvfSlot(
    Key key, Slot<T>* previousSlot, common::offset_t offset, uint8_t fingerprint) {
    auto newSlotId = allocateAOSlot();
    previousSlot->header.nextOvfSlotId = newSlotId;
    auto newSlot = getSlot(SlotInfo{newSlotId, SlotType::OVF});
    auto entryPos = 0u; // Always insert to the first entry when there is a new slot.
    insert(key, newSlot, entryPos, offset, fingerprint);
}

template<>
void HashIndexBuilder<ku_string_t>::insert(std::string_view key, Slot<ku_string_t>* slot,
    uint8_t entryPos, offset_t offset, uint8_t fingerprint) {
    auto& entry = slot->entries[entryPos];
    entry.key = overflowFileHandle->writeString(key);
    entry.value = offset;
    slot->header.setEntryValid(entryPos, fingerprint);
}

template<IndexHashable T>
common::hash_t HashIndexBuilder<T>::hashStored(const T& key) const {
    return HashIndexUtils::hash(key);
}

template<>
common::hash_t HashIndexBuilder<ku_string_t>::hashStored(const ku_string_t& key) const {
    auto kuString = key;
    return HashIndexUtils::hash(
        overflowFileHandle->readString(transaction::TransactionType::WRITE, kuString));
}

template<>
bool HashIndexBuilder<ku_string_t>::equals(
    std::string_view keyToLookup, const ku_string_t& keyInEntry) const {
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
        return overflowFileHandle->equals(
            transaction::TransactionType::WRITE, keyToLookup, keyInEntry);
    }
}

template class HashIndexBuilder<int64_t>;
template class HashIndexBuilder<int32_t>;
template class HashIndexBuilder<int16_t>;
template class HashIndexBuilder<int8_t>;
template class HashIndexBuilder<uint64_t>;
template class HashIndexBuilder<uint32_t>;
template class HashIndexBuilder<uint16_t>;
template class HashIndexBuilder<uint8_t>;
template class HashIndexBuilder<double>;
template class HashIndexBuilder<float>;
template class HashIndexBuilder<int128_t>;
template class HashIndexBuilder<ku_string_t>;

template<IndexHashable T>
void HashIndexBuilder<T>::createEmptyIndexFiles(uint64_t indexPos, FileHandle& fileHandle) {
    InMemDiskArrayBuilder<HashIndexHeader> headerArray(fileHandle,
        NUM_HEADER_PAGES * indexPos + INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, 0 /*numElements*/);
    HashIndexHeader indexHeader(TypeUtils::getPhysicalTypeIDForType<T>());
    headerArray.resize(1, true /*setToZero=*/);
    headerArray[0] = indexHeader;
    InMemDiskArrayBuilder<Slot<T>> pSlots(
        fileHandle, NUM_HEADER_PAGES * indexPos + P_SLOTS_HEADER_PAGE_IDX, 0 /*numElements */);
    // Reserve a slot for oSlots, which is always skipped, as we treat slot idx 0 as NULL.
    InMemDiskArrayBuilder<Slot<T>> oSlots(
        fileHandle, NUM_HEADER_PAGES * indexPos + O_SLOTS_HEADER_PAGE_IDX, 1 /*numElements */);

    headerArray.saveToDisk();
    pSlots.saveToDisk();
    oSlots.saveToDisk();
}

} // namespace storage
} // namespace kuzu
