#pragma once

#include <array>
#include <cstdint>

#include "common/constants.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include <bit>

namespace kuzu {
namespace storage {

using entry_pos_t = uint8_t;
using slot_id_t = uint64_t;

class InMemSlotHeader {
public:
    static const entry_pos_t INVALID_ENTRY_POS = UINT8_MAX;
    InMemSlotHeader() : validityMask{0}, nextOvfSlotId{0} {}

    void reset() {
        validityMask = 0;
        nextOvfSlotId = 0;
    }

    inline bool isEntryValid(uint32_t entryPos) const {
        return validityMask & ((uint32_t)1 << entryPos);
    }

    inline void setEntryValid(entry_pos_t entryPos) { validityMask |= ((uint32_t)1 << entryPos); }

    inline void setEntryInvalid(entry_pos_t entryPos) {
        validityMask &= ~((uint32_t)1 << entryPos);
    }

    inline entry_pos_t numEntries() const { return std::popcount(validityMask); }

public:
    uint32_t validityMask;
    slot_id_t nextOvfSlotId;
};

class SlotHeader : public InMemSlotHeader {
public:
    // For a header of 32 bytes.
    // This is smaller than the possible number of entries with an 1-byte key like uint8_t,
    // but the additional size would limit the number of entries for 8-byte keys, so we
    // instead restrict the capacity to 20
    static constexpr uint8_t FINGERPRINT_CAPACITY = 20;

    inline void setEntryValid(entry_pos_t entryPos, uint8_t fingerprint) {
        InMemSlotHeader::setEntryValid(entryPos);
        fingerprints[entryPos] = fingerprint;
    }

public:
    std::array<uint8_t, FINGERPRINT_CAPACITY> fingerprints;
};

template<typename T>
struct SlotEntry {
    T key;
    common::offset_t value;

    inline uint8_t* data() const { return (uint8_t*)&key; }

    // otherEntry must be a pointer to the beginning of another slot's key field
    inline void copyFrom(const uint8_t* otherEntry) {
        memcpy(data(), otherEntry, sizeof(SlotEntry<T>));
    }
};

template<typename T>
static constexpr uint8_t getSlotCapacity() {
    return std::min((common::HashIndexConstants::SLOT_CAPACITY_BYTES - sizeof(SlotHeader)) /
                        sizeof(SlotEntry<T>),
        static_cast<size_t>(SlotHeader::FINGERPRINT_CAPACITY));
}

template<typename T>
static constexpr uint8_t getInMemSlotCapacity() {
    return ((common::HashIndexConstants::SLOT_CAPACITY_BYTES * 2) - sizeof(InMemSlotHeader)) /
           (sizeof(SlotEntry<T>) + sizeof(common::hash_t));
}

template<typename T>
struct Slot {
    SlotHeader header;
    std::array<SlotEntry<T>, getSlotCapacity<T>()> entries;
};

// Stores full hashes instead of fingerprints since we need to rehash more frequently
// and also need the hashes to merge the in memory hash index with the on-disk hash index
template<typename T>
struct InMemSlot {
    InMemSlotHeader header;
    std::array<common::hash_t, getInMemSlotCapacity<T>()> hashes;
    std::array<SlotEntry<T>, getInMemSlotCapacity<T>()> entries;
};

} // namespace storage
} // namespace kuzu
