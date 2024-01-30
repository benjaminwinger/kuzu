#pragma once

#include <shared_mutex>
#include <utility>

#include "common/types/types.h"
#include "function/hash/hash_functions.h"
#include "hash_index_header.h"
#include "hash_index_slot.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

enum class ChainedSlotsAction : uint8_t {
    EXISTS_IN_SLOTS,
    LOOKUP_IN_SLOTS,
    DELETE_IN_SLOTS,
    FIND_FREE_SLOT
};

enum class SlotType : uint8_t { PRIMARY = 0, OVF = 1 };

struct SlotInfo {
    slot_id_t slotId{UINT64_MAX};
    SlotType slotType{SlotType::PRIMARY};
};

static constexpr common::page_idx_t INDEX_HEADER_ARRAY_HEADER_PAGE_IDX = 0;
static constexpr common::page_idx_t P_SLOTS_HEADER_PAGE_IDX = 1;
static constexpr common::page_idx_t O_SLOTS_HEADER_PAGE_IDX = 2;
static constexpr common::page_idx_t NUM_HEADER_PAGES = 3;
static constexpr uint64_t INDEX_HEADER_IDX_IN_ARRAY = 0;

// T is the key type used to access values
// S is the stored type, which is usually the same as T, with the exception of strings
template<typename T, typename S = T>
class BaseHashIndex {
public:
    explicit BaseHashIndex() : slotCapacity(getSlotCapacity<S>()) {}

    virtual ~BaseHashIndex() = default;

protected:
    slot_id_t getPrimarySlotIdForKey(const HashIndexHeader& indexHeader, T key) const;

    static inline uint64_t getNumRequiredEntries(
        uint64_t numExistingEntries, uint64_t numNewEntries) {
        return ceil((double)(numExistingEntries + numNewEntries) * common::DEFAULT_HT_LOAD_FACTOR);
    }

    virtual void updateSlot(const SlotInfo& slotInfo, const Slot<S>& slot) = 0;

    virtual Slot<S> getSlot(transaction::TransactionType trxType, const SlotInfo& slotInfo) = 0;
    virtual uint32_t appendPSlot() = 0;
    // Returns the new overflow slot id
    virtual uint64_t appendOverflowSlot(Slot<S>&& slot) = 0;

    void rehashSlots(HashIndexHeader& header);
    void splitSlot(HashIndexHeader& header);
    std::vector<std::pair<SlotInfo, Slot<S>>> getChainedSlots(slot_id_t pSlotId);
    void loopChainedSlotsToFindOneWithFreeSpace(SlotInfo& slotInfo, Slot<S>& slot);

    template<typename K, bool isCopyEntry>
    void copyKVOrEntryToSlot(
        const SlotInfo& slotInfo, Slot<S>& slot, K key, common::offset_t value) {
        if (slot.header.numEntries == slotCapacity) {
            // Allocate a new oSlot, insert the entry to the new oSlot, and update slot's
            // nextOvfSlotId.
            Slot<S> newSlot;
            auto entryPos = 0u; // Always insert to the first entry when there is a new slot.
            copyAndUpdateSlotHeader<K, isCopyEntry>(newSlot, entryPos, key, value);
            slot.header.nextOvfSlotId = appendOverflowSlot(std::move(newSlot));
        } else {
            for (auto entryPos = 0u; entryPos < slotCapacity; entryPos++) {
                if (!slot.header.isEntryValid(entryPos)) {
                    copyAndUpdateSlotHeader<K, isCopyEntry>(slot, entryPos, key, value);
                    break;
                }
            }
        }
        updateSlot(slotInfo, slot);
    }

    void copyEntryToSlot(slot_id_t slotId, const S& entry);
    template<typename K, bool isCopyEntry>
    void copyAndUpdateSlotHeader(
        Slot<S>& slot, entry_pos_t entryPos, K key, common::offset_t value) {
        if constexpr (isCopyEntry) {
            memcpy(slot.entries[entryPos].data, &key, this->indexHeader->numBytesPerEntry);
        } else {
            insert(key, slot.entries[entryPos].data, value);
        }
        slot.header.setEntryValid(entryPos);
        slot.header.numEntries++;
    }

    virtual entry_pos_t findMatchedEntryInSlot(
        transaction::TransactionType trxType, const Slot<S>& slot, T key) const = 0;

    template<ChainedSlotsAction action>
    bool performActionInChainedSlots(transaction::TransactionType trxType, HashIndexHeader& header,
        SlotInfo& slotInfo, T key, common::offset_t& result) {
        while (slotInfo.slotType == SlotType::PRIMARY || slotInfo.slotId != 0) {
            auto slot = getSlot(trxType, slotInfo);
            if constexpr (action == ChainedSlotsAction::FIND_FREE_SLOT) {
                if (slot.header.numEntries < this->slotCapacity || slot.header.nextOvfSlotId == 0) {
                    // Found a slot with empty space.
                    break;
                }
            } else {
                auto entryPos = findMatchedEntryInSlot(trxType, slot, key);
                if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
                    if constexpr (action == ChainedSlotsAction::LOOKUP_IN_SLOTS) {
                        result = *(common::offset_t*)(slot.entries[entryPos].data +
                                                      this->indexHeader->numBytesPerKey);
                    } else if constexpr (action == ChainedSlotsAction::DELETE_IN_SLOTS) {
                        slot.header.setEntryInvalid(entryPos);
                        slot.header.numEntries--;
                        updateSlot(slotInfo, slot);
                        header.numEntries--;
                    }
                    return true;
                }
            }
            slotInfo.slotId = slot.header.nextOvfSlotId;
            slotInfo.slotType = SlotType::OVF;
        }
        return false;
    }

    inline common::hash_t hash(const T& key) const {
        common::hash_t hash;
        function::Hash::operation(key, hash);
        return hash;
    }

    virtual common::hash_t hashStored(transaction::TransactionType trxType, const S& key) const = 0;

    virtual inline void insert(T key, uint8_t* entry, common::offset_t offset) {
        memcpy(entry, &key, sizeof(T));
        memcpy(entry + sizeof(T), &offset, sizeof(common::offset_t));
    }

protected:
    std::unique_ptr<HashIndexHeader> indexHeader;
    std::shared_mutex pSlotSharedMutex;
    uint8_t slotCapacity;
};

} // namespace storage
} // namespace kuzu
