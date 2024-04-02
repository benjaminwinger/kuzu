#pragma once

#include "hash_index_slot.h"

namespace kuzu {
namespace storage {

struct HashIndexHeaderOnDisk {
    uint8_t currentLevel;
    slot_id_t nextSplitSlotId;
    uint64_t numEntries;
};

class HashIndexHeader {
public:
    explicit HashIndexHeader()
        : currentLevel{1}, levelHashMask{1}, higherLevelHashMask{3}, nextSplitSlotId{0},
          numEntries{0} {}

    explicit HashIndexHeader(const HashIndexHeaderOnDisk& onDiskHeader)
        : currentLevel{onDiskHeader.currentLevel}, levelHashMask{(1ull << this->currentLevel) - 1},
          higherLevelHashMask{(1ull << (this->currentLevel + 1)) - 1},
          nextSplitSlotId{onDiskHeader.nextSplitSlotId}, numEntries{onDiskHeader.numEntries} {}

    inline void incrementLevel() {
        currentLevel++;
        nextSplitSlotId = 0;
        levelHashMask = (1 << currentLevel) - 1;
        higherLevelHashMask = (1 << (currentLevel + 1)) - 1;
    }
    inline void incrementNextSplitSlotId() {
        if (nextSplitSlotId < (1ull << currentLevel) - 1) {
            nextSplitSlotId++;
        } else {
            incrementLevel();
        }
    }

    inline void write(HashIndexHeaderOnDisk& onDiskHeader) const {
        onDiskHeader.currentLevel = currentLevel;
        onDiskHeader.nextSplitSlotId = nextSplitSlotId;
        onDiskHeader.numEntries = numEntries;
    }

public:
    uint64_t currentLevel;
    uint64_t levelHashMask;
    uint64_t higherLevelHashMask;
    slot_id_t nextSplitSlotId;
    uint64_t numEntries;
};

} // namespace storage
} // namespace kuzu
