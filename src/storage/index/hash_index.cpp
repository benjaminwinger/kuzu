#include "storage/index/hash_index.h"

#include <cstdint>
#include <iostream>
#include <optional>
#include <type_traits>

#include "common/assert.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "common/type_utils.h"
#include "common/types/int128_t.h"
#include "common/types/internal_id_t.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/index/hash_index_header.h"
#include "storage/index/hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

enum class HashIndexLocalLookupState : uint8_t { KEY_FOUND, KEY_DELETED, KEY_NOT_EXIST };

// Local storage consists of two in memory indexes. One (localInsertionIndex) is to keep track of
// all newly inserted entries, and the other (localDeletionIndex) is to keep track of newly deleted
// entries (not available in localInsertionIndex). We assume that in a transaction, the insertions
// and deletions are very small, thus they can be kept in memory.
template<typename T>
class HashIndexLocalStorage {
public:
    using Key = typename std::conditional<std::same_as<T, std::string>, std::string_view, T>::type;
    HashIndexLocalLookupState lookup(Key key, common::offset_t& result) {
        if (localDeletions.contains(key)) {
            return HashIndexLocalLookupState::KEY_DELETED;
        }
        auto elem = localInsertions.find(key);
        if (elem != localInsertions.end()) {
            result = elem->second;
            return HashIndexLocalLookupState::KEY_FOUND;
        } else {
            return HashIndexLocalLookupState::KEY_NOT_EXIST;
        }
    }

    void deleteKey(Key key) {
        auto iter = localInsertions.find(key);
        if (iter != localInsertions.end()) {
            localInsertions.erase(iter);
        } else {
            localDeletions.insert(static_cast<T>(key));
        }
    }

    bool insert(Key key, common::offset_t value) {
        auto iter = localDeletions.find(key);
        if (iter != localDeletions.end()) {
            localDeletions.erase(iter);
        }
        if (localInsertions.contains(key)) {
            return false;
        }
        localInsertions[static_cast<T>(key)] = value;
        return true;
    }

    inline bool hasUpdates() const { return !(localInsertions.empty() && localDeletions.empty()); }

    inline int64_t getNetInserts() const { return localInsertions.size() - localDeletions.size(); }

    inline void clear() {
        localInsertions.clear();
        localDeletions.clear();
    }

    void applyLocalChanges(const std::function<void(T)>& deleteOp,
        const std::function<void(T, common::offset_t)>& insertOp) {
        for (auto& key : localDeletions) {
            deleteOp(key);
        }
        for (auto& [key, val] : localInsertions) {
            insertOp(key, val);
        }
    }

private:
    // When the storage type is string, allow the key type to be string_view with a custom hash
    // function
    using hash_function = typename std::conditional<std::is_same<T, std::string>::value,
        common::StringUtils::string_hash, std::hash<T>>::type;
    std::unordered_map<T, common::offset_t, hash_function, std::equal_to<>> localInsertions;
    std::unordered_set<T, hash_function, std::equal_to<>> localDeletions;
};

template<typename T>
HashIndex<T>::HashIndex(const DBFileIDAndName& dbFileIDAndName,
    const std::shared_ptr<BMFileHandle>& fileHandle, OverflowFileHandle* overflowFileHandle,
    uint64_t indexPos, BufferManager& bufferManager, WAL* wal)
    : dbFileIDAndName{dbFileIDAndName}, bm{bufferManager}, wal{wal}, fileHandle(fileHandle),
      overflowFileHandle(overflowFileHandle), bulkInsertLocalStorage{overflowFileHandle} {
    // TODO: Handle data not existing
    headerArray = std::make_unique<BaseDiskArray<HashIndexHeader>>(*fileHandle,
        dbFileIDAndName.dbFileID, NUM_HEADER_PAGES * indexPos + INDEX_HEADER_ARRAY_HEADER_PAGE_IDX,
        &bm, wal, Transaction::getDummyReadOnlyTrx().get());
    // Read indexHeader from the headerArray, which contains only one element.
    this->indexHeaderForReadTrx = std::make_unique<HashIndexHeader>(
        headerArray->get(INDEX_HEADER_IDX_IN_ARRAY, TransactionType::READ_ONLY));
    this->indexHeaderForWriteTrx = std::make_unique<HashIndexHeader>(*indexHeaderForReadTrx);
    KU_ASSERT(
        this->indexHeaderForReadTrx->keyDataTypeID == TypeUtils::getPhysicalTypeIDForType<T>());
    pSlots = std::make_unique<BaseDiskArray<Slot<T>>>(*fileHandle, dbFileIDAndName.dbFileID,
        NUM_HEADER_PAGES * indexPos + P_SLOTS_HEADER_PAGE_IDX, &bm, wal,
        Transaction::getDummyReadOnlyTrx().get());
    oSlots = std::make_unique<BaseDiskArray<Slot<T>>>(*fileHandle, dbFileIDAndName.dbFileID,
        NUM_HEADER_PAGES * indexPos + O_SLOTS_HEADER_PAGE_IDX, &bm, wal,
        Transaction::getDummyReadOnlyTrx().get());
    // Initialize functions.
    localStorage = std::make_unique<HashIndexLocalStorage<BufferKeyType>>();
}

// For read transactions, local storage is skipped, lookups are performed on the persistent
// storage. For write transactions, lookups are performed in the local storage first, then in
// the persistent storage if necessary. In details, there are three cases for the local storage
// lookup:
// - the key is found in the local storage, directly return true;
// - the key has been marked as deleted in the local storage, return false;
// - the key is neither deleted nor found in the local storage, lookup in the persistent
// storage.
template<typename T>
bool HashIndex<T>::lookupInternal(Transaction* transaction, Key key, offset_t& result) {
    if (transaction->isReadOnly()) {
        return lookupInPersistentIndex(transaction->getType(), key, result);
    } else {
        KU_ASSERT(transaction->isWriteTransaction());
        auto localLookupState = localStorage->lookup(key, result);
        if (localLookupState == HashIndexLocalLookupState::KEY_FOUND) {
            return true;
        } else if (localLookupState == HashIndexLocalLookupState::KEY_DELETED) {
            return false;
        } else if (bulkInsertLocalStorage.lookup(key, result)) {
            return true;
        } else {
            KU_ASSERT(localLookupState == HashIndexLocalLookupState::KEY_NOT_EXIST);
            return lookupInPersistentIndex(transaction->getType(), key, result);
        }
    }
}

// For deletions, we don't check if the deleted keys exist or not. Thus, we don't need to check
// in the persistent storage and directly delete keys in the local storage.
template<typename T>
void HashIndex<T>::deleteInternal(Key key) const {
    localStorage->deleteKey(key);
}

// For insertions, we first check in the local storage. There are three cases:
// - the key is found in the local storage, return false;
// - the key is marked as deleted in the local storage, insert the key to the local storage;
// - the key doesn't exist in the local storage, check if the key exists in the persistent
// index, if
//   so, return false, else insert the key to the local storage.
template<typename T>
bool HashIndex<T>::insertInternal(Key key, offset_t value) {
    offset_t tmpResult;
    auto localLookupState = localStorage->lookup(key, tmpResult);
    if (localLookupState == HashIndexLocalLookupState::KEY_FOUND) {
        return false;
    } else if (localLookupState == HashIndexLocalLookupState::KEY_NOT_EXIST) {
        if (lookupInPersistentIndex(TransactionType::WRITE, key, tmpResult)) {
            return false;
        }
    }
    return localStorage->insert(key, value);
}

template<typename T>
bool HashIndex<T>::lookupInPersistentIndex(TransactionType trxType, Key key, offset_t& result) {
    auto& header = trxType == TransactionType::READ_ONLY ? *this->indexHeaderForReadTrx :
                                                           *this->indexHeaderForWriteTrx;
    // There may not be any primary key slots if we try to lookup on an empty index
    if (header.numEntries == 0) {
        return false;
    }
    auto hashValue = HashIndexUtils::hash(key);
    auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
    auto iter =
        getSlotIterator(HashIndexUtils::getPrimarySlotIdForHash(header, hashValue), trxType);
    do {
        auto entryPos = findMatchedEntryInSlot(trxType, iter.slot, key, fingerprint);
        if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
            result = iter.slot.entries[entryPos].value;
            return true;
        }
    } while (nextChainedSlot(trxType, iter));
    // std::cout << toString(TransactionType::WRITE);
    // validateEntries(TransactionType::WRITE);
    return false;
}

template<typename T>
void HashIndex<T>::insertIntoPersistentIndex(Key key, offset_t value) {
    auto& header = *this->indexHeaderForWriteTrx;
    slot_id_t numRequiredEntries = HashIndexUtils::getNumRequiredEntries(header.numEntries + 1);
    while (numRequiredEntries >
           pSlots->getNumElements(TransactionType::WRITE) * getSlotCapacity<T>()) {
        this->splitSlot(header);
    }
    auto hashValue = HashIndexUtils::hash(key);
    auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
    auto iter = getSlotIterator(
        HashIndexUtils::getPrimarySlotIdForHash(header, hashValue), TransactionType::WRITE);
    // Find a slot with free entries
    while (iter.slot.header.numEntries() == getSlotCapacity<T>() &&
           nextChainedSlot(TransactionType::WRITE, iter))
        ;
    copyKVOrEntryToSlot<Key, false /* insert kv */>(
        iter.slotInfo, iter.slot, key, value, fingerprint);
    updateSlot(iter.slotInfo, iter.slot);
    header.numEntries++;
}

template<typename T>
void HashIndex<T>::deleteFromPersistentIndex(Key key) {
    auto trxType = TransactionType::WRITE;
    auto header = *this->indexHeaderForWriteTrx;
    auto hashValue = HashIndexUtils::hash(key);
    auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
    auto iter =
        getSlotIterator(HashIndexUtils::getPrimarySlotIdForHash(header, hashValue), trxType);
    do {
        auto entryPos = findMatchedEntryInSlot(trxType, iter.slot, key, fingerprint);
        if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
            iter.slot.header.setEntryInvalid(entryPos);
            updateSlot(iter.slotInfo, iter.slot);
            header.numEntries--;
        }
    } while (nextChainedSlot(trxType, iter));
}

template<>
inline common::hash_t HashIndex<ku_string_t>::hashStored(
    transaction::TransactionType /*trxType*/, const ku_string_t& key) const {
    common::hash_t hash;
    auto str = overflowFileHandle->readString(TransactionType::WRITE, key);
    function::Hash::operation(str, hash);
    return hash;
}

template<typename T>
entry_pos_t HashIndex<T>::findMatchedEntryInSlot(
    TransactionType trxType, const Slot<T>& slot, Key key, uint8_t fingerprint) const {
    for (auto entryPos = 0u; entryPos < getSlotCapacity<T>(); entryPos++) {
        if (slot.header.isEntryValid(entryPos) &&
            slot.header.fingerprints[entryPos] == fingerprint &&
            equals(trxType, key, slot.entries[entryPos].key)) {
            return entryPos;
        }
    }
    return SlotHeader::INVALID_ENTRY_POS;
}

template<typename T>
void HashIndex<T>::prepareCommit() {
    if (localStorage->hasUpdates()) {
        wal->addToUpdatedTables(dbFileIDAndName.dbFileID.nodeIndexID.tableID);
        auto netInserts = localStorage->getNetInserts();
        if (netInserts > 0) {
            reserve(netInserts);
        }
        localStorage->applyLocalChanges(
            [this](Key key) -> void { this->deleteFromPersistentIndex(key); },
            [this](
                Key key, offset_t value) -> void { this->insertIntoPersistentIndex(key, value); });
        headerArray->update(INDEX_HEADER_IDX_IN_ARRAY, *indexHeaderForWriteTrx);
    }
    if (bulkInsertLocalStorage.size() > 0) {
        wal->addToUpdatedTables(dbFileIDAndName.dbFileID.nodeIndexID.tableID);
        mergeBulkInserts();
        headerArray->update(INDEX_HEADER_IDX_IN_ARRAY, *indexHeaderForWriteTrx);
    }
}

template<typename T>
void HashIndex<T>::prepareRollback() {
    if (localStorage->hasUpdates()) {
        wal->addToUpdatedTables(dbFileIDAndName.dbFileID.nodeIndexID.tableID);
    }
}

template<typename T>
void HashIndex<T>::checkpointInMemory() {
    if (!localStorage->hasUpdates()) {
        return;
    }
    *indexHeaderForReadTrx = *indexHeaderForWriteTrx;
    headerArray->checkpointInMemoryIfNecessary();
    pSlots->checkpointInMemoryIfNecessary();
    oSlots->checkpointInMemoryIfNecessary();
    localStorage->clear();
    if constexpr (std::same_as<ku_string_t, T>) {
        overflowFileHandle->checkpointInMemory();
    }
}

template<typename T>
void HashIndex<T>::rollbackInMemory() {
    if (!localStorage->hasUpdates()) {
        return;
    }
    headerArray->rollbackInMemoryIfNecessary();
    pSlots->rollbackInMemoryIfNecessary();
    oSlots->rollbackInMemoryIfNecessary();
    localStorage->clear();
    *indexHeaderForWriteTrx = *indexHeaderForReadTrx;
}

template<>
inline bool HashIndex<ku_string_t>::equals(transaction::TransactionType trxType,
    std::string_view keyToLookup, const ku_string_t& keyInEntry) const {
    if (HashIndexUtils::areStringPrefixAndLenEqual(keyToLookup, keyInEntry)) {
        auto entryKeyString = overflowFileHandle->readString(trxType, keyInEntry);
        return memcmp(keyToLookup.data(), entryKeyString.c_str(), entryKeyString.length()) == 0;
    }
    return false;
}

template<>
inline void HashIndex<ku_string_t>::insert(
    std::string_view key, SlotEntry<ku_string_t>& entry, common::offset_t offset) {
    entry.key = overflowFileHandle->writeString(key);
    entry.value = offset;
}

template<typename T>
void HashIndex<T>::rehashSlots(HashIndexHeader& header) {
    auto slotsToSplit = getChainedSlots(header.nextSplitSlotId);
    for (auto& [slotInfo, slot] : slotsToSplit) {
        auto slotHeader = slot.header;
        slot.header.reset();
        updateSlot(slotInfo, slot);
        for (auto entryPos = 0u; entryPos < getSlotCapacity<T>(); entryPos++) {
            if (!slotHeader.isEntryValid(entryPos)) {
                continue; // Skip invalid entries.
            }
            const auto& key = slot.entries[entryPos].key;
            hash_t hash = this->hashStored(TransactionType::WRITE, key);
            auto fingerprint = HashIndexUtils::getFingerprintForHash(hash);
            auto newSlotId = hash & header.higherLevelHashMask;
            if (newSlotId >= pSlots->getNumElements(TransactionType::WRITE)) {
                KU_ASSERT(false);
            }
            copyEntryToSlot(newSlotId, key, fingerprint);
        }
    }
}

template<typename T>
std::vector<std::pair<SlotInfo, Slot<T>>> HashIndex<T>::getChainedSlots(slot_id_t pSlotId) {
    std::vector<std::pair<SlotInfo, Slot<T>>> slots;
    SlotInfo slotInfo{pSlotId, SlotType::PRIMARY};
    while (slotInfo.slotType == SlotType::PRIMARY || slotInfo.slotId != 0) {
        auto slot = getSlot(TransactionType::WRITE, slotInfo);
        slots.emplace_back(slotInfo, slot);
        slotInfo.slotId = slot.header.nextOvfSlotId;
        slotInfo.slotType = SlotType::OVF;
    }
    return slots;
}

template<typename T>
void HashIndex<T>::copyEntryToSlot(slot_id_t slotId, const T& entry, uint8_t fingerprint) {
    auto iter = getSlotIterator(slotId, TransactionType::WRITE);
    do {
        if (iter.slot.header.numEntries() < getSlotCapacity<T>()) {
            // Found a slot with empty space.
            break;
        }
    } while (nextChainedSlot(TransactionType::WRITE, iter));
    copyKVOrEntryToSlot<const T&, true /* copy entry */>(
        iter.slotInfo, iter.slot, entry, UINT32_MAX, fingerprint);
    updateSlot(iter.slotInfo, iter.slot);
}
template<typename T>
void HashIndex<T>::reserve(uint64_t newEntries) {
    slot_id_t numRequiredEntries = HashIndexUtils::getNumRequiredEntries(
        this->indexHeaderForWriteTrx->numEntries + newEntries);
    // Can be no fewer slots that the current level requires
    auto numRequiredSlots =
        std::max((numRequiredEntries + getSlotCapacity<T>() - 1) / getSlotCapacity<T>(),
            1ul << this->indexHeaderForWriteTrx->currentLevel);
    // If there are no entries, we can just re-size the number of primary slots and re-calculate the
    // levels
    if (this->indexHeaderForWriteTrx->numEntries == 0) {
        pSlots->resize(numRequiredSlots);

        auto numSlotsOfCurrentLevel = 1u << this->indexHeaderForWriteTrx->currentLevel;
        while ((numSlotsOfCurrentLevel << 1) <= numRequiredSlots) {
            this->indexHeaderForWriteTrx->incrementLevel();
            numSlotsOfCurrentLevel <<= 1;
        }
        if (numRequiredSlots >= numSlotsOfCurrentLevel) {
            this->indexHeaderForWriteTrx->nextSplitSlotId =
                numRequiredSlots - numSlotsOfCurrentLevel;
        };
    } else {
        // Otherwise, split and re-hash until there are enough primary slots
        // TODO(bmwinger): resize pSlots first, update the levels and then rehash from the original
        // nextSplitSlotId to the new nextSplitSlotId using the final slot function to avoid
        // re-hashing a slot multiple times
        for (auto slots = pSlots->getNumElements(TransactionType::WRITE); slots < numRequiredSlots;
             slots++) {
            splitSlot(*this->indexHeaderForWriteTrx);
        }
    }
}

template<typename T>
void HashIndex<T>::mergeBulkInserts() {
    // TODO: Ideally we can split slots at the same time that we insert new ones
    // Resizing the pSlots diskArray does a write for each added slot, so we don't want to do that
    // Just compute the new number of primary slots, and iterate over each slot, determining if it
    // needs to be split (and how many times, which is complicated) and insert/rehash each element
    // one by one. Rehashed entries should be copied into a new slot and then that new slot (with
    // the entries from the respective slot in the local storage) should be processed immediately to
    // avoid increasing memory usage.
    //
    // On the other hand, two passes may not be significantly slower than one
    reserve(bulkInsertLocalStorage.size());
    // TODO: Remove; they should be equivalent after reserve
    KU_ASSERT(
        bulkInsertLocalStorage.pSlots.size() == pSlots->getNumElements(TransactionType::WRITE));
    KU_ASSERT(this->indexHeaderForWriteTrx->currentLevel ==
              bulkInsertLocalStorage.indexHeader->currentLevel);
    KU_ASSERT(this->indexHeaderForWriteTrx->levelHashMask ==
              bulkInsertLocalStorage.indexHeader->levelHashMask);
    KU_ASSERT(this->indexHeaderForWriteTrx->higherLevelHashMask ==
              bulkInsertLocalStorage.indexHeader->higherLevelHashMask);
    KU_ASSERT(this->indexHeaderForWriteTrx->nextSplitSlotId ==
              bulkInsertLocalStorage.indexHeader->nextSplitSlotId);

    auto originalNumEntries = this->indexHeaderForWriteTrx->numEntries;

    // Storing as many slots in-memory as on-disk shouldn't be necessary (for one it makes memory
    // usage an issue as we may need significantly more memory to store the slots that we would
    // otherwise) Instead, when merging here we can re-hash and split each in-memory slot (into
    // temporary vector buffers instead of slots for improved performance) and then merge each of
    // those one at a time into the disk slots. That will keep the low memory requirements and still
    // let us update each on-disk slot one at a time.
    for (uint64_t slotId = 0; slotId < bulkInsertLocalStorage.pSlots.size(); slotId++) {
        auto localSlot =
            typename HashIndexBuilder<T>::SlotIterator(slotId, &bulkInsertLocalStorage);
        // If mask is empty, skip this slot
        if (!localSlot.slot->header.validityMask) {
            // There should be no entries in the overflow slots, as we never leave gaps in the
            // builder
            continue;
        }
        auto diskSlot = getSlotIterator(slotId, TransactionType::WRITE);
        bool isNewDiskSlot = false;
        bool diskSlotChanged = false;
        slot_id_t diskEntryPos = 0u;
        // Merge slot from local storage to existing slot
        // Values which need to be rehashed to a different slot get moved into local storage
        do {
            for (auto entryPos = 0u; entryPos < getSlotCapacity<T>(); entryPos++) {
                if (localSlot.slot->header.isEntryValid(entryPos)) {
                    // Find the next empty entry, or add a new slot if there are no more entries
                    while (diskSlot.slot.header.isEntryValid(diskEntryPos) ||
                           diskEntryPos >= getSlotCapacity<T>()) {
                        diskEntryPos++;
                        if (diskEntryPos >= getSlotCapacity<T>()) {
                            // If there are no more disk slots in this chain, we need to add one
                            // To avoid updating the slot twice, use the current number of overflow
                            // slots as the next index to be added.
                            auto originalNextOvfSlotId = diskSlot.slot.header.nextOvfSlotId;
                            if (originalNextOvfSlotId == 0) {
                                // If it's a new disk slot, then it will be written to the end of
                                // the overflow slots and we want the following slot id as the next
                                // slot id
                                diskSlot.slot.header.nextOvfSlotId =
                                    oSlots->getNumElements(TransactionType::WRITE) +
                                    (isNewDiskSlot ? 1 : 0);
                                KU_ASSERT(diskSlot.slot.header.nextOvfSlotId !=
                                              diskSlot.slotInfo.slotId ||
                                          diskSlot.slotInfo.slotType != SlotType::OVF);
                            }
                            if (diskSlotChanged) {
                                if (isNewDiskSlot) {
                                    appendOverflowSlot(std::move(diskSlot.slot));
                                } else {
                                    updateSlot(diskSlot.slotInfo, diskSlot.slot);
                                }
                            }
                            if (originalNextOvfSlotId == 0) {
                                diskSlot.slot = Slot<T>();
                                diskSlot.slotInfo = {
                                    oSlots->getNumElements(TransactionType::WRITE), SlotType::OVF};
                                // updateSlot will fail on new slots
                                isNewDiskSlot = true;
                            } else {
                                nextChainedSlot(TransactionType::WRITE, diskSlot);
                            }
                            diskSlotChanged = false;
                            diskEntryPos = 0;
                        }
                    }
                    diskSlotChanged = true;
                    KU_ASSERT(diskEntryPos < getSlotCapacity<T>());
                    copyAndUpdateSlotHeader<const T&, true>(diskSlot.slot, diskEntryPos,
                        localSlot.slot->entries[entryPos].key, UINT32_MAX,
                        localSlot.slot->header.fingerprints[entryPos]);
                    /*auto hash =
                        hashStored(TransactionType::WRITE, localSlot.slot->entries[entryPos].key);
                    auto primarySlot =
                        HashIndexUtils::getPrimarySlotIdForHash(*indexHeaderForWriteTrx, hash);
                    if (primarySlot != slotId) {
                        KU_ASSERT(false);
                    }*/
                    indexHeaderForWriteTrx->numEntries++;
                    diskEntryPos++;
                }
            }
        } while (bulkInsertLocalStorage.nextChainedSlot(localSlot));
        if (diskSlotChanged) {
            if (isNewDiskSlot) {
                appendOverflowSlot(std::move(diskSlot.slot));
            } else {
                updateSlot(diskSlot.slotInfo, diskSlot.slot);
            }
        }
    }
    // validateEntries(transaction::TransactionType::WRITE);
    KU_ASSERT(originalNumEntries + bulkInsertLocalStorage.indexHeader->numEntries ==
              indexHeaderForWriteTrx->numEntries);
}

template<typename T>
void HashIndex<T>::forEach(transaction::TransactionType trxType,
    std::function<void(slot_id_t, uint8_t, SlotEntry<T>)> func) {
    for (auto slotId = 0u; slotId < pSlots->getNumElements(trxType); slotId++) {
        auto iter = getSlotIterator(slotId, trxType);
        do {
            if (!iter.slot.header.validityMask) {
                continue;
            }
            for (auto entryPos = 0; entryPos < getSlotCapacity<T>(); entryPos++) {
                if (iter.slot.header.isEntryValid(entryPos)) {
                    func(slotId, iter.slot.header.fingerprints[entryPos],
                        iter.slot.entries[entryPos]);
                }
            }
        } while (nextChainedSlot(trxType, iter));
    }
}

template<typename T>
void HashIndex<T>::validateEntries(TransactionType trxType) {
    auto& header = trxType == TransactionType::READ_ONLY ? *this->indexHeaderForReadTrx :
                                                           *this->indexHeaderForWriteTrx;
    forEach(trxType, [&](slot_id_t slotId, uint8_t fingerprint, SlotEntry<T> entry) {
        auto hashValue = hashStored(trxType, entry.key);
        auto fingerprintForHash = HashIndexUtils::getFingerprintForHash(hashValue);
        auto slotIdForHash = HashIndexUtils::getPrimarySlotIdForHash(header, hashValue);
        if (slotIdForHash != slotId) {
            throw RuntimeException(
                stringFormat("SlotId {} of key {} does not match the slot {} in which it's stored!",
                    slotIdForHash, TypeUtils::toString(entry.key), slotId));
        }
        if (fingerprint != fingerprintForHash) {
            throw RuntimeException(
                stringFormat("Fingerprint of key {} does not match fingerprint in header!",
                    TypeUtils::toString(entry.key)));
        }
    });
}

template<typename T>
std::string HashIndex<T>::toString(TransactionType trxType) {
    std::string result;
    std::optional<slot_id_t> currentSlotId;
    forEach(trxType, [&](slot_id_t slotId, uint8_t, SlotEntry<T> entry) {
        if (slotId != currentSlotId) {
            result += "\nSlot " + std::to_string(slotId) + ": ";
            currentSlotId = slotId;
        }
        if constexpr (std::same_as<T, ku_string_t>) {
            auto str = overflowFileHandle->readString(trxType, entry.key);
            result += "(" + str + ", ";
        } else {
            result += "(" + TypeUtils::toString(entry.key) + ", ";
        }
        result += std::to_string(entry.value) + ") ";
    });
    return result;
}

template<typename T>
HashIndex<T>::~HashIndex() = default;

template class HashIndex<int64_t>;
template class HashIndex<int32_t>;
template class HashIndex<int16_t>;
template class HashIndex<int8_t>;
template class HashIndex<uint64_t>;
template class HashIndex<uint32_t>;
template class HashIndex<uint16_t>;
template class HashIndex<uint8_t>;
template class HashIndex<double>;
template class HashIndex<float>;
template class HashIndex<int128_t>;
template class HashIndex<ku_string_t>;

PrimaryKeyIndex::PrimaryKeyIndex(const DBFileIDAndName& dbFileIDAndName, bool readOnly,
    common::PhysicalTypeID keyDataType, BufferManager& bufferManager, WAL* wal,
    VirtualFileSystem* vfs)
    : keyDataTypeID(keyDataType) {
    fileHandle = bufferManager.getBMFileHandle(dbFileIDAndName.fName,
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_NO_CREATE,
        BMFileHandle::FileVersionedType::VERSIONED_FILE, vfs);
    if (keyDataTypeID == PhysicalTypeID::STRING) {
        overflowFile =
            std::make_unique<OverflowFile>(dbFileIDAndName, &bufferManager, wal, readOnly, vfs);
    }

    hashIndices.reserve(NUM_HASH_INDEXES);
    TypeUtils::visit(
        keyDataTypeID,
        [&](ku_string_t) {
            for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
                hashIndices.push_back(std::make_unique<HashIndex<ku_string_t>>(
                    dbFileIDAndName, fileHandle, overflowFile->addHandle(), i, bufferManager, wal));
            }
        },
        [&]<HashablePrimitive T>(T) {
            for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
                hashIndices.push_back(std::make_unique<HashIndex<T>>(
                    dbFileIDAndName, fileHandle, nullptr, i, bufferManager, wal));
            }
        },
        [&](auto) { KU_UNREACHABLE; });
}

bool PrimaryKeyIndex::lookup(Transaction* trx, common::ValueVector* keyVector, uint64_t vectorPos,
    common::offset_t& result) {
    bool retVal = false;
    TypeUtils::visit(
        keyDataTypeID,
        [&]<IndexHashable T>(T) {
            T key = keyVector->getValue<T>(vectorPos);
            retVal = lookup(trx, key, result);
        },
        [](auto) { KU_UNREACHABLE; });
    return retVal;
}

bool PrimaryKeyIndex::insert(
    common::ValueVector* keyVector, uint64_t vectorPos, common::offset_t value) {
    bool result = false;
    TypeUtils::visit(
        keyDataTypeID,
        [&]<IndexHashable T>(T) {
            T key = keyVector->getValue<T>(vectorPos);
            result = insert(key, value);
        },
        [](auto) { KU_UNREACHABLE; });
    return result;
}

void PrimaryKeyIndex::delete_(ValueVector* keyVector) {
    TypeUtils::visit(
        keyDataTypeID,
        [&]<IndexHashable T>(T) {
            for (auto i = 0u; i < keyVector->state->selVector->selectedSize; i++) {
                auto pos = keyVector->state->selVector->selectedPositions[i];
                if (keyVector->isNull(pos)) {
                    continue;
                }
                auto key = keyVector->getValue<T>(pos);
                delete_(key);
            }
        },
        [](auto) { KU_UNREACHABLE; });
}

void PrimaryKeyIndex::checkpointInMemory() {
    for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
        hashIndices[i]->checkpointInMemory();
    }
    if (overflowFile) {
        overflowFile->checkpointInMemory();
    }
}

void PrimaryKeyIndex::rollbackInMemory() {
    for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
        hashIndices[i]->rollbackInMemory();
    }
    if (overflowFile) {
        overflowFile->rollbackInMemory();
    }
}

void PrimaryKeyIndex::prepareCommit() {
    for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
        hashIndices[i]->prepareCommit();
    }
    if (overflowFile) {
        overflowFile->prepareCommit();
    }
}

void PrimaryKeyIndex::prepareRollback() {
    for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
        hashIndices[i]->prepareRollback();
    }
}

PrimaryKeyIndex::~PrimaryKeyIndex() = default;

} // namespace storage
} // namespace kuzu
