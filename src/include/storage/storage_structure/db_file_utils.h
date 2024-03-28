#pragma once

#include <cstdint>
#include <functional>

#include "common/copy_constructors.h"
#include "common/types/types.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class PinnedFrame {
public:
    // Pin frame using original file handle
    PinnedFrame(common::page_idx_t originalPageIdx, BufferManager& bufferManager,
        BMFileHandle& originalFileHandle, BufferManager::PageReadPolicy readPolicy)
        : originalPageIdx{originalPageIdx}, pageIdxInWAL{common::INVALID_PAGE_IDX},
          bufferManager{&bufferManager}, originalFileHandle{&originalFileHandle}, wal{nullptr},
          frame{bufferManager.pin(originalFileHandle, originalPageIdx, readPolicy)} {}

    // Pin frame using WAL
    PinnedFrame(common::page_idx_t originalPageIdx, bool insertingNewPage, BMFileHandle& fileHandle,
        DBFileID dbFileID, BufferManager& bufferManager, WAL& wal);

    PinnedFrame(PinnedFrame&& other);
    PinnedFrame& operator=(PinnedFrame&& other);
    DELETE_BOTH_COPY(PinnedFrame);

    inline ~PinnedFrame() { unpin(); }

    uint8_t* get() const { return frame; }

private:
    void unpin();

private:
    common::page_idx_t originalPageIdx;
    common::page_idx_t pageIdxInWAL;
    BufferManager* bufferManager;
    BMFileHandle* originalFileHandle;
    WAL* wal;
    uint8_t* frame;
};

class DBFileUtils {
public:
    constexpr static common::page_idx_t NULL_PAGE_IDX = common::INVALID_PAGE_IDX;

public:
    static std::pair<BMFileHandle*, common::page_idx_t> getFileHandleAndPhysicalPageIdxToPin(
        BMFileHandle& fileHandle, common::page_idx_t physicalPageIdx, WAL& wal,
        transaction::TransactionType trxType);

    static void readWALVersionOfPage(BMFileHandle& fileHandle, common::page_idx_t originalPageIdx,
        BufferManager& bufferManager, WAL& wal, const std::function<void(uint8_t*)>& readOp);

    static common::page_idx_t insertNewPage(
        BMFileHandle& fileHandle, DBFileID dbFileID, BufferManager& bufferManager, WAL& wal,
        const std::function<void(uint8_t*)>& insertOp = [](uint8_t*) -> void {
            // DO NOTHING.
        });

    // Note: This function updates a page "transactionally", i.e., creates the WAL version of the
    // page if it doesn't exist. For the original page to be updated, the current WRITE trx needs to
    // commit and checkpoint.
    static void updatePage(BMFileHandle& fileHandle, DBFileID dbFileID,
        common::page_idx_t originalPageIdx, bool isInsertingNewPage, BufferManager& bufferManager,
        WAL& wal, const std::function<void(uint8_t*)>& updateOp);
};
} // namespace storage
} // namespace kuzu
