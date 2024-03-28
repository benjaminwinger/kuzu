#include "storage/storage_structure/db_file_utils.h"

#include "storage/wal/wal_record.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

PinnedFrame::PinnedFrame(page_idx_t originalPageIdx, bool insertingNewPage,
    BMFileHandle& fileHandle, DBFileID dbFileID, BufferManager& bufferManager, WAL& wal)
    : originalPageIdx{originalPageIdx}, bufferManager{&bufferManager},
      originalFileHandle{&fileHandle}, wal{&wal} {
    fileHandle.addWALPageIdxGroupIfNecessary(originalPageIdx);
    fileHandle.acquireWALPageIdxLock(originalPageIdx);
    try {
        if (fileHandle.hasWALPageVersionNoWALPageIdxLock(originalPageIdx)) {
            pageIdxInWAL = fileHandle.getWALPageIdxNoWALPageIdxLock(originalPageIdx);
            frame = bufferManager.pin(
                *wal.fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::READ_PAGE);
        } else {
            pageIdxInWAL =
                wal.logPageUpdateRecord(dbFileID, originalPageIdx /* pageIdxInOriginalFile */);
            frame = bufferManager.pin(
                *wal.fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::DONT_READ_PAGE);
            if (!insertingNewPage) {
                bufferManager.optimisticRead(
                    fileHandle, originalPageIdx, [&](uint8_t* originalFrame) -> void {
                        memcpy(this->frame, originalFrame, BufferPoolConstants::PAGE_4KB_SIZE);
                    });
            }
            fileHandle.setWALPageIdxNoLock(
                originalPageIdx /* pageIdxInOriginalFile */, pageIdxInWAL);
            wal.fileHandle->setLockedPageDirty(pageIdxInWAL);
        }
    } catch (Exception& e) {
        fileHandle.releaseWALPageIdxLock(originalPageIdx);
        throw;
    }
}

PinnedFrame::PinnedFrame(PinnedFrame&& other)
    : originalPageIdx{other.originalPageIdx}, pageIdxInWAL{other.pageIdxInWAL},
      bufferManager{other.bufferManager},
      originalFileHandle{other.originalFileHandle}, wal{other.wal}, frame{other.frame} {
    // other must not unpin the frame as we've taken ownership
    other.originalPageIdx = INVALID_PAGE_IDX;
    other.pageIdxInWAL = INVALID_PAGE_IDX;
    other.frame = nullptr;
}

PinnedFrame& PinnedFrame::operator=(PinnedFrame&& other) {
    unpin();
    originalPageIdx = other.originalPageIdx;
    pageIdxInWAL = other.pageIdxInWAL;
    bufferManager = other.bufferManager;
    originalFileHandle = other.originalFileHandle;
    wal = other.wal;
    frame = other.frame;
    // other must not unpin the frame as we've taken ownership
    other.originalPageIdx = INVALID_PAGE_IDX;
    other.pageIdxInWAL = INVALID_PAGE_IDX;
    other.frame = nullptr;
    return *this;
}

void PinnedFrame::unpin() {
    if (pageIdxInWAL != INVALID_PAGE_IDX) {
        bufferManager->unpin(*wal->fileHandle, pageIdxInWAL);
        originalFileHandle->releaseWALPageIdxLock(originalPageIdx);
    } else if (originalPageIdx != INVALID_PAGE_IDX) {
        bufferManager->unpin(*originalFileHandle, originalPageIdx);
    }
}

void unpinPageIdxInWALAndReleaseOriginalPageLock(page_idx_t pageIdxInWAL,
    page_idx_t originalPageIdx, BMFileHandle& fileHandle, BufferManager& bufferManager, WAL& wal) {
    if (originalPageIdx != INVALID_PAGE_IDX) {
        bufferManager.unpin(*wal.fileHandle, pageIdxInWAL);
        fileHandle.releaseWALPageIdxLock(originalPageIdx);
    }
}

std::pair<BMFileHandle*, page_idx_t> DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
    BMFileHandle& fileHandle, page_idx_t physicalPageIdx, WAL& wal,
    transaction::TransactionType trxType) {
    if (trxType == transaction::TransactionType::READ_ONLY ||
        !fileHandle.hasWALPageVersionNoWALPageIdxLock(physicalPageIdx)) {
        return std::make_pair(&fileHandle, physicalPageIdx);
    } else {
        return std::make_pair(
            wal.fileHandle.get(), fileHandle.getWALPageIdxNoWALPageIdxLock(physicalPageIdx));
    }
}

common::page_idx_t DBFileUtils::insertNewPage(BMFileHandle& fileHandle, DBFileID dbFileID,
    BufferManager& bufferManager, WAL& wal, const std::function<void(uint8_t*)>& insertOp) {
    auto newOriginalPage = fileHandle.addNewPage();
    auto newWALPage = wal.logPageInsertRecord(dbFileID, newOriginalPage);
    auto walFrame = bufferManager.pin(
        *wal.fileHandle, newWALPage, BufferManager::PageReadPolicy::DONT_READ_PAGE);
    fileHandle.addWALPageIdxGroupIfNecessary(newOriginalPage);
    fileHandle.setWALPageIdx(newOriginalPage, newWALPage);
    insertOp(walFrame);
    wal.fileHandle->setLockedPageDirty(newWALPage);
    bufferManager.unpin(*wal.fileHandle, newWALPage);
    return newOriginalPage;
}

void DBFileUtils::updatePage(BMFileHandle& fileHandle, DBFileID dbFileID,
    page_idx_t originalPageIdx, bool isInsertingNewPage, BufferManager& bufferManager, WAL& wal,
    const std::function<void(uint8_t*)>& updateOp) {
    auto pinnedFrame =
        PinnedFrame(originalPageIdx, isInsertingNewPage, fileHandle, dbFileID, bufferManager, wal);
    updateOp(pinnedFrame.get());
}

void DBFileUtils::readWALVersionOfPage(BMFileHandle& fileHandle, page_idx_t originalPageIdx,
    BufferManager& bufferManager, WAL& wal, const std::function<void(uint8_t*)>& readOp) {
    page_idx_t pageIdxInWAL = fileHandle.getWALPageIdxNoWALPageIdxLock(originalPageIdx);
    auto pinnedFrame = PinnedFrame(
        pageIdxInWAL, bufferManager, *wal.fileHandle, BufferManager::PageReadPolicy::READ_PAGE);
    readOp(pinnedFrame.get());
}

} // namespace storage
} // namespace kuzu
