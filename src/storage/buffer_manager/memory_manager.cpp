#include "storage/buffer_manager/memory_manager.h"

#include <cstring>
#include <mutex>

#include "common/constants.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_format.h"
#include "main/client_context.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/file_handle.h"
#include "storage/store/chunked_node_group.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

MemoryBuffer::MemoryBuffer(MemoryManager* mm, page_idx_t pageIdx, uint8_t* buffer, uint64_t size)
    : buffer{buffer, size}, pageIdx{pageIdx}, mm{mm} {}

MemoryBuffer::~MemoryBuffer() {
    if (buffer.data() != nullptr && !evicted) {
        mm->freeBlock(pageIdx, buffer);
    }
}

void MemoryBuffer::spillToDisk(uint32_t fileHandleIndex) {
    bool expected = false;
    if (evicted.compare_exchange_strong(expected, true)) {
        auto* dataFH = mm->partitionerFileHandles[fileHandleIndex].get();
        auto pageSize = dataFH->getPageSize();
        auto numPages = (buffer.size_bytes() + pageSize - 1) / pageSize;
        auto startPage = dataFH->addNewPages(numPages);
        for (size_t i = startPage; i < numPages; i++) {
            dataFH->writePage(buffer.data() + pageSize * i, i);
        }
        fileIndex = fileHandleIndex;
        filePosition = startPage * pageSize;
        mm->freeBlock(pageIdx, buffer);
    }
}

void MemoryBuffer::loadFromDisk() {
    if (evicted) {
        KU_ASSERT(fileIndex < mm->partitionerFileHandles.size());
        auto* dataFH = mm->partitionerFileHandles[fileIndex].get();
        mm->allocateBuffer(false, buffer.size());
        dataFH->getFileInfo()->readFromFile(buffer.data(), buffer.size(), filePosition);
        evicted = false;
    }
}

MemoryManager::MemoryManager(BufferManager* bm, VirtualFileSystem* vfs,
    main::ClientContext* context)
    : bm{bm}, vfs{vfs}, clientContext{context} {
    pageSize = BufferPoolConstants::PAGE_256KB_SIZE;
    fh = bm->getBMFileHandle("mm-256KB", FileHandle::O_IN_MEM_TEMP_FILE,
        BMFileHandle::FileVersionedType::NON_VERSIONED_FILE, vfs, context, PAGE_256KB);
}

MemoryManager::~MemoryManager() {
    for (auto& fileHandle : partitionerFileHandles) {
        vfs->removeFileIfExists(fileHandle->getFileInfo()->path);
    }
}

std::unique_ptr<MemoryBuffer> MemoryManager::mallocBuffer(bool initializeToZero, uint64_t size) {
    while (!bm->reserve(size) && !fullPartitionerGroups.empty()) {
        std::unique_lock<std::mutex> lock(partitionerGroupsMtx);
        auto groupToFlush = fullPartitionerGroups.top();
        for (size_t i = 0; i < groupToFlush->getNumColumns(); i++) {
            if (partitionerFileHandles.size() <= i) {
                partitionerFileHandles.push_back(
                    std::make_unique<FileHandle>(vfs->joinPath(clientContext->getDatabasePath(),
                                                     stringFormat("partitionerOverflow{}", i)),
                        FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS, vfs, clientContext));
            }
            const auto& chunk = groupToFlush->getColumnChunk(i);
            chunk.buffer->spillToDisk(i);
            /* TODO: compressed spill to disk for column chunks specifically
            auto &dataFH = partitionerFileHandles[i];
            auto preScanMetadata = chunk.getMetadataToFlush();
            auto startPageIdx = dataFH.addNewPages(preScanMetadata.numPages);
            state.metadata = chunk.flushBuffer(&dataFH, startPageIdx, preScanMetadata);
            */
        }

        fullPartitionerGroups.pop();
    }
    void* buffer;
    if (initializeToZero) {
        buffer = calloc(size, 1);
    } else {
        buffer = malloc(size);
    }
    return std::make_unique<MemoryBuffer>(this, INVALID_PAGE_IDX,
        reinterpret_cast<uint8_t*>(buffer), size);
}

std::unique_ptr<MemoryBuffer> MemoryManager::allocateBuffer(bool initializeToZero, uint64_t size) {
    if (size > BufferPoolConstants::PAGE_256KB_SIZE) [[unlikely]] {
        return mallocBuffer(initializeToZero, size);
    }
    page_idx_t pageIdx;
    {
        std::scoped_lock<std::mutex> lock(allocatorLock);
        if (freePages.empty()) {
            pageIdx = fh->addNewPage();
        } else {
            pageIdx = freePages.top();
            freePages.pop();
        }
    }
    auto buffer = bm->pin(*fh, pageIdx, BufferManager::PageReadPolicy::DONT_READ_PAGE);
    auto memoryBuffer = std::make_unique<MemoryBuffer>(this, pageIdx, buffer);
    if (initializeToZero) {
        memset(memoryBuffer->getBuffer().data(), 0, pageSize);
    }
    return memoryBuffer;
}

void MemoryManager::addUnusedChunk(ChunkedNodeGroup* nodeGroup) {
    std::unique_lock<std::mutex> lock(partitionerGroupsMtx);
    fullPartitionerGroups.push(nodeGroup);
}

void MemoryManager::loadFromDisk(ChunkedNodeGroup* nodeGroup) {
    for (auto& chunk : nodeGroup->getColumnChunksUnsafe()) {
        chunk->buffer->loadFromDisk();
    }
}

void MemoryManager::freeBlock(page_idx_t pageIdx, std::span<uint8_t> buffer) {
    if (pageIdx == INVALID_PAGE_IDX) {
        bm->freeUsedMemory(buffer.size());
        std::free(buffer.data());
        return;
    } else {
        bm->unpin(*fh, pageIdx);
        std::unique_lock<std::mutex> lock(allocatorLock);
        freePages.push(pageIdx);
    }
}

} // namespace storage
} // namespace kuzu
