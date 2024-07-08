#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <stack>

#include "common/constants.h"
#include "common/types/types.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include <span>

namespace kuzu {
namespace main {
class ClientContext;
}

namespace common {
class VirtualFileSystem;
}

namespace storage {

class MemoryManager;
class BMFileHandle;
class FileHandle;
class BufferManager;
class ChunkedNodeGroup;

class MemoryBuffer {
    friend class MemoryManager;

public:
    MemoryBuffer(MemoryManager* mm, common::page_idx_t blockIdx, uint8_t* buffer,
        uint64_t size = common::BufferPoolConstants::PAGE_256KB_SIZE);
    ~MemoryBuffer();

    std::span<uint8_t> getBuffer() const {
        KU_ASSERT(!evicted);
        return buffer;
    }

    MemoryManager* getMemoryManager() const { return mm; }
    void loadFromDisk();

private:
    void spillToDisk(uint32_t fileHandleIndex);

    std::span<uint8_t> buffer;
    std::atomic<bool> evicted = false;
    uint64_t filePosition = UINT64_MAX;
    uint32_t fileIndex = UINT32_MAX;
    common::page_idx_t pageIdx;
    MemoryManager* mm;
};

/*
 * The Memory Manager (MM) is used for allocating/reclaiming intermediate memory blocks.
 * It can allocate a memory buffer of size PAGE_256KB from the buffer manager backed by a
 * BMFileHandle with temp in-mem file.
 *
 * The MemoryManager holds a BMFileHandle backed by
 * a temp in-mem file, and is responsible for allocating/reclaiming memory buffers of its size class
 * from the buffer manager. The MemoryManager keeps track of free pages in the BMFileHandle, so
 * that it can reuse those freed pages without allocating new pages. The MemoryManager is
 * thread-safe, so that multiple threads can allocate/reclaim memory blocks with the same size class
 * at the same time.
 *
 * MM will return a MemoryBuffer to the caller, which is a wrapper of the allocated memory block,
 * and it will automatically call its allocator to reclaim the memory block when it is destroyed.
 */
class MemoryManager {
    friend class MemoryBuffer;

public:
    MemoryManager(BufferManager* bm, common::VirtualFileSystem* vfs, main::ClientContext* context);

    ~MemoryManager();

    std::unique_ptr<MemoryBuffer> mallocBuffer(bool initializeToZero, uint64_t size);
    std::unique_ptr<MemoryBuffer> allocateBuffer(bool initializeToZero = false,
        uint64_t size = common::BufferPoolConstants::PAGE_256KB_SIZE);
    inline common::page_offset_t getPageSize() const { return pageSize; }

    BufferManager* getBufferManager() const { return bm; }

    void addUnusedChunk(ChunkedNodeGroup* nodeGroup);
    void loadFromDisk(ChunkedNodeGroup* nodeGroup);

private:
    void freeBlock(common::page_idx_t pageIdx, std::span<uint8_t> buffer);

private:
    BMFileHandle* fh;
    BufferManager* bm;
    common::VirtualFileSystem* vfs;
    main::ClientContext* clientContext;
    common::page_offset_t pageSize;
    std::stack<common::page_idx_t> freePages;
    std::mutex allocatorLock;
    std::mutex partitionerGroupsMtx;
    std::stack<ChunkedNodeGroup*> fullPartitionerGroups;
    std::vector<std::unique_ptr<FileHandle>> partitionerFileHandles;
};

} // namespace storage
} // namespace kuzu
