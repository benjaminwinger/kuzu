#include <cstddef>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>

#include "common/constants.h"
#include "common/file_system/virtual_file_system.h"
#include "common/system_config.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/enums/page_read_policy.h"
#include "storage/file_handle.h"
#include <benchmark/benchmark.h>
// #include "helpers.h"
// #include "storage/compression/bitpacking_utils.h"
// #include "fastpfor/bitpackinghelpers.h"
/*
#include <bitset>
#include <cstdint>

#include "common/null_mask.h"

static void Memcpy(benchmark::State& state) {
    auto source = fixed(blocksize, 1);
    auto dest = fixed(blocksize, 0);
    size_t sum = 0;
    for (auto _ : state) {
        memcpy(source.data(), dest.data(), blocksize * sizeof(datatype));
        for (size_t i = 0; i < dest.size(); i++) {
            sum += dest[i];
        }
    }
    benchmark::DoNotOptimize(sum);
}

template <size_t N>
static void SingleUnpackAll(benchmark::State& state) {
    auto source = fixed(blocksize * N/64, 1);
    datatype out;
    auto dest = fixed(blocksize, 0);
    size_t sum = 0;
    for (auto _ : state) {
        for (uint64_t i = 0; i < blocksize; i++) {
            kuzu::storage::BitpackingUtils<datatype>::unpackSingle(reinterpret_cast<uint8_t*>(source.data())
+ i * N / 64, &dest[i], N, i % 64);
        }
        for (size_t i = 0; i < dest.size(); i++) {
            sum += dest[i];
        }
    }
    benchmark::DoNotOptimize(sum);
}

// Benchmarks summing after in-place decompression
template <size_t N>
static void SingleUnpack(benchmark::State& state) {
    auto source = fixed(blocksize * N/64, 1);
    datatype out;
    size_t sum = 0;
    for (auto _ : state) {
        for (uint64_t i = 0; i < blocksize; i++) {
            kuzu::storage::BitpackingUtils<datatype>::unpackSingle(reinterpret_cast<uint8_t*>(source.data())
+ i * N / 64, &out, N, i % 64); sum += out;
        }
    }
    benchmark::DoNotOptimize(sum);
}

template <size_t N>
static void MultiUnpackAll(benchmark::State& state) {
    auto source = fixed(blocksize * N/64, 1);
    size_t sum = 0;
    auto dest = fixed(blocksize, 0);
    for (auto _ : state) {
        for (uint64_t i = 0; i < blocksize / 32; i++) {
            FastPForLib::fastunpack(reinterpret_cast<uint32_t*>(source.data()) + i * 32 * N / 64,
dest.data() + i * 32, N);
        }
        for (size_t i = 0; i < dest.size(); i++) {
            sum += dest[i];
        }
    }
    benchmark::DoNotOptimize(sum);
}

template <size_t N>
static void MultiUnpackRolling(benchmark::State& state) {
    auto source = fixed(blocksize * N/64, 1);
    size_t sum = 0;
    auto dest = fixed(32, 0);
    for (auto _ : state) {
        for (uint64_t i = 0; i < blocksize / 32; i++) {
            FastPForLib::fastunpack(reinterpret_cast<uint32_t*>(source.data()) + i * N * 32 / 64,
dest.data(), N); for (size_t i = 0; i < dest.size(); i++) { sum += dest[i];
            }
        }
    }
    benchmark::DoNotOptimize(sum);
}


/*
BENCHMARK(Memcpy);
#define BENCH(bitwidth) \
BENCHMARK(SingleUnpack<bitwidth>); \
BENCHMARK(SingleUnpackAll<bitwidth>); \
BENCHMARK(MultiUnpackAll<bitwidth>); \
BENCHMARK(MultiUnpackRolling<bitwidth>);
BENCH(1);
BENCH(2);
BENCH(3);
BENCH(4);
BENCH(5);
BENCH(8);
BENCH(13);
BENCH(16);
BENCH(23);
BENCH(32);
BENCH(37);
BENCH(57);
*/

/*
template <size_t N>
static void BitmaskIterate(benchmark::State& state) {
    std::array<bool, kuzu::common::DEFAULT_VECTOR_CAPACITY> mask{0};
    std::array<uint64_t, kuzu::common::DEFAULT_VECTOR_CAPACITY> data;
    for (int i = 0; i < N; i++) {
        mask[i] = true;
        data[i] = i;
    }
    uint64_t sum = 0;
    for (auto _ : state) {
        for (size_t i = 0; i < mask.size(); i++) {
            if (mask[i]) {
                sum += data[i];
            }
        }
    }
    benchmark::DoNotOptimize(sum);
}
/*
BENCHMARK(BitmaskIterate<1>);
BENCHMARK(BitmaskIterate<10>);
BENCHMARK(BitmaskIterate<100>);
BENCHMARK(BitmaskIterate<1000>);
BENCHMARK(BitmaskIterate<2048>);
*/

/*
template <size_t N>
static void SelVectorIterate(benchmark::State& state) {
    kuzu::common::SelectionVector selVector;
    std::array<uint64_t, kuzu::common::DEFAULT_VECTOR_CAPACITY> data;
    for (size_t i = 0; i < N; i++) {
        selVector.getMutableBuffer()[i] = i;
        data[i] = i;
    }
    selVector.setSelSize(N);
    uint64_t sum = 0;
    for (auto _ : state) {
        for (size_t i = 0; i < selVector.getSelSize(); i++) {
            auto pos = selVector.getSelectedPositions()[i];
            sum += data[pos];
        }
    }
    benchmark::DoNotOptimize(sum);
}

/*
BENCHMARK(SelVectorIterate<1>);
BENCHMARK(SelVectorIterate<10>);
BENCHMARK(SelVectorIterate<100>);
BENCHMARK(SelVectorIterate<1000>);
BENCHMARK(SelVectorIterate<2048>);
*/

/*
static void MemCpyNull(benchmark::State& state) {
    std::array<uint64_t, 100> src;
    for (size_t i = 0; i < 100; i++) {
        src[i] = ~0ull;
    }
    std::array<uint64_t, 100> dest;
    for (auto _ : state) {
        memcpy(dest.data(), src.data(), 2048/8);
    }
}

template <size_t N>
static void NullMaskBench(benchmark::State& state) {
    std::array<uint64_t, 100> src;
    for (size_t i = 0; i < 100; i++) {
        src[i] = ~0ull;
    }
    std::array<uint64_t, 100> dest;
    for (auto _ : state) {
        kuzu::common::NullMask::copyNullMask(src.data(), 0, dest.data(), 0, N);
    }
}

template <size_t N>
static void SetNull(benchmark::State& state) {
    std::array<uint64_t, 100> src;
    for (size_t i = 0; i < 100; i++) {
        src[i] = ~0ull;
    }
    std::array<uint64_t, 100> dest;
    for (auto _ : state) {
        for (size_t i = 0; i < N; i++) {
            kuzu::common::NullMask::setNull(dest.data(), i,
kuzu::common::NullMask::isNull(src.data(), i));
        }
    }
}

template <size_t N>
static void NullMaskBenchUnaligned(benchmark::State& state) {
    std::array<uint64_t, 100> src;
    for (size_t i = 0; i < 100; i++) {
        src[i] = ~0ull;
    }
    std::array<uint64_t, 100> dest;
    for (auto _ : state) {
        kuzu::common::NullMask::copyNullMask(src.data(), 0, dest.data(), 1, N);
    }
}

template <size_t N>
static void GetMinMaxAligned(benchmark::State& state) {
    std::array<uint64_t, 100> src;
    for (size_t i = 0; i < 100; i++) {
        src[i] = ~0ull;
    }
    for (auto _ : state) {
        kuzu::common::NullMask::getMinMax(src.data(), 0, N);
    }
}

template <size_t N>
static void GetMinMaxUnaligned(benchmark::State& state) {
    std::array<uint64_t, 100> src;
    for (size_t i = 0; i < 100; i++) {
        src[i] = ~0ull;
    }
    for (auto _ : state) {
        kuzu::common::NullMask::getMinMax(src.data(), 1, N);
    }
}


BENCHMARK(NullMaskBench<1>);
BENCHMARK(NullMaskBench<2>);
BENCHMARK(NullMaskBench<3>);
BENCHMARK(NullMaskBench<4>);
BENCHMARK(NullMaskBench<5>);
BENCHMARK(NullMaskBench<6>);
BENCHMARK(NullMaskBench<7>);
BENCHMARK(NullMaskBench<8>);
BENCHMARK(NullMaskBench<100>);
BENCHMARK(NullMaskBench<1000>);
BENCHMARK(NullMaskBench<2048>);
BENCHMARK(NullMaskBenchUnaligned<1>);
BENCHMARK(NullMaskBenchUnaligned<2>);
BENCHMARK(NullMaskBenchUnaligned<8>);
BENCHMARK(NullMaskBenchUnaligned<100>);
BENCHMARK(NullMaskBenchUnaligned<1000>);
BENCHMARK(NullMaskBenchUnaligned<2048>);
BENCHMARK(SetNull<1>);
BENCHMARK(SetNull<2>);
BENCHMARK(SetNull<3>);
BENCHMARK(SetNull<4>);
BENCHMARK(SetNull<5>);
BENCHMARK(SetNull<6>);
BENCHMARK(SetNull<7>);
BENCHMARK(SetNull<8>);
BENCHMARK(SetNull<100>);
BENCHMARK(SetNull<1000>);
BENCHMARK(SetNull<2048>);
BENCHMARK(GetMinMaxAligned<1>);
BENCHMARK(GetMinMaxUnaligned<1>);
BENCHMARK(GetMinMaxAligned<8>);
BENCHMARK(GetMinMaxUnaligned<8>);
BENCHMARK(GetMinMaxAligned<2048>);
BENCHMARK(GetMinMaxUnaligned<2048>);
*/

static std::unique_ptr<kuzu::common::VirtualFileSystem> vfs;
static std::unique_ptr<kuzu::storage::BufferManager> bm;
static std::unique_ptr<kuzu::storage::MemoryManager> mm;

static constexpr size_t BufferPool = 10ull * 1024 * 1024 * 1024;
static constexpr size_t maxPages = (BufferPool / kuzu::common::KUZU_PAGE_SIZE) * 10;
static kuzu::storage::FileHandle* fileHandle = nullptr;
static int foo = []() {
    std::filesystem::create_directories("/tmp/tmpdb");
    std::ofstream outfile("/tmp/tmpdb/test");
    outfile.close();
    vfs = std::make_unique<kuzu::common::VirtualFileSystem>("/tmp/tmpdb");
    bm = std::make_unique<kuzu::storage::BufferManager>("/tmp/tmpdb", "/tmp/tmpdb/spilltodisk",
        BufferPool, 1ull * 1024 * 1024 * 1024 * 1024, vfs.get(), false);
    mm = std::make_unique<kuzu::storage::MemoryManager>(bm.get(), vfs.get());
    return 0;
}();

template<double evictable>
static void BufferManagerBenchmark(benchmark::State& state) {
    std::random_device dev;
    std::mt19937 rng(dev());
    std::bernoulli_distribution evictableDist(evictable);
    if (state.thread_index() == 0) {
        // Reset eviction queue
        bm->~BufferManager();
        new (bm.get()) kuzu::storage::BufferManager("/tmp/tmpdb", "/tmp/tmpdb/spilltodisk",
            BufferPool, 32ull * 1024 * 1024 * 1024 * 1024, vfs.get(), false);
        // bm->fileHandles.push_back(std::make_unique<kuzu::storage::FileHandle>("/tmp/tmpdb/test",
        // 0, bm.get(), 0, kuzu::common::PageSizeClass::TEMP_PAGE, vfs.get(), nullptr)); fileHandle
        // = bm->fileHandles.back().get();
        fileHandle = bm->getFileHandle("/tmp/tmpdb/test", 0, vfs.get(), nullptr);
        fileHandle->addNewPages(maxPages);
        // Fill eviction queue
        for (size_t counter = 0; counter < (BufferPool / kuzu::common::KUZU_PAGE_SIZE); counter++) {
            fileHandle->pinPage(counter, kuzu::storage::PageReadPolicy::DONT_READ_PAGE);
            if (evictableDist(rng)) {
                fileHandle->unpinPage(counter);
            }
        }
    }
    std::uniform_int_distribution<std::mt19937::result_type> dist(maxPages / 10, maxPages - 1);

    for (auto _ : state) {
        auto page = dist(rng);
        fileHandle->pinPage(page, kuzu::storage::PageReadPolicy::DONT_READ_PAGE);
        fileHandle->unpinPage(page);
    }
}

BENCHMARK(BufferManagerBenchmark<1.0>)->ThreadRange(1, 12);
BENCHMARK(BufferManagerBenchmark<0.5>)->ThreadRange(1, 12);
BENCHMARK(BufferManagerBenchmark<0.1>)->ThreadRange(1, 12);
BENCHMARK(BufferManagerBenchmark<0.01>)->ThreadRange(1, 12);
/*
BENCHMARK(BufferManagerBenchmark<256 * 1024 * 1024, evictable>);
BENCHMARK(BufferManagerBenchmark<1 * 1024 * 1024 * 1024, evictable>);
BENCHMARK(BufferManagerBenchmark<10ull * 1024 * 1024 * 1024, evictable>);
BENCHMARK(BufferManagerBenchmark<100ull * 1024 * 1024 * 1024, evictable>);
BENCHMARK(BufferManagerBenchmark<300ull * 1024 * 1024 * 1024, evictable>);
*/

BENCHMARK_MAIN();
