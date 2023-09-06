#include "gtest/gtest.h"
#include "storage/copier/compression.h"
#include <bit>

using namespace kuzu::common;
using namespace kuzu::storage;

template<typename T>
void test_compression(CompressionAlg& alg, std::vector<T> src) {
    auto pageSize = 4096;
    auto numValuesPerPage = alg.startCompression((uint8_t*)src.data(), src.size(), pageSize);
    // Just tests one page
    EXPECT_GT(numValuesPerPage, src.size());
    std::vector<uint8_t> dest(pageSize);
    alg.startCompression((uint8_t*)src.data(), src.size(), src.size() * sizeof(T));
    auto numValuesRemaining = src.size();
    // For simplicity, we'll ignore the possibility of it requiring multiple pages
    // TODO(bmwinger): Test reading/writing from multiple pages
    const uint8_t *srcCursor = (uint8_t*)src.data();
    alg.compressNextPage(srcCursor, numValuesRemaining, dest.data(), pageSize);
    std::vector<T> decompressed(src.size());
    alg.decompressFromPage(dest.data(), 0, (uint8_t*)decompressed.data(), 0, src.size());
    EXPECT_EQ(src, decompressed);
    // works with all bit widths
    T value = 0;
    alg.setValueFromUncompressed((uint8_t*)&value, 0, (uint8_t*)dest.data(), 1);
    alg.decompressFromPage(dest.data(), 0, (uint8_t*)decompressed.data(), 0, src.size());
    src[1] = value;
    EXPECT_EQ(decompressed, src);
    EXPECT_EQ(decompressed[1], value);
}

TEST(CompressionTests, BoolCompressionTest) {
    std::vector<uint8_t> src{true, false, true, true, false, true, false};
    auto alg = BoolCompression();
    test_compression(alg, src);
}

TEST(CompressionTests, CopyCompressionTest) {
    std::vector<uint8_t> src{true, false, true, true, false, true, false};
    auto alg = CopyCompression(LogicalType(LogicalTypeID::BOOL));
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTest32) {
    std::vector<int32_t> src(32, 6);
    auto alg = IntegerBitpacking<int32_t, uint32_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).first, std::bit_width(6u));
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTest64) {
    std::vector<int64_t> src(32, 6);
    auto alg = IntegerBitpacking<int64_t, uint64_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).first, std::bit_width(6u));
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTestNegative32) {
    std::vector<int32_t> src(32, -6);
    src[5] = 20;
    auto alg = IntegerBitpacking<int32_t, uint32_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).first, std::bit_width(20u) + 1);
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerPackingTestNegative64) {
    std::vector<int64_t> src(32, -6);
    src[5] = 20;
    auto alg = IntegerBitpacking<int64_t, uint64_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).first, std::bit_width(20u) + 1);
    test_compression(alg, src);
}
