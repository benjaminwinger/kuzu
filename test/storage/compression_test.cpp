#include "gtest/gtest.h"
#include "storage/copier/compression.h"
#include <bit>

using namespace kuzu::common;
using namespace kuzu::storage;

template<typename T>
void test_compression(CompressionAlg& alg, std::vector<T> src) {
    auto compressedSize = alg.numBytesForCompression((uint8_t*)src.data(), src.size());
    std::vector<uint8_t> dest(compressedSize);
    alg.compress((uint8_t*)src.data(), src.size(), dest.data(), compressedSize);
    std::vector<T> decompressed(src.size());
    alg.decompress(dest.data(), 0, (uint8_t*)decompressed.data(), 0, src.size());
    ASSERT_EQ(src, decompressed);
    // works with all bit widths
    T value = 0;
    alg.setValueFromUncompressed((uint8_t*)&value, 0, (uint8_t*)dest.data(), 1);
    alg.decompress(dest.data(), 0, (uint8_t*)decompressed.data(), 0, src.size());
    src[1] = value;
    ASSERT_EQ(decompressed, src);
    ASSERT_EQ(decompressed[1], value);
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

TEST(CompressionTests, IntegerPackingTest) {
    std::vector<int32_t> src(32, 6);
    auto alg = IntegerBitpacking<int32_t, uint32_t>();
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

TEST(CompressionTests, IntegerZigZagPackingTest) {
    std::vector<int32_t> src{1,2,3,4,5,6};
    auto alg = IntegerZigZagBitpacking<int32_t, uint32_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).first, std::bit_width(6u));
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerZigZagPackingTestNegative16) {
    std::vector<int16_t> src(32, -6);
    src[5] = 20;
    auto alg = IntegerZigZagBitpacking<int16_t, uint16_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).first, std::bit_width(20u) + 1);
    test_compression(alg, src);
}

TEST(CompressionTests, IntegerZigZagPackingTestNegative32) {
    std::vector<int32_t> src(32, -6);
    src[5] = 20;
    auto alg = IntegerZigZagBitpacking<int32_t, uint32_t>();
    ASSERT_EQ(alg.getBitWidth((uint8_t*)src.data(), src.size()).first, std::bit_width(20u) + 1);
    test_compression(alg, src);
}
