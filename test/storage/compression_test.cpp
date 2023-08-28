#include "gtest/gtest.h"
#include "storage/copier/compression.h"

using namespace kuzu::common;
using namespace kuzu::storage;

template<typename T>
void test_compression(CompressionAlg& alg, std::vector<T> src) {
    std::vector<uint8_t> dest(src.size());
    alg.compress(src.data(), 0, dest.data(), 0, src.size());
    std::vector<uint8_t> decompressed(src.size());
    alg.decompress(dest.data(), 0, decompressed.data(), 0, src.size());
    for (int i = 0; i < src.size(); i++) {
        T val;
        alg.getValue(dest.data(), i, &val);
        ASSERT_EQ(src[i], val);
    }
    ASSERT_EQ(src, decompressed);
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
