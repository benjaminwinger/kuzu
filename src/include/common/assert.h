#pragma once

#include "common/exception/internal.h"
#include "common/string_format.h"

namespace kuzu {
namespace common {

[[noreturn]] inline void kuAssertFailureInternal(std::string message, const char* file,
    int linenr) {
    // LCOV_EXCL_START
    throw InternalException(
        stringFormat("Assertion failed in file \"{}\" on line {}: {}", file, linenr, message));
    // LCOV_EXCL_STOP
}

#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
#define KU_ASSERT(condition)                                                                       \
    static_cast<bool>(condition) ?                                                                 \
        void(0) :                                                                                  \
        kuzu::common::kuAssertFailureInternal(#condition, __FILE__, __LINE__)
#define KU_ASSERT_EQ(A, B)                                                                         \
    static_cast<bool>((A) == (B)) ?                                                                \
        void(0) :                                                                                  \
        kuzu::common::kuAssertFailureInternal(stringFormat("{} == {}: {} == {}", #A, #B, A, B),    \
            __FILE__, __LINE__)
#else
#define KU_ASSERT(condition) void(0)
#define KU_ASSERT_EQ(A, B) void(0)
#endif

#define KU_UNREACHABLE                                                                             \
    [[unlikely]] kuzu::common::kuAssertFailureInternal("KU_UNREACHABLE", __FILE__, __LINE__)

} // namespace common
} // namespace kuzu
