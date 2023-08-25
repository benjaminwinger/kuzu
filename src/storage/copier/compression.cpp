#include "storage/copier/compression.h"

#include "arrow/array.h"
#include "common/null_mask.h"
#include "common/vector/value_vector.h"

using namespace kuzu::common;
namespace arrow {
class Array;
}

namespace kuzu {
namespace storage {

const LogicalType BoolCompression::LOGICAL_TYPE = LogicalType(LogicalTypeID::BOOL);
const LogicalType InternalIDMapping::LOGICAL_TYPE = LogicalType(LogicalTypeID::INTERNAL_ID);

} // namespace storage
} // namespace kuzu
