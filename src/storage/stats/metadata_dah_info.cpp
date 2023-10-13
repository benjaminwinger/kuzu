#include "storage/stats/metadata_dah_info.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::unique_ptr<MetadataDAHInfo> MetadataDAHInfo::copy() {
    auto result = std::make_unique<MetadataDAHInfo>(dataDAHPageIdx, nullDAHPageIdx);
    result->childrenInfos.resize(childrenInfos.size());
    for (size_t i = 0; i < childrenInfos.size(); ++i) {
        result->childrenInfos[i] = childrenInfos[i]->copy();
    }
    return result;
}

void MetadataDAHInfo::serialize(SerDeser& serializer) const {
    serializer.serializeValue(dataDAHPageIdx);
    serializer.serializeValue(nullDAHPageIdx);
    serializer.serializeVectorOfPtrs(childrenInfos);
}

std::unique_ptr<MetadataDAHInfo> MetadataDAHInfo::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    auto metadataDAHInfo = std::make_unique<MetadataDAHInfo>();
    SerDeser::deserializeValue(metadataDAHInfo->dataDAHPageIdx, fileInfo, offset);
    SerDeser::deserializeValue(metadataDAHInfo->nullDAHPageIdx, fileInfo, offset);
    SerDeser::deserializeVectorOfPtrs(metadataDAHInfo->childrenInfos, fileInfo, offset);
    return metadataDAHInfo;
}

} // namespace storage
} // namespace kuzu
