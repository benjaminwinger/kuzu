#include "storage/wal_replayer_utils.h"

#include "common/file_system/virtual_file_system.h"
#include "storage/index/hash_index.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void WALReplayerUtils::removeHashIndexFile(common::VirtualFileSystem* vfs, table_id_t tableID,
    const std::string& directory) {
    vfs->removeFileIfExists(
        StorageUtils::getNodeIndexFName(vfs, directory, tableID, FileVersionType::ORIGINAL));
}

} // namespace storage
} // namespace kuzu
