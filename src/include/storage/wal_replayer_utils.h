#pragma once

#include <string>

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/types/internal_id_t.h"

namespace kuzu {
namespace catalog {
class NodeTableSchema;
class RelTableSchema;
} // namespace catalog

namespace common {
class VirtualFileSystem;
} // namespace common

namespace storage {

class WALReplayerUtils {
public:
    static void removeHashIndexFile(common::VirtualFileSystem* vfs, common::table_id_t tableID,
        const std::string& directory);
};

} // namespace storage
} // namespace kuzu
