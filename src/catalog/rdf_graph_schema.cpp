#include "catalog/rdf_graph_schema.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void RdfGraphSchema::serializeInternal(SerDeser& serializer) {
    serializer.serializeValue(nodeTableID);
    serializer.serializeValue(relTableID);
}

std::unique_ptr<RdfGraphSchema> RdfGraphSchema::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    table_id_t nodeTableID;
    table_id_t relTableID;
    SerDeser::deserializeValue(nodeTableID, fileInfo, offset);
    SerDeser::deserializeValue(relTableID, fileInfo, offset);
    return std::make_unique<RdfGraphSchema>(nodeTableID, relTableID);
}

} // namespace catalog
} // namespace kuzu
