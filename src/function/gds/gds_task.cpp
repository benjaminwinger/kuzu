#include "function/gds/gds_task.h"

#include "common/constants.h"
#include "common/data_chunk/sel_vector.h"
#include "common/types/types.h"
#include "graph/graph.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static uint64_t computeScanResult(std::span<const nodeID_t> nbrNodeIDs,
    std::span<const relID_t> edgeIDs, SelectionVector& mask, nodeID_t sourceNodeID, EdgeCompute& ec,
    FrontierPair& frontierPair, bool isFwd) {
    KU_ASSERT(nbrNodeIDs.size() == edgeIDs.size());
    return ec.edgeCompute(sourceNodeID, nbrNodeIDs, edgeIDs, mask, isFwd,
        frontierPair.getNextFrontierUnsafe());
}

void FrontierTask::run() {
    FrontierMorsel frontierMorsel;
    auto numApproxActiveNodesForNextIter = 0u;
    auto graph = info.graph;
    auto scanState = graph->prepareScan(info.relTableIDToScan);
    auto localEc = info.edgeCompute.copy();
    // TODO: Maybe write directly into the scanState? Probably won't make a huge difference
    std::array<common::offset_t, DEFAULT_VECTOR_CAPACITY> activeNodes;
    while (sharedState->frontierPair.getNextRangeMorsel(frontierMorsel)) {
        // Will be updated by getNextActiveNodes
        offset_t currentOffset = frontierMorsel.getBeginOffset();
        while (currentOffset < frontierMorsel.getEndOffsetExclusive()) {
            auto nodesToScan = sharedState->frontierPair.curFrontier->getNextActiveNodes(
                activeNodes, currentOffset, frontierMorsel.getEndOffsetExclusive());
            auto tableID = frontierMorsel.getTableID();
            if (nodesToScan.size() == 0) {
                break;
            }
            switch (info.direction) {
            case ExtendDirection::FWD: {
                for (auto [nodes, edges, mask, sourceNode] :
                    graph->scanFwd(tableID, nodesToScan, *scanState)) {
                    numApproxActiveNodesForNextIter += computeScanResult(nodes, edges, mask,
                        sourceNode, *localEc, sharedState->frontierPair, true);
                }
            } break;
            case ExtendDirection::BWD: {
                for (auto [nodes, edges, mask, sourceNode] :
                    graph->scanBwd(tableID, nodesToScan, *scanState)) {
                    numApproxActiveNodesForNextIter += computeScanResult(nodes, edges, mask,
                        sourceNode, *localEc, sharedState->frontierPair, false);
                }
            } break;
            case ExtendDirection::BOTH: {
                for (auto [nodes, edges, mask, sourceNode] :
                    graph->scanFwd(tableID, nodesToScan, *scanState)) {
                    numApproxActiveNodesForNextIter += computeScanResult(nodes, edges, mask,
                        sourceNode, *localEc, sharedState->frontierPair, true);
                }
                for (auto [nodes, edges, mask, sourceNode] :
                    graph->scanBwd(tableID, nodesToScan, *scanState)) {
                    numApproxActiveNodesForNextIter += computeScanResult(nodes, edges, mask,
                        sourceNode, *localEc, sharedState->frontierPair, false);
                }
            } break;
            default:
                KU_UNREACHABLE;
            }
        }
    }
    sharedState->frontierPair.incrementApproxActiveNodesForNextIter(
        numApproxActiveNodesForNextIter);
}

VertexComputeTaskSharedState::VertexComputeTaskSharedState(graph::Graph* graph, VertexCompute& vc,
    uint64_t maxThreadsForExecution)
    : graph{graph}, vc{vc} {
    morselDispatcher = std::make_unique<FrontierMorselDispatcher>(maxThreadsForExecution);
}

void VertexComputeTask::run() {
    FrontierMorsel frontierMorsel;
    auto localVc = sharedState->vc.copy();
    while (sharedState->morselDispatcher->getNextRangeMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextOffset()) {
            common::nodeID_t nodeID = frontierMorsel.getNextNodeID();
            localVc->vertexCompute(nodeID);
        }
    }
    localVc->finalizeWorkerThread();
}
} // namespace function
} // namespace kuzu
