#include "planner/query_planner.h"

#include "binder/query/bound_regular_query.h"
#include "planner/logical_plan/logical_operator/logical_union.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

static std::vector<std::vector<std::unique_ptr<LogicalPlan>>> cartesianProductChildrenPlans(
    std::vector<std::vector<std::unique_ptr<LogicalPlan>>> childrenLogicalPlans) {
    std::vector<std::vector<std::unique_ptr<LogicalPlan>>> resultChildrenPlans;
    for (auto& childLogicalPlans : childrenLogicalPlans) {
        std::vector<std::vector<std::unique_ptr<LogicalPlan>>> curChildResultLogicalPlans;
        for (auto& childLogicalPlan : childLogicalPlans) {
            if (resultChildrenPlans.empty()) {
                std::vector<std::unique_ptr<LogicalPlan>> logicalPlans;
                logicalPlans.push_back(childLogicalPlan->shallowCopy());
                curChildResultLogicalPlans.push_back(std::move(logicalPlans));
            } else {
                for (auto& resultChildPlans : resultChildrenPlans) {
                    std::vector<std::unique_ptr<LogicalPlan>> logicalPlans;
                    for (auto& resultChildPlan : resultChildPlans) {
                        logicalPlans.push_back(resultChildPlan->shallowCopy());
                    }
                    logicalPlans.push_back(childLogicalPlan->shallowCopy());
                    curChildResultLogicalPlans.push_back(std::move(logicalPlans));
                }
            }
        }
        resultChildrenPlans = std::move(curChildResultLogicalPlans);
    }
    return resultChildrenPlans;
}

std::vector<std::unique_ptr<LogicalPlan>> QueryPlanner::getAllPlans(
    const BoundStatement& boundStatement) {
    std::vector<std::unique_ptr<LogicalPlan>> resultPlans;
    auto& regularQuery = (BoundRegularQuery&)boundStatement;
    if (regularQuery.getNumSingleQueries() == 1) {
        resultPlans = planSingleQuery(*regularQuery.getSingleQuery(0));
    } else {
        std::vector<std::vector<std::unique_ptr<LogicalPlan>>> childrenLogicalPlans(
            regularQuery.getNumSingleQueries());
        for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
            childrenLogicalPlans[i] = planSingleQuery(*regularQuery.getSingleQuery(i));
        }
        auto childrenPlans = cartesianProductChildrenPlans(std::move(childrenLogicalPlans));
        for (auto& childrenPlan : childrenPlans) {
            resultPlans.push_back(createUnionPlan(childrenPlan, regularQuery.getIsUnionAll(0)));
        }
    }
    return resultPlans;
}

std::unique_ptr<LogicalPlan> QueryPlanner::getBestPlan(
    std::vector<std::unique_ptr<LogicalPlan>> plans) {
    auto bestPlan = std::move(plans[0]);
    for (auto i = 1u; i < plans.size(); ++i) {
        if (plans[i]->getCost() < bestPlan->getCost()) {
            bestPlan = std::move(plans[i]);
        }
    }
    return bestPlan;
}

std::unique_ptr<LogicalPlan> QueryPlanner::createUnionPlan(
    std::vector<std::unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll) {
    assert(!childrenPlans.empty());
    auto plan = std::make_unique<LogicalPlan>();
    std::vector<std::shared_ptr<LogicalOperator>> children;
    for (auto& childPlan : childrenPlans) {
        children.push_back(childPlan->getLastOperator());
    }
    // we compute the schema based on first child
    auto union_ = make_shared<LogicalUnion>(
        childrenPlans[0]->getSchema()->getExpressionsInScope(), std::move(children));
    for (auto i = 0u; i < childrenPlans.size(); ++i) {
        appendFlattens(union_->getGroupsPosToFlatten(i), *childrenPlans[i]);
        union_->setChild(i, childrenPlans[i]->getLastOperator());
    }
    union_->computeFactorizedSchema();
    plan->setLastOperator(union_);
    if (!isUnionAll) {
        appendDistinct(union_->getExpressionsToUnion(), *plan);
    }
    return plan;
}

std::vector<std::unique_ptr<LogicalPlan>> QueryPlanner::getInitialEmptyPlans() {
    std::vector<std::unique_ptr<LogicalPlan>> plans;
    plans.push_back(std::make_unique<LogicalPlan>());
    return plans;
}

expression_vector QueryPlanner::getPropertiesForNode(NodeExpression& node) {
    expression_vector result;
    for (auto& expression : propertiesToScan) {
        auto property = (PropertyExpression*)expression.get();
        if (property->getVariableName() == node.getUniqueName()) {
            result.push_back(expression);
        }
    }
    return result;
}

expression_vector QueryPlanner::getPropertiesForRel(RelExpression& rel) {
    expression_vector result;
    for (auto& expression : propertiesToScan) {
        auto property = (PropertyExpression*)expression.get();
        if (property->getVariableName() == rel.getUniqueName()) {
            result.push_back(expression);
        }
    }
    return result;
}

std::unique_ptr<JoinOrderEnumeratorContext> QueryPlanner::enterContext(
    expression_vector nodeIDsToScanFromInnerAndOuter) {
    auto prevContext = std::move(context);
    context = std::make_unique<JoinOrderEnumeratorContext>();
    context->nodeIDsToScanFromInnerAndOuter = std::move(nodeIDsToScanFromInnerAndOuter);
    return prevContext;
}

void QueryPlanner::exitContext(std::unique_ptr<JoinOrderEnumeratorContext> prevContext) {
    context = std::move(prevContext);
}

} // namespace planner
} // namespace kuzu
