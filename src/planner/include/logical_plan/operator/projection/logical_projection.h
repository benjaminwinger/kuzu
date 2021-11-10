#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

class LogicalProjection : public LogicalOperator {

public:
    explicit LogicalProjection(vector<shared_ptr<Expression>> expressions,
        vector<uint32_t> discardedGroupsPos, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, expressionsToProject{move(expressions)},
          discardedGroupsPos{move(discardedGroupsPos)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_PROJECTION;
    }

    string getExpressionsForPrinting() const override {
        auto result = expressionsToProject[0]->getUniqueName();
        for (auto i = 1u; i < expressionsToProject.size(); ++i) {
            result += ", " + expressionsToProject[i]->getUniqueName();
        }
        return result;
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalProjection>(
            expressionsToProject, discardedGroupsPos, prevOperator->copy());
    }

public:
    vector<shared_ptr<Expression>> expressionsToProject;
    vector<uint32_t> discardedGroupsPos;
};

} // namespace planner
} // namespace graphflow
