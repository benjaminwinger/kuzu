#include "function/scalar_macro_function.h"

#include "common/ser_deser.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace function {

macro_parameter_value_map ScalarMacroFunction::getDefaultParameterVals() const {
    macro_parameter_value_map defaultArgsToReturn;
    for (auto& defaultArg : defaultArgs) {
        defaultArgsToReturn.emplace(defaultArg.first, defaultArg.second.get());
    }
    return defaultArgsToReturn;
}

std::unique_ptr<ScalarMacroFunction> ScalarMacroFunction::copy() const {
    default_macro_args defaultArgsCopy;
    for (auto& defaultArg : defaultArgs) {
        defaultArgsCopy.emplace_back(defaultArg.first, defaultArg.second->copy());
    }
    return std::make_unique<ScalarMacroFunction>(
        expression->copy(), positionalArgs, std::move(defaultArgsCopy));
}

void ScalarMacroFunction::serialize(SerDeser& serializer) const {
    expression->serialize(serializer);
    serializer.serializeVector(positionalArgs);
    auto vectorSize = defaultArgs.size();
    serializer.serializeValue(vectorSize);
    for (auto& defaultArg : defaultArgs) {
        serializer.serializeValue(defaultArg.first);
        defaultArg.second->serialize(serializer);
    }
}

std::unique_ptr<ScalarMacroFunction> ScalarMacroFunction::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    auto expression = ParsedExpression::deserialize(fileInfo, offset);
    std::vector<std::string> positionalArgs;
    SerDeser::deserializeVector(positionalArgs, fileInfo, offset);
    default_macro_args defaultArgs;
    uint64_t vectorSize;
    SerDeser::deserializeValue(vectorSize, fileInfo, offset);
    defaultArgs.reserve(vectorSize);
    for (auto i = 0u; i < vectorSize; i++) {
        std::string key;
        SerDeser::deserializeValue(key, fileInfo, offset);
        auto val = ParsedExpression::deserialize(fileInfo, offset);
        defaultArgs.emplace_back(std::move(key), std::move(val));
    }
    return std::make_unique<ScalarMacroFunction>(
        std::move(expression), std::move(positionalArgs), std::move(defaultArgs));
}

} // namespace function
} // namespace kuzu
