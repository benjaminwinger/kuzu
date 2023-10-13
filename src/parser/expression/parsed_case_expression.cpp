#include "parser/expression/parsed_case_expression.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

void ParsedCaseAlternative::serialize(SerDeser& serializer) const {
    whenExpression->serialize(serializer);
    thenExpression->serialize(serializer);
}

std::unique_ptr<ParsedCaseAlternative> ParsedCaseAlternative::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    auto whenExpression = ParsedExpression::deserialize(fileInfo, offset);
    auto thenExpression = ParsedExpression::deserialize(fileInfo, offset);
    return std::make_unique<ParsedCaseAlternative>(
        std::move(whenExpression), std::move(thenExpression));
}

std::unique_ptr<ParsedCaseExpression> ParsedCaseExpression::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    std::unique_ptr<ParsedExpression> caseExpression;
    SerDeser::deserializeOptionalValue(caseExpression, fileInfo, offset);
    std::vector<std::unique_ptr<ParsedCaseAlternative>> caseAlternatives;
    SerDeser::deserializeVectorOfPtrs(caseAlternatives, fileInfo, offset);
    std::unique_ptr<ParsedExpression> elseExpression;
    SerDeser::deserializeOptionalValue(elseExpression, fileInfo, offset);
    return std::make_unique<ParsedCaseExpression>(
        std::move(caseExpression), std::move(caseAlternatives), std::move(elseExpression));
}

std::unique_ptr<ParsedExpression> ParsedCaseExpression::copy() const {
    std::vector<std::unique_ptr<ParsedCaseAlternative>> caseAlternativesCopy;
    caseAlternativesCopy.reserve(caseAlternatives.size());
    for (auto& caseAlternative : caseAlternatives) {
        caseAlternativesCopy.push_back(caseAlternative->copy());
    }
    return std::make_unique<ParsedCaseExpression>(alias, rawName, copyChildren(),
        caseExpression ? caseExpression->copy() : nullptr, std::move(caseAlternativesCopy),
        elseExpression ? elseExpression->copy() : nullptr);
}

void ParsedCaseExpression::serializeInternal(SerDeser& serializer) const {
    serializer.serializeOptionalValue(caseExpression);
    serializer.serializeVectorOfPtrs(caseAlternatives);
    serializer.serializeOptionalValue(elseExpression);
}

} // namespace parser
} // namespace kuzu
