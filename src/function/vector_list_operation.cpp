#include "binder/expression_binder.h"
#include "common/types/ku_list.h"
#include "common/vector/value_vector_utils.h"
#include "function/list/operations/list_append_operation.h"
#include "function/list/operations/list_concat_operation.h"
#include "function/list/operations/list_contains.h"
#include "function/list/operations/list_extract_operation.h"
#include "function/list/operations/list_len_operation.h"
#include "function/list/operations/list_position_operation.h"
#include "function/list/operations/list_prepend_operation.h"
#include "function/list/operations/list_slice_operation.h"
#include "function/list/operations/list_sort_operation.h"
#include "function/list/operations/list_sum_operation.h"
#include "function/list/vector_list_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::string getListFunctionIncompatibleChildrenTypeErrorMsg(
    const std::string& functionName, const DataType& left, const DataType& right) {
    return std::string("Cannot bind " + functionName + " with parameter type " +
                       Types::dataTypeToString(left) + " and " + Types::dataTypeToString(right) +
                       ".");
}

void ListCreationVectorOperation::execFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    assert(result.dataType.typeID == VAR_LIST);
    common::StringVector::resetOverflowBuffer(&result);
    for (auto selectedPos = 0u; selectedPos < result.state->selVector->selectedSize;
         ++selectedPos) {
        auto pos = result.state->selVector->selectedPositions[selectedPos];
        auto resultEntry = common::ListVector::addList(&result, parameters.size());
        result.setValue(pos, resultEntry);
        auto resultValues = common::ListVector::getListValues(&result, resultEntry);
        auto resultDataVector = common::ListVector::getDataVector(&result);
        auto numBytesPerValue = resultDataVector->getNumBytesPerValue();
        for (auto i = 0u; i < parameters.size(); i++) {
            auto parameter = parameters[i];
            auto paramPos = parameter->state->isFlat() ?
                                parameter->state->selVector->selectedPositions[0] :
                                pos;
            if (parameter->isNull(paramPos)) {
                resultDataVector->setNull(resultEntry.offset + i, true);
            } else {
                common::ValueVectorUtils::copyValue(resultValues, *resultDataVector,
                    parameter->getData() + parameter->getNumBytesPerValue() * paramPos, *parameter);
            }
            resultValues += numBytesPerValue;
        }
    }
}

std::unique_ptr<FunctionBindData> ListCreationVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    // ListCreation requires all parameters to have the same type or be ANY type. The result type of
    // listCreation can be determined by the first non-ANY type parameter. If all parameters have
    // dataType ANY, then the resultType will be INT64[] (default type).
    auto resultType = DataType{std::make_unique<DataType>(INT64)};
    for (auto i = 0u; i < arguments.size(); i++) {
        if (arguments[i]->getDataType().typeID != common::ANY) {
            resultType = DataType{std::make_unique<DataType>(arguments[i]->getDataType())};
            break;
        }
    }
    // Cast parameters with ANY dataType to resultChildType.
    for (auto i = 0u; i < arguments.size(); i++) {
        auto parameterType = arguments[i]->getDataType();
        if (parameterType != *resultType.getChildType()) {
            if (parameterType.typeID == common::ANY) {
                binder::ExpressionBinder::resolveAnyDataType(
                    *arguments[i], *resultType.getChildType());
            } else {
                throw BinderException(
                    getListFunctionIncompatibleChildrenTypeErrorMsg(LIST_CREATION_FUNC_NAME,
                        arguments[0]->getDataType(), arguments[i]->getDataType()));
            }
        }
    }
    return std::make_unique<FunctionBindData>(resultType);
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListCreationVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_CREATION_FUNC_NAME,
        std::vector<DataTypeID>{ANY}, VAR_LIST, execFunc, nullptr, bindFunc,
        true /*  isVarLength */));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ListLenVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    auto execFunc = UnaryExecFunction<list_entry_t, int64_t, operation::ListLen>;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_LEN_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST}, INT64, execFunc, true /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListExtractVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto resultType = *arguments[0]->getDataType().getChildType();
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    switch (resultType.typeID) {
    case BOOL: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<common::list_entry_t, int64_t, uint8_t, operation::ListExtract>;
    } break;
    case INT64: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<common::list_entry_t, int64_t, int64_t, operation::ListExtract>;
    } break;
    case DOUBLE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<common::list_entry_t, int64_t, double_t, operation::ListExtract>;
    } break;
    case DATE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<common::list_entry_t, int64_t, date_t, operation::ListExtract>;
    } break;
    case TIMESTAMP: {
        vectorOperationDefinition->execFunc = BinaryListExecFunction<common::list_entry_t, int64_t,
            timestamp_t, operation::ListExtract>;
    } break;
    case INTERVAL: {
        vectorOperationDefinition->execFunc = BinaryListExecFunction<common::list_entry_t, int64_t,
            interval_t, operation::ListExtract>;
    } break;
    case STRING: {
        vectorOperationDefinition->execFunc = BinaryListExecFunction<common::list_entry_t, int64_t,
            ku_string_t, operation::ListExtract>;
    } break;
    case VAR_LIST: {
        vectorOperationDefinition->execFunc = BinaryListExecFunction<common::list_entry_t, int64_t,
            list_entry_t, operation::ListExtract>;
    } break;
    default: {
        throw common::NotImplementedException("ListExtractVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(resultType);
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListExtractVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_EXTRACT_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST, INT64}, ANY, nullptr, nullptr, bindFunc,
        false /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_EXTRACT_FUNC_NAME,
        std::vector<DataTypeID>{STRING, INT64}, STRING,
        BinaryExecFunction<ku_string_t, int64_t, ku_string_t, operation::ListExtract>,
        false /* isVarlength */));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListConcatVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    auto execFunc =
        BinaryListExecFunction<list_entry_t, list_entry_t, list_entry_t, operation::ListConcat>;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_CONCAT_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST, VAR_LIST}, VAR_LIST, execFunc, nullptr, bindFunc,
        false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListConcatVectorOperation::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    if (arguments[0]->getDataType() != arguments[1]->getDataType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_CONCAT_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

std::unique_ptr<FunctionBindData> ListAppendVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    if (*arguments[0]->getDataType().getChildType() != arguments[1]->getDataType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_APPEND_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    auto resultType = arguments[0]->getDataType();
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    switch (arguments[1]->getDataType().typeID) {
    case INT64: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<list_entry_t, int64_t, list_entry_t, operation::ListAppend>;
    } break;
    case DOUBLE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<list_entry_t, double_t, list_entry_t, operation::ListAppend>;
    } break;
    case BOOL: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<list_entry_t, uint8_t, list_entry_t, operation::ListAppend>;
    } break;
    case STRING: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<list_entry_t, ku_string_t, list_entry_t, operation::ListAppend>;
    } break;
    case DATE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<list_entry_t, date_t, list_entry_t, operation::ListAppend>;
    } break;
    case TIMESTAMP: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<list_entry_t, timestamp_t, list_entry_t, operation::ListAppend>;
    } break;
    case INTERVAL: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<list_entry_t, interval_t, list_entry_t, operation::ListAppend>;
    } break;
    case VAR_LIST: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<list_entry_t, ku_list_t, list_entry_t, operation::ListAppend>;
    } break;
    default: {
        throw common::NotImplementedException("ListAppendVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(resultType);
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListAppendVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_APPEND_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST, ANY}, VAR_LIST, nullptr, nullptr, bindFunc,
        false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListPrependVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    if (arguments[0]->dataType != *arguments[1]->dataType.getChildType()) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_APPEND_FUNC_NAME, arguments[0]->getDataType(), arguments[1]->getDataType()));
    }
    auto resultType = arguments[1]->getDataType();
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    switch (arguments[0]->getDataType().getTypeID()) {
    case INT64: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<int64_t, list_entry_t, list_entry_t, operation::ListPrepend>;
    } break;
    case DOUBLE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<double_t, list_entry_t, list_entry_t, operation::ListPrepend>;
    } break;
    case BOOL: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<uint8_t, list_entry_t, list_entry_t, operation::ListPrepend>;
    } break;
    case STRING: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<ku_string_t, list_entry_t, list_entry_t, operation::ListPrepend>;
    } break;
    case DATE: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<date_t, list_entry_t, list_entry_t, operation::ListPrepend>;
    } break;
    case TIMESTAMP: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<timestamp_t, list_entry_t, list_entry_t, operation::ListPrepend>;
    } break;
    case INTERVAL: {
        vectorOperationDefinition->execFunc =
            BinaryListExecFunction<interval_t, list_entry_t, list_entry_t, operation::ListPrepend>;
    } break;
    case VAR_LIST: {
        vectorOperationDefinition->execFunc = BinaryListExecFunction<list_entry_t, list_entry_t,
            list_entry_t, operation::ListPrepend>;
    } break;
    default: {
        throw common::NotImplementedException("ListPrependVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(resultType);
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListPrependVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_PREPEND_FUNC_NAME,
        std::vector<DataTypeID>{ANY, VAR_LIST}, VAR_LIST, nullptr, nullptr, bindFunc,
        false /* isVarlength */));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListPositionVectorOperation::getDefinitions() {
    return getBinaryListOperationDefinitions<operation::ListPosition, int64_t>(
        LIST_POSITION_FUNC_NAME, INT64);
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListContainsVectorOperation::getDefinitions() {
    return getBinaryListOperationDefinitions<operation::ListContains, uint8_t>(
        LIST_CONTAINS_FUNC_NAME, BOOL);
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ListSliceVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SLICE_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST, INT64, INT64}, VAR_LIST,
        TernaryListExecFunction<common::list_entry_t, int64_t, int64_t, common::list_entry_t,
            operation::ListSlice>,
        nullptr, bindFunc, false /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SLICE_FUNC_NAME,
        std::vector<DataTypeID>{STRING, INT64, INT64}, STRING,
        TernaryListExecFunction<common::ku_string_t, int64_t, int64_t, common::ku_string_t,
            operation::ListSlice>,
        false /* isVarlength */));
    return result;
}

std::unique_ptr<FunctionBindData> ListSliceVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ListSortVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SORT_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST}, VAR_LIST, nullptr, nullptr, bindFunc,
        false /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SORT_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST, STRING}, VAR_LIST, nullptr, nullptr, bindFunc,
        false /* isVarlength*/));
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SORT_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST, STRING, STRING}, VAR_LIST, nullptr, nullptr, bindFunc,
        false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListSortVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    switch (arguments[0]->dataType.getChildType()->getTypeID()) {
    case INT64: {
        vectorOperationDefinition->execFunc = getExecFunction<int64_t>(arguments);
    } break;
    case DOUBLE: {
        vectorOperationDefinition->execFunc = getExecFunction<double_t>(arguments);
    } break;
    case BOOL: {
        vectorOperationDefinition->execFunc = getExecFunction<uint8_t>(arguments);
    } break;
    case STRING: {
        vectorOperationDefinition->execFunc = getExecFunction<ku_string_t>(arguments);
    } break;
    case DATE: {
        vectorOperationDefinition->execFunc = getExecFunction<date_t>(arguments);
    } break;
    case TIMESTAMP: {
        vectorOperationDefinition->execFunc = getExecFunction<timestamp_t>(arguments);
    } break;
    case INTERVAL: {
        vectorOperationDefinition->execFunc = getExecFunction<interval_t>(arguments);
    } break;
    default: {
        throw common::NotImplementedException("ListSortVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(arguments[0]->getDataType());
}

template<typename T>
scalar_exec_func ListSortVectorOperation::getExecFunction(
    const binder::expression_vector& arguments) {
    if (arguments.size() == 1) {
        return &UnaryListExecFunction<list_entry_t, list_entry_t, operation::ListSort<T>>;
    } else if (arguments.size() == 2) {
        return &BinaryListExecFunction<list_entry_t, ku_string_t, list_entry_t,
            operation::ListSort<T>>;
    } else if (arguments.size() == 3) {
        return &TernaryListExecFunction<list_entry_t, ku_string_t, ku_string_t, list_entry_t,
            operation::ListSort<T>>;
    } else {
        throw common::RuntimeException("Invalid number of arguments");
    }
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ListSumVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SUM_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST}, INT64, nullptr, nullptr, bindFunc,
        false /* isVarlength*/));
    return result;
}

std::unique_ptr<FunctionBindData> ListSumVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    auto resultType = *arguments[0]->getDataType().getChildType();
    switch (resultType.getTypeID()) {
    case INT64: {
        vectorOperationDefinition->execFunc =
            UnaryListExecFunction<list_entry_t, int64_t, operation::ListSum>;
    } break;
    case DOUBLE: {
        vectorOperationDefinition->execFunc =
            UnaryListExecFunction<list_entry_t, double_t, operation::ListSum>;
    } break;
    default: {
        throw common::NotImplementedException("ListSumVectorOperation::bindFunc");
    }
    }
    return std::make_unique<FunctionBindData>(resultType);
}

} // namespace function
} // namespace kuzu
