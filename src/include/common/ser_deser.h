#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/file_utils.h"

namespace kuzu {
namespace common {

class LogicalType;

class SerDeser {
    // TODO: Buffer deserialisation
    std::unique_ptr<uint8_t[]> buffer;
    uint64_t fileOffset, bufferOffset;
    FileInfo* fileInfo;
    static const uint64_t BUFFER_SIZE = 4096;

    void writeToBuffer(uint8_t* data, uint64_t size) {
        if (bufferOffset + size <= BUFFER_SIZE) {
            memcpy(&buffer[bufferOffset], data, size);
            bufferOffset += size;
        } else {
            auto toCopy = BUFFER_SIZE - bufferOffset;
            memcpy(&buffer[bufferOffset], data, toCopy);
            bufferOffset += toCopy;
            flush();
            auto remaining = size - toCopy;
            memcpy(buffer.get(), data + toCopy, remaining);
            bufferOffset += remaining;
        }
    }
    void flush() {
        FileUtils::writeToFile(fileInfo, buffer.get(), bufferOffset, fileOffset);
        fileOffset += bufferOffset;
        bufferOffset = 0;
        memset(buffer.get(), 0, BUFFER_SIZE);
    }

public:
    ~SerDeser() { flush(); }
    explicit SerDeser(FileInfo* fileInfo)
        : buffer(std::make_unique<uint8_t[]>(BUFFER_SIZE)), fileOffset(0), bufferOffset(0),
          fileInfo(fileInfo) {}

    template<typename T>
    void serializeValue(const T& value) {
        static_assert(std::is_trivially_destructible<T>(), "value must be a trivial type");
        writeToBuffer((uint8_t*)&value, sizeof(T));
    }

    template<typename T>
    void serializeOptionalValue(const std::unique_ptr<T>& value) {
        serializeValue(value == nullptr);
        if (value != nullptr) {
            value->serialize(*this);
        }
    }

    template<typename T>
    static void deserializeValue(T& value, FileInfo* fileInfo, uint64_t& offset) {
        static_assert(std::is_trivially_destructible<T>(), "value must be a trivial type");
        FileUtils::readFromFile(fileInfo, (uint8_t*)&value, sizeof(T), offset);
        offset += sizeof(T);
    }

    template<typename T>
    static void deserializeOptionalValue(
        std::unique_ptr<T>& value, FileInfo* fileInfo, uint64_t& offset) {
        bool isNull;
        deserializeValue(isNull, fileInfo, offset);
        if (!isNull) {
            value = T::deserialize(fileInfo, offset);
        }
    }

    template<typename T1, typename T2>
    void serializeUnorderedMap(const std::unordered_map<T1, std::unique_ptr<T2>>& values) {
        uint64_t mapSize = values.size();
        serializeValue(mapSize);
        for (auto& value : values) {
            serializeValue(value.first);
            value.second->serialize(*this);
        }
    }

    template<typename T>
    void serializeVector(const std::vector<T>& values) {
        uint64_t vectorSize = values.size();
        serializeValue<uint64_t>(vectorSize);
        for (auto& value : values) {
            serializeValue<T>(value);
        }
    }

    template<typename T>
    void serializeVectorOfPtrs(const std::vector<std::unique_ptr<T>>& values) {
        uint64_t vectorSize = values.size();
        serializeValue<uint64_t>(vectorSize);
        for (auto& value : values) {
            value->serialize(*this);
        }
    }

    template<typename T1, typename T2>
    static void deserializeUnorderedMap(
        std::unordered_map<T1, std::unique_ptr<T2>>& values, FileInfo* fileInfo, uint64_t& offset) {
        uint64_t mapSize;
        deserializeValue<uint64_t>(mapSize, fileInfo, offset);
        values.reserve(mapSize);
        for (auto i = 0u; i < mapSize; i++) {
            T1 key;
            deserializeValue<T1>(key, fileInfo, offset);
            auto val = T2::deserialize(fileInfo, offset);
            values.emplace(key, std::move(val));
        }
    }

    template<typename T>
    static void deserializeVector(std::vector<T>& values, FileInfo* fileInfo, uint64_t& offset) {
        uint64_t vectorSize;
        deserializeValue(vectorSize, fileInfo, offset);
        values.resize(vectorSize);
        for (auto& value : values) {
            deserializeValue(value, fileInfo, offset);
        }
    }

    template<typename T>
    static void deserializeVectorOfPtrs(
        std::vector<std::unique_ptr<T>>& values, FileInfo* fileInfo, uint64_t& offset) {
        uint64_t vectorSize;
        deserializeValue(vectorSize, fileInfo, offset);
        values.reserve(vectorSize);
        for (auto i = 0u; i < vectorSize; i++) {
            values.push_back(T::deserialize(fileInfo, offset));
        }
    }

    template<typename T>
    void serializeUnorderedSet(const std::unordered_set<T>& values) {
        uint64_t setSize = values.size();
        serializeValue(setSize);
        for (const auto& value : values) {
            serializeValue(value);
        }
    }

    template<typename T>
    static void deserializeUnorderedSet(
        std::unordered_set<T>& values, FileInfo* fileInfo, uint64_t& offset) {
        uint64_t setSize;
        deserializeValue(setSize, fileInfo, offset);
        for (auto i = 0u; i < setSize; i++) {
            T value;
            deserializeValue<T>(value, fileInfo, offset);
            values.insert(value);
        }
    }
};

template<>
void SerDeser::serializeValue(const std::string& value);

template<>
void SerDeser::deserializeValue(std::string& value, FileInfo* fileInfo, uint64_t& offset);

} // namespace common
} // namespace kuzu
