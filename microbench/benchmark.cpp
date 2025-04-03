#include <algorithm>
#include <cstddef>

#include "common/fast_mem.h"
#include <benchmark/benchmark.h>

static std::array<std::string, 42> columns = {"ID", "birthday", "browserUsed", "content",
    "creationDate", "firstName", "gender", "imageFile", "language", "lastName", "length",
    "locationIP", "name", "title", "type", "url", "fName", "isStudent", "isWorker", "age",
    "workedHours", "eyeSight", "birthdate", "registerTime",
    "lastJobDuration"
    "workedHours",
    "usedNames", "courseScoresPerTerm", "grades", "height", "u", "licenseValidInterval", "history",
    "orgCode", "mark", "score", "location", "movein", "note", "content", "audience", "someMap",
    "studyAt"};

template<size_t size>
static void vectorLookup(benchmark::State& state) {
    auto shuffledColumns = std::vector(columns.begin(), columns.begin() + size);
    // first shuffle
    for (size_t k = 0; k < size; k++) {
        size_t r = k + rand() % (size - k); // careful here!
        swap(shuffledColumns[k], shuffledColumns[r]);
    }
    std::vector<std::string> vectorColumns(columns.begin(), columns.begin() + size);
    std::vector<size_t> results(size);
    for (auto _ : state) {
        for (size_t i = 0; i < size; i++) {
            auto iter = std::find(vectorColumns.begin(), vectorColumns.end(), shuffledColumns[i]);
            results[i] = iter - vectorColumns.begin();
        }
    }
    benchmark::DoNotOptimize(results);
}

template<size_t size>
static void binarySearchVectorLookup(benchmark::State& state) {
    auto shuffledColumns = std::vector(columns.begin(), columns.begin() + size);
    // first shuffle
    for (size_t k = 0; k < size; k++) {
        size_t r = k + rand() % (size - k); // careful here!
        swap(shuffledColumns[k], shuffledColumns[r]);
    }
    std::vector<std::string> vectorColumns(columns.begin(), columns.begin() + size);
    std::sort(vectorColumns.begin(), vectorColumns.end());
    std::vector<size_t> results(size);
    for (auto _ : state) {
        for (size_t i = 0; i < size; i++) {
            auto iter =
                std::lower_bound(vectorColumns.begin(), vectorColumns.end(), shuffledColumns[i]);
            if (*iter == shuffledColumns[i]) {
                results[i] = iter - vectorColumns.begin();
            }
        }
    }
    benchmark::DoNotOptimize(results);
}

struct JSONKeyHash {
    inline std::size_t operator()(std::string_view k) const {
        size_t result = 0;
        if (k.size() >= sizeof(size_t)) {
            memcpy(&result, k.data() + k.size() - sizeof(size_t), sizeof(size_t));
        } else {
            result = 0;
            kuzu::json_extension::fastMemcpy(&result, k.data(), k.size());
        }
        return result;
    }
};

struct JSONKeyEquality {
    inline bool operator()(std::string_view a, std::string_view b) const {
        if (a.size() != b.size()) {
            return false;
        }
        return kuzu::json_extension::FastMemcmp(a.data(), b.data(), a.size()) == 0;
    }
};

template<size_t size>
static void unorderedMapLookup(benchmark::State& state) {
    std::unordered_map<std::string_view, size_t> map;
    for (size_t i = 0; i < size; i++) {
        map[columns[i]] = i;
    }
    auto shuffledColumns = std::vector(columns.begin(), columns.begin() + size);
    // first shuffle
    for (size_t k = 0; k < size; k++) {
        size_t r = k + rand() % (size - k); // careful here!
        swap(shuffledColumns[k], shuffledColumns[r]);
    }
    std::vector<size_t> results(size);
    for (auto _ : state) {
        for (size_t i = 0; i < size; i++) {
            results[i] = map[shuffledColumns[i]];
        }
    }
    benchmark::DoNotOptimize(results);
}

template<size_t size>
static void fastUnorderedMapLookup(benchmark::State& state) {
    std::unordered_map<std::string_view, size_t, JSONKeyHash, JSONKeyEquality> map;
    for (size_t i = 0; i < size; i++) {
        map[columns[i]] = i;
    }
    auto shuffledColumns = std::vector(columns.begin(), columns.begin() + size);
    // first shuffle
    for (size_t k = 0; k < size; k++) {
        size_t r = k + rand() % (size - k); // careful here!
        swap(shuffledColumns[k], shuffledColumns[r]);
    }
    std::vector<size_t> results(size);
    for (auto _ : state) {
        for (size_t i = 0; i < size; i++) {
            results[i] = map[shuffledColumns[i]];
        }
    }
    benchmark::DoNotOptimize(results);
}

BENCHMARK(unorderedMapLookup<1>);
BENCHMARK(unorderedMapLookup<4>);
BENCHMARK(unorderedMapLookup<8>);
BENCHMARK(unorderedMapLookup<16>);
BENCHMARK(unorderedMapLookup<20>);
BENCHMARK(unorderedMapLookup<24>);
BENCHMARK(unorderedMapLookup<28>);
BENCHMARK(unorderedMapLookup<32>);
BENCHMARK(unorderedMapLookup<36>);
BENCHMARK(unorderedMapLookup<42>);

BENCHMARK(vectorLookup<1>);
BENCHMARK(vectorLookup<4>);
BENCHMARK(vectorLookup<8>);
BENCHMARK(vectorLookup<16>);
BENCHMARK(vectorLookup<20>);
BENCHMARK(vectorLookup<24>);
BENCHMARK(vectorLookup<28>);
BENCHMARK(vectorLookup<32>);
BENCHMARK(vectorLookup<36>);
BENCHMARK(vectorLookup<42>);

BENCHMARK(fastUnorderedMapLookup<1>);
BENCHMARK(fastUnorderedMapLookup<4>);
BENCHMARK(fastUnorderedMapLookup<8>);
BENCHMARK(fastUnorderedMapLookup<16>);
BENCHMARK(fastUnorderedMapLookup<32>);
BENCHMARK(fastUnorderedMapLookup<42>);

BENCHMARK(binarySearchVectorLookup<1>);
BENCHMARK(binarySearchVectorLookup<4>);
BENCHMARK(binarySearchVectorLookup<8>);
BENCHMARK(binarySearchVectorLookup<16>);
BENCHMARK(binarySearchVectorLookup<32>);
BENCHMARK(binarySearchVectorLookup<42>);

BENCHMARK_MAIN();
