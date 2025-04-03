cmake_minimum_required(VERSION 3.11)
set(CMAKE_CXX_STANDARD 20)

project(kuzu_microbench VERSION 0.0.1 LANGUAGES CXX)

find_package(Threads REQUIRED)

include(FetchContent)
find_package(benchmark)
if (NOT benchmark_FOUND)
    FetchContent_Declare(
            benchmark
            GIT_REPOSITORY https://github.com/google/benchmark.git
            GIT_TAG v1.9.2
    )
    FetchContent_MakeAvailable(benchmark)
endif()

include_directories(../src/include)
include_directories(../extension/json/src/include)
include_directories(../third_party/nlohmann_json)
include_directories(${CMAKE_BINARY_DIR}/src/include/)

add_executable(kuzu_benchmark benchmark.cpp)
target_link_libraries(kuzu_benchmark benchmark::benchmark kuzu)
