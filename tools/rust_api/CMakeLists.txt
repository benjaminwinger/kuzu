cmake_minimum_required(VERSION 3.11)

project(Kuzu-rs VERSION 0.0.1 LANGUAGES CXX)

find_package(Threads REQUIRED)

set(CMAKE_FIND_PACKAGE_RESOLVE_SYMLINKS TRUE)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(USE_BUNDLED_KUZU "Build kuzu along with the bridge instead of linking against an existing installation" OFF)

if (USE_BUNDLED_KUZU)
    set(KUZU_RS_KUZU_VERSION v0.0.3)
    include(FetchContent)

    FetchContent_Declare(kuzu
        GIT_REPOSITORY https://github.com/kuzudb/kuzu
        GIT_TAG ${KUZU_RS_KUZU_VERSION}
    )
    FetchContent_MakeAvailable(kuzu)
else()
    add_library(kuzu STATIC IMPORTED)
    set_target_properties(kuzu PROPERTIES IMPORTED_LOCATION
        ${CMAKE_CURRENT_SOURCE_DIR}/../../build/debug/src/libkuzu.a)
    target_include_directories(kuzu
        INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../../third_party/nlohmann_json
        ${CMAKE_CURRENT_SOURCE_DIR}/../../third_party/spdlog
    )
    # TODO: find system installed Kuzu, or via variable hints
endif()

add_subdirectory(src)
