include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Have to pass this down to every subdirectory, which actually adds the files.
# This doesn't affect parent directories.
add_compile_definitions(KUZU_EXPORTS)
# For some reason the flags pulled in by linking to OpenMP::OpenMP_CXX don't
# work properly
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
add_subdirectory(binder)
add_subdirectory(c_api)
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(expression_evaluator)
add_subdirectory(function)
add_subdirectory(main)
add_subdirectory(optimizer)
add_subdirectory(parser)
add_subdirectory(planner)
add_subdirectory(processor)
add_subdirectory(storage)
add_subdirectory(transaction)
add_subdirectory(extension)

add_library(kuzu STATIC ${ALL_OBJECT_FILES})
add_library(kuzu_shared SHARED ${ALL_OBJECT_FILES})

set(KUZU_LIBRARIES antlr4_cypher antlr4_runtime brotlidec brotlicommon fast_float utf8proc re2 serd Threads::Threads fastpfor miniparquet zstd miniz mbedtls OpenMP::OpenMP_CXX)
if(NOT WIN32)
        set(KUZU_LIBRARIES dl ${KUZU_LIBRARIES})
endif()
target_link_libraries(kuzu PUBLIC ${KUZU_LIBRARIES})
target_link_libraries(kuzu_shared PUBLIC ${KUZU_LIBRARIES})
unset(KUZU_LIBRARIES)

set(KUZU_INCLUDES $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}> ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(kuzu PUBLIC ${KUZU_INCLUDES})
target_include_directories(kuzu_shared PUBLIC ${KUZU_INCLUDES})
unset(KUZU_INCLUDES)

if(WIN32)
        # Anything linking against the static library must not use dllimport.
        target_compile_definitions(kuzu INTERFACE KUZU_STATIC_DEFINE)
endif()

if(NOT WIN32)
        set_target_properties(kuzu_shared PROPERTIES OUTPUT_NAME kuzu)
endif()

install(TARGETS kuzu kuzu_shared)

if(${BUILD_SINGLE_FILE_HEADER})
        # Create a command to generate kuzu.hpp, and then create a target that is
        # always built that depends on it. This allows our generator to detect when
        # exactly to build kuzu.hpp, while still building the target by default.
        find_package(Python3 3.9...4 REQUIRED)
        add_custom_command(
                OUTPUT kuzu.hpp
                COMMAND
                        ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/scripts/collect-single-file-header.py
                DEPENDS
                        ${PROJECT_SOURCE_DIR}/scripts/collect-single-file-header.py kuzu_shared)
        add_custom_target(single_file_header ALL DEPENDS kuzu.hpp)
endif()

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/c_api/kuzu.h TYPE INCLUDE)

if(${BUILD_SINGLE_FILE_HEADER})
        install(FILES ${CMAKE_CURRENT_BINARY_DIR}/kuzu.hpp  TYPE INCLUDE)
endif()
