set(KUZU_RS_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/..)

if (BUILD_TESTS)
    if (CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(CARGO_CMD cargo test --verbose)
        set(CARGO_TARGET_DIR ${KUZU_RS_SOURCE_DIR}/target/debug)
    else ()
        set(CARGO_CMD cargo test --release --verbose)
        set(CARGO_TARGET_DIR ${KUZU_RS_SOURCE_DIR}/target/release)
    endif ()

    set(CARGO_MANIFEST ${KUZU_RS_SOURCE_DIR}/../Cargo.toml)

    set(KUZU_RS_LIB ${CARGO_TARGET_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}kuzu_rs${CMAKE_STATIC_LIBRARY_SUFFIX})

    add_test(KuzuRustBindingTests CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}
        ${CARGO_CMD})
    add_custom_command(
        OUTPUT ${KUZU_RS_LIB}
        COMMAND CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} ${CARGO_CMD}
        DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/rust_part/src/lib.rs.cc
        DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/rust_part/src/lib.rs.h
        DEPENDS ${KUZU_RS_SOURCE_DIR}/include/kuzu_rs.h
        DEPENDS ${KUZU_RS_SOURCE_DIR}/src/lib.rs
        DEPENDS ${KUZU_RS_SOURCE_DIR}/src/kuzu_rs.cpp
        USES_TERMINAL
        COMMENT "Running cargo..."
    )

    add_library(kuzu_rs STATIC IMPORTED)
    set_target_properties(kuzu_rs PROPERTIES IMPORTED_LOCATION
        ${KUZU_RS_LIB})
endif()
