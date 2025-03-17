cmake_minimum_required(VERSION 2.8.12...3.29)
project(HTBench)

option(NATIVE_ARCH "Compile targeting the native architecture" TRUE)

set(CMAKE_CXX_STANDARD 11)

if(NATIVE_ARCH)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
endif()

# Enable fetching the submodule if not initialized
if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/duckdb/CMakeLists.txt")
    message(FATAL_ERROR "DuckDB submodule is missing. Run: git submodule update --init --recursive")
endif()

# Add DuckDB as a subdirectory
add_subdirectory(duckdb)

# Include DuckDB headers
include_directories(duckdb/src/include)

# Add your own source files
include_directories(src)
link_directories(src)

add_executable(HTBench main.cpp
        src/hash_table/compressed_partitioned.hpp
        src/hash_table/utils.hpp
        src/materialization/row_layout.hpp
        src/materialization/aggregates.hpp
        src/materialization/memory_manager.hpp
        src/materialization/memory_manager.cpp
        src/materialization/scatter.hpp)

# Link with DuckDB library
target_link_libraries(HTBench duckdb)
