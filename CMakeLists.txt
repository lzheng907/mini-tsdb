# minitsdb - mini 时序存储引擎
cmake_minimum_required(VERSION 3.10)
project(minitsdb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

if(MSVC)
    add_compile_options(/W4 /permissive- /WX- /utf-8)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else()
    add_compile_options(-Wall -Wextra -Wno-unused-parameter -Werror)
endif()

# 核心库
add_library(minitsdb_core STATIC
    src/bloomfilter.cpp
    src/block_cache.cpp
    src/compaction.cpp
    src/compression.cpp
    src/crc32.cpp
    src/memtable.cpp
    src/random_access_file.cpp
    src/sstable.cpp
    src/status.cpp
    src/tsdb_engine.cpp
    src/version.cpp
    src/wal.cpp
)
target_include_directories(minitsdb_core PUBLIC src)
target_link_libraries(minitsdb_core PUBLIC Threads::Threads)

# 单元测试
add_executable(minitsdb_test tests/test_main.cpp)
target_link_libraries(minitsdb_test PRIVATE minitsdb_core)

add_executable(minitsdb_smoke tests/smoke_test.cpp)
target_link_libraries(minitsdb_smoke PRIVATE minitsdb_core)

# 传感器回放 demo
add_executable(sensor_demo demo/sensor_demo.cpp)
target_link_libraries(sensor_demo PRIVATE minitsdb_core)

# 可观测平台接入 demo
add_executable(observability_demo demo/observability_demo.cpp)
target_link_libraries(observability_demo PRIVATE minitsdb_core)

enable_testing()
add_test(NAME minitsdb_test COMMAND minitsdb_test)
add_test(NAME minitsdb_smoke COMMAND minitsdb_smoke)

# Benchmark / pressure test
add_executable(minitsdb_bench bench/bench_main.cpp)
target_link_libraries(minitsdb_bench PRIVATE minitsdb_core)
