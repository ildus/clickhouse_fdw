CMAKE_MINIMUM_REQUIRED(VERSION 3.0.2)

INCLUDE (cmake/cpp17.cmake)
INCLUDE (cmake/subdirs.cmake)

OPTION(BUILD_BENCHMARK "Build benchmark" OFF)
OPTION(BUILD_TESTS "Build tests" OFF)

PROJECT (CLICKHOUSE-CLIENT LANGUAGES CXX)

    USE_CXX17()

    INCLUDE_DIRECTORIES(.)
    INCLUDE_DIRECTORIES(contrib)

    SUBDIRS (
        clickhouse
        contrib/cityhash
        contrib/lz4
    )

    IF (BUILD_TESTS)
        SUBDIRS(
             contrib/gtest
             tests/simple
             ut
        )
    ENDIF (BUILD_TESTS)

    IF (BUILD_BENCHMARK)
        SUBDIRS(bench)
    ENDIF (BUILD_BENCHMARK)
