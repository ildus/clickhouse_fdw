CMAKE_MINIMUM_REQUIRED(VERSION 3.0.2)

INCLUDE (cmake/cpp17.cmake)
INCLUDE (cmake/subdirs.cmake)

OPTION(BUILD_BENCHMARK "Build benchmark" OFF)
OPTION(BUILD_TESTS "Build tests" OFF)

PROJECT (CLICKHOUSE-CLIENT LANGUAGES CXX)

	IF ("${CMAKE_BUILD_TYPE}" STREQUAL "")
		set(CMAKE_BUILD_TYPE "Debug")
	ENDIF()

	IF ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
		SET(CH_COMPILE_OPTIONS "-Wall -Wextra -Werror -Wstrict-aliasing -g -O2 -fPIC")
	ELSEIF("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
		SET(CH_COMPILE_OPTIONS "-Wall -Wextra -Werror -Wstrict-aliasing -g3 -O0 -fPIC")
	ENDIF()

    USE_CXX17()

	IF (APPLE)
		SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CH_COMPILE_OPTIONS}")
	ELSE ()
		SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CH_COMPILE_OPTIONS} -pthread")
	ENDIF ()
	SET (CMAKE_EXE_LINKER_FLAGS, "${CMAKE_EXE_LINKER_FLAGS} -lpthread")

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
