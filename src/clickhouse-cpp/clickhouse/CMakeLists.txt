SET ( clickhouse-cpp-lib-src
    base/coded.cpp
    base/compressed.cpp
    base/input.cpp
    base/output.cpp
    base/platform.cpp
    base/socket.cpp

    columns/array.cpp
    columns/date.cpp
    columns/decimal.cpp
    columns/enum.cpp
    columns/factory.cpp
    columns/nullable.cpp
    columns/numeric.cpp
    columns/string.cpp
    columns/tuple.cpp
    columns/uuid.cpp
    columns/nothing.cpp

    types/type_parser.cpp
    types/types.cpp

    block.cpp
    client.cpp
    query.cpp
)

IF (UNIX)
	LIST(APPEND CH_COMPILE_OPTIONS "-Wall" "-Wextra" "-Werror" "-Wstrict-aliasing")
	IF ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
		LIST(APPEND CH_COMPILE_OPTIONS "-g" "-O2")
	ELSEIF("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
		LIST(APPEND CH_COMPILE_OPTIONS "-g3" "-O0")
	ENDIF()

	IF (NOT APPLE)
		LIST(APPEND CH_COMPILE_OPTIONS "-pthread")
	ENDIF()
ENDIF()

ADD_LIBRARY (clickhouse-cpp-lib SHARED ${clickhouse-cpp-lib-src})
TARGET_COMPILE_OPTIONS(clickhouse-cpp-lib PRIVATE ${CH_COMPILE_OPTIONS})

SET_TARGET_PROPERTIES(clickhouse-cpp-lib
    PROPERTIES LINKER_LANGUAGE CXX)

TARGET_LINK_LIBRARIES (clickhouse-cpp-lib PRIVATE
    cityhash-lib
    lz4-lib
	pthread
)

ADD_LIBRARY (clickhouse-cpp-lib-static STATIC ${clickhouse-cpp-lib-src})
TARGET_COMPILE_OPTIONS(clickhouse-cpp-lib-static PRIVATE ${CH_COMPILE_OPTIONS})
TARGET_LINK_LIBRARIES (clickhouse-cpp-lib-static PRIVATE
    cityhash-lib
    lz4-lib
	pthread
)
