cmake_minimum_required(VERSION 3.5)
project(libclickhouse_odbc VERSION 1.0.0 LANGUAGES CXX)

set(src clickhouse-client.cpp)
add_library(clickhouse_odbc SHARED ${src})

if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
    set(CMAKE_BUILD_TYPE "Debug")
endif()
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
	target_compile_options(clickhouse_odbc PRIVATE -Wall -Wextra -Wstrict-aliasing -g -O2)
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
	target_compile_options(clickhouse_odbc PRIVATE -Wall -Wextra -Wstrict-aliasing -g3 -O0)
endif()

target_include_directories(clickhouse_odbc
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(clickhouse_odbc -fPIC -ldl stdc++ odbc)
