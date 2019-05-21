cmake_minimum_required(VERSION 3.5)
project(libclickhouse VERSION 1.0.0 LANGUAGES C)

set(src
	src/clickhouse_http.c
	src/clickhouse_binary.c
	src/clickhouse_utils.c
	src/clickhouse_parser.c
)

add_library(clickhouse SHARED ${src})
set_property(TARGET clickhouse PROPERTY POSITION_INDEPENDENT_CODE 1)
target_compile_features(clickhouse PRIVATE c_std_11)
option(BUILD_TESTS "build tests" OFF)

add_compile_definitions(VERSION_REVISION=54420)
add_compile_definitions(VERSION_MAJOR=19)
add_compile_definitions(VERSION_MINOR=8)

include(CheckSymbolExists)
check_symbol_exists(MSG_NOSIGNAL "sys/types.h;sys/socket.h" HAVE_NOSIGNAL)
configure_file(src/clickhouse_config.h.in clickhouse_config.h @ONLY)


if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
    set(CMAKE_BUILD_TYPE "Debug")
endif()

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
	set(COMPILE_OPTIONS -Wall -Wextra -Wstrict-aliasing -g -O2)
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
	set(COMPILE_OPTIONS -Wall -Wextra -Wstrict-aliasing -g3 -O0)
endif()

if (APPLE)
	message("Using libcurl from brew installation...")
	set(ENV{PKG_CONFIG_PATH}  "/usr/local/opt/curl/lib/pkgconfig")
endif()

find_package(PkgConfig REQUIRED)
pkg_check_modules(CURL libcurl REQUIRED)

target_compile_options(clickhouse PRIVATE ${COMPILE_OPTIONS})
target_link_libraries(clickhouse ${CURL_LDFLAGS})

target_include_directories(clickhouse
    PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
		${CURL_INCLUDE_DIRS}
		${CMAKE_CURRENT_BINARY_DIR}
)

if (BUILD_TESTS)
	message(STATUS "building tests: ON")
	find_package(cmocka 1.1.3)

	list(APPEND tests_names "test_clickhouse_http")
	list(APPEND tests_names "test_clickhouse_binary")

	# declare all tests targets
	list(LENGTH tests_names count)
	math(EXPR count "${count} - 1")
	foreach(i RANGE ${count})
		list(GET tests_names ${i} test_name)
		add_executable(${test_name} src/${test_name}.c)
		target_include_directories(${test_name}
			PUBLIC
				$<INSTALL_INTERFACE:include>
				$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
			PRIVATE
				${CMAKE_CURRENT_SOURCE_DIR}/src
				${CURL_INCLUDE_DIRS}
		)
		target_compile_options(${test_name} PRIVATE ${COMPILE_OPTIONS})
		target_link_libraries(
			${test_name}
			cmocka
			-fprofile-arcs
			clickhouse
			${CURL_LIBRARIES}
			${test_link}
		)
		target_compile_features(${test_name}
			PRIVATE c_std_11
		)
		add_test(${test_name} ${test_name})
	endforeach()
	enable_testing()
endif()
