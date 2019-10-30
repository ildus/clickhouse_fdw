cmake_minimum_required(VERSION 3.5)
project(libclickhouse VERSION 1.0.0 LANGUAGES C CXX)

add_subdirectory (clickhouse-cpp)
target_compile_options(clickhouse-cpp-lib-static PRIVATE -fPIC)
target_compile_options(clickhouse-cpp-lib PRIVATE -fPIC)

set(src
)

add_library(clickhouse STATIC ${src})
set_property(TARGET clickhouse PROPERTY POSITION_INDEPENDENT_CODE 1)
target_compile_features(clickhouse PRIVATE c_std_11)
option(BUILD_TESTS "build tests" OFF)

separate_arguments(PGSQL_LDFLAGS UNIX_COMMAND "${PGSQL_LDFLAGS}")
separate_arguments(PGSQL_CPPFLAGS UNIX_COMMAND "${PGSQL_CPPFLAGS}")
target_compile_options(clickhouse PRIVATE ${PGSQL_CPPFLAGS} -fstack-protector -fstack-check)

if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
    set(CMAKE_BUILD_TYPE "Debug")
endif()
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
	target_compile_options(clickhouse PRIVATE -Wall -Wextra -Wstrict-aliasing -g -O2)
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
	target_compile_options(clickhouse PRIVATE -Wall -Wextra -Wstrict-aliasing -g3 -O0)
endif()

if (APPLE)
	message("Using libcurl from brew installation...")
	set(ENV{PKG_CONFIG_PATH}  "/usr/local/opt/curl/lib/pkgconfig")
	target_link_libraries(clickhouse PRIVATE "-bundle_loader ${PGSQL_BINDIR}/postgres")
endif()

set_target_properties(clickhouse PROPERTIES
	BUILD_WITH_INSTALL_RPATH TRUE
	INSTALL_RPATH ${PGSQL_PKGLIBDIR}
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(CURL libcurl REQUIRED)
pkg_check_modules(UUID_LIB uuid REQUIRED)

target_link_libraries(clickhouse ${CURL_LDFLAGS} ${UUID_LIB_LDFLAGS} clickhouse-cpp-lib-static stdc++ ${PGSQL_LDFLAGS})

target_include_directories(clickhouse
    PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/clickhouse-cpp
		${CURL_INCLUDE_DIRS}
		${UUID_LIB_INCLUDE_DIRS}
		${PGSQL_INCLUDEDIR_SERVER}
)

if (BUILD_TESTS)
	message(STATUS "building tests: ON")
	find_package(cmocka 1.1.3)

	list(APPEND tests_names "test_clickhouse_http")
	list(APPEND tests_names "test_clickhouse_binary")
	list(APPEND tests_options "-Wall -Werror -Wextra")
	#list(APPEND tests_link "-Wl,--wrap,free")

	# declare all tests targets
	list(LENGTH tests_names count)
	math(EXPR count "${count} - 1")
	foreach(i RANGE ${count})
		list(GET tests_names ${i} test_name)
		list(GET tests_options 0 test_options)
		#list(GET tests_link ${i} test_link)
		add_executable(${test_name} src/${test_name}.c)
		target_include_directories(${test_name}
			PUBLIC
				$<INSTALL_INTERFACE:include>
				$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
			PRIVATE
				${CMAKE_CURRENT_SOURCE_DIR}/src
				${CURL_INCLUDE_DIRS}
		)
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
