cmake_minimum_required(VERSION 3.5)
project(clickhouse_fdw VERSION 1.1.0 LANGUAGES C CXX)

add_subdirectory (clickhouse-cpp)
target_compile_options(clickhouse-cpp-lib-static PRIVATE -fPIC)
target_compile_options(clickhouse-cpp-lib PRIVATE -fPIC)

set (src
	clickhouse_connection.c
	clickhouse_deparse.c
	clickhouse_fdw.c
	clickhouse_option.c
	clickhouse_shipable.c
	clickhouse_shipable.c
	clickhouse_adjust.c
	clickhouse_link.c

	# library part
	clickhouse_http.c
	clickhouse_parser.c
	clickhouse_binary.cc
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(CURL libcurl REQUIRED)
pkg_check_modules(UUID_LIB uuid REQUIRED)

add_library(clickhouse_fdw MODULE ${src})
set_property(TARGET clickhouse_fdw PROPERTY POSITION_INDEPENDENT_CODE 1)
target_compile_features(clickhouse_fdw PRIVATE c_std_11)

separate_arguments(PGSQL_LDFLAGS UNIX_COMMAND "${PGSQL_LDFLAGS}")
target_link_libraries(clickhouse_fdw PRIVATE ${CURL_LDFLAGS} ${UUID_LIB_LDFLAGS} clickhouse-cpp-lib-static stdc++ ${PGSQL_LDFLAGS})

separate_arguments(PGSQL_CPPFLAGS UNIX_COMMAND "${PGSQL_CPPFLAGS}")
target_compile_options(clickhouse_fdw PRIVATE ${PGSQL_CPPFLAGS} -fstack-protector -fstack-check)

if (APPLE)
	message("Using libcurl from brew installation...")
	set(ENV{PKG_CONFIG_PATH}  "/usr/local/opt/curl/lib/pkgconfig")
	target_link_libraries(clickhouse_fdw PRIVATE "-bundle_loader ${PGSQL_BINDIR}/postgres")
endif()

set_target_properties (clickhouse_fdw PROPERTIES
  OUTPUT_NAME "clickhouse_fdw"
  PREFIX ""
)
set_target_properties(clickhouse_fdw PROPERTIES
	BUILD_WITH_INSTALL_RPATH TRUE
	INSTALL_RPATH ${PGSQL_PKGLIBDIR}
)

include_directories (
	PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/clickhouse-cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/include
		${PGSQL_INCLUDEDIR_SERVER}
		${CURL_INCLUDE_DIRS}
		${UUID_LIB_INCLUDE_DIRS}
)

# concat sql files to one
execute_process(
	COMMAND bash "-c" "cat ${CMAKE_SOURCE_DIR}/src/clickhouse_fdw.control | grep default_version | awk '{gsub(\"\\047\", \"\", $3); print $3}'"
	OUTPUT_VARIABLE EXT_VERSION
	OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(sql_out "${CMAKE_BINARY_DIR}/clickhouse_fdw--${EXT_VERSION}.sql")
set(sql_migration_11 "${CMAKE_CURRENT_SOURCE_DIR}/sql/clickhouse_fdw--1.0--1.1.sql")

add_custom_command(
	OUTPUT ${sql_out}
	COMMAND cat ${CMAKE_CURRENT_SOURCE_DIR}/sql/init.sql ${CMAKE_CURRENT_SOURCE_DIR}/sql/functions.sql> ${sql_out}
	DEPENDS sql/init.sql sql/functions.sql
)
add_custom_target(clickhouse_fdw_sql ALL DEPENDS ${sql_out} ${sql_migration_11})
add_dependencies(clickhouse_fdw clickhouse_fdw_sql)

#------------------------------------------------------------------------------
# sql
#------------------------------------------------------------------------------
set (install_files
	"${sql_out}"
	"${sql_migration_11}"
	"${CMAKE_SOURCE_DIR}/src/clickhouse_fdw.control"
)

#------------------------------------------------------------------------------
# install
#------------------------------------------------------------------------------
install (
	TARGETS clickhouse_fdw
	DESTINATION ${PGSQL_PKGLIBDIR}
)

install (
	FILES ${install_files}
	DESTINATION "${PGSQL_SHAREDIR}/extension"
)
