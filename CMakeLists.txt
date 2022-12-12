cmake_minimum_required(VERSION 3.5 FATAL_ERROR)
project(clickhouse_fdw VERSION 1.4.0 LANGUAGES C)

set(CMAKE_COLOR_MAKEFILE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake ${CMAKE_MODULE_PATH})
message(STATUS "Setting ${CMAKE_PROJECT_NAME} build type - ${CMAKE_BUILD_TYPE}")

#------------------------------------------------------------------------------
# PostgreSQL server paths come from pg_config
#------------------------------------------------------------------------------
find_program(PG_CONFIG pg_config)
if(NOT PG_CONFIG)
	message(FATAL_ERROR "Unable to find 'pg_config'")
endif(NOT PG_CONFIG)

exec_program(${PG_CONFIG} ARGS --version OUTPUT_VARIABLE PGCONFIG_OUTPUT)

string(REGEX REPLACE
    "^PostgreSQL[\t ]+([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\1.\\2.\\3"
    PGSQL_VERSION "${PGCONFIG_OUTPUT}")

unset(PGCONFIG_OUTPUT)

if(PGSQL_VERSION)
    string(REGEX REPLACE
        "([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\1"
        PGSQL_VERSION_MAJOR "${PGSQL_VERSION}")
    string(REGEX REPLACE
        "([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\2"
        PGSQL_VERSION_MINOR "${PGSQL_VERSION}")

    set(PGSQL_NUMERIC_VERSION ${PGSQL_VERSION_MAJOR}${PGSQL_VERSION_MINOR})
endif()

exec_program(${PG_CONFIG} ARGS --includedir OUTPUT_VARIABLE PGSQL_INCLUDEDIR)
exec_program(${PG_CONFIG} ARGS --includedir-server
  OUTPUT_VARIABLE PGSQL_INCLUDEDIR_SERVER)

exec_program(${PG_CONFIG} ARGS --libdir OUTPUT_VARIABLE PGSQL_LIBDIR)
exec_program(${PG_CONFIG} ARGS --pkglibdir OUTPUT_VARIABLE PGSQL_PKGLIBDIR)
exec_program(${PG_CONFIG} ARGS --sharedir OUTPUT_VARIABLE PGSQL_SHAREDIR)
exec_program(${PG_CONFIG} ARGS --bindir OUTPUT_VARIABLE PGSQL_BINDIR)
exec_program(${PG_CONFIG} ARGS --cppflags OUTPUT_VARIABLE PGSQL_CPPFLAGS)
exec_program(${PG_CONFIG} ARGS --ldflags OUTPUT_VARIABLE PGSQL_LDFLAGS)
exec_program(${PG_CONFIG} ARGS --libs OUTPUT_VARIABLE PGSQL_LIBS)

#------------------------------------------------------------------------------
# tests
#------------------------------------------------------------------------------
find_program(PG_REGRESS NAMES pg_regress PATHS ${PGSQL_PKGLIBDIR}/pgxs/src/test/regress/ REQUIRED)
LIST(APPEND REGRESS_TESTS
	http
	binary
	import_schema
	binary_queries
	binary_inserts
	functions
	engines
	deparse_checks
)
add_custom_target(installcheck
	COMMAND ${PG_REGRESS} --inputdir=${PROJECT_SOURCE_DIR}/tests --outputdir=${PROJECT_SOURCE_DIR}/tests \${EXTRA_REGRESS_OPTS} ${REGRESS_TESTS})

#------------------------------------------------------------------------------
# use default install location if not specified
#------------------------------------------------------------------------------
if (NOT DEFINED LIB_INSTALL_DIR)
  set (LIB_INSTALL_DIR lib)
endif()

#------------------------------------------------------------------------------
# source dir
#------------------------------------------------------------------------------
add_subdirectory (src)
