cmake_minimum_required(VERSION 3.5)
project(clickhouse_fdw VERSION 1.0.0 LANGUAGES C)

set (src
	clickhousedb_connection.c
	clickhousedb_deparse.c
	clickhousedb_fdw.c
	clickhousedb_option.c
	clickhousedb_shipable.c
)

add_library(clickhouse_fdw SHARED ${src})
target_compile_options(clickhouse_fdw PRIVATE ${PGSQL_CPPFLAGS})
target_link_libraries(clickhouse_fdw PRIVATE ${PGSQL_LDFLAGS})
target_link_libraries(clickhouse_fdw PRIVATE clickhouse_odbc)
add_dependencies(clickhouse_fdw clickhouse_odbc)

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
		${PGSQL_INCLUDEDIR_SERVER}
)


#------------------------------------------------------------------------------
# sql
#------------------------------------------------------------------------------
set (install_files
	"${CMAKE_SOURCE_DIR}/src/clickhouse_fdw--1.0.sql"
	"${CMAKE_SOURCE_DIR}/src/clickhouse_fdw.control"
)

#------------------------------------------------------------------------------
# install
#------------------------------------------------------------------------------
install (
	TARGETS clickhouse_odbc clickhouse_fdw
	DESTINATION ${PGSQL_PKGLIBDIR}
)

install (
	FILES ${install_files}
	DESTINATION "${PGSQL_SHAREDIR}/extension"
)
