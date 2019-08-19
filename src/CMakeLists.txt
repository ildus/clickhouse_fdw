cmake_minimum_required(VERSION 3.5)
project(clickhouse_fdw VERSION 1.0.0 LANGUAGES C)

set (src
	clickhousedb_connection.c
	clickhousedb_deparse.c
	clickhousedb_fdw.c
	clickhousedb_option.c
	clickhousedb_shipable.c
	clickhousedb_shipable.c
	clickhousedb_adjust.c
	libclickhouse_link.c
	sql/init.sql
	sql/functions.sql
)

add_library(clickhouse_fdw MODULE ${src})

separate_arguments(PGSQL_LDFLAGS UNIX_COMMAND "${PGSQL_LDFLAGS}")
target_link_libraries(clickhouse_fdw PRIVATE clickhouse ${PGSQL_LDFLAGS})

separate_arguments(PGSQL_CPPFLAGS UNIX_COMMAND "${PGSQL_CPPFLAGS}")
target_compile_options(clickhouse_fdw PRIVATE ${PGSQL_CPPFLAGS} -fstack-protector -fstack-check)

add_dependencies(clickhouse_fdw clickhouse)

if (APPLE)
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
		${PGSQL_INCLUDEDIR_SERVER}
)

# concat sql files to one
function(cat IN_FILE OUT_FILE)
  file(READ ${IN_FILE} CONTENTS)
  file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction()

set(sql_files
	"${CMAKE_CURRENT_SOURCE_DIR}/sql/init.sql"
	"${CMAKE_CURRENT_SOURCE_DIR}/sql/functions.sql"
)

# Prepare a temporary file to "cat" to:
file(WRITE clickhouse_fdw.sql.in "")
foreach(sql_file ${sql_files})
  cat(${sql_file} clickhouse_fdw.sql.in)
endforeach()

# copy temporary to destination
execute_process(
	COMMAND bash "-c" "cat ${CMAKE_SOURCE_DIR}/src/clickhouse_fdw.control | grep default_version | awk '{gsub(\"\\047\", \"\", $3); print $3}'"
	OUTPUT_VARIABLE EXT_VERSION
	OUTPUT_STRIP_TRAILING_WHITESPACE
)
configure_file(clickhouse_fdw.sql.in clickhouse_fdw--${EXT_VERSION}.sql COPYONLY)

#------------------------------------------------------------------------------
# sql
#------------------------------------------------------------------------------
set (install_files
	"${CMAKE_BINARY_DIR}/src/clickhouse_fdw--${EXT_VERSION}.sql"
	"${CMAKE_SOURCE_DIR}/src/clickhouse_fdw.control"
)

#------------------------------------------------------------------------------
# install
#------------------------------------------------------------------------------
install (
	TARGETS clickhouse_fdw clickhouse
	DESTINATION ${PGSQL_PKGLIBDIR}
)

install (
	FILES ${install_files}
	DESTINATION "${PGSQL_SHAREDIR}/extension"
)
