/*-------------------------------------------------------------------------
 *
 * clickhouse.hpp
 *
 * IDENTIFICATION
 * 		clickhouse.hpp
 *
 *-------------------------------------------------------------------------
 */

#ifndef __CLICKHOUSE__
#define __CLICKHOUSE__

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <stdbool.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <sqltypes.h>
#include <sql.h>
#include <sqlext.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct Conn
{
	SQLHANDLE henv;
	SQLHANDLE conn;
	SQLHANDLE stmt;
	char *query;
	char      error[512];
} Conn;

int odbc_init();
int odbc_destroy();
Conn* odbc_connect(char *driver, char *host, int port, char *dbname, char *user, char *pass, char *error);
int odbc_disconnect(Conn *conn);
int odbc_execute(Conn *conn);
int odbc_prepare(Conn *conn, char *query);
int odbc_bind_string(Conn *conn, int i, char *v, bool is_null);
int odbc_bind_int(Conn *conn, int i, int v, bool is_null);
int odbc_fetch(Conn *conn);
int odbc_column_count(Conn* conn);
int odbc_get_int(Conn *conn, int i, int *erno);
const char* odbc_get_string(Conn *conn, int i, char* v, int len, bool *is_null);
int odbc_get_date(Conn *conn, int i, char*v);
#ifdef __cplusplus
}
#endif

#endif // __CLICKHOUSE__
