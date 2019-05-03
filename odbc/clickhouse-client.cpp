/*-------------------------------------------------------------------------
 *
 * clickhouseclient.cpp
 *
 * IDENTIFICATION
 * 		clickhouseclient.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <assert.h>
#include <iostream>
#include <queue>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <string.h>
#include <unistd.h>

#include "clickhouse-client.h"

/*
 * Initialize the ODBC driver.
 * */
int odbc_init() { return 0; }

/*
 * Destroy / delete any ODBC's remainings.
 * */
int odbc_destroy() { return 0; }

/*
 * Get the ODBC's error string using the handle. Pass the proper valid handle
 * to get the error string.
 * */
int odbc_error(unsigned int type, const SQLHANDLE &handle, char *error) {
  SQLCHAR sqlstate[1024];
  SQLCHAR message[1024];
  SQLRETURN rc;
  error[0] = '\0';
  rc = SQLGetDiagRec(type, handle, 1, sqlstate, NULL, message, 1024, NULL);
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
    sprintf(error, "%s:%s", sqlstate, message);
    return 0;
  }
  return 0;
}

/*
 * Connect to server, check for error string for detail error.
 * */
Conn *odbc_connect(char *driver, char *host, int port, char *dbname, char *user,
                   char *pass, char *error) {
  Conn *conn = new Conn();
  char url[512] = {0};
  SQLCHAR retconstring[1024] = {0};
  SQLSMALLINT retlen = 1024;
  SQLRETURN rc;

  error[0] = '\0';
  if (SQL_SUCCESS !=
      SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &conn->henv)) {
    delete conn;
    return NULL;
  }

  if (SQL_SUCCESS != SQLSetEnvAttr(conn->henv, SQL_ATTR_ODBC_VERSION,
                                   (SQLPOINTER)SQL_OV_ODBC3, 0)) {
    delete conn;
    return NULL;
  }

  if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_DBC, conn->henv, &conn->conn)) {
    delete conn;
    return NULL;
  }
  SQLSetConnectAttr(conn->conn, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);

  sprintf(url,
          "Driver={%s};url=http://%s:%s@%s:%d/"
          "query?database=%s&max_result_bytes=14000000&buffer_size=3000000;",
          driver, user, pass, host, port, dbname);

  rc = SQLDriverConnect(conn->conn, NULL, (SQLCHAR *)url, SQL_NTS, retconstring,
                        retlen, &retlen, SQL_DRIVER_NOPROMPT);
  switch (rc) {
  case SQL_SUCCESS_WITH_INFO:
    break;
  case SQL_INVALID_HANDLE:
  case SQL_ERROR:

    odbc_error(SQL_HANDLE_DBC, conn->conn, error);
    delete conn;
    return NULL;
  default:
    break;
  }
  return conn;
}

/*
 * Disconnect from the server and clear all the handles.
 * */
int odbc_disconnect(Conn *conn) {
  SQLFreeHandle(SQL_HANDLE_STMT, conn->stmt);
  SQLDisconnect(conn->conn);
  SQLFreeHandle(SQL_HANDLE_ENV, conn->henv);
  delete conn;
  return 0;
}

/*
 * Prepare the query for the execution.
 * */
int odbc_prepare(Conn *conn, char *query) {
  SQLRETURN rc;
  rc = SQLAllocHandle(SQL_HANDLE_STMT, conn->conn, &conn->stmt);
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
    rc = SQLPrepare(conn->stmt, (SQLCHAR *)query, SQL_NTS);
    if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
      return 0;
  }
  odbc_error(SQL_HANDLE_STMT, conn->stmt, conn->error);
  return -1;
}

/*
 * Execute the prepared query.
 * */
int odbc_execute(Conn *conn) {
  SQLRETURN rc;
  rc = SQLExecute(conn->stmt);
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
    return 0;
  odbc_error(SQL_HANDLE_STMT, conn->stmt, conn->error);
  return -1;
}

/*
 * Fetch the result's row from the statment handle. The odbc_execute must
 * be call before that.
 * */
int odbc_fetch(Conn *conn) {
  SQLRETURN rc;
  rc = SQLFetch(conn->stmt);
  switch (rc) {
  case SQL_SUCCESS:
  case SQL_SUCCESS_WITH_INFO:
    return 1; /* Success */
  case SQL_NO_DATA:
    return 0; /* No More Rows */
  case SQL_STILL_EXECUTING:
    return 2; /* Wait */
  case SQL_ERROR:
  case SQL_INVALID_HANDLE: /* Error */
    odbc_error(SQL_HANDLE_STMT, conn->stmt, conn->error);
    break;
  }
  return 1;
}

/*
 * Get string value from the statment.
 * */
const char *odbc_get_string(Conn *conn, int i, char *v, int len,
                            bool *is_null) {
  SQLLEN l = 0;
  SQLRETURN rc = SQLGetData(conn->stmt, i, SQL_C_CHAR, v, len, &l);
  if (l == SQL_NULL_DATA) {
    /* NULL value */
    *is_null = true;
    return NULL;
  }
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
    return v;
  odbc_error(SQL_HANDLE_STMT, conn->stmt, conn->error);
  return NULL;
}

/*
 * Get the integer value from the statment.
 * */
int odbc_get_int(Conn *conn, int i, int *erno) {
  int v;
  *erno = 0;
  SQLRETURN rc = SQLGetData(conn->stmt, i, SQL_C_ULONG, &v, 0, NULL);
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
    return v;
  odbc_error(SQL_HANDLE_STMT, conn->stmt, conn->error);
  *erno = -1;
  return -1;
}

/*
 * Get the datatime value from the statment.
 * */
int odbc_get_date(Conn *conn, int i, char *v) {
  SQLRETURN rc = SQLGetData(conn->stmt, i, SQL_C_TYPE_DATE, v, 64, NULL);
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
    return 64;
  odbc_error(SQL_HANDLE_STMT, conn->stmt, conn->error);
  return -1;
}

/*
 * Bind the string value.
 * */
int odbc_bind_string(Conn *conn, int i, char *v, bool is_null) {
  int l = strlen(v);
  SQLRETURN rc = SQLBindParameter(conn->stmt, i, SQL_PARAM_INPUT, SQL_CHAR,
                                  SQL_LONGVARCHAR, l, 0, v, 0, NULL);
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
    return 0;
  odbc_error(SQL_HANDLE_STMT, conn->stmt, conn->error);
  (void)is_null;
  return -1;
}

/*
 * Bind the integer value.
 * */
int odbc_bind_int(Conn *conn, int i, int v, bool is_null) {
  SQLRETURN rc = SQLBindParameter(conn->stmt, i, SQL_PARAM_INPUT, SQL_C_ULONG,
                                  SQL_INTEGER, 5, 0, &v, 0, NULL);
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
    return 0;
  odbc_error(SQL_HANDLE_STMT, conn->stmt, conn->error);
  (void)is_null;
  return -1;
}

/*
 * Get the column count from the currently parapred query from the satatment
 * handle.
 * */
int odbc_column_count(Conn *conn) {
  int c;
  SQLRETURN rc = SQLNumResultCols(conn->stmt, (SQLSMALLINT *)&c);
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
    return c;
  odbc_error(SQL_HANDLE_STMT, conn->stmt, conn->error);
  return -1;
}

/*
 * Get the count of rows from the server.
 * Not Implemented.
 * */
int odbc_get_row_count(Conn *conn) {
  (void)conn;
  return 0;
}
