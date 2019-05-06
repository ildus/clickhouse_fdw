#include "postgres.h"
#include "miscadmin.h"
#include "foreign/foreign.h"
#include "clickhousedb_fdw.h"
#include "clickhouse_http.h"

static ch_connection http_connect(ForeignServer *server, UserMapping *user);
static void http_disconnect(ConnCacheEntry *entry);
static ch_cursor *http_simple_query(ch_connection conn, const char *query);
static void http_simple_insert(ch_connection conn, const char *query);
static void http_cursor_free(ch_cursor *);
static char **http_fetch_row(ch_cursor *cursor, size_t attcount);

static libclickhouse_methods http_methods = {
	.connect=http_connect,
	.disconnect=http_disconnect,
	.simple_query=http_simple_query,
	.simple_insert=http_simple_insert,
	.cursor_free=http_cursor_free,
	.fetch_row=http_fetch_row
};

libclickhouse_methods	*clickhouse_gate = &http_methods;

/*
 * For non-superusers, insist that the connstr specify a password.  This
 * prevents a password from being picked up from .pgpass, a service file,
 * the environment, etc.  We don't want the postgres user's passwords
 * to be accessible to non-superusers.  (See also dblink_connstr_check in
 * contrib/dblink.)
 */
static void
check_conn_params(const char *password, UserMapping *user)
{
	/* no check required if superuser */
	if (superuser_arg(user->userid))
	{
		return;
	}

	if (password == NULL)
		ereport(ERROR,
		        (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
		         errmsg("password is required"),
		         errdetail("Non-superusers must provide a password in the user mapping.")));
}

static ch_connection
http_connect(ForeignServer *server, UserMapping *user)
{
	char       *host = "127.0.0.1";
	int        port = 8123;
	char       *username = NULL;
	char       *password = NULL;
	char       *dbname = "default";
	char	   *connstring;
	char	   *driver = "http";

	ExtractConnectionOptions(server->options, &driver, &host, &port, &dbname,
							 &username, &password);
	ExtractConnectionOptions(user->options, &driver, &host, &port, &dbname,
							 &username, &password);

	check_conn_params(password, user);

	Assert(strcmp(driver, "http") == 0);
	connstring = psprintf("http://%s:%d", host, port);
	if (username && password)
	{
		char *newconnstring = psprintf("%s?user=%s&password=%s", connstring,
									   username, password);
		pfree(connstring);
		connstring = newconnstring;
	}
	ch_http_connection_t *conn = ch_http_connection(connstring);
	if (conn == NULL)
	{
		char *error = ch_http_last_error();
		if (error == NULL)
			error = "undefined";

		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("could not connect to server: %s", error)));
	}

	pfree(connstring);
	return (ch_connection) conn;
}

/*
 * Disconnect any open connection for a connection cache entry.
 */
static void
http_disconnect(ConnCacheEntry *entry)
{
	if (entry->conn != NULL)
		ch_http_close((ch_http_connection_t *) entry->conn);
}

static ch_cursor *
http_simple_query(ch_connection conn, const char *query)
{
	ch_cursor	*cursor;
	ch_http_response_t *resp = ch_http_simple_query(conn, query);
	if (resp == NULL)
	{
		char *error = ch_http_last_error();
		if (error == NULL)
			error = "undefined";

		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("clickhouse communication error: %s", error)));
	}

	if (resp->http_status != 200)
	{
		char *error = pnstrdup(resp->data, resp->datasize);
		ch_http_response_free(resp);

		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("clickhouse error: %s", error)));
	}

	cursor = palloc(sizeof(ch_cursor));
	cursor->query_response = resp;
	cursor->read_state = palloc0(sizeof(ch_http_read_state));
	ch_http_read_state_init(cursor->read_state, resp->data, resp->datasize);

	return cursor;
}

static void
http_simple_insert(ch_connection conn, const char *query)
{
	ch_cursor	*cursor;
	ch_http_response_t *resp = ch_http_simple_query(conn, query);
	if (resp == NULL)
	{
		char *error = ch_http_last_error();
		if (error == NULL)
			error = "undefined";

		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("clickhouse communication error: %s", error)));
	}

	if (resp->http_status != 200)
	{
		char *error = pnstrdup(resp->data, resp->datasize);
		ch_http_response_free(resp);

		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("clickhouse error: %s", error)));
	}

	ch_http_response_free(resp);
}

static void
http_cursor_free(ch_cursor *cursor)
{
	ch_http_read_state_free(cursor->read_state);
	pfree(cursor->read_state);
	ch_http_response_free(cursor->query_response);
	pfree(cursor);
}

static char **
http_fetch_row(ch_cursor *cursor, size_t attcount)
{
	int rc = CH_CONT;
	ch_http_response_t *resp = cursor->query_response;
	ch_http_read_state *state = cursor->read_state;

	/* all rows or empty table */
	if (state->done || state->data == NULL)
		return NULL;

	char **values = palloc(attcount * sizeof(char *));

	for (int i=0; i < attcount; i++)
	{
		if (rc != CH_CONT)
			elog(ERROR, "result columns less then specified: %d from %d", i, attcount);

		Assert(rc == CH_CONT);
		rc = ch_http_read_next(state);
		values[i] = state->val ? pstrdup(state->val): NULL;
	}

	if (rc != CH_EOL && rc != CH_EOF)
		elog(ERROR, "attcount doesn't match with the clickhouse result");

	return values;
}
