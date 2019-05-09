#include "postgres.h"
#include "utils/builtins.h"
#include "miscadmin.h"
#include "foreign/foreign.h"
#include "clickhousedb_fdw.h"
#include "clickhouse_http.h"

static bool initialized = false;

static ch_connection http_connect(char *connstring);
static void http_disconnect(ch_connection conn);
static ch_cursor *http_simple_query(ch_connection conn, const char *query);
static void http_simple_insert(ch_connection conn, const char *query);
static void http_cursor_free(ch_cursor *);
static char **http_fetch_row(ch_cursor *cursor, size_t attcount);
static text *http_fetch_raw_data(ch_cursor *cursor);

static libclickhouse_methods http_methods = {
	.connect=http_connect,
	.disconnect=http_disconnect,
	.simple_query=http_simple_query,
	.simple_insert=http_simple_insert,
	.cursor_free=http_cursor_free,
	.fetch_row=http_fetch_row,
	.fetch_raw_data=http_fetch_raw_data
};

libclickhouse_methods	*clickhouse_gate = &http_methods;

static int http_progress_callback(void *clientp, double dltotal, double dlnow,
		double ultotal, double ulnow)
{
	if (ProcDiePending || QueryCancelPending)
		return 1;

	return 0;
}

static ch_connection
http_connect(char *connstring)
{
	ch_http_connection_t *conn = ch_http_connection(connstring);
	if (!initialized)
	{
		initialized = true;
		ch_http_init(1, (uint32_t) MyProcPid);
	}

	if (conn == NULL)
	{
		char *error = ch_http_last_error();
		if (error == NULL)
			error = "undefined";

		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("could not connect to server: %s", error)));
	}

	return (ch_connection) conn;
}

/*
 * Disconnect any open connection for a connection cache entry.
 */
static void
http_disconnect(ch_connection conn)
{
	if (conn != NULL)
		ch_http_close((ch_http_connection_t *) conn);
}

/*
 * Return text before version mentioning
 */
static char *
format_error(char *errstring)
{
	int n = strlen(errstring);

	for (int i = 0; i < n; i++)
	{
		if (strncmp(errstring + i, "version", 7) == 0)
			return pnstrdup(errstring, i - 2);
	}

	return errstring;
}

static ch_cursor *
http_simple_query(ch_connection conn, const char *query)
{
	ch_cursor	*cursor;

	ch_http_set_progress_func(http_progress_callback);
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

	if (resp->http_status == 418)
	{
		/* kill aborted query if needed */
		char *query = psprintf("kill query where query_id='%s'", resp->query_id);

		ch_http_response_free(resp);
		ch_http_set_progress_func(NULL);
		resp = ch_http_simple_query(conn, query);
		if (resp != NULL)
			ch_http_response_free(resp);
		pfree(query);

		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("clickhouse query was aborted")));
	}
	else if (resp->http_status != 200)
	{
		char *error = pnstrdup(resp->data, resp->datasize);
		ch_http_response_free(resp);

		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("CH:%s\nQUERY:%s", format_error(error), query)));
	}

	cursor = palloc(sizeof(ch_cursor));
	cursor->query_response = resp;
	cursor->read_state = palloc0(sizeof(ch_http_read_state));
	cursor->query = pstrdup(query);
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
		         errmsg("CH:%s\nQUERY:%s", format_error(error), query)));
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
	ch_http_read_state *state = cursor->read_state;

	/* all rows or empty table */
	if (state->done || state->data == NULL)
		return NULL;

	char **values = palloc(attcount * sizeof(char *));

	for (int i=0; i < attcount; i++)
	{
		rc = ch_http_read_next(state);
		if (state->val[0] != '\0')
			values[i] = pstrdup(state->val);
		else
			values[i] = NULL;
	}

	if (rc != CH_EOL && rc != CH_EOF)
	{
		char *resval = pnstrdup(state->data, state->maxpos + 1);

		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("Columns mistmatch between PostgreSQL and ClickHouse"
					    "\nQUERY: %s\nRESULT: %s", cursor->query, resval)));
	}

	return values;
}

static text *
http_fetch_raw_data(ch_cursor *cursor)
{
	ch_http_read_state *state = cursor->read_state;
	if (state->data == NULL)
		return NULL;

	return cstring_to_text_with_len(state->data, state->maxpos + 1);
}
