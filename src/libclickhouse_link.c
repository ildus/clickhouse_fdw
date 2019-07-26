#include <limits.h>
#include "postgres.h"
#include "catalog/pg_type_d.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"

#include "clickhousedb_fdw.h"
#include "clickhouse_http.h"
#include "clickhouse_binary.hh"

static uint32	global_query_id = 0;
static bool		initialized = false;

static void http_disconnect(void *conn);
static ch_cursor *http_simple_query(void *conn, const char *query);
static void http_simple_insert(void *conn, const char *query);
static void http_cursor_free(ch_cursor *);
static char **http_fetch_row(ch_cursor *cursor, List *attrs, TupleDesc tupdesc,
	Datum *values, bool *nulls);

static libclickhouse_methods http_methods = {
	.disconnect=http_disconnect,
	.simple_query=http_simple_query,
	.simple_insert=http_simple_insert,
	.cursor_free=http_cursor_free,
	.fetch_row=http_fetch_row
};

static void binary_disconnect(void *conn);
static ch_cursor *binary_simple_query(void *conn, const char *query);
static void binary_cursor_free(ch_cursor *cursor);
static void binary_simple_insert(void *conn, const char *query);
static char **binary_fetch_row(ch_cursor *cursor, List* attrs, TupleDesc tupdesc,
		Datum *values, bool *nulls);

static libclickhouse_methods binary_methods = {
	.disconnect=binary_disconnect,
	.simple_query=binary_simple_query,
	.simple_insert=binary_simple_insert,
	.cursor_free=binary_cursor_free,
	.fetch_row=binary_fetch_row
};

static int http_progress_callback(void *clientp, double dltotal, double dlnow,
		double ultotal, double ulnow)
{
	if (ProcDiePending || QueryCancelPending)
		return 1;

	return 0;
}

ch_connection
http_connect(char *connstring)
{
	ch_connection res;
	ch_http_connection_t *conn = ch_http_connection(connstring);
	if (!initialized)
	{
		initialized = true;
		ch_http_init(0, (uint32_t) MyProcPid);
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

	res.conn = conn;
	res.methods = &http_methods;
	res.is_binary = false;
	return res;
}

/*
 * Disconnect any open connection for a connection cache entry.
 */
static void
http_disconnect(void *conn)
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

static void
kill_query(void *conn, const char *query_id)
{
	ch_http_response_t *resp;
	char *query = psprintf("kill query where query_id='%s'", query_id);

	ch_http_set_progress_func(NULL);
	global_query_id++;
	resp = ch_http_simple_query(conn, query, global_query_id);
	if (resp != NULL)
		ch_http_response_free(resp);
	pfree(query);
}

static ch_cursor *
http_simple_query(void *conn, const char *query)
{
	ch_cursor	*cursor;

	ch_http_set_progress_func(http_progress_callback);

	global_query_id++;
	ch_http_response_t *resp = ch_http_simple_query(conn, query, global_query_id);
	if (resp == NULL)
	{
		char *error = ch_http_last_error();
		if (error == NULL)
			error = "undefined";

		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("clickhouse_fdw: communication error: %s", error)));
	}

	if (resp->http_status == 418)
	{
		kill_query(conn, resp->query_id);
		ch_http_response_free(resp);

		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("clickhouse_fdw: query was aborted")));
	}
	else if (resp->http_status != 200)
	{
		char *error = pnstrdup(resp->data, resp->datasize);
		ch_http_response_free(resp);

		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("clickhouse_fdw:%s\nQUERY:%s", format_error(error), query)));
	}

	cursor = palloc(sizeof(ch_cursor));
	cursor->query_response = resp;
	cursor->read_state = palloc0(sizeof(ch_http_read_state));
	cursor->query = pstrdup(query);
	ch_http_read_state_init(cursor->read_state, resp->data, resp->datasize);

	return cursor;
}

static void
http_simple_insert(void *conn, const char *query)
{
	ch_cursor	*cursor;

	global_query_id++;
	ch_http_response_t *resp = ch_http_simple_query(conn, query, global_query_id);
	if (resp == NULL)
	{
		char *error = ch_http_last_error();
		if (error == NULL)
			error = "undefined";

		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("clickhouse_fdw: communication error: %s", error)));
	}

	if (resp->http_status != 200)
	{
		char *error = pnstrdup(resp->data, resp->datasize);
		ch_http_response_free(resp);

		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("clickhouse_fdw:%s\nQUERY:%s", format_error(error), query)));
	}

	ch_http_response_free(resp);
}

static void
http_cursor_free(ch_cursor *cursor)
{
	ch_http_read_state_free(cursor->read_state);
	pfree(cursor->read_state);
	if (cursor->query)
		pfree(cursor->query);
	ch_http_response_free(cursor->query_response);
	pfree(cursor);
}

static char **
http_fetch_row(ch_cursor *cursor, List *attrs, TupleDesc tupdesc, Datum *v, bool *n)
{
	int		rc = CH_CONT;
	size_t	attcount = list_length(attrs);

	if (attcount == 0)
		/* SELECT NULL */
		attcount = 1;

	ch_http_read_state *state = cursor->read_state;

	/* all rows or empty table */
	if (state->done || state->data == NULL)
		return NULL;

	char **values = palloc(attcount * sizeof(char *));

	for (int i=0; i < attcount; i++)
	{
		rc = ch_http_read_next(state);
		if (state->val[0] == '\\' && state->val[1] == 'N')
			values[i] = NULL;
		else if (state->val[0] != '\0')
			values[i] = pstrdup(state->val);
		else
			values[i] = NULL;
	}

	if (attcount > 0 && rc != CH_EOL && rc != CH_EOF)
	{
		char *resval = pnstrdup(state->data, state->maxpos + 1);

		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("clickhouse_fdw; columns mismatch in result"
					    "\nQUERY: %s\nRESULT: %s", cursor->query, resval)));
	}

	return values;
}

text *
http_fetch_raw_data(ch_cursor *cursor)
{
	ch_http_read_state *state = cursor->read_state;
	if (state->data == NULL)
		return NULL;

	return cstring_to_text_with_len(state->data, state->maxpos + 1);
}

/*** BINARY PROTOCOL ***/

ch_connection
binary_connect(ch_connection_details *details)
{
	char *ch_error = NULL;
	ch_connection res;
	ch_binary_connection_t *conn = ch_binary_connect(details->host, details->port,
			details->dbname, details->username, details->password, &ch_error);

	if (conn == NULL)
	{
		Assert(ch_error);
		char *error = pstrdup(ch_error);
		free(ch_error);

		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("clickhouse_fdw: connection error: %s", error)));
	}

	res.conn = conn;
	res.methods = &binary_methods;
	res.is_binary = true;
	return res;
}

static void
binary_disconnect(void *conn)
{
	if (conn != NULL)
		ch_binary_close((ch_binary_connection_t *) conn);
}

static ch_cursor *
binary_simple_query(void *conn, const char *query)
{
	ch_cursor	*cursor;
	ch_binary_read_state_t *state;

	ch_binary_response_t *resp = ch_binary_simple_query(conn, query,
		&QueryCancelPending);

	if (!resp->success)
	{
		char *error = pstrdup(resp->error);
		ch_binary_response_free(resp);

		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("clickhouse_fdw: %s", error)));
	}

	cursor = palloc(sizeof(ch_cursor));
	cursor->query_response = resp;
	state = (ch_binary_read_state_t *) palloc0(sizeof(ch_binary_read_state_t));
	cursor->query = pstrdup(query);
	cursor->read_state = state;
	ch_binary_read_state_init(cursor->read_state, resp);

	if (state->error)
	{
		char *error = pstrdup(state->error);
		ch_binary_read_state_free(state);
		ch_binary_response_free(resp);

		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("clickhouse_fdw: could not initialize read state: %s",
					 error)));
	}

	return cursor;
}

static Oid types_map[21] = {
	InvalidOid, /* chb_Void */
	INT2OID,
	INT2OID,
	INT4OID,
	INT8OID,
	INT2OID,	/* chb_UInt8 */
	INT4OID,
	INT8OID,
	INT8OID,	/* overflow risk */
	FLOAT4OID,
	FLOAT8OID,
	TEXTOID,
	TEXTOID,
	TIMESTAMPOID,
	TIMESTAMPOID,
	InvalidOid,	/* chb_Array, depends on array type */
	InvalidOid,	/* chb_Nullable, just skip it */
	InvalidOid,	/* composite type, TYPTYPE_COMPOSITE */
	chb_Enum8,
	chb_Enum16,
	chb_UUID
};

static Datum
make_datum(void *rowval, ch_binary_coltype coltype, Oid *restype)
{
	Assert(rowval != NULL);

	if (restype)
		*restype = types_map[coltype];

	switch (coltype)
	{
		case chb_Int8:
			return Int16GetDatum((int16)(*(int8 *) rowval));
		case chb_UInt8:
			return Int16GetDatum((int16)(*(uint8 *) rowval));
		case chb_Int16:
			return Int16GetDatum(*(int16 *) rowval);
		case chb_UInt16:
			return Int32GetDatum((int32)(*(uint16 *) rowval));
		case chb_Int32:
			return Int32GetDatum(*(int32 *) rowval);
		case chb_UInt32:
			return Int64GetDatum((int64)(*(uint32 *) rowval));
		case chb_Int64:
			return Int64GetDatum(*(int64 *) rowval);
		case chb_UInt64:
			{
				uint64	val = *(uint64 *) rowval;
				if (val > LONG_MAX)
					elog(ERROR, "clickhouse_fdw: int64 overflow");

				return Int64GetDatum((int64) val);
			}
		case chb_Float32:
			return Float4GetDatum(*(float *) rowval);
		case chb_Float64:
			return Float8GetDatum(*(double *) rowval);
		case chb_FixedString:
		case chb_String:
			return CStringGetTextDatum((const char *) rowval);
		case chb_DateTime:
		case chb_Date:
			{
				Timestamp t = (Timestamp) time_t_to_timestamptz((pg_time_t)(*(time_t *) rowval));
				return TimestampGetDatum(t);
			}
		case chb_Array:
			{
				size_t		i;
				Datum	   *out_datums;
				ch_binary_array_t *arr = rowval;
				Oid			elmtype = types_map[arr->coltype],
							arraytype;
				int			lb = 1;
				ArrayType  *aout;
				int16		typlen;
				bool		typbyval;
				char		typalign;

				if (elmtype == InvalidOid)
					/* TODO: support more complex arrays. But first check that
					 * ClickHouse supports them (thigs like multidimensional
					 * arrays and such */
					elog(ERROR, "clickhouse_fdw: array too complex for conversion");

				arraytype = get_array_type(elmtype);
				if (arraytype == InvalidOid)
					elog(ERROR, "clickhouse_fdw: could not find array type for %d", elmtype);

				if (restype)
					*restype = arraytype;

				if (arr->len == 0)
					return PointerGetDatum(construct_empty_array(elmtype));

				out_datums = palloc(sizeof(Datum) * arr->len);

				for (i = 0; i < arr->len; ++i)
					out_datums[i] = make_datum(arr->values[i], arr->coltype, NULL);

				get_typlenbyvalalign(elmtype, &typlen, &typbyval, &typalign);
				aout = construct_array(out_datums, arr->len, elmtype,
					typlen, typbyval, typalign);
				return PointerGetDatum(aout);
			}
	}

	elog(ERROR, "clickhouse_fdw: %d type from ClickHouse is not supported", coltype);
}

static char **
binary_fetch_row(ch_cursor *cursor, List *attrs, TupleDesc tupdesc,
	Datum *values, bool *nulls)
{
	ListCell   *lc;
	size_t		j;
	ch_binary_read_state_t *state = cursor->read_state;
	void				  **row_values = ch_binary_read_row(state);
	size_t					attcount = list_length(attrs);

	if (state->error)
	{
		char *error = pstrdup(state->error);
		ch_binary_response_free(state->resp);
		ch_binary_read_state_free(state);

		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("clickhouse_fdw: error while reading row: %s",
					 error)));
	}

	if (row_values == NULL)
		return NULL;

	if (attcount == 0)
	{
		if (state->resp->columns_count == 1 && (state->coltypes[0] == chb_Void))
		{
			/* SELECT NULL, nulls array already contains nulls */
			goto ok;
		}
		else
		{
			ch_binary_response_free(state->resp);
			ch_binary_read_state_free(state);
			elog(ERROR, "clickhouse_fdw: unexpected state: atttributes "
					"count == 0 and haven't got NULL in the response");
		}
	}
	else if (attcount != state->resp->columns_count)
	{
		ch_binary_response_free(state->resp);
		ch_binary_read_state_free(state);

		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("clickhouse_fdw: columns mismatch in result"
					    "\nquery: %s", cursor->query)));
	}

	j = 0;
	foreach(lc, attrs)
	{
		int		i = lfirst_int(lc);
		void   *rowval = row_values[j];

		if (state->coltypes[j] == chb_Void || rowval == NULL)
			nulls[i - 1] = true;
		else
		{
			Oid restype;
			Oid pgtype = TupleDescAttr(tupdesc, i - 1)->atttypid;
			values[i - 1] = make_datum(rowval, state->coltypes[j], &restype);
			if (restype != pgtype)
			{
				/* try to convert */
				Oid			castfunc;
				CoercionPathType ctype;

				ctype = find_coercion_pathway(pgtype, restype,
											  COERCION_EXPLICIT,
											  &castfunc);
				switch (ctype)
				{
					case COERCION_PATH_FUNC:
						values[i - 1] = OidFunctionCall1(castfunc, values[i - 1]);
						break;
					case COERCION_PATH_RELABELTYPE:
						/* all good */
						break;
					default:
						elog(ERROR, "clickhouse_fdw: could not cast value from %d to %d",
								restype, pgtype);
				}
			}
			nulls[i - 1] = false;
		}
		j++;
	}

ok:
	return (char **) values;
}

static void
binary_cursor_free(ch_cursor *cursor)
{
	ch_binary_read_state_free(cursor->read_state);
	pfree(cursor->read_state);
	if (cursor->query)
		pfree(cursor->query);
	ch_binary_response_free(cursor->query_response);
	pfree(cursor);
}

static void
binary_simple_insert(void *conn, const char *query)
{
	elog(ERROR, "clickhouse_fdw: insertion is not implemented for binary protocol yet");
}
