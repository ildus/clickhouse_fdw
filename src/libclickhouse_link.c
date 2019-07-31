#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/uuid.h"
#include "access/htup_details.h"

#include "clickhousedb_fdw.h"
#include "clickhouse_http.h"
#include "clickhouse_binary.hh"

static uint32	global_query_id = 0;
static bool		initialized = false;

static void http_disconnect(void *conn);
static ch_cursor *http_simple_query(void *conn, const char *query);
static void http_simple_insert(void *conn, const char *query);
static void http_cursor_free(void *);
static void **http_fetch_row(ch_cursor *cursor, List *attrs, TupleDesc tupdesc,
	Datum *values, bool *nulls);

static libclickhouse_methods http_methods = {
	.disconnect=http_disconnect,
	.simple_query=http_simple_query,
	.simple_insert=http_simple_insert,
	.fetch_row=http_fetch_row
};

static void binary_disconnect(void *conn);
static ch_cursor *binary_simple_query(void *conn, const char *query);
static void binary_cursor_free(void *cursor);
static void binary_simple_insert(void *conn, const char *query);
static void **binary_fetch_row(ch_cursor *cursor, List* attrs, TupleDesc tupdesc,
		Datum *values, bool *nulls);

static libclickhouse_methods binary_methods = {
	.disconnect=binary_disconnect,
	.simple_query=binary_simple_query,
	.simple_insert=binary_simple_insert,
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
	int			attempts = 0;
	MemoryContext	tempcxt,
					oldcxt;
	ch_cursor	*cursor;

	ch_http_set_progress_func(http_progress_callback);

again:
	global_query_id++;
	ch_http_response_t *resp = ch_http_simple_query(conn, query, global_query_id);

	if (resp == NULL)
		elog(ERROR, "out of memory");

	attempts++;
	if (resp->http_status == 419)
	{
		char *error = pnstrdup(resp->data, resp->datasize);
		ch_http_response_free(resp);

		if (attempts < 3)
			goto again;

		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("clickhouse_fdw: communication error: %s", error)));
	}
	else if (resp->http_status == 418)
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

	/* we could not control properly deallocation of libclickhouse memory, so
	 * we use memory context callbacks for that */
	tempcxt = AllocSetContextCreate(PortalContext, "clickhouse_fdw cursor",
										ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(tempcxt);

	cursor = palloc0(sizeof(ch_cursor));
	cursor->query_response = resp;
	cursor->read_state = palloc0(sizeof(ch_http_read_state));
	cursor->query = pstrdup(query);
	cursor->request_time = resp->pretransfer_time * 1000;
	cursor->total_time = resp->total_time * 1000;
	ch_http_read_state_init(cursor->read_state, resp->data, resp->datasize);

	cursor->memcxt = tempcxt;
	cursor->callback.func = http_cursor_free;
	cursor->callback.arg = cursor;
	MemoryContextRegisterResetCallback(tempcxt, &cursor->callback);
	MemoryContextSwitchTo(oldcxt);

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
http_cursor_free(void *c)
{
	ch_cursor *cursor = c;

	ch_http_read_state_free(cursor->read_state);
	ch_http_response_free(cursor->query_response);
}

static void **
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
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg_internal("clickhouse_fdw: columns mismatch"),
				 errdetail("Number of returned columns does not match "
						   "expected column count (%lu).", attcount)));
	}

	return (void **) values;
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
	MemoryContext	tempcxt,
					oldcxt;
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

	tempcxt = AllocSetContextCreate(PortalContext, "clickhouse_fdw cursor",
										ALLOCSET_DEFAULT_SIZES);

	oldcxt = MemoryContextSwitchTo(tempcxt);
	cursor = palloc0(sizeof(ch_cursor));
	cursor->query_response = resp;
	state = (ch_binary_read_state_t *) palloc0(sizeof(ch_binary_read_state_t));
	cursor->query = pstrdup(query);
	cursor->read_state = state;
	ch_binary_read_state_init(cursor->read_state, resp);

	cursor->memcxt = tempcxt;
	cursor->callback.func = binary_cursor_free;
	cursor->callback.arg = cursor;
	MemoryContextRegisterResetCallback(tempcxt, &cursor->callback);
	MemoryContextSwitchTo(oldcxt);

	if (state->error)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("clickhouse_fdw: could not initialize read state: %s",
					 state->error)));
	}

	return cursor;
}

static Oid types_map[27] = {
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
	InvalidOid,	/* composite type */
	INT2OID,	/* enum8 */
	INT2OID,	/* enum16 */
	UUIDOID,
	InvalidOid,
	InvalidOid,
	InvalidOid,
	InvalidOid,
	InvalidOid,
	InvalidOid	/* just in case we update library and forget to add new types */
};

static Datum
make_datum(void *rowval, ch_binary_coltype coltype, Oid pgtype)
{
	Datum	ret;
	Oid		valtype = types_map[coltype];

	Assert(rowval != NULL);
	switch (coltype)
	{
		case chb_Int8:
			ret = Int16GetDatum((int16)(*(int8 *) rowval));
			break;
		case chb_UInt8:
			ret = Int16GetDatum((int16)(*(uint8 *) rowval));
			break;
		case chb_Int16:
			ret = Int16GetDatum(*(int16 *) rowval);
			break;
		case chb_UInt16:
			ret = Int32GetDatum((int32)(*(uint16 *) rowval));
			break;
		case chb_Int32:
			ret = Int32GetDatum(*(int32 *) rowval);
			break;
		case chb_UInt32:
			ret = Int64GetDatum((int64)(*(uint32 *) rowval));
			break;
		case chb_Int64:
			ret = Int64GetDatum(*(int64 *) rowval);
			break;
		case chb_UInt64:
			{
				uint64	val = *(uint64 *) rowval;
				if (val > LONG_MAX)
					elog(ERROR, "clickhouse_fdw: int64 overflow");

				ret = Int64GetDatum((int64) val);
			}
			break;
		case chb_Float32:
			ret = Float4GetDatum(*(float *) rowval);
			break;
		case chb_Float64:
			ret = Float8GetDatum(*(double *) rowval);
			break;
		case chb_FixedString:
		case chb_String:
			ret = CStringGetTextDatum((const char *) rowval);
			break;
		case chb_DateTime:
		case chb_Date:
			{
				Timestamp t = (Timestamp) time_t_to_timestamptz((pg_time_t)(*(time_t *) rowval));
				ret = TimestampGetDatum(t);
			}
			break;
		case chb_Array:
			{
				size_t		i;
				Datum	   *out_datums;
				ch_binary_array_t *arr = rowval;
				Oid			elmtype = types_map[arr->coltype];
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

				valtype = get_array_type(elmtype);
				if (valtype == InvalidOid)
					elog(ERROR, "clickhouse_fdw: could not find array type for %d", elmtype);

				if (arr->len == 0)
					ret = PointerGetDatum(construct_empty_array(elmtype));
				else
				{
					out_datums = palloc(sizeof(Datum) * arr->len);

					for (i = 0; i < arr->len; ++i)
						out_datums[i] = make_datum(arr->values[i], arr->coltype, InvalidOid);

					get_typlenbyvalalign(elmtype, &typlen, &typbyval, &typalign);
					aout = construct_array(out_datums, arr->len, elmtype,
						typlen, typbyval, typalign);
					ret = PointerGetDatum(aout);
				}
			}
			break;
		case chb_UUID:
			{
				pg_uuid_t	*val = (pg_uuid_t *) rowval;
				StaticAssertStmt(val + offsetof(pg_uuid_t, data) == val,
					"pg_uuid_t should have only array");
				ret = UUIDPGetDatum(val);
			}
			break;
		case chb_Enum8:
			{
				int8 val = *(int8 *) rowval;
				ret = Int16GetDatum((int16) val);
			}
			break;
		case chb_Enum16:
			{
				int16 val = *(int16 *) rowval;
				ret = Int16GetDatum(val);
			}
			break;
		case chb_Tuple:
			{
				Datum		result;
				HeapTuple	htup;
				Datum	   *tuple_values;
				bool	   *tuple_nulls;
				TupleDesc	desc;
				size_t		i;
				CustomObjectDef	*cdef = checkForCustomType(pgtype);

				ch_binary_tuple_t *tuple = rowval;

				if (tuple->len == 0)
					elog(ERROR, "clickhouse_fdw: returned tuple is empty");

				desc = CreateTemplateTupleDesc(tuple->len, false);
				tuple_values = palloc(sizeof(Datum) * desc->natts);

				/* TODO: support NULLs in tuple */
				tuple_nulls = palloc0(sizeof(bool) * desc->natts);

				for (i = 0; i < desc->natts; ++i)
				{
					ch_binary_coltype	coltype = tuple->coltypes[i];
					Oid elmtype = types_map[coltype];

					if (coltype == chb_Array)
					{
						ch_binary_array_t *arr = tuple->values[i];
						elmtype = types_map[arr->coltype];
						elmtype = get_array_type(elmtype);
					}

					if (elmtype == InvalidOid)
						elog(ERROR, "clickhouse_fdw: tuple too complex for conversion");

					TupleDescInitEntry(desc, (AttrNumber) i + 1, "",
									   elmtype, -1, 0);

					tuple_values[i] = make_datum(tuple->values[i], coltype, InvalidOid);
				}

				desc = BlessTupleDesc(desc);

				htup = heap_form_tuple(desc, tuple_values, tuple_nulls);
				pfree(tuple_values);
				pfree(tuple_nulls);

				if (cdef && cdef->rowfunc != InvalidOid)
				{
					ret = heap_copy_tuple_as_datum(htup, desc);
					heap_freetuple(htup);

					return OidFunctionCall1(cdef->rowfunc, ret);
				}
				else if (pgtype != RECORDOID)
				{
					bool			pinned = false;
					TupleDesc		pgdesc;
					TypeCacheEntry *typentry;
					TupleConversionMap *tupmap;
					HeapTuple		temptup;

					typentry = lookup_type_cache(pgtype,
												 TYPECACHE_TUPDESC |
												 TYPECACHE_DOMAIN_BASE_INFO);

					if (typentry->typtype == TYPTYPE_DOMAIN)
						pgdesc = lookup_rowtype_tupdesc_noerror(typentry->domainBaseType,
															  typentry->domainBaseTypmod,
															  false);
					else
					{
						if (typentry->tupDesc == NULL)
							ereport(ERROR,
									(errcode(ERRCODE_WRONG_OBJECT_TYPE),
									 errmsg("type %s is not composite",
											format_type_be(pgtype))));

						pinned = true;
						pgdesc = typentry->tupDesc;
						PinTupleDesc(pgdesc);
					}

					tupmap = convert_tuples_by_position(desc, pgdesc,
						"clickhouse_fdw: could not map tuple to returned type");
					if (tupmap)
					{
						temptup = do_convert_tuple(htup, tupmap);
						pfree(tupmap);
						heap_freetuple(htup);
						htup = temptup;
					}

					ret = heap_copy_tuple_as_datum(htup, pgdesc);
					heap_freetuple(htup);
					if (pinned)
						ReleaseTupleDesc(pgdesc);
				}
				else
				{
					ret = heap_copy_tuple_as_datum(htup, desc);
					heap_freetuple(htup);
				}

				/* no additional conversion here */
				return ret;
			}
			break;
		default:
			elog(ERROR, "clickhouse_fdw: %d type from ClickHouse is not supported", coltype);
	}

	Assert(valtype != InvalidOid);
	Assert(valtype != RECORDOID);

	if (pgtype != InvalidOid && valtype != pgtype)
	{
		Oid			castfunc;
		CoercionPathType ctype;

		/* try to convert */
		ctype = find_coercion_pathway(pgtype, valtype,
									  COERCION_EXPLICIT,
									  &castfunc);
		switch (ctype)
		{
			case COERCION_PATH_FUNC:
				ret = OidFunctionCall1(castfunc, ret);
				break;
			case COERCION_PATH_RELABELTYPE:
				/* all good */
				break;
			default:
				elog(ERROR, "clickhouse_fdw: could not cast value from %s to %s",
						format_type_be(valtype), format_type_be(pgtype));
		}
	}

	return ret;
}

static void **
binary_fetch_row(ch_cursor *cursor, List *attrs, TupleDesc tupdesc,
	Datum *values, bool *nulls)
{
	ListCell   *lc;
	size_t		j;
	ch_binary_read_state_t *state = cursor->read_state;
	void				  **row_values = ch_binary_read_row(state);
	size_t					attcount = list_length(attrs);

	if (state->error)
		ereport(ERROR,
		        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
		         errmsg("clickhouse_fdw: error while reading row: %s",
					 state->error)));

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
			elog(ERROR, "clickhouse_fdw: unexpected state: atttributes "
					"count == 0 and haven't got NULL in the response");
	}
	else if (attcount != state->resp->columns_count)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg_internal("clickhouse_fdw: columns mismatch"),
				 errdetail("Number of returned columns (%lu) does not match "
						   "expected column count (%lu).",
						   state->resp->columns_count, attcount)));
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

			values[i - 1] = make_datum(rowval, state->coltypes[j], pgtype);
			nulls[i - 1] = false;
		}
		j++;
	}

ok:
	return (void **) values;
}

static void
binary_cursor_free(void *c)
{
	ch_cursor *cursor = c;
	ch_binary_read_state_free(cursor->read_state);
	ch_binary_response_free(cursor->query_response);
}

static void
binary_simple_insert(void *conn, const char *query)
{
	elog(ERROR, "clickhouse_fdw: insertion is not implemented for binary protocol yet");
}

List *
construct_create_tables(ImportForeignSchemaStmt *stmt, ForeignServer *server)
{
	elog(ERROR, "clickhouse_fdw: IMPORT FOREIGN SCHEMA is not implemented yet");
}
