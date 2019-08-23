#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/uuid.h"
#include "utils/fmgroids.h"
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

static bool is_canceled(void)
{
	/* this variable is bool on pg < 12, but sig_atomic_t on above versions */
	if (QueryCancelPending)
		return true;

	return false;
}

ch_connection
chfdw_http_connect(char *connstring)
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
chfdw_http_fetch_raw_data(ch_cursor *cursor)
{
	ch_http_read_state *state = cursor->read_state;
	if (state->data == NULL)
		return NULL;

	return cstring_to_text_with_len(state->data, state->maxpos + 1);
}

/*** BINARY PROTOCOL ***/

ch_connection
chfdw_binary_connect(ch_connection_details *details)
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

	ch_binary_response_t *resp = ch_binary_simple_query(conn, query, &is_canceled);

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
	TEXTOID,	/* enum8 */
	TEXTOID,	/* enum16 */
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
		case chb_Enum8:
		case chb_Enum16:
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
		case chb_Tuple:
			{
				Datum		result;
				HeapTuple	htup;
				Datum	   *tuple_values;
				bool	   *tuple_nulls;
				TupleDesc	desc;
				size_t		i;
				CustomObjectDef	*cdef = chfdw_check_for_custom_type(pgtype);

				ch_binary_tuple_t *tuple = rowval;

				if (tuple->len == 0)
					elog(ERROR, "clickhouse_fdw: returned tuple is empty");

				desc = CreateTemplateTupleDescCompat(tuple->len);
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

				if (cdef || pgtype == RECORDOID || pgtype == TEXTOID)
				{
					ret = heap_copy_tuple_as_datum(htup, desc);
					heap_freetuple(htup);

					if (cdef && cdef->rowfunc != InvalidOid)
					{
						/* there is convertor from row to pgtype */
						ret = OidFunctionCall1(cdef->rowfunc, ret);
					}
					else if (pgtype == TEXTOID)
					{
						/* a lot of allocations, not so efficient */
						ret = CStringGetTextDatum(DatumGetCString(OidFunctionCall1(F_RECORD_OUT, ret)));
					}
				}
				else
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
						temptup = execute_attr_map_tuple(htup, tupmap);
						pfree(tupmap);
						heap_freetuple(htup);
						htup = temptup;
					}

					ret = heap_copy_tuple_as_datum(htup, pgdesc);
					heap_freetuple(htup);
					if (pinned)
						ReleaseTupleDesc(pgdesc);
				}

				/* no additional conversion needed */
				return ret;
			}
			break;
		default:
			elog(ERROR, "clickhouse_fdw: %d type from ClickHouse is not supported", coltype);
	}

	Assert(valtype != InvalidOid);

	if (pgtype != InvalidOid && valtype != pgtype)
	{
		Oid			castfunc;
		CoercionPathType ctype;

		if (valtype == TEXTOID)
		{
			Type		baseType;
			Oid			baseTypeId;
			int32		typmod = -1;

			baseTypeId = getBaseTypeAndTypmod(pgtype, &typmod);
			if (baseTypeId != INTERVALOID)
				typmod = -1;

			baseType = typeidType(baseTypeId);
			ret = stringTypeDatum(baseType, TextDatumGetCString(ret), typmod);
			ReleaseSysCache(baseType);
		}
		else
		{
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

	if (tupdesc)
	{
		Assert(values && nulls);

		j = 0;
		foreach(lc, attrs)
		{
			int		i = lfirst_int(lc);
			void   *rowval = row_values[j];

			/* we should always get some value type or NULL */
			Assert(state->coltypes[j] != chb_Nullable);

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
	}

ok:
	return (void **) row_values;
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

#define STR_TYPES_COUNT 16
static char *str_types_map[STR_TYPES_COUNT][2] = {
	{"Int8", "INT2"},
	{"UInt8", "INT2"},
	{"Int16", "INT2"},
	{"UInt16", "INT4"},
	{"Int32", "INT4"},
	{"UInt32", "INT8"},
	{"Int64", "INT8"},
	{"UInt64", "INT8"}, //overflow risk
	{"Float32", "REAL"},
	{"Float64", "DOUBLE PRECISION"},
	{"Decimal", "NUMERIC"},
	{"Boolean", "BOOLEAN"},
	{"String", "TEXT"},
	{"Date", "DATE"},
	{"DateTime", "TIMESTAMP"},
	{"UUID", "UUID"}
};

List *
chfdw_construct_create_tables(ImportForeignSchemaStmt *stmt, ForeignServer *server)
{
	Oid				userid = GetUserId();
	UserMapping	   *user = GetUserMapping(userid, server->serverid);
	ch_connection	conn = chfdw_get_connection(user);
	ch_cursor	   *cursor;
	char		   *query,
				   *driver;
	List		   *result = NIL,
				   *datts = NIL;
	char		  **row_values;

	ch_connection_details	details;

	details.dbname = "default";
	chfdw_extract_options(server->options, &driver, &details.host,
		&details.port, &details.dbname, &details.username, &details.password);

	query = psprintf("select name, engine, engine_full from system.tables where database='%s'", details.dbname);
	cursor = conn.methods->simple_query(conn.conn, query);

	datts = list_make5_int(1,2,3,4,5);
	datts = lappend_int(datts, 6);
	datts = lappend_int(datts, 7);

	/*
	 * We use only char values from result, so basicly we don't need
	 * to convert anything for binary
	 */
	while ((row_values = (char **) conn.methods->fetch_row(cursor,
				list_make3_int(1,2,3), NULL, NULL, NULL)) != NULL)
	{
		StringInfoData	buf;
		ch_cursor  *table_def;
		char	   *table_name = row_values[0];
		char	   *engine = row_values[1];
		char	   *engine_full = row_values[2];
		char	  **dvalues;
		bool		first = true;

		if (table_name == NULL)
			continue;

		if (list_length(stmt->table_list))
		{
			ListCell *lc;
			bool found = false;

			foreach(lc, stmt->table_list)
			{
				RangeVar   *rv = (RangeVar *) lfirst(lc);
				if (strcmp(rv->relname, table_name) == 0)
					found = true;
			}

			if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT && found)
				continue;
			else if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO && !found)
				continue;
		}

		initStringInfo(&buf);
		appendStringInfo(&buf, "CREATE FOREIGN TABLE %s.%s (\n",
			stmt->local_schema, table_name);
		query = psprintf("describe table %s.%s", details.dbname, table_name);
		table_def = conn.methods->simple_query(conn.conn, query);

		while ((dvalues = (char **) conn.methods->fetch_row(table_def,
			datts, NULL, NULL, NULL)) != NULL)
		{
			bool	is_nullable = false,
					is_array = false,
					add_type = true;

			char   *remote_type = dvalues[1],
				   *pos;

			if (!first)
				appendStringInfoString(&buf, ",\n");
			first = false;

			/* name */
			appendStringInfo(&buf, "\t\"%s\" ", dvalues[0]);
			while ((pos = strstr(remote_type, "(")) != NULL)
			{
				if (strncmp(remote_type, "Decimal", strlen("Decimal")) == 0)
				{
					appendStringInfoString(&buf, "NUMERIC");
					appendStringInfoString(&buf, pos);
					if (strstr(pos, ",") == NULL)
						elog(ERROR, "clickhouse_fdw: could not import Decimal field, "
							"should be two parameters on definition");

					add_type = false;
					break;
				}
				else if (strncmp(remote_type, "FixedString", strlen("FixedString")) == 0)
				{
					appendStringInfoString(&buf, "VARCHAR");
					appendStringInfoString(&buf, pos);
					add_type = false;
					break;
				}
				else if (strncmp(remote_type, "Enum8", strlen("Enum8")) == 0)
				{
					appendStringInfoString(&buf, "TEXT");
					add_type = false;
					break;
				}
				else if (strncmp(remote_type, "Enum16", strlen("Enum16")) == 0)
				{
					appendStringInfoString(&buf, "TEXT");
					add_type = false;
					break;
				}
				else if (strncmp(remote_type, "Tuple", strlen("Tuple")) == 0)
				{
					appendStringInfoString(&buf, "TEXT");
					elog(NOTICE, "clickhouse_fdw: ClickHouse <Tuple> type was "
						"translated to <TEXT> type, please create composite type and alter the column if needed");
					add_type = false;
					break;
				}
				else if (strncmp(remote_type, "Array", strlen("Array")) == 0)
					is_array = true;
				else if (strncmp(remote_type, "Nullable", strlen("Nullable")) == 0)
					is_nullable = true;

				remote_type = pos + 1;
			}

			if (add_type)
			{
				bool found = false;
				if ((pos = strstr(remote_type, ")")) != NULL)
				{
					/* we need to remove that last brackets */
					*pos = '\0';
				}

				for (size_t i = 0; i < STR_TYPES_COUNT; i++)
				{
					if (strcmp(str_types_map[i][0], remote_type) == 0)
					{
						found = true;
						appendStringInfoString(&buf, str_types_map[i][1]);
						break;
					}
				}

				if (!found)
					elog(ERROR, "clickhouse_fdw: could not map type: %s", remote_type);
			}

			if (is_array)
				appendStringInfoString(&buf, "[]");

			if (!is_nullable)
				appendStringInfoString(&buf, " NOT NULL");
		}

		appendStringInfo(&buf, "\n) SERVER %s OPTIONS (table_name '%s'",
			server->servername, table_name);

		if (engine && engine_full && strcmp(engine, "CollapsingMergeTree") == 0)
		{
			char *sub = strstr(engine_full, ")");
			if (sub)
			{
				sub[1] = '\0';
				appendStringInfo(&buf, ", engine '%s'", engine_full);
			}
		}

		appendStringInfoString(&buf, ");\n");
		result = lappend(result, buf.data);
		MemoryContextDelete(table_def->memcxt);
	}

	MemoryContextDelete(cursor->memcxt);
	return result;
}
