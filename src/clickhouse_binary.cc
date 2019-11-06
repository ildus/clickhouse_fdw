#include <clickhouse/client.h>
#include <clickhouse/types/types.h>
#include <iostream>
#include <endian.h>
#include <cassert>
#include <stdexcept>

extern "C" {

#include "postgres.h"
#include "pgtime.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"
#include "utils/uuid.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/array.h"
#include "clickhouse_binary.hh"
#include "clickhouse_internal.h"

using namespace clickhouse;

ch_binary_connection_t *ch_binary_connect(char *host, int port,
		char *database, char *user, char *password, char **error)
{
	ch_binary_connection_t	*conn = NULL;
	ClientOptions	*options = new ClientOptions();

	options->SetPingBeforeQuery(true);
	if (host)
		options->SetHost(std::string(host));
	if (port)
		options->SetPort(port);
	if (database)
		options->SetDefaultDatabase(std::string(database));
	if (user)
		options->SetUser(std::string(user));
	if (password)
		options->SetPassword(std::string(password));

	//options->SetRethrowException(false);
	conn = new ch_binary_connection_t();

	try
	{
		Client *client = new Client(*options);
		conn->client = client;
		conn->options = options;
	}
	catch (const std::exception& e)
	{
		delete conn;
		conn = NULL;
		if (error)
			*error = strdup(e.what());
	}
	return conn;
}

static void
set_resp_error(ch_binary_response_t *resp, const char *str)
{
	if (resp->error)
		return;

	resp->error = (char *) malloc(strlen(str) + 1);
	strcpy(resp->error, str);
}

static void
set_state_error(ch_binary_read_state_t *state, const char *str)
{
	assert(state->error == NULL);
	state->error = (char *) malloc(strlen(str) + 1);
	strcpy(state->error, str);
}

ch_binary_response_t *ch_binary_simple_query(ch_binary_connection_t *conn,
	const char *query, bool (*check_cancel)(void) )
{
	Client	*client = (Client *) conn->client;
	auto resp = new ch_binary_response_t();
	auto chquery = std::string(query);
	auto values = new std::vector<std::vector<clickhouse::ColumnRef>>();

	assert(resp->values == NULL);
	try
	{
		client->SelectCancelable(chquery,
				[&resp, &values, &check_cancel] (const Block& block) {
			if (check_cancel && check_cancel())
			{
				set_resp_error(resp, "query was canceled");
				return false;
			}

			if (block.GetColumnCount() == 0)
				return true;

			auto vec = std::vector<clickhouse::ColumnRef>();

			if (resp->columns_count && block.GetColumnCount() != resp->columns_count)
			{
				set_resp_error(resp, "columns mismatch in blocks");
				return false;
			}

			resp->columns_count = block.GetColumnCount();
			resp->blocks_count++;

			for (size_t i = 0; i < resp->columns_count; ++i)
				vec.push_back(block[i]);

			values->push_back(std::move(vec));
			return true;
		});
	}
	catch (const std::exception& e)
	{
		values->clear();
		set_resp_error(resp, e.what());
		delete values;
		values = NULL;
	}

	resp->values = (void *) values;
	resp->success = (resp->error == NULL);
	return resp;
}

void *
ch_binary_prepare_insert(void *conn, ResultRelInfo *rri, List *target_attrs,
		char *query, char *table_name)
{
	if (table_name == NULL)
		elog(ERROR, "expected table name");

	Client	*client = (Client *) ((ch_binary_connection_t *) conn)->client;
	auto tn = std::string(table_name);

	try
	{
		client->InsertWithSample(tn, [] (const Block& sample_block) {
			if (sample_block.GetColumnCount() == 0)
				return true;

			auto vec = std::vector<clickhouse::ColumnRef>();
			elog(NOTICE, "columns count: %d", sample_block.GetColumnCount());

			return true;
		});
	}
	catch (const std::exception& e)
	{
		elog(NOTICE, "clickhouse_fdw: insertion error - %s", e.what());
	}
	return NULL;
}

void
ch_binary_insert_tuple(void *istate, TupleTableSlot *slot)
{
	elog(NOTICE, "insert ok");
}

void ch_binary_close(ch_binary_connection_t *conn)
{
	delete (Client *) conn->client;
	delete (ClientOptions *) conn->options;
}

void ch_binary_response_free(ch_binary_response_t *resp)
{
	if (resp->values)
	{
		auto values = (std::vector<std::vector<clickhouse::ColumnRef>> *) resp->values;
		values->clear();
		delete values;
	}

	if (resp->error)
		free(resp->error);

	delete resp;
}

void ch_binary_read_state_init(ch_binary_read_state_t *state, ch_binary_response_t *resp)
{
	state->resp = resp;
	state->block = 0;
	state->row = 0;
	state->done = false;
	state->error = NULL;
	state->coltypes = NULL;
	state->values = NULL;
	state->nulls = NULL;

	if (resp->error)
	{
		state->done = true;
		set_state_error(state, resp->error);
		return;
	}

	try
	{
		assert(resp->values);
		auto& values = *((std::vector<std::vector<clickhouse::ColumnRef>> *) resp->values);

		if (resp->columns_count && values.size() > 0)
		{
			state->coltypes = new Oid[resp->columns_count];
			state->values = new Datum[resp->columns_count];
			state->nulls = new bool[resp->columns_count];
		}
	}
	catch (const std::exception& e)
	{
		set_state_error(state, e.what());
	}
}

static Oid
get_corr_array_type(Type::Code code)
{
	switch (code)
	{
		case Type::Code::Int8:
		case Type::Code::Int16:
		case Type::Code::UInt8: return INT2OID;
		case Type::Code::Int32:
		case Type::Code::UInt16: return INT4OID;
		case Type::Code::Int64:
		case Type::Code::UInt64:
		case Type::Code::UInt32: return INT8OID;
		case Type::Code::Float32: return FLOAT4OID;
		case Type::Code::Float64: return FLOAT8OID;
		case Type::Code::FixedString:
		case Type::Code::Enum8:
		case Type::Code::Enum16:
		case Type::Code::String: return TEXTOID;
		case Type::Code::Date: return DATEOID;
		case Type::Code::DateTime: return TIMESTAMPOID;
		case Type::Code::UUID: return UUIDOID;
		default:
			elog(ERROR, "clickhouse_fdw: unsupported type for arrays");
	}
}

static Datum
make_datum(clickhouse::ColumnRef col, size_t row, Oid *valtype, bool *is_null)
{
	Datum	ret = (Datum) 0;

nested:
	*valtype = InvalidOid;
	*is_null = false;
	switch (col->Type()->GetCode())
	{
		case Type::Code::UInt8:
		{
			int16 val = col->As<ColumnUInt8>()->At(row);
			ret = Int16GetDatum(val);
			*valtype = INT2OID;
		}
		break;
		case Type::Code::UInt16:
		{
			int16	val = col->As<ColumnUInt16>()->At(row);
			ret = Int16GetDatum(val);
			*valtype = INT4OID;
		}
		break;
		case Type::Code::UInt32:
		{
			int64	val = col->As<ColumnUInt32>()->At(row);
			ret = Int64GetDatum(val);
			*valtype = INT8OID;
		}
		break;
		case Type::Code::UInt64:
		{
			uint64	val = col->As<ColumnUInt64>()->At(row);
			if (val > LONG_MAX)
				elog(ERROR, "clickhouse_fdw: int64 overflow");

			ret = Int64GetDatum((int64) val);
			*valtype = INT8OID;
		}
		break;
		case Type::Code::Int8:
		{
			int16 val = col->As<ColumnInt8>()->At(row);
			ret = CharGetDatum(val);
			*valtype = INT2OID;
		}
		break;
		case Type::Code::Int16:
		{
			int16	val = col->As<ColumnInt16>()->At(row);
			ret = Int16GetDatum(val);
			*valtype = INT2OID;
		}
		break;
		case Type::Code::Int32:
		{
			int	val = col->As<ColumnInt32>()->At(row);
			ret = Int32GetDatum(val);
			*valtype = INT4OID;
		}
		break;
		case Type::Code::Int64:
		{
			int64	val = col->As<ColumnInt64>()->At(row);
			ret = Int64GetDatum(val);
			*valtype = INT8OID;
		}
		break;
		case Type::Code::Float32:
		{
			float	val = col->As<ColumnFloat32>()->At(row);
			ret = Float4GetDatum(val);
			*valtype = FLOAT4OID;
		}
		break;
		case Type::Code::Float64:
		{
			double	val = col->As<ColumnFloat64>()->At(row);
			ret = Float8GetDatum(val);
			*valtype = FLOAT8OID;
		}
		break;
		case Type::Code::FixedString:
		{
			const char *str = col->As<ColumnFixedString>()->At(row).c_str();
			ret = CStringGetTextDatum(str);
			*valtype = TEXTOID;
		}
		break;
		case Type::Code::String:
		{
			const char *str = col->As<ColumnString>()->At(row).c_str();
			ret = CStringGetTextDatum(str);
			*valtype = TEXTOID;
		}
		break;
		case Type::Code::Enum8:
		{
			const char *str = col->As<ColumnEnum8>()->NameAt(row).c_str();
			ret = CStringGetTextDatum(str);
			*valtype = TEXTOID;
		}
		break;
		case Type::Code::Enum16:
		{
			const char *str = col->As<ColumnEnum16>()->NameAt(row).c_str();
			ret = CStringGetTextDatum(str);
			*valtype = TEXTOID;
		}
		break;
		case Type::Code::Date:
		{
			auto val = static_cast<pg_time_t> (col->As<ColumnDate>()->At(row));
			*valtype = DATEOID;

			if (val == 0)
				*is_null = true;
			else
			{
				Timestamp t = (Timestamp) time_t_to_timestamptz(val);
				ret = TimestampGetDatum(t);
				ret = DirectFunctionCall1(timestamp_date, ret);
			}
		}
		break;
		case Type::Code::DateTime:
		{
			auto val = static_cast<pg_time_t> (col->As<ColumnDateTime>()->At(row));
			*valtype = TIMESTAMPOID;

			if (val == 0)
				*is_null = true;
			else
			{
				Timestamp t = (Timestamp) time_t_to_timestamptz(val);
				ret = TimestampGetDatum(t);
			}
		}
		break;
		case Type::Code::UUID:
		{
			/* we form char[16] from two uint64 numbers, and they should
			 * be big endian */
			UInt128 val = col->As<ColumnUUID>()->At(row);
			pg_uuid_t	*uuid_val = (pg_uuid_t *) palloc(sizeof(pg_uuid_t));

			val.first = htobe64(val.first);
			val.second = htobe64(val.second);
			memcpy(uuid_val->data, &val.first, 8);
			memcpy(uuid_val->data + 8, &val.second, 8);

			ret = UUIDPGetDatum(uuid_val);
			*valtype = UUIDOID;
		}
		break;
		case Type::Code::Nullable:
		{
			auto nullable = col->As<ColumnNullable>();
			if (nullable->IsNull(row))
			{
				*is_null = true;
			}
			else
			{
				col = nullable->Nested();
				goto nested;
			}
		}
		break;
		case Type::Code::Array:
		{
			auto	arr = col->As<ColumnArray>()->GetAsColumn(row);
			size_t	len = arr->Size();

			*valtype = get_array_type(get_corr_array_type(arr->Type()->GetCode()));
			if (*valtype == InvalidOid)
				elog(ERROR, "clickhouse_fdw: could not find array type for %d",
						*valtype);

			if (len == 0)
				ret = PointerGetDatum(construct_empty_array(*valtype));
			else
			{
				int16		typlen;
				bool		typbyval;
				char		typalign;
				Oid			item_type;
				bool		item_isnull;
				void	   *arrout;

				Datum *out_datums = (Datum *) palloc(sizeof(Datum) * len);

				for (size_t i = 0; i < len; ++i)
					out_datums[i] = make_datum(arr, i, &item_type, &item_isnull);

				get_typlenbyvalalign(item_type, &typlen, &typbyval, &typalign);
				arrout = construct_array(out_datums, len, item_type, typlen,
						typbyval, typalign);
				ret = PointerGetDatum(arrout);
				pfree(out_datums);
			}
		}
		break;
		case Type::Code::Tuple:
		{
			Datum		result;
			HeapTuple	htup;
			TupleDesc	desc;
			auto tuple = col->As<ColumnTuple>();
			auto len = tuple->TupleSize();

			if (len == 0)
				elog(ERROR, "clickhouse_fdw: returned tuple is empty");

#if PG_VERSION_NUM < 120000
			desc = CreateTemplateTupleDesc(len, false);
#else
			desc = CreateTemplateTupleDesc(len);
#endif
			auto tuple_values = (Datum *) palloc(sizeof(Datum) * desc->natts);
			auto tuple_nulls = (bool *) palloc0(sizeof(bool) * desc->natts);

			for (size_t i = 0; i < desc->natts; ++i)
			{
				Oid		item_type;
				auto tuple_col = (*tuple)[i];

				tuple_values[i] = make_datum(tuple_col, row, &item_type,
						&tuple_nulls[i]);
				TupleDescInitEntry(desc, (AttrNumber) i + 1, "", item_type, -1, 0);
			}

			desc = BlessTupleDesc(desc);
			auto slot = (ch_binary_tuple_t *) palloc(sizeof(ch_binary_tuple_t));
			slot->desc = desc;

			htup = heap_form_tuple(slot->desc, tuple_values, tuple_nulls);
			slot->tup = htup;

			pfree(tuple_values);
			pfree(tuple_nulls);

			/* this one will need additional work, since we just return raw slot */
			ret = PointerGetDatum(slot);
			*valtype = RECORDOID;
		}
		break;
		default:
			elog(ERROR, "clickhouse_fdw: unsupported type");
	}

	return ret;
}

bool ch_binary_read_row(ch_binary_read_state_t *state)
{
	/* coltypes is NULL means there are no blocks */
	bool res = false;

	if (state->done || state->coltypes == NULL || state->error)
		return false;

	assert(state->resp->values);
	auto& values = *((std::vector<std::vector<clickhouse::ColumnRef>> *) state->resp->values);
	try {
again:
		assert(state->block < state->resp->blocks_count);
		auto&	block = values[state->block];
		size_t	row_count  = block[0]->Size();

		if (row_count == 0)
			goto next_row;

		for (size_t i = 0; i < state->resp->columns_count; i++)
		{
			/* fill value and null arrays */
			state->values[i] = make_datum(block[i], state->row,
					&state->coltypes[i], &state->nulls[i]);
		}
		res = true;

next_row:
		state->row++;
		if (state->row >= row_count)
		{
			state->row = 0;
			state->block++;
			if (state->block >= state->resp->blocks_count)
				state->done = true;
			else if (row_count == 0)
				goto again;
		}
	}
	catch (const std::exception& e)
	{
		set_state_error(state, e.what());
	}

	return res;
}

void ch_binary_read_state_free(ch_binary_read_state_t *state)
{
	if (state->coltypes)
	{
		delete[] state->coltypes;
		delete[] state->values;
		delete[] state->nulls;
	}

	if (state->error)
		free(state->error);
}

}
