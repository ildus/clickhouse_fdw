#include <clickhouse/client.h>
#include <clickhouse/types/types.h>
#include "clickhouse_internal.h"
#include <iostream>
#include <endian.h>
#include <cassert>
#include <stdexcept>

extern "C" {

#include "postgres.h"
#include "pgtime.h"
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"
#include "utils/uuid.h"
#include "utils/timestamp.h"
#include "clickhouse_binary.hh"

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
	auto values = new std::vector<std::vector<ColumnRef>>();

	assert(resp->values == NULL);
	try
	{
		client->SelectCancelable(chquery, [&resp, &values, &check_cancel] (const Block& block) {
			if (check_cancel && check_cancel())
			{
				set_resp_error(resp, "query was canceled");
				return false;
			}

			if (block.GetColumnCount() == 0)
				return true;

			auto vec = std::vector<ColumnRef>();

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

ch_binary_response_t *ch_binary_simple_insert(ch_binary_connection_t *conn,
	char *table_name, ch_binary_block_t *blocks, size_t nblocks, size_t nrows)
{
	throw std::logic_error("not implemented");
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
		auto values = (std::vector<std::vector<ColumnRef>> *) resp->values;
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
	state->coltypes = NULL;
	state->block = 0;
	state->row = 0;
	state->done = false;
	state->error = NULL;
	state->gc = (void *) new std::vector<std::shared_ptr<void>>();

	if (resp->error)
	{
		state->done = true;
		set_state_error(state, resp->error);
		return;
	}

	try
	{
		assert(resp->values);
		auto& values = *((std::vector<std::vector<ColumnRef>> *) resp->values);

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

static Datum
make_datum(ch_binary_read_state_t *state, ColumnRef col,
	size_t row, Oid *valtype, bool *is_null)
{
	Datum	ret = (Datum) 0;
	auto	gc = (std::vector<std::shared_ptr<void>> *) state->gc;

nested:
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
			if (val == 0)
				*is_null = true;
			else
			{
				Timestamp t = (Timestamp) time_t_to_timestamptz(val);
				ret = TimestampGetDatum(t);
				ret = DirectFunctionCall1(timestamp_date, ret);
			}
			*valtype = DATEOID;
		}
		break;
		case Type::Code::DateTime:
		{
			auto val = static_cast<pg_time_t> (col->As<ColumnDate>()->At(row));
			if (val == 0)
				*is_null = true;
			else
			{
				Timestamp t = (Timestamp) time_t_to_timestamptz(val);
				ret = TimestampGetDatum(t);
			}
			*valtype = TIMESTAMPOID;
		}
		break;
		case Type::Code::UUID:
		{
			/* we form char[16] from two uint64 numbers, and they should
			 * be big endian */
			UInt128 val = col->As<ColumnUUID>()->At(row);
			uint64	parts[2];

			parts[0] = htobe64(std::get<0>(val));
			parts[1] = htobe64(std::get<1>(val));
			pg_uuid_t	*uuid_val = (pg_uuid_t *) parts;

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
				*valtype = InvalidOid;
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
			auto arr = col->As<ColumnArray>()->GetAsColumn(row);

			std::shared_ptr<ch_binary_array_t> res(new ch_binary_array_t());
			std::shared_ptr<void *> values(new void*[arr->Size()],
					std::default_delete<void*[]>());

			res->coltype = (ch_binary_coltype) arr->Type()->GetCode();
			res->len = arr->Size();

			for (size_t i = 0; i < res->len; i++)
				values.get()[i] = get_value(state, arr, i, NULL);

			res->values = values.get();

			gc->push_back(res);
			gc->push_back(values);
			gc->push_back(arr);

			return (void *) res.get();
		}
		break;
		case Type::Code::Tuple:
		{
			auto tuple = col->As<ColumnTuple>();

			std::shared_ptr<ch_binary_tuple_t> res(new ch_binary_tuple_t());
			std::shared_ptr<ch_binary_coltype> coltypes(new ch_binary_coltype[tuple->TupleSize()],
					std::default_delete<ch_binary_coltype[]>());
			std::shared_ptr<void *> values(new void*[tuple->TupleSize()],
					std::default_delete<void*[]>());

			for (size_t i = 0; i < tuple->TupleSize(); i++)
			{
				auto col = (*tuple)[i];
				values.get()[i] = get_value(state, (*tuple)[i], row, &(coltypes.get()[i]));
				if (values.get()[i] == NULL)
					coltypes.get()[i] = chb_Void;
			}

			res->len = tuple->TupleSize();
			res->coltypes = coltypes.get();
			res->values = values.get();

			gc->push_back(res);
			gc->push_back(coltypes);
			gc->push_back(values);

			return (void *) res.get();
		}
		break;
		default:
			elog(ERROR, "clickhouse_fdw: unsupported type");
	}

	return ret;
}

bool ch_binary_read_row(ch_binary_read_state_t *state)
{
	auto gc = (std::vector<std::shared_ptr<void>> *) state->gc;

	if (state->done || state->coltypes == NULL || state->error)
		return false;

	assert(state->resp->values);
	auto& values = *((std::vector<std::vector<ColumnRef>> *) state->resp->values);
	try {
again:
		assert(state->block < state->resp->blocks_count);
		auto&	block = values[state->block];
		size_t	row_count  = block[0]->Size();

		if (row_count == 0)
			goto next_row;

		for (size_t i = 0; i < state->resp->columns_count; i++)
			state->values[i] = make_datum(state, block[i], state->row,
					&state->coltypes[i], &state->nulls[i]);

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
	return true;
}

void ch_binary_read_state_free(ch_binary_read_state_t *state)
{
	auto gc = (std::vector<std::shared_ptr<void>> *) state->gc;

	if (state->coltypes)
		delete[] state->coltypes;

	if (state->error)
		free(state->error);

	gc->clear();
	delete gc;
}

}
