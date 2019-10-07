#include <clickhouse/client.h>
#include <clickhouse/types/types.h>
#include "clickhouse_internal.h"
#include "clickhouse_binary.hh"
#include <iostream>
#include <endian.h>
#include <cassert>

using namespace clickhouse;

extern "C" {

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
#define APPEND_DATA(t, type)						\
do {												\
	auto col = std::make_shared<t>();				\
	for (size_t j = 0; j < nrows; j++)				\
		col->Append(((type*) block->coldata)[j]);	\
	chblock.AppendColumn(block->colname, col);		\
} while (0);
	Client	*client = (Client *) conn->client;
	auto	resp = new ch_binary_response_t();
	Block	chblock;
	auto	columns = std::make_shared<std::vector<ColumnRef>>();

	try
	{
		for (size_t i = 0; i < nblocks; i++)
		{
			ch_binary_block_t	*block = &blocks[i];
			ColumnRef	cref;

			switch (block->coltype)
			{
				case chb_Int8:
					APPEND_DATA(ColumnInt8, int8_t);
					break;
				case chb_Int16:
					APPEND_DATA(ColumnInt16, int16_t);
					break;
				case chb_Int32:
					APPEND_DATA(ColumnInt32, int32_t);
					break;
				case chb_Int64:
					APPEND_DATA(ColumnInt64, int64_t);
					break;
				case chb_UInt8:
					APPEND_DATA(ColumnUInt8, uint8_t);
					break;
				case chb_UInt16:
					APPEND_DATA(ColumnUInt16, uint16_t);
					break;
				case chb_UInt32:
					APPEND_DATA(ColumnUInt32, uint32_t);
					break;
				case chb_UInt64:
					APPEND_DATA(ColumnUInt64, uint64_t);
					break;
				case chb_Float32:
					APPEND_DATA(ColumnFloat32, float);
					break;
				case chb_Float64:
					APPEND_DATA(ColumnFloat64, double);
					break;
				case chb_FixedString:
					{
						auto col = std::make_shared<ColumnFixedString>(block->n_arg);
						for (size_t j = 0; j < nrows; j++)
							col->Append(std::string(((char**) block->coldata)[j]));
						chblock.AppendColumn(block->colname, col);
					}
					break;
				case chb_String:
					APPEND_DATA(ColumnString, char*);
					break;
				case chb_DateTime:
					APPEND_DATA(ColumnDateTime, std::time_t);
					break;
				case chb_Date:
					APPEND_DATA(ColumnDate, std::time_t);
					break;
				case chb_UUID:
					{
						auto col = std::make_shared<ColumnUUID>();
						for (size_t j = 0; j < nrows; j++)
						{
							/* we expect blocks by 16 (uuid_t) */
							auto first = ((uint64_t *) block->coldata)[j * 2];
							auto second = ((uint64_t *) block->coldata)[j * 2 + 1];

							UInt128 val = std::make_pair(be64toh(first), be64toh(second));
							col->Append(val);
						}
						chblock.AppendColumn(block->colname, col);
					}
					break;
				default:
					/* TODO: all types */
					throw std::logic_error("unsupported type");
			}
		}

		client->Insert(table_name, chblock);
	}
	catch (const std::exception& e)
	{
		set_resp_error(resp, e.what());
	}

	resp->success = (resp->error == NULL);
	return resp;
#undef APPEND_DATA
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
			state->coltypes = new ch_binary_coltype[resp->columns_count];

			size_t i = 0;
			for (auto& col: values[0])
				state->coltypes[i++] = (ch_binary_coltype) (col->Type()->GetCode());
		}
	}
	catch (const std::exception& e)
	{
		set_state_error(state, e.what());
	}
}

static void *get_value(ch_binary_read_state_t *state, ColumnRef col,
	size_t row, ch_binary_coltype *valtype)
{
	ch_binary_coltype	type;
	auto gc = (std::vector<std::shared_ptr<void>> *) state->gc;

nested:
	type = (ch_binary_coltype) col->Type()->GetCode();
	if (valtype)
		*valtype = type;

	switch (type)
	{
		case chb_UInt8:
			return (void *) &(col->As<ColumnUInt8>()->At(row));
		case chb_UInt16:
			return (void *) &(col->As<ColumnUInt16>()->At(row));
		case chb_UInt32:
			return (void *) &(col->As<ColumnUInt32>()->At(row));
		case chb_UInt64:
			return (void *) &(col->As<ColumnUInt64>()->At(row));
		case chb_Int8:
			return (void *) &(col->As<ColumnInt8>()->At(row));
		case chb_Int16:
			return (void *) &(col->As<ColumnInt16>()->At(row));
		case chb_Int32:
			return (void *) &(col->As<ColumnInt32>()->At(row));
		case chb_Int64:
			return (void *) &(col->As<ColumnInt64>()->At(row));
		case chb_Float32:
			return (void *) &(col->As<ColumnFloat32>()->At(row));
		case chb_Float64:
			return (void *) &(col->As<ColumnFloat64>()->At(row));
		case chb_FixedString:
			{
				const char *str = col->As<ColumnFixedString>()->At(row).c_str();
				return (void *) str;
			}
		case chb_String:
			{
				const char *str = col->As<ColumnString>()->At(row).c_str();
				return (void *) str;
			}
		case chb_Enum8:
			{
				const char *str = col->As<ColumnEnum8>()->NameAt(row).c_str();
				return (void *) str;
			}
		case chb_Enum16:
			{
				const char *str = col->As<ColumnEnum16>()->NameAt(row).c_str();
				return (void *) str;
			}
		case chb_Array:
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
		case chb_Tuple:
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
		case chb_Date:
			{
				auto val = std::make_shared<uint64_t>(
						static_cast<uint64_t> (col->As<ColumnDate>()->At(row)));
				if (*val == 0)
					return NULL;

				gc->push_back(val);
				return (void *) val.get();
			}
		case chb_DateTime:
			{
				auto val = std::make_shared<uint64_t>(
						static_cast<uint64_t> (col->As<ColumnDateTime>()->At(row)));
				if (*val == 0)
					return NULL;

				gc->push_back(val);
				return (void *) val.get();
			}
		case chb_UUID:
			{
				/* we form char[16] from two uint64 numbers, and they should
				 * be big endian */
				UInt128 val = col->As<ColumnUUID>()->At(row);
				std::shared_ptr<uint64_t> sp(new uint64_t[2], std::default_delete<uint64_t[]>());
				sp.get()[0] = htobe64(std::get<0>(val));
				sp.get()[1] = htobe64(std::get<1>(val));
				gc->push_back(sp);

				return (void *) sp.get();
			}
		case chb_Nullable:
			{
				auto nullable = col->As<ColumnNullable>();
				if (!nullable->IsNull(row))
				{
					col = nullable->Nested();
					goto nested;
				}
			}
			break;
		case chb_Void:
			break;
	}

	return NULL;
}

void **ch_binary_read_row(ch_binary_read_state_t *state)
{
	void **res = NULL;
	auto gc = (std::vector<std::shared_ptr<void>> *) state->gc;

	if (state->done || state->coltypes == NULL || state->error)
		return NULL;

	assert(state->resp->values);
	auto& values = *((std::vector<std::vector<ColumnRef>> *) state->resp->values);
	try {
again:
		assert(state->block < state->resp->blocks_count);
		auto&	block = values[state->block];
		size_t	row_count  = block[0]->Size();

		if (row_count == 0)
			goto next_row;

		res = (void **) malloc(sizeof(void *) * state->resp->columns_count);
		if (res == NULL)
			throw std::bad_alloc();

		for (size_t i = 0; i < state->resp->columns_count; i++)
		{
			auto col = block[i];

			res[i] = get_value(state, col, state->row, &state->coltypes[i]);
			if (res[i] == NULL)
				state->coltypes[i] = chb_Void;
		}

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
		if (res != NULL)
			free(res);

		set_state_error(state, e.what());
		res = NULL;
	}

	std::shared_ptr<void> ptr(res, free);
	gc->push_back(ptr);
	return res;
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
