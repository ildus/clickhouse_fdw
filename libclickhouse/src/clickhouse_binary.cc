#include <clickhouse/client.h>
#include <clickhouse/types/types.h>
#include "clickhouse_internal.h"
#include "clickhouse_binary.hh"
#include <assert.h>
#include <iostream>
#include <endian.h>

using namespace clickhouse;

extern "C" {

ch_binary_connection_t *ch_binary_connect(char *host, int port,
		char *database, char *user, char *password)
{
	ch_binary_connection_t	*conn = NULL;
	ClientOptions	*options = new ClientOptions();

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
		std::cout << e.what();
	}
	return conn;
}

static void
set_resp_error(ch_binary_response_t *resp, const char *str)
{
	assert(resp->error == NULL);
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
	const char *query)
{
	Client	*client = (Client *) conn->client;
	auto resp = new ch_binary_response_t();
	auto chquery = std::string(query);
	auto values = new std::vector<std::vector<ColumnRef>>();

	assert(resp->values == NULL);
	try
	{
		client->SelectCancelable(chquery, [&resp, &values] (const Block& block) {
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
	ch_binary_coltype type, size_t row)
{
	auto gc = (std::vector<std::shared_ptr<void>> *) state->gc;

nested:
	switch (type)
	{
		case chb_UInt8:
			return (void *) &(col->As<ColumnUInt8>()->At(row));
			break;
		case chb_UInt16:
			return (void *) &(col->As<ColumnUInt16>()->At(row));
			break;
		case chb_UInt32:
			return (void *) &(col->As<ColumnUInt32>()->At(row));
			break;
		case chb_UInt64:
			return (void *) &(col->As<ColumnUInt64>()->At(row));
			break;
		case chb_Int8:
			return (void *) &(col->As<ColumnInt8>()->At(row));
			break;
		case chb_Int16:
			return (void *) &(col->As<ColumnInt16>()->At(row));
			break;
		case chb_Int32:
			return (void *) &(col->As<ColumnInt32>()->At(row));
			break;
		case chb_Int64:
			return (void *) &(col->As<ColumnInt64>()->At(row));
			break;
		case chb_Float32:
			return (void *) &(col->As<ColumnFloat32>()->At(row));
			break;
		case chb_Float64:
			return (void *) &(col->As<ColumnFloat64>()->At(row));
			break;
		case chb_FixedString:
			{
				const char *str = col->As<ColumnFixedString>()->At(row).c_str();
				return (void *) str;
			}
			break;
		case chb_String:
			{
				const char *str = col->As<ColumnString>()->At(row).c_str();
				return (void *) str;
			}
			break;
		case chb_Enum8:
			return (void *) &(col->As<ColumnEnum8>()->At(row));
			break;
		case chb_Enum16:
			return (void *) &(col->As<ColumnEnum16>()->At(row));
			break;
		case chb_Array:
			/* TODO: fix array */
			break;
		case chb_Tuple:
			/* TODO: fix tuple */
			break;
		case chb_Date:
			{
				auto val = std::make_shared<uint64_t>(static_cast<uint64_t> (col->As<ColumnDate>()->At(row)));
				gc->push_back(val);
				return (void *) val.get();
			}
			break;
		case chb_DateTime:
			{
				auto val = std::make_shared<uint64_t>(static_cast<uint64_t> (col->As<ColumnDateTime>()->At(row)));
				gc->push_back(val);
				return (void *) val.get();
			}
			break;
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
			break;
		case chb_Nullable:
			{
				auto nullable = col->As<ColumnNullable>();
				if (nullable->IsNull(row))
					break;
				else
				{
					col = nullable->Nested();
					type = (ch_binary_coltype) col->Type()->GetCode();
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
	bool skip_block = false;

	if (state->done || state->coltypes == NULL || state->error)
		return NULL;

	assert(state->resp->values);
	auto& values = *((std::vector<std::vector<ColumnRef>> *) state->resp->values);
	try {
		res = (void **) malloc(sizeof(void *) * state->resp->columns_count);
		if (res == NULL)
			throw std::bad_alloc();

again:
		assert(state->block < state->resp->blocks_count);
		auto& block = values[state->block];

		size_t	row_count  = block[0]->Size();
		if (row_count == 0)
		{
			skip_block = true;
			goto skip;
		}

		for (size_t i = 0; i < state->resp->columns_count; i++)
		{
			auto col = block[i];

			state->coltypes[i] = (ch_binary_coltype) col->Type()->GetCode();
			res[i] = get_value(state, col, state->coltypes[i], state->row);
			if (res[i] == NULL)
				state->coltypes[i] = chb_Void;
		}

skip:
		state->row++;
		if (state->row >= row_count)
		{
			state->row = 0;
			state->block++;
			if (state->block >= state->resp->blocks_count)
				state->done = true;
			else if (skip_block)
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

	return res;
}

void ch_binary_read_state_free(ch_binary_read_state_t *state)
{
	auto gc = (std::vector<std::shared_ptr<void>> *) state->gc;

	if (state->coltypes)
		delete state->coltypes;

	if (state->error)
		free(state->error);

	gc->clear();
}

}
