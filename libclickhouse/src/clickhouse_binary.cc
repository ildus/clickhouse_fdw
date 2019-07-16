#include <clickhouse/client.h>
#include "clickhouse_internal.h"
#include "clickhouse_binary.hh"
#include <assert.h>

using namespace clickhouse;

extern "C" {

ch_binary_connection_t *ch_binary_connect(char *host, int port,
		char *database, char *user, char *password)
{
	auto options = new ClientOptions();

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

	options->SetRethrowException(false);

	Client *client = new Client(*options);
	auto conn = new ch_binary_connection_t();

	conn->client = client;
	conn->options = options;
	return conn;
}

static void
set_resp_error(ch_binary_response_t *resp, const char *str)
{
	assert(resp->error == NULL);
	resp->error = (char *) malloc(strlen(str) + 1);
	strcpy(resp->error, str);
}

ch_binary_response_t *ch_binary_simple_query(ch_binary_connection_t *conn,
		const char *query, uint32_t query_id)
{
	Client	&client = (Client &) conn->client;
	auto resp = new ch_binary_response_t();

	assert(resp->values == NULL);
	client.SelectCancelable(query, [resp] (const Block& block) {
		size_t	curblock = resp->blocks_count;

		if (resp->columns_count && block.GetColumnCount() != resp->columns_count)
		{
			set_resp_error(resp, "columns mismatch in blocks");
			return false;
		}

		resp->columns_count = block.GetColumnCount();
		resp->blocks_count += 1;

		if (resp->values == NULL)
			resp->values = (void **) malloc(resp->blocks_count * resp->columns_count
									* sizeof(ColumnRef));
		else
			resp->values = (void **) realloc(resp->values,
								  resp->blocks_count * resp->columns_count
									* sizeof(ColumnRef));

		for (size_t i = 0; i < resp->columns_count; ++i)
		{
			ColumnRef	col = block[i],
						col2;
			size_t		arrpos = curblock * i;

			col2 = col->As<Column>();
			resp->values[arrpos] = reinterpret_cast<void *>(&col2);
		}

		return true;
	});

	resp->success = resp->error == NULL;
	return resp;
}

void ch_binary_close(ch_binary_connection_t *conn)
{
	delete (Client *) conn->client;
	delete (ClientOptions *) conn->options;
}

void ch_binary_response_free(ch_binary_response_t *resp)
{
	for (size_t i = 0; i < resp->blocks_count * resp->columns_count; i++)
	{
		ColumnRef	*col = reinterpret_cast<ColumnRef *>(resp->values[i]);
		assert(col->unique());
		delete col;
	}
}

}
