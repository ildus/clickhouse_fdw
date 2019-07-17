#include <clickhouse/client.h>
#include "clickhouse_internal.h"
#include "clickhouse_binary.hh"
#include <assert.h>
#include <iostream>

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

	options->SetRethrowException(false);

	try
	{
		Client *client = new Client(*options);
		conn = new ch_binary_connection_t();
		conn->client = client;
		conn->options = options;
	}
	catch (const std::exception& e)
	{
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

ch_binary_response_t *ch_binary_simple_query(ch_binary_connection_t *conn,
	const char *query)
{
	Client	*client = (Client *) conn->client;
	auto resp = new ch_binary_response_t();
	auto chquery = std::string(query);
	auto values = new std::vector<std::vector<ColumnRef> *>();

	assert(resp->values == NULL);
	try
	{
		client->SelectCancelable(chquery, [&resp, &values] (const Block& block) {
			if (block.GetColumnCount() == 0)
				return true;

			auto vec = new std::vector<ColumnRef>(block.GetColumnCount());

			if (resp->columns_count && block.GetColumnCount() != resp->columns_count)
			{
				set_resp_error(resp, "columns mismatch in blocks");
				return false;
			}

			resp->columns_count = block.GetColumnCount();

			for (size_t i = 0; i < resp->columns_count; ++i)
				vec->push_back(block[i]->As<Column>());

			values->push_back(vec);
			return true;
		});
	}
	catch (const std::exception& e)
	{
		set_resp_error(resp, e.what());
	}

	resp->values = (void *) values;
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
	auto values = (std::vector<std::vector<ColumnRef> *> *) resp->values;
	for (auto block : *values)
	{
		for (auto col: *block) {
			assert(col.unique());
			delete &col;
		}
	}
	delete resp;
}

}
