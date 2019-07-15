#include <clickhouse/client.h>
#include "clickhouse_internal.h"
#include "clickhouse_binary.hh"

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

ch_binary_response_t *ch_binary_simple_query(ch_binary_connection_t *conn,
		const char *query, uint32_t query_id)
{
	Client	&client = (Client &) conn->client;
	auto resp = new ch_binary_response_t();

	resp->block = NULL;
	client.Select(query, [resp] (const Block& block)
    {
		resp->block = &block;
    }
);
	return resp;
}

void ch_binary_close(ch_binary_connection_t *conn)
{
	delete (Client *) conn->client;
	delete (ClientOptions *) conn->options;
}

void ch_binary_response_free(ch_binary_response_t *resp)
{
	delete (Client &) (*resp->block);
}

}
