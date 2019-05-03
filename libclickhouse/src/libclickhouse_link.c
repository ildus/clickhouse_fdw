#include "postgres.h"
#include "clickhouse_http.h"

ch_connection http_connect(ForeignServer *server, UserMapping *user);
void http_disconnect(ConnCacheEntry *entry);
void check_params(const char *password, UserMapping *user);
void http_simple_query(ch_connection conn, const char *query);

static libclickhouse_methods http_methods = {
	.connect=http_connect,
	.disconnect=http_disconnect,
	.check_conn_params=check_params,
	.simple_query = http_simple_query
};

libclickhouse_methods	*clickhouse_gate = &http_methods;

ch_connection
http_connect(ForeignServer *server, UserMapping *user)
{
	char       *host = "127.0.0.1";
	int        port = 8123;
	char       *username = NULL;
	char       *password = NULL;
	char       *dbname = "default";
	char	   *connstring;

	ExtractConnectionOptions(server->options, &driver, &host, &port, &dbname,
							 &username, &password);
	ExtractConnectionOptions(user->options, &driver, &host, &port, &dbname,
							 &username, &password);

	Assert(strcmp(driver, "http") == 0);
	connstring = psprintf("http://%s:%s", host, port);
	if (username && password)
	{
		char *newconnstring = psprintf("%s?user=%s&password=%s", connstring, username, password);
		pfree(connstring);
		connstring = newconnstring;
	}
	ch_http_connection_t *conn = ch_http_connection(connstring);
	pfree(connstring);

	return (ch_connection) conn;
}
