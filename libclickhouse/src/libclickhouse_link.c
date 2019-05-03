#include "postgres.h"
#include "clickhouse_http.h"
#include "clickhouse_fdw.h"

static ch_connection http_connect(ForeignServer *server, UserMapping *user);
static void http_disconnect(ConnCacheEntry *entry);
static void http_simple_query(ch_connection conn, const char *query);

static libclickhouse_methods http_methods = {
	.connect=http_connect,
	.disconnect=http_disconnect,
	.simple_query=http_simple_query
};

libclickhouse_methods	*clickhouse_gate = &http_methods;

/*
 * For non-superusers, insist that the connstr specify a password.  This
 * prevents a password from being picked up from .pgpass, a service file,
 * the environment, etc.  We don't want the postgres user's passwords
 * to be accessible to non-superusers.  (See also dblink_connstr_check in
 * contrib/dblink.)
 */
static void
check_conn_params(const char *password, UserMapping *user)
{
	/* no check required if superuser */
	if (superuser_arg(user->userid))
	{
		return;
	}

	if (password == NULL)
		ereport(ERROR,
		        (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
		         errmsg("password is required"),
		         errdetail("Non-superusers must provide a password in the user mapping.")));
}

static ch_connection
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

	check_conn_params(password, user);

	Assert(strcmp(driver, "http") == 0);
	connstring = psprintf("http://%s:%s", host, port);
	if (username && password)
	{
		char *newconnstring = psprintf("%s?user=%s&password=%s", connstring,
									   username, password);
		pfree(connstring);
		connstring = newconnstring;
	}
	ch_http_connection_t *conn = ch_http_connection(connstring);
	if (conn == NULL)
		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("could not connect to server \"%s\"",
		                server->servername),
		         errdetail_internal("%s", pchomp(error))));

	pfree(connstring);

	return (ch_connection) conn;
}

/*
 * Disconnect any open connection for a connection cache entry.
 */
static void
http_disconnect(ConnCacheEntry *entry)
{
	if (entry->conn != NULL)
		ch_http_close(entry->conn);
}
