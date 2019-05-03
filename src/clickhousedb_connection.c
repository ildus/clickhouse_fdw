/*-------------------------------------------------------------------------
 *
 * clickhousedb_connection.c
 *		  Connection management functions for clickhousedb_fdw
 *
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/clickhousedb_fdw/clickhousedb_connection.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "clickhouse-client.h"

#include "access/htup_details.h"
#include "catalog/pg_user_mapping.h"
#include "access/xact.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/latch.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


#include "clickhousedb_fdw.h"
/*
 * Connection cache hash table entry
 */
typedef struct ConnCacheKey
{
	Oid			userid;
	bool    read;
} ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	Conn	   *conn;			/* connection to foreign server, or NULL */
	/* Remaining fields are invalid when conn is NULL: */
	int			xact_depth;		/* 0 = no xact open, 1 = main xact open, 2 =
                                 * one level of subxact open, etc */
	bool		have_prep_stmt; /* have we prepared any stmts in this xact? */
	bool		have_error;		/* have any subxacts aborted in this xact? */
	bool		changing_xact_state;	/* xact state change in process */
	bool		invalidated;	/* true if reconnect is pending */
	bool    read;   /* Separet entry for read/write */
	uint32		server_hashvalue;	/* hash value of foreign server OID */
	uint32		mapping_hashvalue;	/* hash value of user mapping OID */
} ConnCacheEntry;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

/* for assigning cursor numbers and prepared statement numbers */
static unsigned int cursor_number = 0;
static unsigned int prep_stmt_number = 0;

/* tracks whether any work is needed in callback functions */
static bool xact_got_connection = false;

/* prototypes of private functions */
static Conn *connect_pg_server(ForeignServer *server, UserMapping *user);
static void disconnect_pg_server(ConnCacheEntry *entry);
static void check_conn_params(const char *password, UserMapping *user);
static void configure_remote_session(Conn *conn);
static void do_sql_command(Conn *conn, const char *sql);
static void begin_remote_xact(ConnCacheEntry *entry);
static void pgfdw_xact_callback(XactEvent event, void *arg);
static void pgfdw_subxact_callback(SubXactEvent event,
                                   SubTransactionId mySubid,
                                   SubTransactionId parentSubid,
                                   void *arg);
static void pgfdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue);
static void pgfdw_reject_incomplete_xact_state_change(ConnCacheEntry *entry);
static bool pgfdw_cancel_query(Conn *conn);
static bool pgfdw_exec_cleanup_query(Conn *conn, const char *query,
                                     bool ignore_errors);


/*
 * Get a Conn* which can be used to execute queries on the remote PostgreSQL
 * server with the user's authorization.  A new connection is established
 * if we don't already have a suitable one, and a transaction is opened at
 * the right subtransaction nesting depth if we didn't do that already.
 *
 * will_prep_stmt must be true if caller intends to create any prepared
 * statements.  Since those don't go away automatically at transaction end
 * (not even on error), we need this flag to cue manual cleanup.
 */
Conn *
GetConnection(UserMapping *user, bool will_prep_stmt, bool read)
{
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	elog(DEBUG2, "> %s:%d", __FUNCTION__, __LINE__);
	/* First time through, initialize connection cache hashtable */
	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		/* allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("clickhousedb_fdw connections", 8,
		                             &ctl,
		                             HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		/*
		 * Register some callback functions that manage connection cleanup.
		 * This should be done just once in each backend.
		 */
		RegisterXactCallback(pgfdw_xact_callback, NULL);
		RegisterSubXactCallback(pgfdw_subxact_callback, NULL);
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
		                              pgfdw_inval_callback, (Datum) 0);
		CacheRegisterSyscacheCallback(USERMAPPINGOID,
		                              pgfdw_inval_callback, (Datum) 0);
	}

	/* Set flag that we did GetConnection during the current transaction */
	xact_got_connection = true;

	/* Create hash key for the entry.  Assume no pad bytes in key struct */
	key.userid = user->umid;
	key.read = read;

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		/*
		 * We need only clear "conn" here; remaining fields will be filled
		 * later when "conn" is set.
		 */
		entry->conn = NULL;
	}

	/* Reject further use of connections which failed abort cleanup. */
	pgfdw_reject_incomplete_xact_state_change(entry);

	/*
	 * If the connection needs to be remade due to invalidation, disconnect as
	 * soon as we're out of all transactions.
	 */
	if (entry->conn != NULL && entry->invalidated && entry->xact_depth == 0)
	{
		elog(DEBUG3, "closing connection %p for option changes to take effect",
		     entry->conn);
		disconnect_pg_server(entry);
	}

	/*
	 * We don't check the health of cached connection here, because it would
	 * require some overhead.  Broken connection will be detected when the
	 * connection is actually used.
	 */

	/*
	 * If cache entry doesn't have a connection, we have to establish a new
	 * connection.  (If connect_pg_server throws an error, the cache entry
	 * will remain in a valid empty state, ie conn == NULL.)
	 */
	if (entry->conn == NULL)
	{
		ForeignServer *server = GetForeignServer(user->serverid);

		/* Reset all transient state fields, to be sure all are clean */
		entry->xact_depth = 0;
		entry->have_prep_stmt = false;
		entry->have_error = false;
		entry->changing_xact_state = false;
		entry->invalidated = false;
		entry->read = read;
		entry->server_hashvalue =
		    GetSysCacheHashValue1(FOREIGNSERVEROID,
		                          ObjectIdGetDatum(server->serverid));
		entry->mapping_hashvalue =
		    GetSysCacheHashValue1(USERMAPPINGOID,
		                          ObjectIdGetDatum(user->umid));

		/* Now try to make the connection */
		entry->conn = clickhouse_gate->connect(server, user);

		elog(DEBUG3,
		     "new clickhousedb_fdw connection %p for server \"%s\" (user mapping oid %u, userid %u)",
		     entry->conn, server->servername, user->umid, user->userid);
	}

	/*
	 * Start a new transaction or subtransaction if needed.
	 */
	begin_remote_xact(entry);

	/* Remember if caller will prepare statements */
	entry->have_prep_stmt |= will_prep_stmt;

	elog(DEBUG2, "< %s:%d", __FUNCTION__, __LINE__);
	return entry->conn;
}

/*
 * Connect to remote server using specified server and user mapping properties.
 */
static Conn *
connect_pg_server(ForeignServer *server, UserMapping *user)
{
	Conn	   *volatile conn = NULL;
	char       *driver = psprintf("%s/%s", pkglib_path, "libclickhouse_odbc.so");
	char       *host = "127.0.0.1";
	int        port = 8123;
	char       *username = "";
	char       *password = "";
	char       *dbname = "default";
	char       error[512];

	ExtractConnectionOptions(server->options, &driver, &host, &port, &dbname,
	                         &username, &password);
	ExtractConnectionOptions(user->options, &driver, &host, &port, &dbname,
	                         &username, &password);

	/* verify connection parameters and make connection */
	check_conn_params(password, user);

	conn = odbc_connect(driver, host, port, dbname, username, password, error);
	if (conn == NULL)
		ereport(ERROR,
		        (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
		         errmsg("could not connect to server \"%s\"",
		                server->servername),
		         errdetail_internal("%s", pchomp(error))));

	/* Prepare new session for use */
	configure_remote_session(conn);

	return conn;
}

/*
 * Disconnect any open connection for a connection cache entry.
 */
static void
disconnect_pg_server(ConnCacheEntry *entry)
{
	if (entry->conn != NULL)
	{
		entry->conn = NULL;
	}
}

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

	if (password[0] == '\0')
		ereport(ERROR,
		        (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
		         errmsg("password is required"),
		         errdetail("Non-superusers must provide a password in the user mapping.")));
}

/*
 * Issue SET commands to make sure remote session is configured properly.
 *
 * We do this just once at connection, assuming nothing will change the
 * values later.  Since we'll never send volatile function calls to the
 * remote, there shouldn't be any way to break this assumption from our end.
 * It's possible to think of ways to break it at the remote end, eg making
 * a foreign table point to a view that includes a set_config call ---
 * but once you admit the possibility of a malicious view definition,
 * there are any number of ways to break things.
 */
static void
configure_remote_session(Conn *conn)
{
}

/*
 * Convenience subroutine to issue a non-data-returning SQL command to remote
 */
static void
do_sql_command(Conn *conn, const char *sql)
{
	if (!odbc_prepare(conn, (char *)sql))
	{
		chfdw_report_error(ERROR, conn, false, sql);
	}

	if (!odbc_execute(conn))
	{
		chfdw_report_error(ERROR, conn, false, sql);
	}
}

static void
begin_remote_xact(ConnCacheEntry *entry)
{

}

/*
 * Release connection reference count created by calling GetConnection.
 */
void
ReleaseConnection(Conn *conn)
{
	/*
	 * Currently, we don't actually track connection references because all
	 * cleanup is managed on a transaction or subtransaction basis instead. So
	 * there's nothing to do here.
	 */
}

/*
 * Assign a "unique" number for a cursor.
 *
 * These really only need to be unique per connection within a transaction.
 * For the moment we ignore the per-connection point and assign them across
 * all connections in the transaction, but we ask for the connection to be
 * supplied in case we want to refine that.
 *
 * Note that even if wraparound happens in a very long transaction, actual
 * collisions are highly improbable; just be sure to use %u not %d to print.
 */
unsigned int
GetCursorNumber(Conn *conn)
{
	return ++cursor_number;
}

/*
 * Assign a "unique" number for a prepared statement.
 *
 * This works much like GetCursorNumber, except that we never reset the counter
 * within a session.  That's because we can't be 100% sure we've gotten rid
 * of all prepared statements on all connections, and it's not really worth
 * increasing the risk of prepared-statement name collisions by resetting.
 */
unsigned int
GetPrepStmtNumber(Conn *conn)
{
	return ++prep_stmt_number;
}

/*
 * Submit a query and wait for the result.
 *
 * This function is interruptible by signals.
 *
 * Caller is responsible for the error handling on the result.
 */
void
chfdw_exec_query(Conn *conn, const char *query)
{
	/*
	 * Submit a query.  Since we don't use non-blocking mode, this also can
	 * block.  But its risk is relatively small, so we ignore that for now.
	 */
	if (!odbc_prepare(conn, (char *)query))
	{
		chfdw_report_error(ERROR, conn, false, query);
	}

	if (!odbc_execute(conn))
	{
		chfdw_report_error(ERROR, conn, false, query);
	}
}

/*
 * Report an error we got from the remote server.
 *
 * elevel: error level to use (typically ERROR, but might be less)
 * res: CHresult containing the error
 * conn: connection we did the query on
 * clear: if true, CHclear the result (otherwise caller will handle it)
 * sql: NULL, or text of remote command we tried to execute
 *
 * Note: callers that choose not to throw ERROR for a remote error are
 * responsible for making sure that the associated ConnCacheEntry gets
 * marked with have_error = true.
 */
void
chfdw_report_error(int elevel, Conn *conn,
                   bool clear, const char *sql)
{
	char *message_primary = pchomp(conn->error);
	ereport(ERROR,
	        (errcode(ERRCODE_CONNECTION_EXCEPTION),
	         errmsg("%s\nquery: %s",
	                message_primary, sql)));
}

/*
 * pgfdw_xact_callback --- cleanup at main-transaction end.
 */
static void
pgfdw_xact_callback(XactEvent event, void *arg)
{
}

/*
 * pgfdw_subxact_callback --- cleanup at subtransaction end.
 */
static void
pgfdw_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
                       SubTransactionId parentSubid, void *arg)
{
}

/*
 * Connection invalidation callback function
 *
 * After a change to a pg_foreign_server or pg_user_mapping catalog entry,
 * mark connections depending on that entry as needing to be remade.
 * We can't immediately destroy them, since they might be in the midst of
 * a transaction, but we'll remake them at the next opportunity.
 *
 * Although most cache invalidation callbacks blow away all the related stuff
 * regardless of the given hashvalue, connections are expensive enough that
 * it's worth trying to avoid that.
 *
 * NB: We could avoid unnecessary disconnection more strictly by examining
 * individual option values, but it seems too much effort for the gain.
 */
static void
pgfdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

	/* ConnectionHash must exist already, if we're registered */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore invalid entries */
		if (entry->conn == NULL)
		{
			continue;
		}

		/* hashvalue == 0 means a cache reset, must clear all state */
		if (hashvalue == 0 ||
		        (cacheid == FOREIGNSERVEROID &&
		         entry->server_hashvalue == hashvalue) ||
		        (cacheid == USERMAPPINGOID &&
		         entry->mapping_hashvalue == hashvalue))
		{
			entry->invalidated = true;
		}
	}
}

/*
 * Raise an error if the given connection cache entry is marked as being
 * in the middle of an xact state change.  This should be called at which no
 * such change is expected to be in progress; if one is found to be in
 * progress, it means that we aborted in the middle of a previous state change
 * and now don't know what the remote transaction state actually is.
 * Such connections can't safely be further used.  Re-establishing the
 * connection would change the snapshot and roll back any writes already
 * performed, so that's not an option, either. Thus, we must abort.
 */
static void
pgfdw_reject_incomplete_xact_state_change(ConnCacheEntry *entry)
{
	HeapTuple	tup;
	Form_pg_user_mapping umform;
	ForeignServer *server;

	/* nothing to do for inactive entries and entries of sane state */
	if (entry->conn == NULL || !entry->changing_xact_state)
	{
		return;
	}

	/* make sure this entry is inactive */
	disconnect_pg_server(entry);

	/* find server name to be shown in the message below */
	tup = SearchSysCache1(USERMAPPINGOID,
	                      ObjectIdGetDatum(entry->key.userid));
	if (!HeapTupleIsValid(tup))
	{
		elog(ERROR, "cache lookup failed for user mapping %u", entry->key.userid);
	}
	umform = (Form_pg_user_mapping) GETSTRUCT(tup);
	server = GetForeignServer(umform->umserver);
	ReleaseSysCache(tup);

	ereport(ERROR,
	        (errcode(ERRCODE_CONNECTION_EXCEPTION),
	         errmsg("connection to server \"%s\" was lost",
	                server->servername)));
}

/*
 * Cancel the currently-in-progress query (whose query text we do not have)
 * and ignore the result.  Returns true if we successfully cancel the query
 * and discard any pending result, and false if not.
 */
static bool
pgfdw_cancel_query(Conn *conn)
{
	return true;
}

/*
 * Submit a query during (sub)abort cleanup and wait up to 30 seconds for the
 * result.  If the query is executed without error, the return value is true.
 * If the query is executed successfully but returns an error, the return
 * value is true if and only if ignore_errors is set.  If the query can't be
 * sent or times out, the return value is false.
 */
static bool
pgfdw_exec_cleanup_query(Conn *conn, const char *query, bool ignore_errors)
{
	return true;
}

