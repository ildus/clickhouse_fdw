#ifndef CLICKHOUSE_BINARY_H
#define CLICKHOUSE_BINARY_H

#include <stdint.h>

typedef struct ch_binary_timeouts ch_binary_timeouts_t;
typedef struct ch_binary_settings ch_binary_settings_t;
typedef struct ch_binary_connection ch_binary_connection_t;

struct ch_binary_timeouts
{
	int recv_timeout;	/* seconds */
	int send_timeout;	/* seconds */
};

struct ch_binary_settings
{
	int compression;	/* 0 - turned off, 1 - lz4 */
};

ch_binary_connection_t	*ch_binary_connect(char *host, uint16_t port,
	char *default_database, char *user, char *password, char *client_name,
	int connection_timeout);
void ch_binary_configure_connection(ch_binary_connection_t *conn,
	ch_binary_settings_t *settings, ch_binary_timeouts_t *timeouts);
void ch_binary_disconnect(ch_binary_connection_t *conn);

int ch_binary_errno(void);
const char *ch_binary_last_error(void);

#endif
