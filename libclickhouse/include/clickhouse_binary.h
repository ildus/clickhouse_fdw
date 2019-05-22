#ifndef CLICKHOUSE_BINARY_H
#define CLICKHOUSE_BINARY_H

#include <stdint.h>

ch_binary_connection_t	*ch_binary_connect(char *host, uint16_t port,
	char *default_database, char *user, char *password, char *client_name);
void ch_binary_configure_connection(void *timeouts);
void ch_binary_disconnect(ch_binary_connection_t *conn);

int ch_binary_errno(void);
const char *ch_binary_last_error(void);

#endif
