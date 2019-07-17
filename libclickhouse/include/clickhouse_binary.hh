#ifndef CLICKHOUSE_BINARY_H
#define CLICKHOUSE_BINARY_H

typedef struct ch_binary_connection_t ch_binary_connection_t;
typedef struct ch_binary_response_t
{
	bool				success;
	void			   *values;
	size_t				columns_count;

	char				query_id[30];
	char			   *error;
	int					errorlen;
} ch_binary_response_t;

#ifdef __cplusplus
extern "C" {
#endif

extern ch_binary_connection_t *ch_binary_connect(char *host, int port,
		char *database, char *user, char *password);
extern void ch_binary_close(ch_binary_connection_t *conn);
extern ch_binary_response_t *ch_binary_simple_query(ch_binary_connection_t *conn,
		const char *query);
extern void ch_binary_response_free(ch_binary_response_t *resp);

#ifdef __cplusplus
}
#endif

#endif
