#ifndef CLICKHOUSE_HTTP_H
#define CLICKHOUSE_HTTP_H

#include <stdint.h>

typedef enum
{
	CH_TAB_SEPARATED
} ch_http_format_t;

typedef struct ch_http_connection_t ch_http_connection_t;
typedef struct ch_http_response_t
{
	ch_http_format_t	format;
	char			   *data;
	size_t				datasize;
	long				http_status;
	char				query_id[30];
} ch_http_response_t;

typedef enum
{
	CH_CONT,
	CH_EOL,
	CH_EOF
} ch_read_status;

typedef struct {
	char   *data;
	size_t	buflen;
	size_t	curpos;
	size_t	maxpos;
	char   *val;
	bool	done;
} ch_http_read_state;

void ch_http_init(int verbose, uint32_t query_id_prefix);
void ch_http_set_progress_func(void *progressfunc);
ch_http_connection_t *ch_http_connection(char *connstring);
void ch_http_close(ch_http_connection_t *conn);
ch_http_response_t *ch_http_simple_query(ch_http_connection_t *conn, const char *query);
char *ch_http_last_error(void);
void ch_http_read_state_init(ch_http_read_state *state, char *data, size_t datalen);
void ch_http_read_state_free(ch_http_read_state *state);
int ch_http_read_next(ch_http_read_state *state);
void ch_http_response_free(ch_http_response_t *resp);

#endif
