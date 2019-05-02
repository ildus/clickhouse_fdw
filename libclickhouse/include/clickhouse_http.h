#ifndef CLICKHOUSE_HTTP_H
#define CLICKHOUSE_HTTP_H

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
} ch_http_response_t;

typedef enum
{
	CH_EOF = -1
} ch_read_status;

typedef struct {
	int		buflen;
	int		curpos;
	char   *val;
} ch_http_read_state;

void ch_http_init(int verbose);
ch_http_connection_t *ch_http_connection(char *connstring);
void ch_http_close(ch_http_connection_t *conn);
ch_http_response_t *ch_http_simple_query(ch_http_connection_t *conn, const char *query);
char *ch_http_last_error(void);
int ch_http_read_tab_separated(ch_http_read_state *state, char *data, int maxpos);

#endif
