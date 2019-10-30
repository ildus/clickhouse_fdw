#ifndef CLICKHOUSE_BINARY_H
#define CLICKHOUSE_BINARY_H

typedef struct ch_binary_connection_t ch_binary_connection_t;
typedef struct ch_binary_response_t
{
	void			   *values;
	size_t				columns_count;
	size_t				blocks_count;
	char			   *error;
	bool				success;
} ch_binary_response_t;

/* gcc should support this */
typedef struct {
	ch_binary_response_t	*resp;
	Oid		*coltypes;
	Datum	*values;
	bool	*nulls;

	size_t	block;		/* current block */
	size_t	row;		/* row in current block */
	void   *gc;			/* allocated objects while reading */
	char   *error;
	bool	done;
} ch_binary_read_state_t;

typedef struct {
	HeapTuple	tup;
	TupleDesc	desc;
} ch_binary_tuple_t;

#ifdef __cplusplus
extern "C" {
#endif

extern ch_binary_connection_t *ch_binary_connect(char *host, int port,
		char *database, char *user, char *password, char **error);
extern void ch_binary_close(ch_binary_connection_t *conn);
extern ch_binary_response_t *ch_binary_simple_query(ch_binary_connection_t *conn,
		const char *query, bool (*check_cancel)(void));
extern void ch_binary_response_free(ch_binary_response_t *resp);
extern ch_binary_response_t *ch_binary_simple_insert(ch_binary_connection_t *conn,
	char *table_name, void *blocks, size_t nblocks, size_t nrows);

void ch_binary_read_state_init(ch_binary_read_state_t *state, ch_binary_response_t *resp);
void ch_binary_read_state_free(ch_binary_read_state_t *state);
bool ch_binary_read_row(ch_binary_read_state_t *state);

#ifdef __cplusplus
}
#endif

#endif
