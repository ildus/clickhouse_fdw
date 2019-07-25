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

typedef enum {
	chb_Void = 0,
	chb_Int8,
	chb_Int16,
	chb_Int32,
	chb_Int64,
	chb_UInt8,
	chb_UInt16,
	chb_UInt32,
	chb_UInt64,
	chb_Float32,
	chb_Float64,
	chb_String,
	chb_FixedString,
	chb_DateTime,
	chb_Date,
	chb_Array,
	chb_Nullable,
	chb_Tuple,
	chb_Enum8,
	chb_Enum16,
	chb_UUID,
} ch_binary_coltype;

/* gcc should support this */
typedef struct {
	ch_binary_response_t	*resp;
	ch_binary_coltype		*coltypes;

	size_t	block;		/* current block */
	size_t	row;		/* row in current block */
	void   *gc;			/* allocated objects while reading */
	char   *error;
	bool	done;
} ch_binary_read_state_t;

typedef struct {
	size_t				len;
	ch_binary_coltype  *coltypes;
	void			  **values;
} ch_binary_tuple_t;

typedef struct {
	size_t				len;
	ch_binary_coltype	coltype;
	void			  **values;
} ch_binary_array_t;

typedef struct {
	ch_binary_coltype	coltype;
	char			   *colname;
	void			   *coldata;
	size_t				n_arg;		/* FixedString(n) */
} ch_binary_block_t;

#ifdef __cplusplus
extern "C" {
#endif

extern ch_binary_connection_t *ch_binary_connect(char *host, int port,
		char *database, char *user, char *password, char **error);
extern void ch_binary_close(ch_binary_connection_t *conn);
extern ch_binary_response_t *ch_binary_simple_query(ch_binary_connection_t *conn,
		const char *query, volatile bool *cancel);
extern void ch_binary_response_free(ch_binary_response_t *resp);
extern ch_binary_response_t *ch_binary_simple_insert(ch_binary_connection_t *conn,
	char *table_name, ch_binary_block_t *blocks, size_t nblocks, size_t nrows);

void ch_binary_read_state_init(ch_binary_read_state_t *state, ch_binary_response_t *resp);
void ch_binary_read_state_free(ch_binary_read_state_t *state);
void **ch_binary_read_row(ch_binary_read_state_t *state);

#ifdef __cplusplus
}
#endif

#endif
