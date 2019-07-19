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
typedef unsigned __int128 uint128_t;
typedef struct {
	ch_binary_response_t	*resp;
	ch_binary_coltype		*coltypes;

	size_t	block;		/* current block */
	size_t	row;		/* row in current block */
	void   *gc;			/* allocated objects while reading */
	char   *error;
	bool	done;
} ch_binary_read_state_t;

#ifdef __cplusplus
extern "C" {
#endif

extern ch_binary_connection_t *ch_binary_connect(char *host, int port,
		char *database, char *user, char *password);
extern void ch_binary_close(ch_binary_connection_t *conn);
extern ch_binary_response_t *ch_binary_simple_query(ch_binary_connection_t *conn,
		const char *query);
extern void ch_binary_response_free(ch_binary_response_t *resp);

void ch_binary_read_state_init(ch_binary_read_state_t *state, ch_binary_response_t *resp);
void ch_binary_read_state_free(ch_binary_read_state_t *state);
void **ch_binary_read_row(ch_binary_read_state_t *state);

#ifdef __cplusplus
}
#endif

#endif
