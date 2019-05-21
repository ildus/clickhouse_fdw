#include "curl/curl.h"
#include <arpa/inet.h>

#define min(A, B) ((B)<(A)?(B):(A))
#define max(A, B) ((B)>(A)?(B):(A))

typedef struct ch_http_connection_t
{
	CURL			   *curl;
	CURLU			   *url;
	char			   *base_url;
	size_t				base_url_len;
} ch_http_connection_t;

typedef struct ch_binary_connection_t
{
	int					fd;		/* comm socket */
	char			   *host;
	int					port;
	char			   *default_database;
	char			   *user;
	char			   *password;
	char			   *client_name;

	/* server part */
	char			   *server_name;
	char			   *server_display_name;
	char			   *server_timezone;
	uint32_t			server_version_minor;
	uint32_t			server_version_major;
	uint32_t			server_revision;
	uint32_t			server_version_patch;
} ch_binary_connection_t;

enum {
	CH_Client_Hello = 0,               /// Name, version, revision, default DB
	CH_Client_Query = 1,               /// Query id, query settings, stage up to which the query must be executed,
										/// whether the compression must be used,
										/// query text (without data for INSERTs).
	CH_Client_Data = 2,                /// A block of data (compressed or not).
	CH_Client_Cancel = 3,              /// Cancel the query execution.
	CH_Client_Ping = 4,                /// Check that connection to the server is alive.
	CH_Client_TablesStatusRequest = 5, /// Check status of tables on the server.
	CH_Client_KeepAlive = 6            /// Keep the connection alive
};

enum {
	CH_Hello = 0,                /// Name, version, revision.
	CH_Data = 1,                 /// A block of data (compressed or not).
	CH_Exception = 2,            /// The exception during query execution.
	CH_Progress = 3,             /// Query execution progress: rows read, bytes read.
	CH_Pong = 4,                 /// Ping response
	CH_EndOfStream = 5,          /// All packets were transmitted
	CH_ProfileInfo = 6,          /// Packet with profiling info.
	CH_Totals = 7,               /// A block with totals (compressed or not).
	CH_Extremes = 8,             /// A block with minimums and maximums (compressed or not).
	CH_TablesStatusResponse = 9, /// A response to TablesStatus request.
	CH_Log = 10,                 /// System logs of the query execution
	CH_TableColumns = 11,        /// Columns' description for default values calculation
};

typedef struct ch_binary_io_state_t
{
	char   *buf;
	size_t	len;
	size_t	pos;
} ch_binary_io_state_t;

extern void ch_error(const char *fmt, ...);

inline static bool
check_io_boundary(ch_binary_io_state_t *iostate, size_t len)
{
    if (iostate->pos + len >= iostate->len)
	{
		ch_error("too large data block: %d, max: %d", len, iostate->len - iostate->pos);
		return false;
	}
	return true;
}

inline static uint32_t
read_uint32_binary(ch_binary_io_state_t *iostate)
{
	uint32_t	res;
	memcpy(&res, iostate->buf + iostate->pos, sizeof(uint32_t));
	iostate->pos += sizeof(uint32_t);
	return res;
}

inline static void
extend_io_state(ch_binary_io_state_t *iostate, size_t len)
{
	if (iostate->buf == NULL)
	{
		iostate->len = max(1024, len);
		iostate->buf = malloc(iostate->len);
	}
	else if (iostate->pos + len >= iostate->len)
	{
		iostate->len += max(64, len * 2);
		iostate->buf = realloc(iostate->buf, iostate->len);
	}
}

inline static void
write_uint32_binary(ch_binary_io_state_t *iostate, uint32_t val)
{
	extend_io_state(iostate, sizeof(uint32_t));
	memcpy(iostate->buf + iostate->pos, &val, sizeof(uint32_t));
	iostate->pos += sizeof(uint32_t);
}

inline static char *
read_string_binary(ch_binary_io_state_t *iostate)
{
    size_t size = read_uint32_binary(iostate);

	if (check_io_boundary(iostate, size))
	{
		char *s = malloc(size + 1);
		memcpy(s, iostate->buf + iostate->pos, size);
		iostate->pos += size;
		return s;
	}

	return NULL;
}

inline static void
write_pod_binary(ch_binary_io_state_t *iostate, char *data, size_t len)
{
	extend_io_state(iostate, len);
	memcpy(iostate->buf + iostate->pos, data, len);
	iostate->pos += len;
}

inline static void
write_string_binary(ch_binary_io_state_t *iostate, char *s)
{
	int len = strlen(s);
	write_uint32_binary(iostate, len);
	write_pod_binary(iostate, s, len);
}

inline static void
init_io_state(ch_binary_io_state_t *iostate)
{
	iostate->buf = NULL;
	iostate->len = 0;
	iostate->pos = 0;
}

inline static void
reset_io_state(ch_binary_io_state_t *iostate)
{
	if (iostate->buf)
		free(iostate->buf);
}

inline static void
write_char_binary(ch_binary_io_state_t *iostate, char val)
{
	extend_io_state(iostate, 1);
	iostate->buf[iostate->pos++] = val;
}

inline static char
read_char_binary(ch_binary_io_state_t *iostate)
{
	return iostate->buf[iostate->pos++];
}

inline static void
write_bool_binary(ch_binary_io_state_t *iostate, bool val)
{
    write_char_binary(iostate, val ? '1' : '0');
}

inline static bool
read_bool_binary(ch_binary_io_state_t *iostate)
{
	return read_char_binary(iostate) == '1';
}
