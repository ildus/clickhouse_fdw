#ifndef CLICKHOUSE_NET_H
#define CLICKHOUSE_NET_H

#include <stdlib.h>
#include <stdint.h>
#include <assert.h>

#define min(A, B) ((B)<(A)?(B):(A))
#define max(A, B) ((B)>(A)?(B):(A))

typedef struct ch_binary_connection ch_binary_connection_t;

 // readahead code borrowed from "yandex/odyssey"
typedef struct ch_readahead_t
{
	int		sock;
	struct timeval *timeout;

	char   *buf;
	size_t  size;
	size_t  pos;
	size_t	pos_read;
} ch_readahead_t;

struct ch_binary_connection
{
	int					sock;		/* comm socket */
	char			   *host;
	int					port;
	char			   *default_database;
	char			   *user;
	char			   *password;
	char			   *client_name;

	ch_readahead_t		in;
	ch_readahead_t		out;

	/* settings */
	int					compression;

	/* timeouts */
	int					connection_timeout;
	struct timeval		send_timeout;
	struct timeval		recv_timeout;

	/* server part */
	char			   *server_name;
	char			   *server_display_name;
	char			   *server_timezone;
	uint64_t			server_version_minor;
	uint64_t			server_version_major;
	uint64_t			server_revision;
	uint64_t			server_version_patch;
};

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
	CH_MaxPacketType
};

extern void ch_error(const char *fmt, ...);
extern int sock_read(ch_readahead_t *readahead);
bool ch_ping(ch_binary_connection_t *conn);

static inline int
ch_readahead_init(int sock, ch_readahead_t *readahead, struct timeval *timeout)
{
	readahead->sock     = sock;
	readahead->buf      = malloc(8192);
	if (readahead->buf == NULL)
		return -1;

	readahead->size     = 8192;
	readahead->pos      = 0;
	readahead->pos_read = 0;
	readahead->timeout  = timeout;

	return 0;
}

static inline void
ch_readahead_free(ch_readahead_t *readahead)
{
	if (readahead->buf)
		free(readahead->buf);
}

static inline size_t
ch_readahead_left(ch_readahead_t *readahead)
{
	assert(readahead->buf);
	return readahead->size - readahead->pos;
}

static inline size_t
ch_readahead_unread(ch_readahead_t *readahead)
{
	return readahead->pos - readahead->pos_read;
}

static inline int
ch_readahead_unread_check(ch_readahead_t *readahead, size_t size)
{
	/*
	 * We can't check for `size` bytes since we usually expect char
	 * or variable-length encoded uint64
	 * */
	if (ch_readahead_unread(readahead) == 0)
	{
		int n = sock_read(readahead);
		if (n <= 0)
		{
			ch_error("could not read data from socket, needed %d (max) bytes", size);
			return n;
		}
	}
	return ch_readahead_unread(readahead);
}

static inline char*
ch_readahead_pos(ch_readahead_t *readahead)
{
	return readahead->buf + readahead->pos;
}

static inline char*
ch_readahead_pos_read(ch_readahead_t *readahead)
{
	return readahead->buf + readahead->pos_read;
}

static inline void
ch_readahead_pos_advance(ch_readahead_t *readahead, int value)
{
	readahead->pos += value;
}

static inline void
ch_readahead_pos_read_advance(ch_readahead_t *readahead, int value)
{
	readahead->pos_read += value;
}

static inline void
ch_readahead_reuse(ch_readahead_t *readahead)
{
	size_t unread = ch_readahead_unread(readahead);

	if (unread == 0) {
		readahead->pos      = 0;
		readahead->pos_read = 0;
		return;
	}

	/* save next packet header */
	memmove(readahead->buf, readahead->buf + readahead->pos_read, unread);
	readahead->pos      = unread;
	readahead->pos_read = 0;
}

inline static void
ch_readahead_extend(ch_readahead_t *readahead, size_t add)
{
	assert(readahead->buf != NULL);
	if (ch_readahead_left(readahead) < add)
	{
		readahead->size += max(64, add * 2);
		readahead->buf = realloc(readahead->buf, readahead->size);
	}
}

inline static bool
check_io_boundary(ch_readahead_t *readahead, size_t size)
{
    if (readahead->pos_read + size >= readahead->size)
	{
		ch_error("too large data block: %d, max: %d", size, readahead->size - readahead->pos);
		return false;
	}
	return true;
}

inline static uint64_t
read_varuint_binary(ch_readahead_t *readahead)
{
	uint64_t	x = 0;
	size_t		n = ch_readahead_unread_check(readahead, sizeof(uint64_t));

    x = 0;
    for (size_t i = 0; i < min(9, n); ++i)
    {
		uint64_t byte = ch_readahead_pos_read(readahead)[0];
		ch_readahead_pos_read_advance(readahead, sizeof(uint8_t));

        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80))
			break;
    }

	return x;
}

inline static void
write_varuint_binary(ch_readahead_t *readahead, uint64_t x)
{
	ch_readahead_extend(readahead, sizeof(uint64_t) + 1);

    for (size_t i = 0; i < 9; ++i)
    {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F)
            byte |= 0x80;

		ch_readahead_pos(readahead)[0] = byte;
		ch_readahead_pos_advance(readahead, sizeof(uint8_t));

        x >>= 7;
        if (!x)
            break;
    }
}

inline static char *
read_string_binary(ch_readahead_t *readahead)
{
	size_t left;
    size_t size = read_varuint_binary(readahead),
		   size_left = size;

	if (check_io_boundary(readahead, size))
	{
		char *s = malloc(size + 1),
			 *p = s;

		while (size_left && ((left = ch_readahead_left(readahead)) > 0))
		{
			size_t cp = left > size_left ? size_left : left;

			memcpy(p, ch_readahead_pos_read(readahead), cp);
			ch_readahead_pos_read_advance(readahead, cp);
			p += cp;
			size_left -= cp;

			if (size_left == 0)
				break;

			/* free old data and read new */
			ch_readahead_reuse(readahead);
			if (sock_read(readahead) < 0)
			{
				ch_error("server communication error");
				return NULL;
			}
		}
		s[size] = '\0';
		return s;
	}

	ch_error("string reading error");
	return NULL;
}

inline static char *
read_string_binary_unsafe(ch_readahead_t *readahead)
{
    size_t size = read_varuint_binary(readahead);

	if (check_io_boundary(readahead, size))
	{
		char *s = malloc(size + 1);
		memcpy(s, ch_readahead_pos_read(readahead), size);
		s[size] = '\0';
		ch_readahead_pos_read_advance(readahead, size);
		return s;
	}

	return NULL;
}

inline static void
write_pod_binary(ch_readahead_t *readahead, char *data, size_t size)
{
	ch_readahead_extend(readahead, size);
	memcpy(ch_readahead_pos(readahead), data, size);
	ch_readahead_pos_advance(readahead, size);
}

inline static void
write_string_binary(ch_readahead_t *readahead, char *s)
{
	int len = strlen(s);
	write_varuint_binary(readahead, len);
	write_pod_binary(readahead, s, len);
}

inline static void
write_char_binary(ch_readahead_t *readahead, char val)
{
	ch_readahead_extend(readahead, 1);
	ch_readahead_pos(readahead)[0] = val;
	ch_readahead_pos_advance(readahead, sizeof(char));
}

inline static char
read_char_binary(ch_readahead_t *readahead)
{
	if (ch_readahead_unread_check(readahead, sizeof(char)))
	{
		char res = ch_readahead_pos_read(readahead)[0];
		ch_readahead_pos_read_advance(readahead, sizeof(char));
		return res;
	}
	return '\0';
}

inline static void
write_bool_binary(ch_readahead_t *readahead, bool val)
{
    write_char_binary(readahead, val ? '1' : '0');
}

inline static bool
read_bool_binary(ch_readahead_t *readahead)
{
	return read_char_binary(readahead) == '1';
}

#endif /* CLICKHOUSE_NET_H */
