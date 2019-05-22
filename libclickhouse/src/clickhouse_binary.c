#include <stdio.h>
#include <assert.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include "clickhouse_net.h"
#include "clickhouse_binary.h"
#include "clickhouse_config.h"

static bool in_error_state = false;
static char last_error[2048];

#ifdef __GNUC__
	 __attribute__ (( format( gnu_printf, 1, 2 ) ))
#endif
void
ch_error(const char *fmt, ...)
{
	in_error_state = true;

	va_list args;
	va_start(args, fmt);
	vsnprintf(last_error, sizeof(last_error), fmt, args);
	va_end(args);
}

static void
ch_reset_error(void)
{
	in_error_state = false;
}

int
ch_binary_errno(void)
{
	return in_error_state ? 1 : 0;
}

const char *
ch_binary_last_error(void)
{
	if (in_error_state)
		return (const char *) last_error;
	return NULL;
}

static int
ch_connect(struct sockaddr *sa)
{
	int addrlen,
		rc,
		sock;

	sock = socket(sa->sa_family, SOCK_STREAM, 0);
	if (sock == -1)
		return -1;

	if (sa->sa_family == AF_INET)
		addrlen = sizeof(struct sockaddr_in);
	else if (sa->sa_family == AF_INET6)
		addrlen = sizeof(struct sockaddr_in6);
	else if (sa->sa_family == AF_UNIX)
		addrlen = sizeof(struct sockaddr_un);
	else
		return -1;

	rc = connect(sock, sa, addrlen);
	if (rc)
		return -1;

#ifdef TCP_NODELAY
	int on = 1;
	if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY,
				   (char *) &on, sizeof(on)) < 0)
	{
		close(sock);
		ch_error("setsockopt(%s) failed: %m", "TCP_NODELAY");
		return -1;
	}
#endif

	on = 1;
	if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE,
				   (char *) &on, sizeof(on)) < 0)
	{
		close(sock);
		ch_error("setsockopt(%s) failed: %m", "SO_KEEPALIVE");
		return -1;
	}

	return sock;
}

static bool
has_control_character(char *s)
{
	for (size_t i = 0; i < strlen(s); i++)
		if (s[i] < 31)
			return true;

	return false;
};

static bool
ch_binary_send(ch_binary_connection_t *conn)
{
	int n;
	int flags = 0;

#ifdef HAVE_NOSIGNAL
	flags = MSG_NOSIGNAL;
#endif

	ch_reset_error();
	if (ch_readahead_unread(&conn->out) == 0)
		// nothing to send
		return true;

again:
	n = send(conn->sock,
			ch_readahead_pos_read(&conn->out),
			ch_readahead_unread(&conn->out), flags);

	ch_readahead_pos_read_advance(&conn->out, ch_readahead_unread(&conn->out));

	if (n < 0)
	{
		int result_errno = errno;
		switch (result_errno)
		{
#ifdef EAGAIN
			case EAGAIN:
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
#endif
			case EINTR:
				goto again;

			case EPIPE:
#ifdef ECONNRESET
				/* FALL THRU */
			case ECONNRESET:
#endif
				ch_error("server closed the connection unexpectedly");
				break;

			default:
				ch_error("could not send data to server");
				break;
		}

		return false;
	}

	return true;
}

int
sock_read(int sock, ch_readahead_t *readahead)
{
	int		n;
	size_t	left = ch_readahead_left(readahead);

	if (!left)
	{
		/* reader should deal with unread data first */
		return ch_readahead_unread(readahead);
	}

again:
	n = recv(sock, ch_readahead_pos(readahead), left, 0);
	if (n < 0)
	{
		int result_errno = errno;
		switch (result_errno)
		{
#ifdef EAGAIN
			case EAGAIN:
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
#endif
			case EINTR:
				goto again;

#ifdef ECONNRESET
				/* FALL THRU */
			case ECONNRESET:
#endif
				ch_error("server closed the connection unexpectedly");
				break;

			default:
				ch_error("could not send data to server");
				break;
		}

		return -1;
	}

	ch_readahead_pos_advance(readahead, n);
	return n;
}

static int
ch_binary_read_header(ch_binary_connection_t *conn)
{
	uint64_t	val;

	ch_readahead_reuse(&conn->in);
	if (ch_readahead_unread(&conn->in) == 0)
	{
		if (sock_read(conn->sock, &conn->in) <= 0)
			return -1;
	}

	if (ch_readahead_unread(&conn->in) == 0)
	{
		ch_error("server communication error");
		return -1;
	}

	val = read_varuint_binary(&conn->in);
	if (val >= CH_MaxPacketType)
	{
		ch_error("imcompatible server, invalid packet type");
		return -1;
	}

	return (int) val;
}

/* send hello packet */
static bool
say_hello(ch_binary_connection_t *conn)
{
	ch_reset_error();
    if (has_control_character(conn->default_database)
        || has_control_character(conn->user)
        || has_control_character(conn->password)
        || has_control_character(conn->client_name))
	{
        ch_error("Parameters 'default_database', 'user' and 'password' must not contain ASCII control characters");
		return false;
	}

	ch_readahead_reuse(&conn->in);
	assert(conn->out.pos == 0);

    write_varuint_binary(&conn->out, CH_Client_Hello);
    write_string_binary(&conn->out, conn->client_name);
    write_varuint_binary(&conn->out, VERSION_MAJOR);
    write_varuint_binary(&conn->out, VERSION_MINOR);
    write_varuint_binary(&conn->out, VERSION_REVISION);
    write_string_binary(&conn->out, conn->default_database);
    write_string_binary(&conn->out, conn->user);
    write_string_binary(&conn->out, conn->password);

	bool res = ch_binary_send(conn);
	return res;
}

/* read hello packet */
static bool
get_hello(ch_binary_connection_t *conn)
{
	int packet_type;

	ch_reset_error();
	packet_type = ch_binary_read_header(conn);
	if (packet_type < 0)
		return false;

    if (packet_type == CH_Hello)
    {
		conn->server_name = read_string_binary(&conn->in);
		if (conn->server_name == NULL)
			return false;

		conn->server_version_major = read_varuint_binary(&conn->in);
		conn->server_version_minor = read_varuint_binary(&conn->in);
		conn->server_revision = read_varuint_binary(&conn->in);

        if (conn->server_revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE)
		{
            conn->server_timezone = read_string_binary(&conn->in);
			if (conn->server_timezone == NULL)
				return false;
		}
        if (conn->server_revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME)
		{
            conn->server_display_name = read_string_binary(&conn->in);
			if (conn->server_display_name == NULL)
				return false;
		}
        if (conn->server_revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH)
			conn->server_version_patch = read_varuint_binary(&conn->in);
        else
            conn->server_version_patch = conn->server_revision;
    }
    else
    {
		ch_error("wrong packet on hello: %d", packet_type);
		return false;
    }

	if (ch_binary_errno() > 0)
		// something happened in between
		return false;

	return true;
}

ch_binary_connection_t *
ch_binary_connect(char *host, uint16_t port, char *default_database,
		char *user, char *password, char *client_name)
{
	ch_binary_connection_t	*conn;

	struct addrinfo *ai = NULL;
	struct sockaddr	*saddr = NULL;

	ch_reset_error();
	if (!host || !port)
	{
		ch_error("host or port wasn't specified");
		return NULL;
	}

	int rc_resolve = -1;
	if (strchr(host, ':'))
	{
		struct sockaddr_in6 *saddr_v6 = calloc(sizeof(struct sockaddr_in6), 1);

		/* v6 */
		saddr_v6->sin6_family = AF_INET6;
		saddr_v6->sin6_port   = htons(port);
		rc_resolve = inet_pton(AF_INET6, host, &saddr_v6->sin6_addr);
		saddr = (struct sockaddr*) saddr_v6;
	}
	else
	{
		struct sockaddr_in *saddr_v4 = calloc(sizeof(struct sockaddr_in), 1);

		/* v4 or hostname */
		saddr_v4->sin_family = AF_INET;
		saddr_v4->sin_port   = htons(port);
		rc_resolve = inet_pton(AF_INET, host, &saddr_v4->sin_addr);
		saddr = (struct sockaddr*) saddr_v4;
	}

	/* if it wasn't proper ip, try getaddrinfo */
	if (rc_resolve != 1)
	{
		int rc;
		char sport[16];
		snprintf(sport, sizeof(sport), "%d", port);

		/* free calloced saddr from before */
		free(saddr);

		rc = getaddrinfo(host, sport, NULL, &ai);
		if (rc != 0)
		{
			ch_error("could not resolve host and port");
			return NULL;
		}

		assert(ai != NULL);
		saddr = ai->ai_addr;
	}

	int sock = ch_connect(saddr);

	if (ai)
		freeaddrinfo(ai);

	if (sock == -1)
	{
		ch_error("could not create connection to ClickHouse");
		return NULL;
	}

	conn = calloc(sizeof(ch_binary_connection_t), 1);
	conn->sock = sock;
	conn->host = strdup(host);
	conn->port = port;

	/* set default values if needed */
	user = user ? user : "default";
	default_database  = default_database ? default_database : "default";
	password = password ? password : "";
	client_name = client_name ? client_name : "fdw";

	conn->user = strdup(user);
	conn->password = strdup(password);
	conn->default_database = strdup(default_database);
	conn->client_name = strdup(client_name);

	ch_readahead_init(sock, &conn->in);
	ch_readahead_init(sock, &conn->out);

	/* exchange hello packets and initialize server fields in connection */
	if (say_hello(conn) && get_hello(conn))
		/* all good */
		return conn;

	ch_binary_disconnect(conn);
	return NULL;
}

void
ch_binary_disconnect(ch_binary_connection_t *conn)
{
	if (conn->sock)
		close(conn->sock);

	free(conn->host);
	free(conn->default_database);
	free(conn->user);
	free(conn->password);
	free(conn->client_name);

	if (conn->server_name)
		free(conn->server_name);
	if (conn->server_timezone)
		free(conn->server_timezone);

	free(conn);
}
