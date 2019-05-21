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
#include "clickhouse_internal.h"
#include "clickhouse_binary.h"
#include "clickhouse_config.h"

static char last_error[2048];

#ifdef __GNUC__
	 __attribute__ (( format( gnu_printf, 1, 2 ) ))
#endif
void
ch_error(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(last_error, sizeof(last_error), fmt, args);
	va_end(args);
}

static int
ch_connect(struct sockaddr *sa)
{
	int addrlen,
		rc,
		fd;

	fd = socket(sa->sa_family, SOCK_STREAM, 0);
	if (fd == -1)
		return -1;

	if (sa->sa_family == AF_INET)
		addrlen = sizeof(struct sockaddr_in);
	else if (sa->sa_family == AF_INET6)
		addrlen = sizeof(struct sockaddr_in6);
	else if (sa->sa_family == AF_UNIX)
		addrlen = sizeof(struct sockaddr_un);
	else
		return -1;

	rc = connect(fd, sa, addrlen);
	if (rc)
		return -1;

	return fd;
}

static bool
has_control_character(char *s)
{
	int i = strlen(s);
	while (--i)
		if (s[i] < 31)
			return true;

	return false;
};

static bool
ch_binary_write(ch_binary_connection_t *conn, ch_binary_io_state_t *io)
{
	int n;
	int flags = 0;

#ifdef HAVE_NOSIGNAL
	flags = MSG_NOSIGNAL;
#endif

again:
	n = send(conn->fd, io->buf, io->pos, flags);
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

static bool
ch_binary_read(ch_binary_connection_t *conn, ch_binary_io_state_t *io)
{
	char	buf[8192];
	int		n;

again:
	while ((n = recv(conn->fd, buf, sizeof(buf), 0)) != 0)
	{
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

			return false;
		}

		extend_io_state(io, n);
		write_pod_binary(io, buf, n);
	}

	return true;
}

/* send hello packet */
static bool
say_hello(ch_binary_connection_t *conn)
{
	ch_binary_io_state_t	io;
	init_io_state(&io);

    if (has_control_character(conn->default_database)
        || has_control_character(conn->user)
        || has_control_character(conn->password)
        || has_control_character(conn->client_name))
	{
        ch_error("Parameters 'default_database', 'user' and 'password' must not contain ASCII control characters");
		return false;
	}

    write_uint32_binary(&io, CH_Client_Hello);
    write_string_binary(&io, conn->client_name);
    write_uint32_binary(&io, VERSION_MAJOR);
    write_uint32_binary(&io, VERSION_MINOR);
    write_uint32_binary(&io, VERSION_REVISION);
    write_string_binary(&io, conn->default_database);
    write_string_binary(&io, conn->user);
    write_string_binary(&io, conn->password);

	bool res = ch_binary_write(conn, &io);
	reset_io_state(&io);
	return res;
}

/* read hello packet */
static bool
get_hello(ch_binary_connection_t *conn)
{
	ch_binary_io_state_t	io;
	uint32_t	packet_type;

	init_io_state(&io);
	if (!ch_binary_read(conn, &io))
		return false;

	packet_type = read_uint32_binary(&io);

    if (packet_type == CH_Hello)
    {
		conn->server_name = read_string_binary(&io);
		conn->server_version_major = read_uint32_binary(&io);
		conn->server_version_minor = read_uint32_binary(&io);
		conn->server_revision = read_uint32_binary(&io);

        if (conn->server_revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE)
            conn->server_timezone = read_string_binary(&io);
        if (conn->server_revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME)
            conn->server_display_name = read_string_binary(&io);
        if (conn->server_revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH)
			conn->server_version_patch = read_uint32_binary(&io);
        else
            conn->server_version_patch = conn->server_revision;
    }
    else
    {
		ch_error("wrong packet on hello: %u", packet_type);
		return false;
    }
	return true;
}

ch_binary_connection_t *
ch_binary_connect(char *host, uint16_t port, char *default_database,
		char *user, char *password, char *client_name)
{
	ch_binary_connection_t	*conn;

	struct addrinfo *ai = NULL;
	struct sockaddr	*saddr = NULL;

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

	int fd = ch_connect(saddr);

	if (ai)
		freeaddrinfo(ai);

	if (fd == -1)
	{
		ch_error("could not create connection to ClickHouse");
		return NULL;
	}

	conn = calloc(sizeof(ch_binary_connection_t), 1);
	conn->fd = fd;
	conn->host = strdup(host);
	conn->port = port;
	if (user)
		conn->user = strdup(user);
	if (password)
		conn->password = strdup(password);
	if (default_database)
		conn->default_database = strdup(default_database);
	if (client_name)
		conn->client_name = strdup(client_name);

	if (say_hello(conn) && get_hello(conn))
		/* all good */
		return conn;

	ch_binary_free_connection(conn);
	return NULL;
}

void
ch_binary_disconnect(ch_binary_connection_t *conn)
{
	conn->fd = 0;
}

void
ch_binary_free_connection(ch_binary_connection_t *conn)
{
	if (conn->fd)
		close(conn->fd);

	free(conn->host);

	if (conn->default_database)
		free(conn->default_database);
	if (conn->user)
		free(conn->user);
	if (conn->password)
		free(conn->password);
	if (conn->client_name)
		free(conn->client_name);
	if (conn->server_name)
		free(conn->server_name);
	if (conn->server_timezone)
		free(conn->server_timezone);

	free(conn);
}
