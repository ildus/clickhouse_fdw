#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <clickhouse_http.h>
#include <clickhouse_internal.h>

static char curl_error_buffer[CURL_ERROR_SIZE];
static bool curl_error_happened = false;
static int	curl_verbose = 0;

void ch_http_init(int verbose)
{
	curl_verbose = verbose;
	curl_global_init(CURL_GLOBAL_ALL);
}

ch_http_connection_t *ch_http_connection(char *connstring)
{
	int rc;

	curl_error_happened = false;
	ch_http_connection_t *conn = malloc(sizeof(ch_http_connection_t));
	if (conn == NULL)
		return NULL;

	conn->curl = curl_easy_init();
	if (!conn->curl)
	{
		snprintf(curl_error_buffer, CURL_ERROR_SIZE, "OOM");
		goto cleanup;
	}

	conn->url = curl_url();
	if (!conn->url)
	{
		snprintf(curl_error_buffer, CURL_ERROR_SIZE, "OOM");
		goto cleanup;
	}

	rc = curl_url_set(conn->url, CURLUPART_URL, connstring, 0);
	if (rc)
	{
		snprintf(curl_error_buffer, CURL_ERROR_SIZE, "set base url: %d", rc);
		goto cleanup;
	}

	rc = curl_url_set(conn->url, CURLUPART_SCHEME, "http", 0);
	if (rc)
	{
		snprintf(curl_error_buffer, CURL_ERROR_SIZE, "set http: %d", rc);
		goto cleanup;
	}

	conn->format = CH_TAB_SEPARATED;
	return conn;

cleanup:
	curl_error_happened = true;
	if (conn->url)
		curl_url_cleanup(conn->url);

	free(conn);
	return NULL;
}

size_t write_data(void *buffer, size_t size, size_t nmemb, void *userp)
{
	ch_http_response_t	*res = userp;
	res->data = buffer;
	res->datasize = size * nmemb;

	return size * nmemb;
}

ch_http_response_t *ch_http_simple_query(ch_http_connection_t *conn, const char *query)
{
	CURLcode	errcode;

	ch_http_response_t	*resp = malloc(sizeof(ch_http_response_t));
	if (resp == NULL)
		return NULL;

	assert(conn && conn->curl);
	assert(conn->format == CH_TAB_SEPARATED);

	curl_easy_reset(conn->curl);
	curl_easy_setopt(conn->curl, CURLOPT_WRITEFUNCTION, write_data);
	curl_easy_setopt(conn->curl, CURLOPT_WRITEDATA, resp);
	curl_easy_setopt(conn->curl, CURLOPT_ERRORBUFFER, curl_error_buffer);
	curl_easy_setopt(conn->curl, CURLOPT_POSTFIELDS, query);
	curl_easy_setopt(conn->curl, CURLOPT_VERBOSE, curl_verbose);
	curl_easy_setopt(conn->curl, CURLOPT_PATH_AS_IS, 1);
	curl_easy_setopt(conn->curl, CURLOPT_CURLU, conn->url);

	curl_error_happened = false;
	errcode = curl_easy_perform(conn->curl);
	if (errcode != CURLE_OK)
	{
		curl_error_happened = true;
		free(resp);
		return NULL;
	}

	// all good with request, but we need http status to make sure
	// query went ok
	curl_easy_getinfo(conn->curl, CURLINFO_RESPONSE_CODE, &resp->http_status);
	if (curl_verbose && resp->http_status != 200)
		fprintf(stderr, "%s", resp->data);

	return resp;
}

void ch_http_close(ch_http_connection_t *conn)
{
	curl_url_cleanup(conn->url);
	curl_easy_cleanup(conn->curl);
}

char *ch_http_last_error(void)
{
	if (curl_error_happened)
		return strdup(curl_error_buffer);

	return NULL;
}
