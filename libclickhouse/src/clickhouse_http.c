#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#include <clickhouse_http.h>
#include <clickhouse_internal.h>

static char curl_error_buffer[CURL_ERROR_SIZE];
static bool curl_error_happened = false;
static int	curl_verbose = 0;
static void *curl_progressfunc = NULL;
static bool curl_initialized = false;
static char ch_query_id_prefix[5];

void ch_http_init(int verbose, uint32_t query_id_prefix)
{
	curl_verbose = verbose;
	snprintf(ch_query_id_prefix, sizeof(ch_query_id_prefix), "%x", query_id_prefix);

	if (!curl_initialized)
	{
		curl_initialized = true;
		curl_global_init(CURL_GLOBAL_ALL);
	}
}

void ch_http_set_progress_func(void *progressfunc)
{
	curl_progressfunc = progressfunc;
}

size_t write_data(void *contents, size_t size, size_t nmemb, void *userp)
{
	size_t realsize			= size * nmemb;
	ch_http_response_t *res	= userp;

	if (res->data == NULL)
		res->data = malloc(realsize + 1);
	else
		res->data = realloc(res->data, res->datasize + realsize + 1);

	memcpy(&(res->data[res->datasize]), contents, realsize);
	res->data[res->datasize] = 0;
	res->datasize += realsize;

	return realsize;
}

ch_http_connection_t *ch_http_connection(char *connstring)
{
	curl_error_happened = false;
	ch_http_connection_t *conn = malloc(sizeof(ch_http_connection_t));
	if (conn == NULL)
	{
		curl_error_happened = true;
		snprintf(curl_error_buffer, CURL_ERROR_SIZE, "OOM");
		return NULL;
	}

	conn->curl = curl_easy_init();
	if (!conn->curl)
		goto cleanup;

	conn->url = curl_url();
	if (!conn->url)
		goto cleanup;

	conn->base_url = strdup(connstring);
	if (conn->base_url == NULL)
		goto cleanup;

	conn->base_url_len = strlen(conn->base_url);
	conn->format = CH_TAB_SEPARATED;

	return conn;

cleanup:
	snprintf(curl_error_buffer, CURL_ERROR_SIZE, "OOM");
	curl_error_happened = true;
	if (conn->url)
		curl_url_cleanup(conn->url);
	if (conn->base_url)
		free(conn->base_url);

	free(conn);
	return NULL;
}

static void set_query_id(ch_http_response_t *resp)
{
	static uint32_t pseudo_unique = 0;
	snprintf(resp->query_id, 4 + 1 + 4, "%s-%x", ch_query_id_prefix,
				++pseudo_unique);
}

ch_http_response_t *ch_http_simple_query(ch_http_connection_t *conn, const char *query)
{
	char		*url;
	CURLcode	errcode;

	ch_http_response_t	*resp = calloc(sizeof(ch_http_response_t), 1);
	if (resp == NULL)
		return NULL;

	set_query_id(resp);

	assert(conn && conn->curl);
	assert(conn->format == CH_TAB_SEPARATED);

	curl_easy_reset(conn->curl);

	/* constant */
	curl_easy_setopt(conn->curl, CURLOPT_WRITEFUNCTION, write_data);
	curl_easy_setopt(conn->curl, CURLOPT_ERRORBUFFER, curl_error_buffer);
	curl_easy_setopt(conn->curl, CURLOPT_PATH_AS_IS, 1);
	curl_easy_setopt(conn->curl, CURLOPT_CURLU, conn->url);

	/* variable */
	curl_easy_setopt(conn->curl, CURLOPT_WRITEDATA, resp);
	curl_easy_setopt(conn->curl, CURLOPT_POSTFIELDS, query);
	curl_easy_setopt(conn->curl, CURLOPT_VERBOSE, curl_verbose);
	if (curl_progressfunc)
	{
		curl_easy_setopt(conn->curl, CURLOPT_NOPROGRESS, 0L);
		curl_easy_setopt(conn->curl, CURLOPT_XFERINFOFUNCTION, curl_progressfunc);
		curl_easy_setopt(conn->curl, CURLOPT_XFERINFODATA, conn);
	}
	else
		curl_easy_setopt(conn->curl, CURLOPT_NOPROGRESS, 1L);

	url = malloc(conn->base_url_len + 30 /* query_id */ + 11 /* ?query_id= */);
	sprintf(url, "%s?query_id=%s", conn->base_url, resp->query_id);
	curl_url_set(conn->url, CURLUPART_URL, url, 0);

	curl_error_happened = false;
	errcode = curl_easy_perform(conn->curl);
	if (errcode == CURLE_ABORTED_BY_CALLBACK)
	{
		resp->http_status = 418; /* I'm teapot */
		return resp;
	}
	else if (errcode != CURLE_OK)
	{
		resp->http_status = 418; /* i'm teapot */
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
	free(conn->base_url);
	curl_url_cleanup(conn->url);
	curl_easy_cleanup(conn->curl);
}

char *ch_http_last_error(void)
{
	if (curl_error_happened)
		return curl_error_buffer;

	return NULL;
}

void ch_http_response_free(ch_http_response_t *resp)
{
	if (resp->data)
		free(resp->data);

	free(resp);
}
