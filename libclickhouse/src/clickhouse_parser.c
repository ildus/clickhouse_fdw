#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <curl/curl.h>

#include <clickhouse_http.h>
#include <clickhouse_internal.h>

void ch_http_read_state_init(ch_http_read_state *state, char *data, size_t datalen)
{
	state->data = data;
	state->maxpos = datalen - 1;
	state->buflen = 1024;
	state->val = malloc(state->buflen);
	state->done = false;
}

void ch_http_read_state_free(ch_http_read_state *state)
{
	free(state->val);
}

int ch_http_read_next(ch_http_read_state *state)
{
	size_t pos = state->curpos,
		   len = 0;
	char  *data = state->data;

	if (state->done)
		return CH_EOF;

	state->val[0] = '\0';
	while (pos < state->maxpos && data[pos] != '\t' && data[pos] != '\n')
	{
		state->val[len++] = data[pos++];
		if (len == state->buflen)
		{
			state->buflen *= 2;
			state->val = realloc(state->val, state->buflen);
		}
	}

	state->val[len] = '\0';
	state->curpos = pos + 1;

	if (data[pos] == '\t')
		return CH_CONT;

	assert(data[pos] == '\n');
	int res = pos < state->maxpos ? CH_EOL : CH_EOF;
	state->done = (res == CH_EOF);

	return res;
}
