#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <curl/curl.h>

#include <clickhouse_http.h>
#include <clickhouse_internal.h>

int ch_http_read_tab_separated(ch_http_read_state *state, char *data, int maxpos)
{
	int pos = state->curpos,
		len = 0;

	if (state->curpos == CH_EOF)
	{
		state->val = NULL;
		return state->curpos;
	}

	if (state->val == NULL)
	{
		pos = state->curpos = 0;
		state->buflen = 1024;
		state->val = malloc(state->buflen);
	}

	while (pos < maxpos && data[pos] != '\t' && data[pos] != '\n')
	{
		state->val[len++] = data[pos++];
		if (len == state->buflen)
		{
			state->buflen *= 2;
			state->val = realloc(state->val, state->buflen);
		}
	}
	while (pos < maxpos && (data[pos] == '\t' || data[pos] == '\n' || data[pos] == '\r'))
		pos++;

	state->val[len] = '\0';
	state->curpos = pos < maxpos ? pos : -1;
	return state->curpos;
}
