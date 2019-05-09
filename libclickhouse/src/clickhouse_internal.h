#include "curl/curl.h"

typedef struct ch_http_connection_t
{
	CURL			   *curl;
	CURLU			   *url;
	ch_http_format_t	format;
	char			   *base_url;
	size_t				base_url_len;
} ch_http_connection_t;
