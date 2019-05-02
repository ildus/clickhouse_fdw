#include "curl/curl.h"

typedef struct ch_http_connection_t
{
	CURL			   *curl;
	CURLU			   *url;
	ch_http_format_t	format;
} ch_http_connection_t;
