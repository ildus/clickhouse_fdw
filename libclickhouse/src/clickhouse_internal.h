#include "curl/curl.h"
#include <arpa/inet.h>

typedef struct ch_http_connection_t
{
	CURL			   *curl;
	CURLU			   *url;
	char			   *base_url;
	size_t				base_url_len;
} ch_http_connection_t;
