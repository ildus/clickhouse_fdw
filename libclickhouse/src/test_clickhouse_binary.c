#include <cmocka.h>

#include "clickhouse_binary.hh"
#include <clickhouse_internal.h>

static void test_simple_query(void **s) {
	ch_binary_connection_t	*conn = ch_binary_connect(NULL, NULL, NULL, NULL, NULL);
	ch_binary_response_t		*res = ch_binary_simple_query(conn, "SELECT 1,2,3,NULL", 1);

	assert(res->success);
	ch_binary_response_free(res);
	ch_binary_close(conn);
}
