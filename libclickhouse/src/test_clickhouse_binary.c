#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <setjmp.h>
#include <string.h>
#include <assert.h>
#include <cmocka.h>

#include "clickhouse_binary.hh"
#include <clickhouse_internal.h>

static void test_simple_query(void **s) {
	ch_binary_connection_t	*conn = ch_binary_connect("localhost", 9000, NULL, NULL, NULL);
	assert_ptr_not_equal(conn, NULL);

	ch_binary_response_t	*res = ch_binary_simple_query(conn, "select 1, NULL, number from numbers(3);");

	if (!res->success)
		printf("%s\n", res->error);

	assert_true(res->success);
	ch_binary_response_free(res);
	ch_binary_close(conn);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_simple_query),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
