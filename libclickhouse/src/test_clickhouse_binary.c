#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include <setjmp.h>
#include <string.h>
#include <assert.h>
#include <cmocka.h>

#include "clickhouse_binary.hh"
#include <clickhouse_internal.h>

static void test_simple_query(void **s) {
	ch_binary_read_state_t	state;
	ch_binary_connection_t	*conn = ch_binary_connect("localhost", 9000, NULL, NULL, NULL);
	assert_ptr_not_equal(conn, NULL);

	ch_binary_response_t	*res = ch_binary_simple_query(conn, "select 1, NULL, number from numbers(3);");

	assert_true(res->success);
	ch_binary_read_state_init(&state, res);
	assert_int_equal(state.coltypes[0], chb_UInt8);
	assert_int_equal(state.coltypes[1], chb_Nullable);
	assert_int_equal(state.coltypes[2], chb_UInt64);

	void **values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_int_equal(*(uint8_t *) values[0], 1);
	assert_ptr_equal(values[1], NULL);
	assert_int_equal(state.coltypes[1], chb_Void);
	assert_int_equal(*(uint64_t *) values[2], 0);

	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_int_equal(*(uint8_t *) values[0], 1);
	assert_ptr_equal(values[1], NULL);
	assert_int_equal(state.coltypes[1], chb_Void);
	assert_int_equal(*(uint64_t *) values[2], 1);

	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_int_equal(*(uint8_t *) values[0], 1);
	assert_ptr_equal(values[1], NULL);
	assert_int_equal(state.coltypes[1], chb_Void);
	assert_int_equal(*(uint64_t *) values[2], 2);

	ch_binary_response_free(res);
	ch_binary_close(conn);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_simple_query),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
