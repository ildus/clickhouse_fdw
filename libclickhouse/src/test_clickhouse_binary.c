#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <setjmp.h>
#include <string.h>
#include <cmocka.h>
#include <clickhouse_internal.h>
#include "clickhouse_binary.h"

static void test_binary(void **s) {
	ch_binary_io_state_t	io;

	init_io_state(&io);
	write_uint32_binary(&io, 2);
	assert_int_equal(io.len, 1024);
	write_uint32_binary(&io, 3);
	write_string_binary(&io, "stringstring");
	write_bool_binary(&io, true);
	write_bool_binary(&io, false);
	write_char_binary(&io, 'a');
	write_char_binary(&io, 'b');

	io.pos = 0;
	assert_int_equal(read_uint32_binary(&io), 2);
	assert_int_equal(read_uint32_binary(&io), 3);
	assert_int_equal(strcmp(read_string_binary(&io), "stringstring"), 0);
	assert_true(read_bool_binary(&io));
	assert_false(read_bool_binary(&io));
	assert_int_equal(read_char_binary(&io), 'a');
	assert_int_equal(read_char_binary(&io), 'b');

	(void) s;	/* keep compiler quiet */
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_binary),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
