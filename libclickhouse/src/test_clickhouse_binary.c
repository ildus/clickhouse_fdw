#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <setjmp.h>
#include <string.h>
#include <cmocka.h>
#include <clickhouse_net.h>
#include "clickhouse_binary.h"

static void test_binary(void **s) {
	ch_readahead_t	io;

	ch_readahead_init(0, &io, NULL);
	write_varuint_binary(&io, 2);
	assert_int_equal(io.size, 8192);
	write_varuint_binary(&io, 3);
	write_string_binary(&io, "stringstring");
	write_bool_binary(&io, true);
	write_bool_binary(&io, false);
	write_char_binary(&io, 'a');
	write_char_binary(&io, 'b');

	assert_true(ch_readahead_unread(&io) > 0);
	assert_int_equal(read_varuint_binary(&io), 2);
	assert_int_equal(read_varuint_binary(&io), 3);
	assert_int_equal(strcmp(read_string_binary_unsafe(&io), "stringstring"), 0);
	assert_true(read_bool_binary(&io));
	assert_false(read_bool_binary(&io));
	assert_int_equal(read_char_binary(&io), 'a');
	assert_int_equal(read_char_binary(&io), 'b');
	assert_int_equal(ch_readahead_unread(&io), 0);

	ch_readahead_free(&io);

	(void) s;	/* keep compiler quiet */
}

static void test_connection(void **s) {
	ch_binary_connection_t *conn = ch_binary_connect("localhost", 9000, NULL, NULL, NULL, NULL, 10);
	assert_ptr_not_equal(conn, NULL);
	ch_binary_disconnect(conn);

	(void) s;	/* keep compiler quiet */
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_binary),
        cmocka_unit_test(test_connection)
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
