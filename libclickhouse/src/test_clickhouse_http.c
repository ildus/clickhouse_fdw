#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <setjmp.h>
#include <string.h>
#include <assert.h>
#include <cmocka.h>

#include "clickhouse_http.h"
#include <clickhouse_internal.h>

static void test_simple_query(void **s) {
	int rc;
	ch_http_read_state state;

	ch_http_init(1);
	ch_http_connection_t	*conn = ch_http_connection("http://localhost:8123");
	ch_http_response_t		*res = ch_http_simple_query(conn, "SELECT 1,2,3,NULL");

	assert_ptr_not_equal(conn, NULL);
	assert_ptr_not_equal(res, NULL);

	memset(&state, 0, sizeof(state));
	ch_http_read_state_init(&state, res->data, res->datasize);
	rc = ch_http_read_next(&state);
	assert_int_equal(atoi(state.val), 1);
	assert_int_equal(rc, CH_CONT);

	rc = ch_http_read_next(&state);
	assert_int_equal(atoi(state.val), 2);
	assert_int_equal(rc, CH_CONT);

	rc = ch_http_read_next(&state);
	assert_int_equal(atoi(state.val), 3);
	assert_int_equal(rc, CH_EOF);
	ch_http_read_state_free(&state);

	assert_int_equal(res->datasize, 6);
	assert_ptr_equal(ch_http_last_error(), NULL);
	ch_http_close(conn);

	(void) s;	/* keep compiler quiet */
}

static void test_complex_query(void **s) {
	int rc;
	ch_http_read_state state;

	ch_http_init(1);
	ch_http_connection_t *conn = ch_http_connection("http://localhost:8123");

	char *drop = "DROP TABLE IF EXISTS t1";
	char *q = "CREATE TABLE IF NOT EXISTS t1 ("
		"	a UInt32,"
		"	b UInt32"
		") ENGINE = StripeLog";
	char *q1 = "INSERT INTO t1 VALUES (%d, %d)";
	char buf[1024];

	// drop
	ch_http_response_t *res = ch_http_simple_query(conn, drop);
	assert_ptr_not_equal(res, NULL);
	assert_int_equal(res->http_status, 200);

	// create table
	res = ch_http_simple_query(conn, q);
	assert_ptr_not_equal(res, NULL);
	assert_int_equal(res->http_status, 200);

	// insert
	snprintf(buf, sizeof(buf), q1, 1, 2);
	res = ch_http_simple_query(conn, buf);
	assert_ptr_not_equal(res, NULL);
	assert_int_equal(res->http_status, 200);

	// insert
	snprintf(buf, sizeof(buf), q1, 3, 4);
	res = ch_http_simple_query(conn, buf);
	assert_ptr_not_equal(res, NULL);
	assert_int_equal(res->http_status, 200);

	// insert
	snprintf(buf, sizeof(buf), q1, 5, 6);
	res = ch_http_simple_query(conn, buf);
	assert_ptr_not_equal(res, NULL);
	assert_int_equal(res->http_status, 200);

	// select
	res = ch_http_simple_query(conn, "select * from t1");
	assert_ptr_not_equal(res, NULL);
	assert_int_equal(res->http_status, 200);

	// read data and check
	memset(&state, 0, sizeof(state));
	ch_http_read_state_init(&state, res->data, res->datasize);
	for (int i = 1; i < 7; i++)
	{
		rc = ch_http_read_next(&state);
		int val = atoi(state.val);

		assert_ptr_equal(ch_http_last_error(), NULL);
		assert_int_equal(val, i);

		if (val == 6)
			assert_int_equal(rc, CH_EOF);
		else if (val % 2 == 0)
			assert_int_equal(rc, CH_EOL);
		else
			assert_int_equal(rc, CH_CONT);
	}
	ch_http_read_state_free(&state);
	assert_int_equal(rc, CH_EOF);

	// test 400 bad request
	res = ch_http_simple_query(conn, "select from t2");
	assert_ptr_not_equal(res, NULL);
	assert_int_equal(res->http_status, 400);

	(void) s;	/* keep compiler quiet */
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_simple_query),
		cmocka_unit_test(test_complex_query)
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
