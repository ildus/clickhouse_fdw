#define _XOPEN_SOURCE
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include <setjmp.h>
#include <string.h>
#include <assert.h>
#include <cmocka.h>
#include <time.h>
#include <uuid/uuid.h>

#include "clickhouse_binary.hh"
#include <clickhouse_internal.h>

uint64_t get_timestamp(char *str)
{
	struct tm date;
	memset(&date, 0, sizeof(struct tm));
	strptime(str, "%Y-%m-%d %H:%M:%S", &date);
	return mktime(&date) - timezone;
}

static void test_simple_query(void **s) {

	ch_binary_read_state_t	state;
	ch_binary_connection_t	*conn = ch_binary_connect("localhost", 9000, NULL, NULL, NULL);
	assert_ptr_not_equal(conn, NULL);

	// QUERY 1
	ch_binary_response_t	*res = ch_binary_simple_query(conn,
		"select 1, NULL, number from numbers(3);");

	assert_true(res->success);
	ch_binary_read_state_init(&state, res);
	assert_int_equal(state.coltypes[0], chb_UInt8);
	assert_int_equal(state.coltypes[1], chb_Nullable);
	assert_int_equal(state.coltypes[2], chb_UInt64);

	// 1 row
	void **values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_int_equal(*(uint8_t *) values[0], 1);
	assert_ptr_equal(values[1], NULL);
	assert_int_equal(state.coltypes[1], chb_Void);
	assert_int_equal(*(uint64_t *) values[2], 0);

	// 2 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_int_equal(*(uint8_t *) values[0], 1);
	assert_ptr_equal(values[1], NULL);
	assert_int_equal(state.coltypes[1], chb_Void);
	assert_int_equal(*(uint64_t *) values[2], 1);

	// 3 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_int_equal(*(uint8_t *) values[0], 1);
	assert_ptr_equal(values[1], NULL);
	assert_int_equal(state.coltypes[1], chb_Void);
	assert_int_equal(*(uint64_t *) values[2], 2);

	values = ch_binary_read_row(&state);
	assert_ptr_equal(values, NULL);

	ch_binary_read_state_free(&state);
	ch_binary_response_free(res);

	// QUERY 2
	res = ch_binary_simple_query(conn,
		"select addDays(toDate('1990-01-01 00:00:00'), number), "
			"addDays(toDateTime('1991-02-02 10:01:02'), number) from numbers(2);");
	ch_binary_read_state_init(&state, res);

	// 1 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_int_equal(*(uint64_t *) values[0], get_timestamp("1990-01-01 00:00:00"));
	assert_int_equal(*(uint64_t *) values[1], get_timestamp("1991-02-02 10:01:02"));

	// 2 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_int_equal(*(uint64_t *) values[0], get_timestamp("1990-01-02 00:00:00"));
	assert_int_equal(*(uint64_t *) values[1], get_timestamp("1991-02-03 10:01:02"));

	// empty
	values = ch_binary_read_row(&state);
	assert_ptr_equal(values, NULL);

	ch_binary_read_state_free(&state);
	ch_binary_response_free(res);

	// QUERY 3
	res = ch_binary_simple_query(conn,
		"select toUInt8(number), toUInt16(number), toUInt32(number), toUInt64(number),"
	    "		toInt8(number), toInt16(number), toInt32(number), toInt64(number),"
		"		toFloat32(0.1 + number), toFloat64(0.2 + number) from numbers(10, 2);");

	ch_binary_read_state_init(&state, res);

	// 1 row
	values = ch_binary_read_row(&state);
	assert_int_equal(state.resp->columns_count, 10);
	assert_ptr_not_equal(values, NULL);
	assert_int_equal(*(uint8_t *) values[0], 10);
	assert_int_equal(*(uint16_t *) values[1], 10);
	assert_int_equal(*(uint32_t *) values[2], 10);
	assert_int_equal(*(uint64_t *) values[3], 10);
	assert_int_equal(*(int8_t *) values[4], 10);
	assert_int_equal(*(int16_t *) values[5], 10);
	assert_int_equal(*(int32_t *) values[6], 10);
	assert_int_equal(*(int64_t *) values[7], 10);
	assert_float_equal(*(float *) values[8], (float) 10.1, 0.01);
	assert_float_equal(*(double *) values[9], (double) 10.2, 0.01);

	// 2 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_int_equal(*(uint8_t *) values[0], 11);
	assert_int_equal(*(uint16_t *) values[1], 11);
	assert_int_equal(*(uint32_t *) values[2], 11);
	assert_int_equal(*(uint64_t *) values[3], 11);
	assert_int_equal(*(int8_t *) values[4], 11);
	assert_int_equal(*(int16_t *) values[5], 11);
	assert_int_equal(*(int32_t *) values[6], 11);
	assert_int_equal(*(int64_t *) values[7], 11);
	assert_float_equal(*(float *) values[8], (float) 11.1, 0.01);
	assert_float_equal(*(double *) values[9], (double) 11.2, 0.01);

	// empty
	values = ch_binary_read_row(&state);
	assert_ptr_equal(values, NULL);

	ch_binary_read_state_free(&state);
	ch_binary_response_free(res);

	// QUERY 4
	res = ch_binary_simple_query(conn,
		"select toFixedString('asdf', 10), "
			"toString('asdf1') from numbers(2);");
	assert_true(res->success);
	ch_binary_read_state_init(&state, res);

	// 1 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_string_equal((char *) values[0], "asdf");
	assert_string_equal((char *) values[1], "asdf1");

	// 2 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_string_equal((char *) values[0], "asdf");
	assert_string_equal((char *) values[1], "asdf1");

	// empty
	values = ch_binary_read_row(&state);
	assert_ptr_equal(values, NULL);

	ch_binary_read_state_free(&state);
	ch_binary_response_free(res);

	// QUERY 5
	uuid_t	parsed;
	assert_int_equal(uuid_parse("f4bf890f-f9dc-4332-ad5c-0c18e73f28e9", parsed), 0);
	res = ch_binary_simple_query(conn, "select toUUID('f4bf890f-f9dc-4332-ad5c-0c18e73f28e9');");
	assert_true(res->success);
	ch_binary_read_state_init(&state, res);

	// 1 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_int_equal(sizeof(uuid_t), 16);
	assert_memory_equal((char *) values[0], parsed, sizeof(uuid_t));

	// empty
	values = ch_binary_read_row(&state);
	assert_ptr_equal(values, NULL);

	ch_binary_read_state_free(&state);
	ch_binary_response_free(res);

	// QUERY 6, tuple and array
	res = ch_binary_simple_query(conn,
		"select (toUInt32(number), toString(number)), [number + 1, number + 2] "
		"from numbers(1, 2);");
	assert_true(res->success);
	ch_binary_read_state_init(&state, res);
	assert_int_equal(state.coltypes[0], chb_Tuple);
	assert_int_equal(state.coltypes[1], chb_Array);

	// 1 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	ch_binary_tuple_t *tup = values[0];
	assert_int_equal(tup->len, 2);
	assert_int_equal(tup->coltypes[0], chb_UInt32);
	assert_int_equal(tup->coltypes[1], chb_String);
	assert_int_equal(*(uint32_t *) tup->values[0], 1);
	assert_string_equal((char *) tup->values[1], "1");

	ch_binary_array_t *arr = values[1];
	assert_int_equal(arr->len, 2);
	assert_int_equal(arr->coltype, chb_UInt64);
	assert_int_equal(*(uint64_t *) arr->values[0], 2);
	assert_int_equal(*(uint64_t *) arr->values[1], 3);

	// 2 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	tup = values[0];
	assert_int_equal(tup->len, 2);
	assert_int_equal(tup->coltypes[0], chb_UInt32);
	assert_int_equal(tup->coltypes[1], chb_String);
	assert_int_equal(*(uint32_t *) tup->values[0], 2);
	assert_string_equal((char *) tup->values[1], "2");

	arr = values[1];
	assert_int_equal(arr->len, 2);
	assert_int_equal(arr->coltype, chb_UInt64);
	assert_int_equal(*(uint64_t *) arr->values[0], 3);
	assert_int_equal(*(uint64_t *) arr->values[1], 4);

	// empty
	values = ch_binary_read_row(&state);
	assert_ptr_equal(values, NULL);

	ch_binary_read_state_free(&state);
	ch_binary_response_free(res);

	// QUERY FAIL
	res = ch_binary_simple_query(conn,
		"select asdf(asdf) from numbers(2);");
	assert_false(res->success);
	assert_ptr_not_equal(res->error, NULL);

	/* try to read, it should be null */
	ch_binary_read_state_init(&state, res);
	assert_ptr_not_equal(state.error, NULL);
	values = ch_binary_read_row(&state);
	assert_ptr_equal(values, NULL);
	ch_binary_read_state_free(&state);

	ch_binary_response_free(res);
	ch_binary_close(conn);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_simple_query),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
