#define _XOPEN_SOURCE 700
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
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

static void test_simple_query(void **s)
{
	ch_binary_read_state_t	state;
	ch_binary_connection_t	*conn = ch_binary_connect("localhost", 9000, NULL, NULL, NULL);
	assert_ptr_not_equal(conn, NULL);

	// QUERY 1
	ch_binary_response_t	*res = ch_binary_simple_query(conn,
		"select 1, NULL, number from numbers(3);", NULL);

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
			"addDays(toDateTime('1991-02-02 10:01:02'), number) from numbers(2);", NULL);
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
		"		toFloat32(0.1 + number), toFloat64(0.2 + number) from numbers(10, 2);", NULL);

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
		"select toFixedString('qwer', 10), "
			"toString('qwer1') from numbers(2);", NULL);
	assert_true(res->success);
	ch_binary_read_state_init(&state, res);

	// 1 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_string_equal((char *) values[0], "qwer");
	assert_string_equal((char *) values[1], "qwer1");

	// 2 row
	values = ch_binary_read_row(&state);
	assert_ptr_not_equal(values, NULL);
	assert_string_equal((char *) values[0], "qwer");
	assert_string_equal((char *) values[1], "qwer1");

	// empty
	values = ch_binary_read_row(&state);
	assert_ptr_equal(values, NULL);

	ch_binary_read_state_free(&state);
	ch_binary_response_free(res);

	// QUERY 5
	uuid_t	parsed;
	assert_int_equal(uuid_parse("f4bf890f-f9dc-4332-ad5c-0c18e73f28e9", parsed), 0);
	res = ch_binary_simple_query(conn, "select toUUID('f4bf890f-f9dc-4332-ad5c-0c18e73f28e9');", NULL);
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
		"from numbers(1, 2);", NULL);
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
		"select qwer(qwer) from numbers(2);", NULL);
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

static void test_query_canceling(void **s)
{
	bool cancel = false;

	ch_binary_connection_t	*conn = ch_binary_connect("localhost", 9000, NULL, NULL, NULL);
	assert_ptr_not_equal(conn, NULL);
	ch_binary_response_t	*res = ch_binary_simple_query(conn,
		"select 1, NULL, number from numbers(3);", &cancel);
	assert_true(res->success);
	cancel = true;
	res = ch_binary_simple_query(conn,
		"select number, sleep(3) from numbers(1, 1000000000);", &cancel);
	assert_false(res->success);
	assert_string_equal(res->error, "query was canceled");
	ch_binary_response_free(res);
	ch_binary_close(conn);
}

static void query(ch_binary_connection_t *conn, char *sql)
{
	ch_binary_response_t *res = ch_binary_simple_query(conn, sql, NULL);
	if (!res->success)
		printf("%s\n", res->error);

	assert_true(res->success);
	ch_binary_response_free(res);
}

static void test_insertion(void **s)
{
	ch_binary_connection_t	*conn = ch_binary_connect("localhost", 9000, NULL, NULL, NULL);
	ch_binary_response_t	*res;
	ch_binary_read_state_t	state;

	assert_ptr_not_equal(conn, NULL);

	query(conn, "DROP DATABASE IF EXISTS test");
	query(conn, "CREATE DATABASE test");
	query(conn, "CREATE TABLE IF NOT EXISTS test.numbers ("
		"a UInt8, b UInt16, c UInt32, d UInt64, "
		"e Int8, f Int16, g Int32, h Int64, "
		"i Float32, j Float64 "
		") ENGINE = Memory");

	const size_t	nrows = 4;
	const size_t	ncolumns = 10;
	ch_binary_block_t	blocks[ncolumns];

	uint8_t		*uint8_data = (uint8_t *) malloc(sizeof(uint8_t) * nrows);
	uint16_t	*uint16_data = (uint16_t *) malloc(sizeof(uint16_t) * nrows);
	uint32_t	*uint32_data = (uint32_t *) malloc(sizeof(uint32_t) * nrows);
	uint64_t	*uint64_data = (uint64_t *) malloc(sizeof(uint64_t) * nrows);
	int8_t		*int8_data = (int8_t *) malloc(sizeof(int8_t) * nrows);
	int16_t		*int16_data = (int16_t *) malloc(sizeof(int16_t) * nrows);
	int32_t		*int32_data = (int32_t *) malloc(sizeof(int32_t) * nrows);
	int64_t		*int64_data = (int64_t *) malloc(sizeof(int64_t) * nrows);
	float		*float_data = (float *) malloc(sizeof(float) * nrows);
	double		*double_data = (double *) malloc(sizeof(double) * nrows);

	for (size_t i = 0; i < nrows; i++)
	{
		uint8_data[i] = 10 + i;
		uint16_data[i] = 20 + i;
		uint32_data[i] = 30 + i;
		uint64_data[i] = 40 + i;
		int8_data[i] = 50 + i;
		int16_data[i] = 60 + i;
		int32_data[i] = 70 + i;
		int64_data[i] = 80 + i;
		float_data[i] = 90 + i;
		double_data[i] = 100 + i;
	}

	blocks[0].colname = "a";
	blocks[0].coltype = chb_UInt8;
	blocks[0].coldata = uint8_data;

	blocks[1].colname = "b";
	blocks[1].coltype = chb_UInt16;
	blocks[1].coldata = uint16_data;

	blocks[2].colname = "c";
	blocks[2].coltype = chb_UInt32;
	blocks[2].coldata = uint32_data;

	blocks[3].colname = "d";
	blocks[3].coltype = chb_UInt64;
	blocks[3].coldata = uint64_data;

	blocks[4].colname = "e";
	blocks[4].coltype = chb_Int8;
	blocks[4].coldata = int8_data;

	blocks[5].colname = "f";
	blocks[5].coltype = chb_Int16;
	blocks[5].coldata = int16_data;

	blocks[6].colname = "g";
	blocks[6].coltype = chb_Int32;
	blocks[6].coldata = int32_data;

	blocks[7].colname = "h";
	blocks[7].coltype = chb_Int64;
	blocks[7].coldata = int64_data;

	blocks[8].colname = "i";
	blocks[8].coltype = chb_Float32;
	blocks[8].coldata = float_data;

	blocks[9].colname = "j";
	blocks[9].coltype = chb_Float64;
	blocks[9].coldata = double_data;

	res = ch_binary_simple_insert(conn, "test.numbers", blocks, ncolumns, nrows);
	if (!res->success)
		printf("%s\n", res->error);

	assert_true(res->success);
	ch_binary_response_free(res);

	res = ch_binary_simple_query(conn, "select * from test.numbers", NULL);
	assert_true(res->success);
	ch_binary_read_state_init(&state, res);

	void **values = ch_binary_read_row(&state);
	assert_int_equal(*(uint8_t *) values[0], 10);
	assert_int_equal(*(uint16_t *) values[1], 20);
	assert_int_equal(*(uint32_t *) values[2], 30);
	assert_int_equal(*(uint64_t *) values[3], 40);
	assert_int_equal(*(int8_t *) values[4], 50);
	assert_int_equal(*(int16_t *) values[5], 60);
	assert_int_equal(*(int32_t *) values[6], 70);
	assert_int_equal(*(int64_t *) values[7], 80);
	assert_float_equal(*(float *) values[8], 90, 0.01);
	assert_float_equal(*(double *) values[9], 100, 0.01);

	values = ch_binary_read_row(&state);
	assert_int_equal(*(uint8_t *) values[0], 11);
	assert_int_equal(*(uint16_t *) values[1], 21);
	assert_int_equal(*(uint32_t *) values[2], 31);
	assert_int_equal(*(uint64_t *) values[3], 41);
	assert_int_equal(*(int8_t *) values[4], 51);
	assert_int_equal(*(int16_t *) values[5], 61);
	assert_int_equal(*(int32_t *) values[6], 71);
	assert_int_equal(*(int64_t *) values[7], 81);
	assert_float_equal(*(float *) values[8], 91, 0.01);
	assert_float_equal(*(double *) values[9], 101, 0.01);

	values = ch_binary_read_row(&state);
	assert_int_equal(*(uint8_t *) values[0], 12);
	assert_int_equal(*(uint16_t *) values[1], 22);
	assert_int_equal(*(uint32_t *) values[2], 32);
	assert_int_equal(*(uint64_t *) values[3], 42);
	assert_int_equal(*(int8_t *) values[4], 52);
	assert_int_equal(*(int16_t *) values[5], 62);
	assert_int_equal(*(int32_t *) values[6], 72);
	assert_int_equal(*(int64_t *) values[7], 82);
	assert_float_equal(*(float *) values[8], 92, 0.01);
	assert_float_equal(*(double *) values[9], 102, 0.01);

	values = ch_binary_read_row(&state);
	assert_int_equal(*(uint8_t *) values[0], 13);
	assert_int_equal(*(uint16_t *) values[1], 23);
	assert_int_equal(*(uint32_t *) values[2], 33);
	assert_int_equal(*(uint64_t *) values[3], 43);
	assert_int_equal(*(int8_t *) values[4], 53);
	assert_int_equal(*(int16_t *) values[5], 63);
	assert_int_equal(*(int32_t *) values[6], 73);
	assert_int_equal(*(int64_t *) values[7], 83);
	assert_float_equal(*(float *) values[8], 93, 0.01);
	assert_float_equal(*(double *) values[9], 103, 0.01);

	values = ch_binary_read_row(&state);
	assert_ptr_equal(values, NULL);
	assert_ptr_equal(state.error, NULL);
	ch_binary_read_state_free(&state);

	ch_binary_response_free(res);
	ch_binary_close(conn);
}

/* string, fixedstring, uuid, date, datetime */
static void test_insertion_complex(void **s)
{
	ch_binary_connection_t	*conn = ch_binary_connect("localhost", 9000, NULL, NULL, NULL);
	ch_binary_response_t	*res;
	ch_binary_read_state_t	state;

	assert_ptr_not_equal(conn, NULL);

	query(conn, "DROP DATABASE IF EXISTS test");
	query(conn, "CREATE DATABASE test");
	query(conn, "CREATE TABLE IF NOT EXISTS test.complex ("
		"a String, b FixedString(3), c Date, d DateTime, e UUID) ENGINE = Memory");

	const size_t	nrows = 2;
	const size_t	ncolumns = 5;
	ch_binary_block_t	blocks[ncolumns];

	char	**string_data = (char**) malloc(sizeof(char*) * nrows);
	char	**fixedstring_data = (char**) malloc(sizeof(char*) * nrows);
	time_t	*date_data = (time_t *) malloc(sizeof(time_t) * nrows);
	time_t	*datetime_data = (time_t *) malloc(sizeof(time_t) * nrows);
	uuid_t	*uuid_data = (uuid_t *) malloc(sizeof(uuid_t) * nrows);

	for (size_t i = 0; i < nrows; i++)
	{
		char buf[50];
		snprintf(buf, sizeof(buf), "%dqwerqwer", i);

		string_data[i] = strdup(buf);
		fixedstring_data[i] = strdup(buf);

		snprintf(buf, sizeof(buf), "1990-01-0%d 10:00:00", i + 1);
		date_data[i] = get_timestamp(buf);
		snprintf(buf, sizeof(buf), "1991-01-0%d 11:00:00", i + 2);
		datetime_data[i] = get_timestamp(buf);

		snprintf(buf, sizeof(buf), "f4bf890f-f9dc-4332-ad5c-0c18e73f28e%d", i);
		uuid_parse(strdup(buf), uuid_data[i]);
	}

	blocks[0].colname = "a";
	blocks[0].coltype = chb_String;
	blocks[0].coldata = string_data;

	blocks[1].colname = "b";
	blocks[1].coltype = chb_FixedString;
	blocks[1].coldata = fixedstring_data;
	blocks[1].n_arg = 3;

	blocks[2].colname = "c";
	blocks[2].coltype = chb_Date;
	blocks[2].coldata = date_data;

	blocks[3].colname = "d";
	blocks[3].coltype = chb_DateTime;
	blocks[3].coldata = datetime_data;

	blocks[4].colname = "e";
	blocks[4].coltype = chb_UUID;
	blocks[4].coldata = uuid_data;

	res = ch_binary_simple_insert(conn, "test.complex", blocks, ncolumns, nrows);
	if (!res->success)
		printf("%s\n", res->error);

	assert_true(res->success);
	ch_binary_response_free(res);

	res = ch_binary_simple_query(conn, "select a,b,c,d,toString(e) from test.complex", NULL);
	if (!res->success)
		printf("%s\n", res->error);
	assert_true(res->success);
	ch_binary_read_state_init(&state, res);

	void **values = ch_binary_read_row(&state);
	assert_string_equal((char *) values[0], "0qwerqwer");
	assert_string_equal((char *) values[1], "0qw");
	assert_true(get_timestamp("1990-01-01 00:00:00") == *(time_t *) values[2]);
	assert_true(get_timestamp("1991-01-02 11:00:00") == *(time_t *) values[3]);
	assert_string_equal((char *) values[4], "f4bf890f-f9dc-4332-ad5c-0c18e73f28e0");

	values = ch_binary_read_row(&state);
	assert_string_equal((char *) values[0], "1qwerqwer");
	assert_string_equal((char *) values[1], "1qw");
	assert_true(get_timestamp("1990-01-02 00:00:00") == *(time_t *) values[2]);
	assert_true(get_timestamp("1991-01-03 11:00:00") == *(time_t *) values[3]);
	assert_string_equal((char *) values[4], "f4bf890f-f9dc-4332-ad5c-0c18e73f28e1");

	values = ch_binary_read_row(&state);
	assert_ptr_equal(values, NULL);
	assert_ptr_equal(state.error, NULL);
	ch_binary_read_state_free(&state);

	ch_binary_response_free(res);
	ch_binary_close(conn);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        //cmocka_unit_test(test_simple_query),
        //cmocka_unit_test(test_query_canceling),
        cmocka_unit_test(test_insertion),
        cmocka_unit_test(test_insertion_complex),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
