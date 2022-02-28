CREATE EXTENSION clickhouse_fdw;
SET datestyle = 'ISO';
CREATE SERVER loopback FOREIGN DATA WRAPPER clickhouse_fdw
    OPTIONS(dbname 'regression', driver 'http');
CREATE SERVER loopback_bin FOREIGN DATA WRAPPER clickhouse_fdw
    OPTIONS(dbname 'regression', driver 'binary');
CREATE SCHEMA clickhouse;
CREATE SCHEMA clickhouse_bin;
CREATE SCHEMA clickhouse_limit;
CREATE SCHEMA clickhouse_except;
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback_bin;

SELECT clickhousedb_raw_query('DROP DATABASE IF EXISTS regression');
SELECT clickhousedb_raw_query('CREATE DATABASE regression');
SELECT clickhousedb_raw_query('CREATE DATABASE regression_2');

-- integer types
SELECT clickhousedb_raw_query('CREATE TABLE regression.ints (
    c1 Int8, c2 Int16, c3 Int32, c4 Int64,
    c5 UInt8, c6 UInt16, c7 UInt32, c8 UInt64,
    c9 Float32, c10 Nullable(Float64)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query('INSERT INTO regression.ints SELECT
    number, number + 1, number + 2, number + 3, number + 4, number + 5,
    number + 6, number + 7, number + 8.1, number + 9.2 FROM numbers(10);');
SELECT clickhousedb_raw_query('INSERT INTO regression.ints SELECT
    number, number + 1, number + 2, number + 3, number + 4, number + 5,
    number + 6, number + 7, number + 8.1, NULL FROM numbers(10, 2);');

SELECT clickhousedb_raw_query('CREATE TABLE regression.types (
    c1 Date, c2 DateTime, c3 String, c4 FixedString(5), c5 UUID,
    c6 Enum8(''one'' = 1, ''two'' = 2),
    c7 Enum16(''one'' = 1, ''two'' = 2, ''three'' = 3),
    c9 Nullable(FixedString(50)),
    c8 LowCardinality(String)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query('INSERT INTO regression.types SELECT
    addDays(toDate(''1990-01-01''), number),
    addMinutes(addSeconds(addDays(toDateTime(''1990-01-01 10:00:00''), number), number), number),
    format(''number {0}'', toString(number)),
    format(''num {0}'', toString(number)),
    format(''f4bf890f-f9dc-4332-ad5c-0c18e73f28e{0}'', toString(number)),
    ''two'',
    ''three'',
    toString(number),
    format(''cardinal {0}'', toString(number))
    FROM numbers(10);');

SELECT clickhousedb_raw_query('CREATE TABLE regression.types2 (
    c1 LowCardinality(Nullable(String))
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1) SETTINGS allow_nullable_key = 1;
');
SELECT clickhousedb_raw_query('INSERT INTO regression.types2 SELECT
    format(''cardinal {0}'', toString(number + 1))
    FROM numbers(10);');

SELECT clickhousedb_raw_query('CREATE TABLE regression.ip (
    c1 IPv4,
    c2 IPv6
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query($$
    INSERT INTO regression.ip VALUES
    ('116.106.34.242', '2001:44c8:129:2632:33:0:252:2'),
    ('116.106.34.243', '2a02:e980:1e::1'),
    ('116.106.34.244', '::1');
$$);

-- array types
SELECT clickhousedb_raw_query('CREATE TABLE regression.arrays (
    c1 Array(Int), c2 Array(String)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query('INSERT INTO regression.arrays SELECT
    [number, number + 1],
    [format(''num{0}'', toString(number)), format(''num{0}'', toString(number + 1))]
    FROM numbers(10);');

-- tuple
SELECT clickhousedb_raw_query('CREATE TABLE regression.tuples (
    c1 Int8,
    c2 Tuple(Int, String, Float32),
	c3 Nested(a Int, b Int),
	c4 Int16
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query('INSERT INTO regression.tuples SELECT
    number,
    (number, toString(number), number + 1.0),
	[toInt32(number),1,1],
	[toInt32(number),2,2],
	toInt16(number)
    FROM numbers(10);');

-- datetime with timezones
SELECT clickhousedb_raw_query('CREATE TABLE regression.timezones (
	t1 DateTime64(6,''UTC''),
	t2 DateTime64(6,''Europe/Berlin''),
	t4 DateTime(''Europe/Berlin''),
	t5 DateTime64(6))
	ENGINE = MergeTree ORDER BY (t1) SETTINGS index_granularity=8192;');

SELECT clickhousedb_raw_query('INSERT INTO regression.timezones VALUES (
	''2020-01-01 11:00:00'',
	''2020-01-01 11:00:00'',
	''2020-01-01 11:00:00'',
	''2020-01-01 11:00:00'')');

SELECT clickhousedb_raw_query('INSERT INTO regression.timezones VALUES (
	''2020-01-01 12:00:00'',
	''2020-01-01 12:00:00'',
	''2020-01-01 12:00:00'',
	''2020-01-01 12:00:00'')');

IMPORT FOREIGN SCHEMA "regression" FROM SERVER loopback INTO clickhouse;

\d+ clickhouse.ints;
\d+ clickhouse.types;
\d+ clickhouse.types2;
\d+ clickhouse.arrays;
\d+ clickhouse.tuples;
\d+ clickhouse.timezones;
\d+ clickhouse.ip;

SELECT * FROM clickhouse.ints ORDER BY c1 DESC LIMIT 4;
SELECT * FROM clickhouse.types ORDER BY c1 LIMIT 2;
SELECT * FROM clickhouse.types2 ORDER BY c1 LIMIT 2;
SELECT * FROM clickhouse.arrays ORDER BY c1 LIMIT 2;
SELECT * FROM clickhouse.tuples ORDER BY c1 LIMIT 2;
SELECT * FROM clickhouse.timezones ORDER BY t1 LIMIT 2;
SELECT * FROM clickhouse.ip ORDER BY c1;

IMPORT FOREIGN SCHEMA "regression" FROM SERVER loopback_bin INTO clickhouse_bin;

\d+ clickhouse_bin.ints;
\d+ clickhouse_bin.types;
\d+ clickhouse_bin.types2;
\d+ clickhouse_bin.arrays;
\d+ clickhouse_bin.tuples;
\d+ clickhouse_bin.timezones;
\d+ clickhouse_bin.ip;

SELECT * FROM clickhouse_bin.ints ORDER BY c1 DESC LIMIT 4;
SELECT * FROM clickhouse_bin.types ORDER BY c1 LIMIT 2;
SELECT * FROM clickhouse_bin.types2 ORDER BY c1 LIMIT 2;
SELECT * FROM clickhouse_bin.arrays ORDER BY c1 LIMIT 2;
SELECT * FROM clickhouse_bin.tuples ORDER BY c1 LIMIT 2;
SELECT * FROM clickhouse_bin.timezones ORDER BY t1 LIMIT 2;
SELECT * FROM clickhouse.ip ORDER BY c1;

IMPORT FOREIGN SCHEMA "regression" LIMIT TO (ints, types) FROM SERVER loopback INTO clickhouse_limit;

\d+ clickhouse_limit.ints;
\d+ clickhouse_limit.types;
\d+ clickhouse_limit.arrays;
\d+ clickhouse_limit.tuples;

IMPORT FOREIGN SCHEMA "regression" EXCEPT (ints, types) FROM SERVER loopback INTO clickhouse_except;

\d+ clickhouse_except.ints;
\d+ clickhouse_except.types;
\d+ clickhouse_except.arrays;
\d+ clickhouse_except.tuples;

-- check custom database
SELECT clickhousedb_raw_query('CREATE TABLE regression_2.custom_option (a Int64) ENGINE = MergeTree ORDER BY (a)');
IMPORT FOREIGN SCHEMA "regression_2" FROM SERVER loopback INTO clickhouse;

EXPLAIN VERBOSE SELECT * FROM clickhouse.custom_option;
ALTER FOREIGN TABLE clickhouse.custom_option OPTIONS (DROP database);
EXPLAIN VERBOSE SELECT * FROM clickhouse.custom_option;

DROP USER MAPPING FOR CURRENT_USER SERVER loopback;
DROP USER MAPPING FOR CURRENT_USER SERVER loopback_bin;

SELECT clickhousedb_raw_query('DROP DATABASE regression');
SELECT clickhousedb_raw_query('DROP DATABASE regression_2');
DROP EXTENSION clickhouse_fdw CASCADE;
