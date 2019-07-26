CREATE EXTENSION clickhouse_fdw;
SET datestyle = 'ISO';
CREATE SERVER loopback FOREIGN DATA WRAPPER clickhouse_fdw
    OPTIONS(dbname 'regression', driver 'binary');

SELECT clickhousedb_raw_query('DROP DATABASE IF EXISTS regression');
SELECT clickhousedb_raw_query('CREATE DATABASE regression');

-- integer types
SELECT clickhousedb_raw_query('CREATE TABLE regression.ints (
    c1 Int8, c2 Int16, c3 Int32, c4 Int64,
    c5 UInt8, c6 UInt16, c7 UInt32, c8 UInt64,
    c9 Float32, c10 Float64
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query('INSERT INTO regression.ints SELECT
    number, number + 1, number + 2, number + 3, number + 4, number + 5,
    number + 6, number + 7, number + 8.1, number + 9.2 FROM numbers(10);');

-- date and string types
SELECT clickhousedb_raw_query('CREATE TABLE regression.dates (
    c1 Date, c2 DateTime, c3 String, c4 FixedString(5)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query('INSERT INTO regression.dates SELECT
    addDays(toDate(''1990-01-01''), number),
    addMinutes(addSeconds(addDays(toDateTime(''1990-01-01 10:00:00''), number), number), number),
    format(''number {0}'', toString(number)),
    format(''num {0}'', toString(number))
    FROM numbers(10);');

-- array types
SELECT clickhousedb_raw_query('CREATE TABLE regression.arrays (
    c1 Array(Int), c2 Array(String)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query('INSERT INTO regression.arrays SELECT
    [number, number + 1],
    [format(''num{0}'', toString(number)), format(''num{0}'', toString(number + 1))]
    FROM numbers(10);');

CREATE ROLE user1 SUPERUSER;
CREATE USER MAPPING FOR user1 SERVER loopback;
SET ROLE user1;

CREATE FOREIGN TABLE fints (
	c1 int2,
	c2 int2,
	c3 int,
	c4 int8,
	c5 int2,
	c6 int,
	c7 int8,
	c8 int8,
    c9 float4,
    c10 float8
) SERVER loopback OPTIONS (table_name 'ints');

CREATE FOREIGN TABLE fdates (
	c1 date,
	c2 timestamp without time zone,
    c3 text,
    c4 text
) SERVER loopback OPTIONS (table_name 'dates');

CREATE FOREIGN TABLE farrays (
	c1 int[],
    c2 text[]
) SERVER loopback OPTIONS (table_name 'arrays');

-- integers
SELECT * FROM fints ORDER BY c1;
SELECT c2, c1, c8, c3, c4, c7, c6, c5 FROM fints ORDER BY c1;
SELECT a, b FROM (SELECT c1 * 10 as a, c8 * 11 as b FROM fints ORDER BY a LIMIT 2) t1;
SELECT NULL FROM fints LIMIT 2;

-- dates
SELECT * FROM fdates ORDER BY c1;
SELECT c2, c1, c4, c3 FROM fdates ORDER BY c1;

-- arrays
SELECT * FROM farrays ORDER BY c1;

RESET ROLE;
DROP OWNED BY user1;
DROP ROLE user1;

SELECT clickhousedb_raw_query('DROP DATABASE regression');
DROP EXTENSION IF EXISTS clickhouse_fdw CASCADE;
