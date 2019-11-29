CREATE EXTENSION clickhouse_fdw;
CREATE SERVER loopback FOREIGN DATA WRAPPER clickhouse_fdw OPTIONS(dbname 'regression', driver 'binary');
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;

SELECT clickhousedb_raw_query('drop database if exists regression');
SELECT clickhousedb_raw_query('create database regression');
SELECT clickhousedb_raw_query('CREATE TABLE regression.ints (
    c1 Int8, c2 Int16, c3 Int32, c4 Int64
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');

SELECT clickhousedb_raw_query('CREATE TABLE regression.uints (
    c1 UInt8, c2 UInt16, c3 UInt32, c4 UInt64
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');

SELECT clickhousedb_raw_query('CREATE TABLE regression.floats (
    c1 Float32, c2 Float64
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');

SELECT clickhousedb_raw_query('CREATE TABLE regression.null_ints (
    c1 Int8, c2 Nullable(Int32)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');

IMPORT FOREIGN SCHEMA "a" FROM SERVER loopback INTO public;

/* ints */
INSERT INTO ints
	SELECT i, i + 1, i + 2, i+ 3 FROM generate_series(1, 3) i;
SELECT * FROM ints ORDER BY c1;
INSERT INTO ints (c1, c4, c3, c2)
	SELECT i, i + 1, i + 2, i+ 3 FROM generate_series(4, 6) i;
SELECT * FROM ints ORDER BY c1;

/* check dropping columns (that will change attnums) */
ALTER TABLE ints DROP COLUMN c1;
ALTER TABLE ints ADD COLUMN c1 SMALLINT;
INSERT INTO ints (c1, c2, c3, c4)
	SELECT i, i + 1, i + 2, i+ 3 FROM generate_series(7, 8) i;
SELECT c1, c2, c3, c4 FROM ints ORDER BY c1;

/* check other number types */
INSERT INTO uints
	SELECT i, i + 1, i + 2, i+ 3 FROM generate_series(1, 3) i;
SELECT * FROM uints ORDER BY c1;
INSERT INTO floats
	SELECT i * 1.1, i + 2.1 FROM generate_series(1, 3) i;
SELECT * FROM floats ORDER BY c1;

/* check nullable */
INSERT INTO null_ints SELECT i, case WHEN i % 2 = 0 THEN NULL ELSE i END FROM generate_series(1, 10) i;
SELECT * FROM null_ints ORDER BY c1;

DROP USER MAPPING FOR CURRENT_USER SERVER loopback;
DROP EXTENSION clickhouse_fdw CASCADE;
