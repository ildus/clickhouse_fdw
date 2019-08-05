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

-- integer types
SELECT clickhousedb_raw_query('CREATE TABLE regression.ints (
    c1 Int8, c2 Int16, c3 Int32, c4 Int64,
    c5 UInt8, c6 UInt16, c7 UInt32, c8 UInt64,
    c9 Float32, c10 Nullable(Float64)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');

SELECT clickhousedb_raw_query('CREATE TABLE regression.types (
    c1 Date, c2 DateTime, c3 String, c4 FixedString(5), c5 UUID,
    c6 Enum8(''one'' = 1, ''two'' = 2),
    c7 Enum16(''one'' = 1, ''two'' = 2, ''three'' = 3),
    c8 LowCardinality(String)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');

-- array types
SELECT clickhousedb_raw_query('CREATE TABLE regression.arrays (
    c1 Array(Int), c2 Array(String)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');

-- tuple
SELECT clickhousedb_raw_query('CREATE TABLE regression.tuples (
    c1 Int8,
    c2 Tuple(Int, String, Float32)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');

IMPORT FOREIGN SCHEMA "<does not matter>" FROM SERVER loopback INTO clickhouse;

\d+ clickhouse.ints;
\d+ clickhouse.types;
\d+ clickhouse.arrays;
\d+ clickhouse.tuples;

IMPORT FOREIGN SCHEMA "<does not matter>" FROM SERVER loopback_bin INTO clickhouse_bin;

\d+ clickhouse_bin.ints;
\d+ clickhouse_bin.types;
\d+ clickhouse_bin.arrays;
\d+ clickhouse_bin.tuples;

IMPORT FOREIGN SCHEMA "<does not matter>" LIMIT TO (ints, types) FROM SERVER loopback INTO clickhouse_limit;

\d+ clickhouse_limit.ints;
\d+ clickhouse_limit.types;
\d+ clickhouse_limit.arrays;
\d+ clickhouse_limit.tuples;

IMPORT FOREIGN SCHEMA "<does not matter>" EXCEPT (ints, types) FROM SERVER loopback INTO clickhouse_except;

\d+ clickhouse_except.ints;
\d+ clickhouse_except.types;
\d+ clickhouse_except.arrays;
\d+ clickhouse_except.tuples;

DROP USER MAPPING FOR CURRENT_USER SERVER loopback;
DROP USER MAPPING FOR CURRENT_USER SERVER loopback_bin;

SELECT clickhousedb_raw_query('DROP DATABASE regression');
DROP EXTENSION clickhouse_fdw CASCADE;
