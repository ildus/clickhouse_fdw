CREATE EXTENSION clickhouse_fdw;
SET datestyle = 'ISO';
CREATE SERVER loopback FOREIGN DATA WRAPPER clickhouse_fdw
    OPTIONS(dbname 'regression', driver 'http');
CREATE SCHEMA clickhouse;
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;

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

IMPORT FOREIGN SCHEMA "<does not matter>" FROM SERVER loopback INTO clickhouse;

\d+ clickhouse.ints;
\d+ clickhouse.types;
\d+ clickhouse.arrays;

DROP USER MAPPING FOR CURRENT_USER SERVER loopback;

SELECT clickhousedb_raw_query('DROP DATABASE regression');
DROP EXTENSION clickhouse_fdw CASCADE;
