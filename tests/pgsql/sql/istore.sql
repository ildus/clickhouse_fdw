CREATE EXTENSION clickhouse_fdw;
CREATE EXTENSION istore;
CREATE SERVER ch_default FOREIGN DATA WRAPPER clickhouse_fdw OPTIONS(dbname 'regression');
CREATE USER MAPPING FOR CURRENT_USER SERVER ch_default;

-- create remote table
SELECT clickhousedb_raw_query('DROP DATABASE IF EXISTS regression');
SELECT clickhousedb_raw_query('CREATE DATABASE regression');
SELECT clickhousedb_raw_query('CREATE TABLE regression.t2 (`dt` Date, `a_ids` Array(Int32), `a_values` Array(Int32)) ENGINE = MergeTree() PARTITION BY dt ORDER BY dt SETTINGS index_granularity = 8192');
SELECT clickhousedb_raw_query('INSERT INTO regression.t2 VALUES (''2019-10-10'', [1,2,3], [11, 22, 33])');
SELECT clickhousedb_raw_query('INSERT INTO regression.t2 VALUES (''2019-10-10'', [1,2,3,4], [11,22, 33,44])');
SELECT clickhousedb_raw_query('INSERT INTO regression.t2 VALUES (''2019-10-11'', [3,4,5], [33, 44, 55])');

-- TODO:sign column
CREATE FOREIGN TABLE t2 (dt date NOT NULL, a istore) SERVER ch_default
	OPTIONS (table_name 't2', engine 'collapsingmergetree');

-- check all good
EXPLAIN (VERBOSE) SELECT * FROM t2 ORDER BY dt;
SELECT * FROM t2 ORDER BY dt;

EXPLAIN (VERBOSE) SELECT dt, sum(a) FROM t2 GROUP BY dt ORDER BY dt;
SELECT dt, sum(a) FROM t2 GROUP BY dt ORDER BY dt;

SELECT clickhousedb_raw_query('DROP DATABASE regression');
DROP EXTENSION IF EXISTS clickhouse_fdw CASCADE;
DROP EXTENSION IF EXISTS istore CASCADE;
