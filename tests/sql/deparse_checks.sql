CREATE EXTENSION clickhouse_fdw;
CREATE SERVER loopback FOREIGN DATA WRAPPER clickhouse_fdw OPTIONS(dbname 'regression');
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;

SELECT clickhousedb_raw_query('drop database if exists regression');
SELECT clickhousedb_raw_query('create database regression');
SELECT clickhousedb_raw_query('
	create table regression.t1 (a int, b Int8)
	engine = MergeTree()
	order by a');

SELECT clickhousedb_raw_query('
	insert into regression.t1 select number % 10, number % 10 > 5 from numbers(1, 100);');

IMPORT FOREIGN SCHEMA "regression" FROM SERVER loopback INTO public;
\d+ t1
ALTER TABLE t1 ALTER COLUMN b SET DATA TYPE bool;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT (CASE WHEN b THEN 1 ELSE 2 END) as g1, MAX(a) FROM t1 GROUP BY g1;
SELECT (CASE WHEN b THEN 1 ELSE 2 END) as g1, MAX(a) FROM t1 GROUP BY g1;

DROP USER MAPPING FOR CURRENT_USER SERVER loopback;
SELECT clickhousedb_raw_query('DROP DATABASE regression');
DROP EXTENSION IF EXISTS clickhouse_fdw CASCADE;
