CREATE EXTENSION clickhouse_fdw;
CREATE SERVER loopback FOREIGN DATA WRAPPER clickhouse_fdw OPTIONS(dbname 'regression');
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;

SELECT clickhousedb_raw_query('drop database if exists regression');
SELECT clickhousedb_raw_query('create database regression');
SELECT clickhousedb_raw_query('
	create table regression.t1 (a int, b int)
	engine = MergeTree()
	order by a');

SELECT clickhousedb_raw_query('
	create table regression.t2 (a int, b AggregateFunction(sum, Int32))
	engine = AggregatingMergeTree()
	order by a');

SELECT clickhousedb_raw_query('
	insert into regression.t1 select number % 10, number from numbers(1, 100);');

SELECT clickhousedb_raw_query('
	insert into regression.t2 select number % 10 as a, sumState(toInt32(number)) as b from numbers(1, 100) group by a;');

SELECT clickhousedb_raw_query('
	create materialized view regression.t1_aggr
		engine=AggregatingMergeTree()
		order by a populate as select a, sumState(b) as b from regression.t1 group by a;');

IMPORT FOREIGN SCHEMA "a" FROM SERVER loopback INTO public;

\d+ t1
\d+ t1_aggr
\d+ t2

EXPLAIN (VERBOSE, COSTS OFF) SELECT a, sum(b) FROM t1 GROUP BY a;
SELECT a, sum(b) FROM t1 GROUP BY a ORDER BY a;
EXPLAIN (VERBOSE, COSTS OFF) SELECT a, sum(b) FROM t1_aggr GROUP BY a;
SELECT a, sum(b) FROM t1_aggr GROUP BY a ORDER BY a;

EXPLAIN (VERBOSE, COSTS OFF) SELECT a, sum(b) FROM t2 GROUP BY a;
SELECT a, sum(b) FROM t2 GROUP BY a ORDER BY a;

DROP USER MAPPING FOR CURRENT_USER SERVER loopback;
SELECT clickhousedb_raw_query('DROP DATABASE regression');
DROP EXTENSION IF EXISTS clickhouse_fdw CASCADE;
