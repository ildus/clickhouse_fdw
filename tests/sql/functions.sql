CREATE EXTENSION clickhouse_fdw;
SET datestyle = 'ISO';
CREATE SERVER loopback FOREIGN DATA WRAPPER clickhouse_fdw
    OPTIONS(dbname 'regression', driver 'binary');
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;

SELECT clickhousedb_raw_query('DROP DATABASE IF EXISTS regression');
SELECT clickhousedb_raw_query('CREATE DATABASE regression');

-- argMax, argMin
SELECT clickhousedb_raw_query($$
	CREATE TABLE regression.t1 (a int, b int, c DateTime) ENGINE = MergeTree ORDER BY (a);
$$);

SELECT clickhousedb_raw_query($$
	INSERT INTO regression.t1 VALUES (1, 1, '2019-01-01 10:00:00');
$$);
SELECT clickhousedb_raw_query($$
	INSERT INTO regression.t1 VALUES (2, 2, '2019-01-02 10:00:00');
$$);

CREATE FOREIGN TABLE t1 (a int, b int, c timestamp) SERVER loopback;
EXPLAIN (VERBOSE) SELECT argMin(a, b) FROM t1;
SELECT argMin(a, b) FROM t1;
EXPLAIN (VERBOSE) SELECT argMax(a, b) FROM t1;
SELECT argMax(a, b) FROM t1;
EXPLAIN (VERBOSE) SELECT argMin(a, c) FROM t1;
SELECT argMin(a, c) FROM t1;
EXPLAIN (VERBOSE) SELECT argMax(a, c) FROM t1;
SELECT argMax(a, c) FROM t1;

EXPLAIN (VERBOSE) SELECT date_trunc('day', c at time zone 'UTC') as d1 FROM t1 GROUP BY d1 ORDER BY d1;
SELECT date_trunc('day', c at time zone 'UTC') as d1 FROM t1 GROUP BY d1 ORDER BY d1;

DROP USER MAPPING FOR CURRENT_USER SERVER loopback;
SELECT clickhousedb_raw_query('DROP DATABASE regression');
DROP EXTENSION IF EXISTS clickhouse_fdw CASCADE;
