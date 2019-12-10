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
CREATE FOREIGN TABLE t2 (a int, b int, c timestamp with time zone) SERVER loopback OPTIONS (table_name 't1');

EXPLAIN (VERBOSE, COSTS OFF)
	SELECT coalesce(a::text, b::text, c::text) FROM t1 GROUP BY a, b, c;
SELECT coalesce(a::text, b::text, c::text) FROM t1 GROUP BY a, b, c;

EXPLAIN (VERBOSE, COSTS OFF)
	SELECT a, sum(b) FROM t1 WHERE a IN (1,2,3) GROUP BY a;
SELECT a, sum(b) FROM t1 WHERE a IN (1,2,3) GROUP BY a;

EXPLAIN (VERBOSE, COSTS OFF)
	SELECT a, sum(b) FROM t1 WHERE a NOT IN (1,2,3) GROUP BY a;
SELECT a, sum(b) FROM t1 WHERE a NOT IN (1,2,3) GROUP BY a;

EXPLAIN (VERBOSE, COSTS OFF) SELECT argMin(a, b) FROM t1;
SELECT argMin(a, b) FROM t1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT argMax(a, b) FROM t1;
SELECT argMax(a, b) FROM t1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT argMin(a, c) FROM t1;
SELECT argMin(a, c) FROM t1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT argMax(a, c) FROM t1;
SELECT argMax(a, c) FROM t1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT date_trunc('day', c at time zone 'UTC') as d1 FROM t1 GROUP BY d1 ORDER BY d1;
SELECT date_trunc('day', c at time zone 'UTC') as d1 FROM t1 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT date_trunc('day', c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;
SELECT date_trunc('day', c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT extract('day' from c at time zone 'UTC') as d1 FROM t1 GROUP BY d1 ORDER BY d1;
SELECT extract('day' from c at time zone 'UTC') as d1 FROM t1 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT extract('day' from c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;
SELECT extract('day' from c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT extract('doy' from c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;
SELECT extract('doy' from c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT extract('dow' from c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;
SELECT extract('dow' from c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT extract('minute' from c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;
SELECT extract('minute' from c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;

DROP USER MAPPING FOR CURRENT_USER SERVER loopback;
SELECT clickhousedb_raw_query('DROP DATABASE regression');
DROP EXTENSION IF EXISTS clickhouse_fdw CASCADE;
