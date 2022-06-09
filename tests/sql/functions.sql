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

SELECT clickhousedb_raw_query($$
	drop dictionary if exists regression.t3_dict
$$);

SELECT clickhousedb_raw_query('
	create table regression.t3 (a Int32, b Nullable(Int32))
	engine = MergeTree()
	order by a');
SELECT clickhousedb_raw_query('CREATE TABLE regression.t3_map (key1 Int32, key2 String,
        val String) engine=TinyLog();');
SELECT clickhousedb_raw_query('CREATE TABLE regression.t4 (val String) engine=TinyLog();');

CREATE FOREIGN TABLE t1 (a int, b int, c timestamp) SERVER loopback;
CREATE FOREIGN TABLE t2 (a int, b int, c timestamp with time zone) SERVER loopback OPTIONS (table_name 't1');
CREATE FOREIGN TABLE t3 (a int, b int) SERVER loopback;
CREATE FOREIGN TABLE t3_map (key1 int, key2 text, val text) SERVER loopback;
CREATE FOREIGN TABLE t4 (val text) SERVER loopback;

INSERT INTO t3 SELECT i, i + 1 FROM generate_series(1, 10) i;
INSERT INTO t3_map SELECT i, 'key'|| i::text, 'val' || i::text FROM generate_series(1, 10) i;
INSERT INTO t4 SELECT 'val' || i::text FROM generate_series(1, 2) i;

SELECT clickhousedb_raw_query($$
	create dictionary regression.t3_dict
    (key1 Int32, key2 String, val String)
    primary key key1, key2
    source(clickhouse(host '127.0.0.1' port 9000 db 'regression' table 't3_map' user 'default' password ''))
    layout(complex_key_hashed())
    lifetime(10);
$$);

-- check coalesce((cast as Nullable...
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT coalesce(a::text, b::text, c::text) FROM t1 GROUP BY a, b, c;
SELECT coalesce(a::text, b::text, c::text) FROM t1 GROUP BY a, b, c;

-- check IN functions
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT a, sum(b) FROM t1 WHERE a IN (1,2,3) GROUP BY a;
SELECT a, sum(b) FROM t1 WHERE a IN (1,2,3) GROUP BY a;

EXPLAIN (VERBOSE, COSTS OFF)
	SELECT a, sum(b) FROM t1 WHERE a NOT IN (1,2,3) GROUP BY a;
SELECT a, sum(b) FROM t1 WHERE a NOT IN (1,2,3) GROUP BY a;

-- check argMin, argMax
EXPLAIN (VERBOSE, COSTS OFF) SELECT argMin(a, b) FROM t1;
SELECT argMin(a, b) FROM t1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT argMax(a, b) FROM t1;
SELECT argMax(a, b) FROM t1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT argMin(a, c) FROM t1;
SELECT argMin(a, c) FROM t1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT argMax(a, c) FROM t1;
SELECT argMax(a, c) FROM t1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT uniq_exact(a) FROM t1;
SELECT uniq_exact(a) FROM t1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT uniq_exact(a) FILTER(WHERE b>1) FROM t1;
SELECT uniq_exact(a) FILTER(WHERE b>1) FROM t1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT date_trunc('dAy', c at time zone 'UTC') as d1 FROM t1 GROUP BY d1 ORDER BY d1;
SELECT date_trunc('day', c at time zone 'UTC') as d1 FROM t1 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT date_trunc('day', c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;
SELECT date_trunc('day', c at time zone 'UTC') as d1 FROM t2 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT date_part('day'::text, timezone('UTC'::text, c)) as d1 FROM t1 GROUP BY d1 ORDER BY d1;
SELECT date_part('day'::text, timezone('UTC'::text, c)) as d1 FROM t1 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT date_part('day'::text, timezone('UTC'::text, c)) as d1 FROM t2 GROUP BY d1 ORDER BY d1;
SELECT date_part('day'::text, timezone('UTC'::text, c)) as d1 FROM t2 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT date_part('doy'::text, timezone('UTC'::text, c)) as d1 FROM t2 GROUP BY d1 ORDER BY d1;
SELECT date_part('doy'::text, timezone('UTC'::text, c)) as d1 FROM t2 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT date_part('dow'::text, timezone('UTC'::text, c)) as d1 FROM t2 GROUP BY d1 ORDER BY d1;
SELECT date_part('dow'::text, timezone('UTC'::text, c)) as d1 FROM t2 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT date_part('minuTe'::text, timezone('UTC'::text, c)) as d1 FROM t2 GROUP BY d1 ORDER BY d1;
SELECT date_part('minuTe'::text, timezone('UTC'::text, c)) as d1 FROM t2 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT date_trunc('SeCond', c at time zone 'UTC') as d1 FROM t1 GROUP BY d1 ORDER BY d1;
SELECT date_trunc('SeCond', c at time zone 'UTC') as d1 FROM t1 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT date_part('ePoch'::text, timezone('UTC'::text, c)) as d1 FROM t2 GROUP BY d1 ORDER BY d1;
SELECT date_part('ePoch'::text, timezone('UTC'::text, c)) as d1 FROM t2 GROUP BY d1 ORDER BY d1;

EXPLAIN (VERBOSE, COSTS OFF) SELECT ltrim(val) AS a, btrim(val) AS b, rtrim(val) AS c FROM t4 GROUP BY a,b,c ORDER BY a;
SELECT ltrim(val) AS a, btrim(val) AS b, rtrim(val) AS c FROM t4 GROUP BY a,b,c ORDER BY a;

EXPLAIN (VERBOSE, COSTS OFF) SELECT strpos(val, 'val') AS a FROM t4 GROUP BY a ORDER BY a;
SELECT strpos(val, 'val') AS a FROM t4 GROUP BY a ORDER BY a;

--- check dictGet
-- dictGet is broken for now
EXPLAIN (VERBOSE, COSTS OFF) SELECT a, dictGet('regression.t3_dict', 'val', (a, 'key' || a::text)) as val, sum(b) FROM t3 GROUP BY a, val ORDER BY a;
-- SELECT a, dictGet('regression.t3_dict', 'val', (a, 'key' || a::text)) as val, sum(b) FROM t3 GROUP BY a, val ORDER BY a;

EXPLAIN (VERBOSE, COSTS OFF) SELECT a, dictGet('regression.t3_dict', 'val', (1, 'key' || a::text)) as val, sum(b) FROM t3 GROUP BY a, val ORDER BY a;
-- SELECT a, dictGet('regression.t3_dict', 'val', (1, 'key' || a::text)) as val, sum(b) FROM t3 GROUP BY a, val ORDER BY a;

DROP USER MAPPING FOR CURRENT_USER SERVER loopback;
SELECT clickhousedb_raw_query('DROP DATABASE regression');
DROP EXTENSION IF EXISTS clickhouse_fdw CASCADE;
