CREATE EXTENSION clickhouse_fdw;
CREATE EXTENSION istore;
CREATE SERVER ch_default FOREIGN DATA WRAPPER clickhouse_fdw OPTIONS(dbname 'regression');
CREATE USER MAPPING FOR CURRENT_USER SERVER ch_default;

-- create remote table
SELECT clickhousedb_raw_query('DROP DATABASE IF EXISTS regression');
SELECT clickhousedb_raw_query('CREATE DATABASE regression');
SELECT clickhousedb_raw_query('
	CREATE TABLE regression.t2 (`dt` Date, id Int32, `a_keys` Array(Int32), `a_values` Array(Int32))
	ENGINE = MergeTree()
	PARTITION BY dt
	ORDER BY (dt, id)
	SETTINGS index_granularity = 8192');
SELECT clickhousedb_raw_query('INSERT INTO regression.t2
	VALUES (''2019-10-10'', 1, [1,2,3], [11, 22, 33])');
SELECT clickhousedb_raw_query('INSERT INTO regression.t2
	VALUES (''2019-10-10'', 2, [1,2,3,4], [11,22, 33,44])');
SELECT clickhousedb_raw_query('INSERT INTO regression.t2
	VALUES (''2019-10-11'', 3, [3,4,5], [33, 44, 55])');

-- without sign
CREATE FOREIGN TABLE t2 (dt date NOT NULL, id int, a istore) SERVER ch_default;

-- default sign
SELECT clickhousedb_raw_query('
	CREATE TABLE regression.t3(`dt` Date, id Int32, `a_keys` Array(Int32),
		`a_values` Array(Int32), `sign` Int8)
	ENGINE = CollapsingMergeTree(sign)
	PARTITION BY dt
	ORDER BY (dt, a_keys)
	SETTINGS index_granularity = 8192');

SELECT clickhousedb_raw_query('INSERT INTO regression.t3
	VALUES (''2019-10-10'', 1, [1,2,3], [11, 22, 33], 1)');
SELECT clickhousedb_raw_query('INSERT INTO regression.t3
	VALUES (''2019-10-11'', 2, [3,4,5], [33, 44, 55], 1)');
SELECT clickhousedb_raw_query('INSERT INTO regression.t3
	VALUES (''2019-10-11'', 2, [3,4,5], [33, 44, 55], -1)');
SELECT clickhousedb_raw_query('INSERT INTO regression.t3
	VALUES (''2019-10-11'', 3, [3,4,5], [33, 44, 66], 1)');

CREATE FOREIGN TABLE t3 (dt date NOT NULL, id int, a istore) SERVER ch_default
	OPTIONS (table_name 't3', engine 'CollapsingMergeTree');

-- custom sign
SELECT clickhousedb_raw_query('CREATE TABLE regression.t4
		(`dt` Date, `id` Int32, `a_keys` Array(Int32), `a_values` Array(Int32), `Sign` Int8)
		ENGINE = CollapsingMergeTree(Sign)
		PARTITION BY dt
		ORDER BY (dt, a_keys) SETTINGS index_granularity = 8192');
CREATE FOREIGN TABLE t4 (dt date NOT NULL, id int, a istore) SERVER ch_default
	OPTIONS (engine 'CollapsingMergeTree(Sign)');
SELECT clickhousedb_raw_query('INSERT INTO regression.t4 VALUES (''2019-10-10'', 1, [1,2,3], [11, 22, 33], 1)');
SELECT clickhousedb_raw_query('INSERT INTO regression.t4 VALUES (''2019-10-10'', 1, [1,2,3], [11, 22, 33], -1)');
SELECT clickhousedb_raw_query('INSERT INTO regression.t4 VALUES (''2019-10-10'', 2, [1,2,3,4], [11,22, 33,44], 1)');
SELECT clickhousedb_raw_query('INSERT INTO regression.t4 VALUES (''2019-10-11'', 3, [3,4,5], [33, 44, 55], 1)');

-- check all good
EXPLAIN (VERBOSE) SELECT * FROM t2 ORDER BY dt;
SELECT * FROM t2 ORDER BY id;

EXPLAIN (VERBOSE) SELECT dt, sum(a) FROM t2 GROUP BY dt ORDER BY dt;
SELECT dt, sum(a) FROM t2 GROUP BY dt ORDER BY dt;

EXPLAIN (VERBOSE) SELECT dt, sum(a->1) FROM t2 GROUP BY dt ORDER BY dt;
SELECT dt, sum(a->1) FROM t2 GROUP BY dt ORDER BY dt;

EXPLAIN (VERBOSE) SELECT dt, sum(a) FROM t3 GROUP BY dt ORDER BY dt;
SELECT dt, sum(a) FROM t3 GROUP BY dt ORDER BY dt;

EXPLAIN (VERBOSE) SELECT dt, sum(a) FROM t4 GROUP BY dt ORDER BY dt;
SELECT dt, sum(a) FROM t4 GROUP BY dt ORDER BY dt;

-- aggregations with signs on builtin columns
SELECT clickhousedb_raw_query('
	CREATE TABLE regression.t5 (dt Date, key Int32, val Int32, `Sign` Int8)
	engine = CollapsingMergeTree(Sign)
	order by (dt, key) partition by dt settings index_granularity = 8192;'
);

SELECT clickhousedb_raw_query('insert into regression.t5 select ''2010-10-10'', number, number, 1 from numbers(10);');
SELECT clickhousedb_raw_query('insert into regression.t5 select ''2010-10-10'', number, number, -1 from numbers(5);');
SELECT clickhousedb_raw_query('insert into regression.t5 select ''2010-10-11'', number, number, 1 from numbers(10);');
SELECT clickhousedb_raw_query('insert into regression.t5 select ''2010-10-11'', number, number, -1 from numbers(5);');
CREATE FOREIGN TABLE t5 (dt date NOT NULL, key int, val int)
SERVER ch_default OPTIONS (table_name 't5', engine 'CollapsingMergeTree(Sign)');

EXPLAIN (VERBOSE) SELECT sum(val) FROM t5;
SELECT sum(val) FROM t5;

EXPLAIN (VERBOSE) SELECT sum(val + 10) FROM t5;
SELECT sum(val + 10) FROM t5;

EXPLAIN (VERBOSE) SELECT sum(val) FILTER (WHERE val > 6) FROM t5;
SELECT sum(val) FILTER (WHERE val > 6) FROM t5;

EXPLAIN (VERBOSE) SELECT count(*) FROM t5;
SELECT count(*) FROM t5;

EXPLAIN (VERBOSE) SELECT count(val) FROM t5;
SELECT count(val) FROM t5;

EXPLAIN (VERBOSE) SELECT count(val + 10) FROM t5;
SELECT count(val + 10) FROM t5;

EXPLAIN (VERBOSE) SELECT count(val) FILTER (WHERE val > 6) FROM t5;
SELECT count(val) FILTER (WHERE val > 6) FROM t5;

EXPLAIN (VERBOSE) SELECT avg(val) FROM t5;
SELECT avg(val) FROM t5;

EXPLAIN (VERBOSE) SELECT avg(val + 10) FROM t5;
SELECT avg(val + 10) FROM t5;

EXPLAIN (VERBOSE) SELECT avg(val) FILTER (WHERE val > 6) FROM t5;
SELECT avg(val) FILTER (WHERE val > 6) FROM t5;

SELECT clickhousedb_raw_query('DROP DATABASE regression');

DROP USER MAPPING FOR CURRENT_USER SERVER ch_default;
DROP EXTENSION clickhouse_fdw CASCADE;
DROP EXTENSION istore CASCADE;
