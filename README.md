[![Build Status](https://travis-ci.org/adjust/clickhouse_fdw.svg?branch=master)](https://travis-ci.org/adjust/clickhouse_fdw)

`clickhouse_fdw` - ClickHouse Foreign Data Wrapper for PostgreSQL
=================================================================

Originally forked from: https://github.com/Percona-Lab/clickhousedb_fdw. Differences:

* remove ODBC, use HTTP or binary protocols
* fix JOINS
* push down `CASE WHEN`
* support `coalesce`, `date_trunc` and other specific functions
* support query cancelation
* use `cmake` for Makefile generation (for out for source builds and proper dependency checks)
* support arrays and `ANY`, `ALL` functions
* support CollapsingMergeTree engine (with `sign` column) in aggregations
* add infrastructure for adding custom behaviours in the code (for custom types)
* code cleanup
* many other improvements

The `clickhouse_fdw` is open-source. It is a Foreign Data Wrapper (FDW) for one
of the fastest column store databases; "Clickhouse". This FDW allows you to
SELECT from, and INSERT into, a ClickHouse database from within a PostgreSQL v11 server.

The FDW supports advanced features like aggregate pushdown and joins pushdown.
These significantly improve performance by utilizing the remote server’s resources
for these resource intensive operations.

Supported PostgreSQL Versions
------------------------------

PostgreSQL 11-12

Installation
----------------

###### Prerequisite

The `clickhouse_fdw` uses an HTTP interface provided by ClickHouse. `libcurl` and
`uuid` libraries should be installed in the system.

###### Installing `clickhouse_fdw`

```
git clone https://github.com/adjust/clickhouse_fdw.git
cd clickhouse_fdw
mkdir build && cd build
cmake ..
make && make install
```

Usage
-----

You need to set up the sample database and tables in the ClickHouse database.
Here we create a sample database name test_database and two sample tables
tax_bills_nyc and tax_bills:

    CREATE DATABASE test_database;
    USE test_database;
    CREATE TABLE tax_bills_nyc
    (
        bbl Int64,
        owner_name String,
        address String,
        tax_class String,
        tax_rate String,
        emv Float64,
        tbea Float64,
        bav Float64,
        tba String,
        property_tax String,
        condonumber String,
        condo String,
        insertion_date DateTime MATERIALIZED now()
    )
    ENGINE = MergeTree PARTITION BY tax_class ORDER BY (owner_name)

    CREATE TABLE tax_bills
    (
        bbl Int64,
        owner_name String
    )
    ENGINE = MergeTree
    PARTITION BY bbl
    ORDER BY bbl;

Download the sample data from the taxbills.nyc website and put the data in the table:

    curl -X GET 'http://taxbills.nyc/tax_bills_june15_bbls.csv' | \
		clickhouse-client --input_format_allow_errors_num=10 \
		--query="INSERT INTO test_database.tax_bills_nyc FORMAT CSV"

Now the data is ready in the ClickHouse, the next step is to set up the PostgreSQL side.
First we need to create a FDW extension and a ClickHouse foreign server:

    CREATE EXTENSION clickhouse_fdw;
    CREATE SERVER clickhouse_svr FOREIGN DATA WRAPPER clickhouse_fdw OPTIONS(dbname 'test_database');

By default the server will use `http` protocol. But we could use binary protocol:

    CREATE SERVER clickhouse_svr FOREIGN DATA WRAPPER clickhouse_fdw
		OPTIONS(dbname 'test_database', driver 'binary');

Available parameters:

	* dbname
	* host
	* port
	* driver

Now create user mapping and foreign tables:

    CREATE USER MAPPING FOR CURRENT_USER SERVER clickhouse_svr
		OPTIONS (user 'default', password '');
	IMPORT FOREIGN SCHEMA "kk" FROM SERVER clickhouse_svr INTO public;

	SELECT bbl,tbea,bav,insertion_date FROM tax_bills_nyc LIMIT 5;
        bbl     | tbea  |  bav   |   insertion_date
	------------+-------+--------+---------------------
     1001200009 | 72190 | 648900 | 2019-08-03 11:04:38
     4000620001 |  8961 |  80550 | 2019-08-03 11:04:38
     4157860094 | 13317 | 119700 | 2019-08-03 11:04:38
     4123850237 |    50 |    450 | 2019-08-03 11:04:38
     4103150163 |  2053 |  18450 | 2019-08-03 11:04:38

    INSERT INTO tax_bills SELECT bbl, tbea from tax_bills_nyc LIMIT 100;

    EXPLAIN VERBOSE SELECT bbl,tbea,bav,insertion_date FROM tax_bills_nyc LIMIT 5;
                                         QUERY PLAN
    --------------------------------------------------------------------------------------------
    Limit  (cost=0.00..0.00 rows=1 width=32)
    Output: bbl, tbea, bav, insertion_date
     ->  Foreign Scan on public.tax_bills_nyc  (cost=0.00..0.00 rows=0 width=32)
             Output: bbl, tbea, bav, insertion_date
             Remote SQL: SELECT bbl, tbea, bav, insertion_date FROM test_database.tax_bills_nyc
    (5 rows)

Aggregate Pushdown.
-------------------

Aggregate pushdown is a new feature of PostgreSQL FDW. There are currently very
few foreign data wrappers that support aggregate pushdown: `clickhouse_fdw` is one of them.
Planner decides which aggregate to pushdown or not. Here is an example for both cases.

    postgres=# EXPLAIN VERBOSE SELECT count(bbl) FROM tax_bills_nyc LIMIT 5;
                                    QUERY PLAN                                                                
    ---------------------------------------------------------------------------
    Limit  (cost=0.00..0.01 rows=1 width=8)
    Output: (count(bbl))
    ->  Aggregate  (cost=0.00..0.01 rows=1 width=8)
             Output: count(bbl)
             ->  Foreign Scan on public.tax_bills_nyc  (cost=0.00..0.00 rows=0 width=8)
                Output: bbl, owner_name, address, tax_class, tax_rate, emv, tbea, bav, tba, property_tax, condonumber, condo, insertion_date
                   Remote SQL: SELECT bbl FROM test_database.tax_bills_nyc
    (7 rows)

        EXPLAIN VERBOSE SELECT count(bbl) FROM tax_bills_nyc;
                                QUERY PLAN                            
    ------------------------------------------------------------------
    Foreign Scan  (cost=1.00..-1.00 rows=1000 width=8)
    Output: (count(bbl))
    Relations: Aggregate on (tax_bills_nyc)
    Remote SQL: SELECT count(bbl) FROM test_database.tax_bills_nyc
    (4 rows)



Join Pushdown
---------------

Join pushdown is also a very new feature of PostgreSQL FDW’s. The `clickhouse_fdw` also supports join pushdown.

    EXPLAIN VERBOSE SELECT t2.bbl, t2.owner_name, t1.bav FROM tax_bills_nyc t1 RIGHT OUTER JOIN tax_bills t2 ON (t1.bbl = t2.bbl);
                                                                        QUERY PLAN                                                                     
    
    -------------------------------------------------------------------------------------------------------------------------------------------------------
    ----
    Foreign Scan  (cost=1.00..-1.00 rows=1000 width=50)
    Output: t2.bbl, t2.owner_name, t1.bav
    Relations: (tax_bills t2) LEFT JOIN (tax_bills_nyc t1)
    Remote SQL: SELECT r2.bbl, r2.owner_name, r1.bav FROM  test_database.tax_bills r2 ALL LEFT JOIN test_database.tax_bills_nyc r1 ON (((r1.bbl = r2.bbl
    )))
    (4 rows)

[1]: https://www.postgresql.org/
[2]: http://www.clickhouse.com
[3]: https://github.com/ildus/clickhouse_fdw/issues/new
