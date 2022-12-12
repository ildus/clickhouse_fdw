`clickhouse_fdw` - ClickHouse Foreign Data Wrapper for PostgreSQL
=================================================================

The `clickhouse_fdw` is open-source. It is a Foreign Data Wrapper (FDW) for `ClickHouse` column oriented database.

Supported PostgreSQL Versions
------------------------------

PostgreSQL 11-14

Installation
----------------

###### Prerequisite

The `clickhouse_fdw` uses an HTTP interface provided by ClickHouse. `libcurl` and
`uuid` libraries should be installed in the system. Make sure that ClickHouse
uses UTC timezone.

###### Installing `clickhouse_fdw`

```
git clone git@github.com:ildus/clickhouse_fdw.git
cd clickhouse_fdw
mkdir build && cd build
cmake ..
make && make install
```

###### Centos 7 Notes

You should be using modern compiler, available in [devtoolset-7](https://www.softwarecollections.org/en/scls/rhscl/devtoolset-7/). Once installed, activate it with the command:

```sh
source scl_source enable devtoolset-7
```

Minimal libcurl compatible with clickhouse-fdw is 7.43.0. It is not available in the official Centos repo. You can download recent RPMs from [here](https://cbs.centos.org/koji/buildinfo?buildID=1408)

You can upgrade libcurl with command:

```sh
sudo rpm -Uvh  *curl*rpm --nodeps
sudo yum install libmetalink
```

`uuid` can be installed with:

```sh
yum install libuuid-devel
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
    ENGINE = MergeTree PARTITION BY tax_class ORDER BY (owner_name);

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

    CREATE USER MAPPING FOR CURRENT_USER SERVER clickhouse_svr OPTIONS (user 'default', password '');
    IMPORT FOREIGN SCHEMA "test_database" FROM SERVER clickhouse_svr INTO public;

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

AggregatingMergeTree
--------------------

For columns where you need `Merge` prefix in aggregations just add `AggregateFunction` option with aggregation function name. Example:

```
ALTER TABLE table ALTER COLUMN b OPTIONS (SET AggregateFunction 'count');
```

Or use `IMPORT SCHEMA` for automatic definitions.

[1]: https://www.postgresql.org/
[2]: http://www.clickhouse.com
[3]: https://github.com/ildus/clickhouse_fdw/issues/new
