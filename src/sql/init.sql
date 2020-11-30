-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION clickhouse_fdw" to load this file. \quit

CREATE FUNCTION clickhousedb_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION clickhousedb_raw_query(TEXT, TEXT DEFAULT 'host=localhost port=8123')
RETURNS TEXT
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION clickhousedb_fdw_validator(text[], oid)
RETURNS VOID
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER clickhouse_fdw
	HANDLER clickhousedb_fdw_handler
	VALIDATOR clickhousedb_fdw_validator;
