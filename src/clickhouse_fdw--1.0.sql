-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION clickhouse_fdw" to load this file. \quit

CREATE FUNCTION clickhousedb_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION clickhousedb_raw_query(TEXT, TEXT DEFAULT 'http://localhost:8123')
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

CREATE OR REPLACE FUNCTION argmax_transfn(state anyarray, arg anyelement,
	val anyelement)
RETURNS anyarray
AS $$
BEGIN
	IF state IS NULL THEN RETURN ARRAY[arg,val]; END IF;
	IF val > state[2] THEN RETURN ARRAY[arg,val]; END IF;

	RETURN state;
END
$$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION argmax_finalfn(state anyarray)
RETURNS anyelement
AS $$
BEGIN
	RETURN state[1];
END
$$ LANGUAGE 'plpgsql' IMMUTABLE STRICT;

CREATE AGGREGATE argMax(anyelement, anyelement)
(
    sfunc = argmax_transfn,
    stype = anyarray,
	finalfunc = argmax_finalfn
);

CREATE OR REPLACE FUNCTION argmin_transfn(state anyarray, arg anyelement,
	val anyelement)
RETURNS anyarray
AS $$
BEGIN
	IF state IS NULL THEN RETURN ARRAY[arg,val]; END IF;
	IF val < state[2] THEN RETURN ARRAY[arg,val]; END IF;

	RETURN state;
END
$$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION argmin_finalfn(state anyarray)
RETURNS anyelement
AS $$
BEGIN
	RETURN state[1];
END
$$ LANGUAGE 'plpgsql' IMMUTABLE STRICT;

CREATE AGGREGATE argMin(anyelement, anyelement)
(
    sfunc = argmin_transfn,
    stype = anyarray,
	finalfunc = argmin_finalfn
);
