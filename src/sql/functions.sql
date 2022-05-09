-- this is reference for argMax and argMin functions, but these functions
-- should not be used on postgres side and always pushed down to ClickHouse
CREATE FUNCTION ch_argmax(anyelement, anyelement, bigint) RETURNS anyelement
AS $$ BEGIN
	RAISE EXCEPTION 'argMax should be pushed down';
END $$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE AGGREGATE argMax(anyelement, bigint)
(
    sfunc = ch_argmax,
    stype = anyelement
);

CREATE FUNCTION ch_argmin(anyelement, anyelement, bigint) RETURNS anyelement
AS $$ BEGIN
	RAISE EXCEPTION 'argMin should be pushed down';
END $$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE AGGREGATE argMin(anyelement, bigint)
(
    sfunc = ch_argmin,
    stype = anyelement
);

CREATE FUNCTION ch_argmin(anyelement, anyelement, timestamp) RETURNS anyelement
AS $$ BEGIN
	RAISE EXCEPTION 'this aggregation should be pushed down';
END $$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE AGGREGATE argMin(anyelement, timestamp)
(
    sfunc = ch_argmin,
    stype = anyelement
);

CREATE AGGREGATE argMax(anyelement, timestamp)
(
    sfunc = ch_argmin,
    stype = anyelement
);

CREATE FUNCTION dictGet(text, text, anyelement)
RETURNS text
AS 'MODULE_PATHNAME', 'clickhousedb_mock'
LANGUAGE C VOLATILE STRICT;

/*
uniqExact
*/
CREATE FUNCTION ch_uniq_exact(bigint, anyelement) RETURNS bigint
AS $$ BEGIN
	RAISE EXCEPTION 'uniq_exact should be pushed down';
END $$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE AGGREGATE uniq_exact(anyelement)
(
    sfunc = ch_uniq_exact,
    stype = bigint
);

