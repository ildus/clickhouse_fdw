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

