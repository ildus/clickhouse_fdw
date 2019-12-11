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

/* adjust specific */
CREATE FUNCTION chfdw_install_adjust_functions()
RETURNS VOID AS $$
BEGIN
	CREATE FUNCTION region_map(int DEFAULT 0)
	RETURNS hstore
	AS 'MODULE_PATHNAME', 'clickhousedb_mock'
	LANGUAGE C IMMUTABLE STRICT;

	CREATE FUNCTION region_mapfb(int DEFAULT 0)
	RETURNS hstore
	AS 'MODULE_PATHNAME', 'clickhousedb_mock'
	LANGUAGE C IMMUTABLE STRICT;
END $$ LANGUAGE plpgsql;

DO LANGUAGE plpgsql $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'ajtime') THEN
		IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'hstore') THEN
			CREATE EXTENSION hstore;
		END IF;
		PERFORM chfdw_install_adjust_functions();
	END IF;
END$$;
