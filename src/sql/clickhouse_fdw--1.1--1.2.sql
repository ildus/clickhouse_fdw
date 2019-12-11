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
