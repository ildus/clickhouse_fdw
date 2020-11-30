DROP FUNCTION clickhousedb_raw_query(TEXT, TEXT);
CREATE  FUNCTION clickhousedb_raw_query(TEXT, TEXT DEFAULT 'host=localhost port=8123')
RETURNS TEXT
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
