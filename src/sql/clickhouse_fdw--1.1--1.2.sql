CREATE FUNCTION dictGet(text, text, anyelement)
RETURNS text
AS 'MODULE_PATHNAME', 'clickhousedb_mock'
LANGUAGE C VOLATILE STRICT;
