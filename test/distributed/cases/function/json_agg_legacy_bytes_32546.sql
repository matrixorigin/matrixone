-- Legacy character columns can retain non-UTF-8 bytes. Both aggregate values
-- and object keys must become valid JSON text after canonical admission.
DROP DATABASE IF EXISTS json_agg_legacy_bytes_32546;
CREATE DATABASE json_agg_legacy_bytes_32546;
USE json_agg_legacy_bytes_32546;
CREATE TABLE t(k VARCHAR(20), v VARCHAR(20));
INSERT INTO t VALUES (x'ff', x'fe');
SELECT HEX(k) AS source_key, HEX(v) AS source_value FROM t;
SELECT HEX(JSON_UNQUOTE(JSON_EXTRACT(JSON_ARRAYAGG(v), '$[0]'))) AS array_value FROM t;
SELECT HEX(JSON_UNQUOTE(JSON_EXTRACT(JSON_KEYS(JSON_OBJECTAGG(k, v)), '$[0]'))) AS object_key FROM t;
SELECT HEX(JSON_UNQUOTE(JSON_EXTRACT(JSON_OBJECTAGG(k, v), '$."�"'))) AS object_value FROM t;
DROP DATABASE json_agg_legacy_bytes_32546;
