-- JSON_SEARCH one/all, real locations, case sensitivity and path traversal.
SET @old_sql_mode = @@sql_mode;
SET sql_mode = '';

SELECT JSON_SEARCH('{"a":"x"}', 'one', 'x') AS one_match;
SELECT JSON_SEARCH('{"a":["x","x"],"one potato":{"x":"x"},"nested":[{"x":"x"}]}', 'all', 'x') AS all_matches;
SELECT JSON_SEARCH('["abc","ABC"]', 'one', 'abc') AS case_exact;
SELECT JSON_SEARCH('["abc","ABC"]', 'one', 'aBc') AS case_mismatch;
SELECT JSON_SEARCH('{"a":"x"}', 'all', 'no-match') IS NULL AS no_match;

-- Overlapping filters are deduplicated and explicit paths are evaluated in source order.
SELECT JSON_SEARCH('{"a":["x","x"]}', 'all', 'x', NULL, '$.a[*]', '$.a[1]') AS overlapping_paths;
SELECT JSON_SEARCH('{"a":"x"}', 'one', 'x', NULL, '$.missing') IS NULL AS missing_path;
SELECT JSON_SEARCH('{"a":"x"}', 'one', 'x', NULL, 'not-a-path') AS invalid_path;

-- LIKE wildcards, default/custom escape and NO_BACKSLASH_ESCAPES.
SELECT JSON_SEARCH('["a_b","axb","a%b"]', 'all', 'a!_b', '!') AS custom_escape;
SELECT JSON_SEARCH('["a_b","axb","a%b"]', 'all', 'a\%b') AS default_escape;
SELECT JSON_SEARCH('["a_b","axb","a%b"]', 'all', 'a\%b', NULL) AS explicit_null_escape;
SELECT JSON_SEARCH('["a_b","axb","a%b"]', 'all', 'a_b', '') AS empty_escape_disabled;

SET sql_mode = 'NO_BACKSLASH_ESCAPES';
SELECT JSON_SEARCH('["a_b","axb","a%b"]', 'all', 'a_b', '') AS empty_escape_nbe;
SELECT JSON_SEARCH('["a_b","axb","a%b"]', 'all', 'a_b', NULL) AS explicit_null_escape_nbe;
SET sql_mode = '';

-- NULL and malformed document/mode/path priority.
SELECT JSON_SEARCH(NULL, 'all', 'x') AS null_document;
SELECT JSON_SEARCH(NULL, 'invalid-mode', 'x', '', 'not-a-path') IS NULL AS ordered_null;
SELECT JSON_SEARCH('not-json', 'one', 'x') AS invalid_document;
SELECT JSON_SEARCH('{"a":"x"}', 'invalid-mode', 'x') AS invalid_mode;
SELECT JSON_SEARCH('{"a":"x"}', 'one', 'x', NULL, 'a_%_b') AS malformed_path;
SELECT JSON_SEARCH('{"a":"x"}', 'one', 'x', '界!') AS multirune_escape;

-- VARCHAR document and search term columns use the case-sensitive SEARCH comparator.
DROP TABLE IF EXISTS t_json_search;
CREATE TABLE t_json_search (id INT PRIMARY KEY, doc VARCHAR(1024), term VARCHAR(64));
INSERT INTO t_json_search VALUES
  (1, '{"a":["x","X"]}', 'x'),
  (2, '{"a":["x","X"]}', 'X'),
  (3, '{"a":["x","X"]}', 'missing');
SELECT JSON_SEARCH(doc, 'all', term) AS loc FROM t_json_search ORDER BY id;
DROP TABLE t_json_search;

-- Views retain the v136 requirement; generated columns fail closed.
DROP VIEW IF EXISTS v_json_search;
CREATE VIEW v_json_search AS SELECT JSON_SEARCH('{"a":"x"}', 'one', 'x') AS loc;
SELECT loc FROM v_json_search;
DROP VIEW v_json_search;
CREATE TABLE t_json_search_generated (
  g JSON GENERATED ALWAYS AS (JSON_SEARCH('{"a":"x"}', 'one', 'x')) STORED
);
DROP TABLE IF EXISTS t_json_search_generated;

-- Prepared text, path, NULL, mismatch, error and same-statement reuse.
SET @json_search_doc = '{"a":"x"}';
SET @json_search_term = 'x';
PREPARE json_search_text FROM 'SELECT JSON_SEARCH(?, ''one'', ?) AS loc';
EXECUTE json_search_text USING @json_search_doc, @json_search_term;
SET @json_search_term = 'X';
EXECUTE json_search_text USING @json_search_doc, @json_search_term;
SET @json_search_term = NULL;
EXECUTE json_search_text USING @json_search_doc, @json_search_term;
SET @json_search_term = 'x';
SET @json_search_path = '$.a';
PREPARE json_search_path FROM 'SELECT JSON_SEARCH(?, ''one'', ?, NULL, ?) AS loc';
EXECUTE json_search_path USING @json_search_doc, @json_search_term, @json_search_path;
DEALLOCATE PREPARE json_search_text;
DEALLOCATE PREPARE json_search_path;

-- An explicitly supplied escape is compile-time constant.
PREPARE json_search_escape FROM 'SELECT JSON_SEARCH(''["a_b"]'', ''one'', ''a!_b'', ?) AS loc';

SELECT COUNT(*) AS residue
FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = 't_json_search';
SET sql_mode = @old_sql_mode;
SET @old_sql_mode = NULL;
SET @json_search_doc = NULL;
SET @json_search_term = NULL;
SET @json_search_path = NULL;
