-- @label:bvt

DROP TABLE IF EXISTS merge_top_large_src;
DROP TABLE IF EXISTS merge_top_large_dst;
CREATE TABLE merge_top_large_src (id BIGINT PRIMARY KEY, payload VARCHAR(64));
CREATE TABLE merge_top_large_dst (id BIGINT, payload VARCHAR(64));
INSERT INTO merge_top_large_src
SELECT result, CONCAT('payload-', result) FROM generate_series(1, 20000) g;
-- Force a multi-scope AP plan; the large LIMIT must use Top -> MergeOrder -> Limit.
SET SESSION optimizer_hints = 'execType=2';
INSERT INTO merge_top_large_dst
SELECT id, payload FROM merge_top_large_src ORDER BY id DESC LIMIT 17000;
SET SESSION optimizer_hints = '';
SELECT COUNT(*), MIN(id), MAX(id), MIN(LENGTH(payload)), MAX(LENGTH(payload))
FROM merge_top_large_dst;
TRUNCATE TABLE merge_top_large_dst;
SET SESSION optimizer_hints = 'execType=2';
PREPARE merge_top_dynamic_limit FROM
'INSERT INTO merge_top_large_dst SELECT id, payload FROM merge_top_large_src ORDER BY id DESC LIMIT ?';
SET @merge_top_limit = 17000;
EXECUTE merge_top_dynamic_limit USING @merge_top_limit;
DEALLOCATE PREPARE merge_top_dynamic_limit;
SET SESSION optimizer_hints = '';
SELECT COUNT(*), MIN(id), MAX(id), MIN(LENGTH(payload)), MAX(LENGTH(payload))
FROM merge_top_large_dst;
DROP TABLE merge_top_large_dst;
DROP TABLE merge_top_large_src;
