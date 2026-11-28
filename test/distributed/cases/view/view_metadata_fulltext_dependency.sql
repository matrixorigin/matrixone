-- A missing FULLTEXT dependency must not abort metadata reads of other objects (#32763).
SET @saved_fulltext_index = @@session.experimental_fulltext_index;
SET experimental_fulltext_index = 1;
DROP DATABASE IF EXISTS tc_meta_target;
DROP DATABASE IF EXISTS tc_meta_view;

CREATE DATABASE tc_meta_target;
CREATE TABLE tc_meta_target.t (
    id INT PRIMARY KEY,
    payload VARCHAR(30)
);
INSERT INTO tc_meta_target.t VALUES (1, 'unchanged');

CREATE DATABASE tc_meta_view;
USE tc_meta_view;
CREATE TABLE docs (id INT PRIMARY KEY, body TEXT);
INSERT INTO docs VALUES (1, 'hello matrixone'), (2, 'other text');
CREATE FULLTEXT INDEX ft_body ON docs(body);
CREATE VIEW v_good AS
    SELECT id FROM docs WHERE MATCH(body) AGAINST('hello');

CREATE VIEW v_plain AS SELECT id FROM docs;

-- The catalog client's cross-database JOIN retains its hex/CONVERT predicates.
SELECT c.attname, c.attnotnull, c.att_is_hidden,
       s.DATA_TYPE, s.COLLATION_NAME, s.EXTRA
FROM mo_catalog.mo_columns c
JOIN mo_catalog.mo_tables t
  ON c.account_id = t.account_id AND c.att_relname_id = t.rel_id
LEFT JOIN information_schema.COLUMNS s
  ON s.TABLE_SCHEMA = t.reldatabase
 AND s.TABLE_NAME = t.relname
 AND s.COLUMN_NAME = c.attname
WHERE t.account_id = current_account_id()
  AND (
    t.reldatabase = CONVERT(X'74635f6d6574615f746172676574' USING utf8mb4)
    OR (@@lower_case_table_names <> 0 AND LOWER(t.reldatabase) =
        LOWER(CONVERT(X'74635f6d6574615f746172676574' USING utf8mb4)))
  )
  AND (
    t.relname = CONVERT(X'74' USING utf8mb4)
    OR (@@lower_case_table_names <> 0 AND LOWER(t.relname) =
        LOWER(CONVERT(X'74' USING utf8mb4)))
  )
ORDER BY c.attnum;

DROP INDEX ft_body ON docs;
SELECT c.attname, c.attnotnull, c.att_is_hidden,
       s.DATA_TYPE, s.COLLATION_NAME, s.EXTRA
FROM mo_catalog.mo_columns c
JOIN mo_catalog.mo_tables t
  ON c.account_id = t.account_id AND c.att_relname_id = t.rel_id
LEFT JOIN information_schema.COLUMNS s
  ON s.TABLE_SCHEMA = t.reldatabase
 AND s.TABLE_NAME = t.relname
 AND s.COLUMN_NAME = c.attname
WHERE t.account_id = current_account_id()
  AND (
    t.reldatabase = CONVERT(X'74635f6d6574615f746172676574' USING utf8mb4)
    OR (@@lower_case_table_names <> 0 AND LOWER(t.reldatabase) =
        LOWER(CONVERT(X'74635f6d6574615f746172676574' USING utf8mb4)))
  )
  AND (
    t.relname = CONVERT(X'74' USING utf8mb4)
    OR (@@lower_case_table_names <> 0 AND LOWER(t.relname) =
        LOWER(CONVERT(X'74' USING utf8mb4)))
  )
ORDER BY c.attnum;

-- Enumerate view candidates across both databases, with stable output limited to this fixture.
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA IN ('tc_meta_target', 'tc_meta_view')
  AND TABLE_NAME IN ('t', 'v_good', 'v_plain')
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
SHOW WARNINGS;

-- Targeting the invalid view directly still reports the dependency failure.
DESC v_good;

CREATE FULLTEXT INDEX ft_body ON docs(body);
SELECT c.attname, c.attnotnull, c.att_is_hidden,
       s.DATA_TYPE, s.COLLATION_NAME, s.EXTRA
FROM mo_catalog.mo_columns c
JOIN mo_catalog.mo_tables t
  ON c.account_id = t.account_id AND c.att_relname_id = t.rel_id
LEFT JOIN information_schema.COLUMNS s
  ON s.TABLE_SCHEMA = t.reldatabase
 AND s.TABLE_NAME = t.relname
 AND s.COLUMN_NAME = c.attname
WHERE t.account_id = current_account_id()
  AND (
    t.reldatabase = CONVERT(X'74635f6d6574615f746172676574' USING utf8mb4)
    OR (@@lower_case_table_names <> 0 AND LOWER(t.reldatabase) =
        LOWER(CONVERT(X'74635f6d6574615f746172676574' USING utf8mb4)))
  )
  AND (
    t.relname = CONVERT(X'74' USING utf8mb4)
    OR (@@lower_case_table_names <> 0 AND LOWER(t.relname) =
        LOWER(CONVERT(X'74' USING utf8mb4)))
  )
ORDER BY c.attnum;
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA IN ('tc_meta_target', 'tc_meta_view')
  AND TABLE_NAME IN ('t', 'v_good', 'v_plain')
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
SHOW WARNINGS;

DROP DATABASE tc_meta_view;
DROP DATABASE tc_meta_target;
SET experimental_fulltext_index = @saved_fulltext_index;
