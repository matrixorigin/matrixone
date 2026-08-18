DROP DATABASE IF EXISTS view_metadata_refresh;
CREATE DATABASE view_metadata_refresh;
USE view_metadata_refresh;

CREATE TABLE source_table (
    id INT NOT NULL,
    code VARCHAR(12) DEFAULT 'seed',
    amount DECIMAL(10,2) DEFAULT 1.25,
    state ENUM('new','ready') DEFAULT 'new',
    flags SET('a','b') DEFAULT 'a'
);
CREATE VIEW direct_view AS
    SELECT id, code AS code_alias, amount, state, flags, amount * 2 AS doubled
    FROM source_table;
CREATE VIEW chain_view (renamed_code, derived_amount) AS
    SELECT code_alias, doubled FROM direct_view;

CREATE TABLE identity_before AS
    SELECT relname, rel_id, rel_logical_id, creator, owner,
           cast(json_extract(viewdef, '$.Stmt') AS VARCHAR(6000)) AS view_sql,
           cast(json_extract(viewdef, '$.DefaultDatabase') AS VARCHAR(6000)) AS default_database,
           cast(json_extract(viewdef, '$.sql_mode') AS VARCHAR(6000)) AS sql_mode,
           cast(json_extract(viewdef, '$.security_type') AS VARCHAR(6000)) AS security_type
    FROM mo_catalog.mo_tables
    WHERE reldatabase = 'view_metadata_refresh'
      AND relname IN ('direct_view','chain_view');

ALTER TABLE source_table
    MODIFY code VARCHAR(48) DEFAULT 'wider',
    MODIFY amount DECIMAL(18,4) NOT NULL DEFAULT 2.5000,
    MODIFY state ENUM('new','ready','done') DEFAULT 'ready',
    MODIFY flags SET('a','b','c') DEFAULT 'b';

-- @wait_expect(2, 60)
SELECT count(*) FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
  AND target_relation_name IN ('direct_view','chain_view')
  AND status = 'CURRENT';

DESC direct_view;
DESC chain_view;
SELECT table_name, column_name, column_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'view_metadata_refresh'
  AND table_name IN ('direct_view','chain_view')
ORDER BY table_name, ordinal_position;

CREATE TABLE copied_from_view AS SELECT * FROM direct_view;
DESC copied_from_view;
INSERT INTO copied_from_view (id, doubled) VALUES (1, 0);
SELECT id, code_alias, amount, state, flags FROM copied_from_view;

ALTER TABLE source_table ALGORITHM=COPY,
    MODIFY code VARCHAR(80) DEFAULT 'copied',
    MODIFY amount DECIMAL(22,6) NOT NULL DEFAULT 3.750000;

-- @wait_expect(2, 60)
SELECT count(*) FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
  AND target_relation_name IN ('direct_view','chain_view')
  AND status = 'CURRENT';

DESC direct_view;
DESC chain_view;

BEGIN;
ALTER TABLE source_table ALGORITHM=COPY,
    MODIFY code VARCHAR(96) DEFAULT 'first';
ALTER TABLE source_table ALGORITHM=COPY,
    MODIFY code VARCHAR(112) DEFAULT 'final';
COMMIT;

-- @wait_expect(2, 60)
SELECT count(*) FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
  AND target_relation_name IN ('direct_view','chain_view')
  AND status = 'CURRENT';

DESC direct_view;
DESC chain_view;
SELECT count(*) = 2 AS generations_current
FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
  AND target_relation_name IN ('direct_view','chain_view')
  AND status = 'CURRENT'
  AND target_generation = completed_generation;
SELECT count(*) = 2 AS identity_preserved
FROM identity_before b JOIN mo_catalog.mo_tables t
 ON t.reldatabase = 'view_metadata_refresh' AND t.relname = b.relname
 AND t.rel_id = b.rel_id AND t.rel_logical_id = b.rel_logical_id
 AND t.creator = b.creator AND t.owner = b.owner
 AND cast(json_extract(if(t.viewdef = '', '{}', t.viewdef), '$.Stmt') AS VARCHAR(6000)) = b.view_sql
 AND cast(json_extract(if(t.viewdef = '', '{}', t.viewdef), '$.DefaultDatabase') AS VARCHAR(6000)) = b.default_database
 AND cast(json_extract(if(t.viewdef = '', '{}', t.viewdef), '$.sql_mode') AS VARCHAR(6000)) = b.sql_mode
 AND cast(json_extract(if(t.viewdef = '', '{}', t.viewdef), '$.security_type') AS VARCHAR(6000)) = b.security_type;
SHOW CREATE VIEW direct_view;
SHOW CREATE VIEW chain_view;

CREATE TABLE left_source (id INT NOT NULL);
CREATE TABLE right_source (id INT NOT NULL);
INSERT INTO left_source VALUES (1);
INSERT INTO right_source VALUES (2);
CREATE VIEW outer_join_view AS
    SELECT l.id, r.id AS rid FROM left_source l LEFT JOIN right_source r ON l.id = r.id;
ALTER TABLE right_source MODIFY id BIGINT NOT NULL;
-- @wait_expect(1, 60)
SELECT count(*) FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
  AND target_relation_name = 'outer_join_view'
  AND status = 'CURRENT';
CREATE TABLE copied_outer_join AS SELECT * FROM outer_join_view;
SELECT count(*) FROM copied_outer_join WHERE rid IS NULL;

CREATE TABLE invalid_left (a INT);
CREATE TABLE invalid_right (a INT);
CREATE VIEW invalid_direct AS
SELECT invalid_left.a, invalid_right.a AS b FROM invalid_left LEFT JOIN invalid_right USING(a);
CREATE VIEW invalid_downstream AS SELECT a FROM invalid_direct;
ALTER TABLE invalid_left CHANGE a renamed_a BIGINT;
-- @wait_expect(2, 60)
SELECT count(*) FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
AND target_relation_name IN ('invalid_direct','invalid_downstream')
AND status = 'INVALID' AND failure_code = 1;
SELECT target_relation_name, status, failure_code
FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
AND target_relation_name IN ('invalid_direct','invalid_downstream')
ORDER BY target_relation_name;
DROP TABLE invalid_left;
CREATE TABLE invalid_left (c1 INT, c2 INT);
SHOW CREATE TABLE invalid_left;

CREATE TABLE restored_source (a INT);
CREATE VIEW restored_direct AS SELECT a FROM restored_source;
CREATE VIEW restored_downstream AS SELECT a FROM restored_direct;
DROP SNAPSHOT IF EXISTS view_metadata_before_widening;
CREATE SNAPSHOT view_metadata_before_widening FOR ACCOUNT;
ALTER TABLE restored_source MODIFY a BIGINT;
-- @wait_expect(2, 60)
SELECT count(*) FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
AND target_relation_name IN ('restored_direct','restored_downstream')
AND status = 'CURRENT';
DESC restored_downstream;
RESTORE TABLE view_metadata_refresh.restored_source{SNAPSHOT='view_metadata_before_widening'};
-- @wait_expect(2, 60)
SELECT count(*) FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
AND target_relation_name IN ('restored_direct','restored_downstream')
AND status = 'CURRENT';
DESC restored_direct;
DESC restored_downstream;

CREATE VIEW snapshot_direct AS
SELECT a FROM restored_source{SNAPSHOT='view_metadata_before_widening'};
CREATE VIEW snapshot_downstream AS SELECT a FROM snapshot_direct;
CREATE TABLE copied_snapshot_view AS SELECT * FROM snapshot_downstream;
DESC copied_snapshot_view;
DROP SNAPSHOT view_metadata_before_widening;
SELECT count(*) = 2 AS snapshot_closure_invalidated
FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
AND target_relation_name IN ('snapshot_direct','snapshot_downstream')
AND status <> 'CURRENT';

CREATE TABLE removed_source (a INT);
CREATE VIEW removed_direct AS SELECT a FROM removed_source;
CREATE VIEW removed_downstream AS SELECT a FROM removed_direct;
DROP TABLE removed_source;
SELECT count(*) = 2 AS removal_closure_invalidated
FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
AND target_relation_name IN ('removed_direct','removed_downstream')
AND status <> 'CURRENT';

CREATE TABLE cte_limited_source (a INT);
CREATE VIEW cte_limited_direct AS SELECT a FROM cte_limited_source;
CREATE VIEW cte_limited_downstream AS SELECT a FROM cte_limited_direct;
SET cte_max_recursion_depth = 0;
SET cte_max_memory_bytes = 1;
DROP TABLE cte_limited_source;
SELECT count(*) = 2 AS system_cte_limits_used
FROM mo_catalog.mo_view_refresh
WHERE target_database_name = 'view_metadata_refresh'
AND target_relation_name IN ('cte_limited_direct','cte_limited_downstream')
AND status <> 'CURRENT';
SET cte_max_recursion_depth = DEFAULT;
SET cte_max_memory_bytes = DEFAULT;

DROP DATABASE view_metadata_refresh;
