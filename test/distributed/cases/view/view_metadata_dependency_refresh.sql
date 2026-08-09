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

DESC direct_view;
DESC chain_view;

BEGIN;
ALTER TABLE source_table ALGORITHM=COPY,
    MODIFY code VARCHAR(96) DEFAULT 'first';
ALTER TABLE source_table ALGORITHM=COPY,
    MODIFY code VARCHAR(112) DEFAULT 'final';
COMMIT;

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

DROP DATABASE view_metadata_refresh;
