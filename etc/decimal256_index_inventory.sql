-- Run separately as an administrator in EVERY account during the upgrade window.
-- Conservative inventory: every regular secondary/unique index on a table
-- containing DECIMAL256, including indexes whose hidden suffix uses its PK.
-- This intentionally includes unaffected indexes; do not auto-execute DROP DDL.
SELECT DISTINCT c.TABLE_SCHEMA, c.TABLE_NAME,
       i.name AS index_name, i.type AS index_kind, i.algo, i.is_visible
FROM information_schema.columns c
JOIN mo_catalog.mo_tables t
  ON t.reldatabase = c.TABLE_SCHEMA AND t.relname = c.TABLE_NAME
 AND t.account_id = current_account_id()
JOIN mo_catalog.mo_indexes i ON i.table_id = t.rel_id
WHERE UPPER(c.DATA_TYPE) = 'DECIMAL' AND c.NUMERIC_PRECISION > 38
  AND i.type IN ('UNIQUE', 'MULTIPLE', 'SPATIAL')
  AND (i.algo IS NULL OR LOWER(i.algo) IN ('', 'btree', 'rtree'))
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, i.name;
