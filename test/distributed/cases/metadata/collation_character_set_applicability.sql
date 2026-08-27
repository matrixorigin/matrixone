-- @bvt:issue#25103
-- MySQL exposes this mapping as an information_schema object.  It must carry
-- the supported collation-to-character-set rows used by metadata consumers.
SELECT COUNT(*)
FROM information_schema.COLLATION_CHARACTER_SET_APPLICABILITY;

SELECT COUNT(*)
FROM (
    SELECT *
    FROM information_schema.COLLATION_CHARACTER_SET_APPLICABILITY
    LIMIT 1
) AS applicability;

SELECT cs.CHARACTER_SET_NAME,
       cs.DEFAULT_COLLATE_NAME,
       c.COLLATION_NAME,
       c.IS_DEFAULT
FROM information_schema.CHARACTER_SETS AS cs
LEFT JOIN information_schema.COLLATIONS AS c
  ON c.CHARACTER_SET_NAME = cs.CHARACTER_SET_NAME
 AND c.IS_DEFAULT = 'YES'
WHERE cs.CHARACTER_SET_NAME IN ('utf8', 'utf8mb4')
ORDER BY cs.CHARACTER_SET_NAME;

SELECT ccsa.CHARACTER_SET_NAME FROM information_schema.TABLES AS tbl JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS ccsa ON ccsa.COLLATION_NAME = tbl.TABLE_COLLATION WHERE tbl.TABLE_SCHEMA = 'information_schema' AND tbl.TABLE_NAME = 'TABLES' LIMIT 1;

SELECT COUNT(*)
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'information_schema'
  AND TABLE_NAME = 'COLLATION_CHARACTER_SET_APPLICABILITY'
  AND COLUMN_NAME IN ('COLLATION_NAME', 'CHARACTER_SET_NAME');
