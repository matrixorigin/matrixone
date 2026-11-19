-- @case
-- @desc: UNION/UNION ALL keep pure BIT branch types at max source width (#32123)
-- @label:bvt

DROP DATABASE IF EXISTS union_bit_width_regression;
CREATE DATABASE union_bit_width_regression;
USE union_bit_width_regression;
CREATE TABLE bit8_t (id INT, b BIT(8));
INSERT INTO bit8_t VALUES (1, b'0'), (2, b'11111111'), (3, NULL), (4, b'11111111');
CREATE TABLE bit16_t (id INT, b BIT(16));
INSERT INTO bit16_t VALUES (1, b'1'), (2, b'11111111'), (3, NULL);
CREATE TABLE bit9_t (id INT, b BIT(9));
INSERT INTO bit9_t VALUES (2, b'100000010'), (3, NULL);
CREATE TABLE bit1_t (id INT, b BIT(1));
INSERT INTO bit1_t VALUES (1, b'0'), (2, b'1');
CREATE TABLE bit64_t (id INT, b BIT(64));
INSERT INTO bit64_t VALUES (1, b'1111111111111111111111111111111111111111111111111111111111111111'), (2, b'11111111'), (3, NULL);
CREATE TABLE bit64_empty (id INT, b BIT(64));

-- Mixed 8/16 widths: the common type is BIT(16), so every materialized row
-- occupies 2 bytes and keeps the exact source values.
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM (SELECT b AS d FROM bit8_t UNION ALL SELECT b FROM bit16_t) AS u WHERE d IS NOT NULL ORDER BY num, width;
SELECT COUNT(*) AS null_rows FROM (SELECT b AS d FROM bit8_t UNION ALL SELECT b FROM bit16_t) AS u WHERE d IS NULL;
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM (SELECT b AS d FROM bit16_t UNION ALL SELECT b FROM bit8_t) AS u WHERE d IS NOT NULL ORDER BY num, width;

-- Same-width pure BIT union: BIT(8) is retained, one byte per row, duplicates kept.
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM (SELECT b AS d FROM bit8_t UNION ALL SELECT b FROM bit8_t) AS u WHERE d IS NOT NULL ORDER BY num, width;
SELECT COUNT(*) AS null_rows FROM (SELECT b AS d FROM bit8_t UNION ALL SELECT b FROM bit8_t) AS u WHERE d IS NULL;

-- UNION DISTINCT keeps the same domain: mixed-width de-dupes at BIT(16),
-- same-source de-dupes at BIT(8).
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM (SELECT b AS d FROM bit8_t UNION SELECT b FROM bit16_t) AS u WHERE d IS NOT NULL ORDER BY num, width;
SELECT COUNT(*) AS null_rows FROM (SELECT b AS d FROM bit8_t UNION SELECT b FROM bit16_t) AS u WHERE d IS NULL;
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM (SELECT b AS d FROM bit8_t UNION SELECT b FROM bit8_t) AS u WHERE d IS NOT NULL ORDER BY num, width;
SELECT COUNT(*) AS null_rows FROM (SELECT b AS d FROM bit8_t UNION SELECT b FROM bit8_t) AS u WHERE d IS NULL;

-- Four branches BIT(1)/BIT(9)/BIT(16)/BIT(64): common type BIT(64), 8 bytes
-- per row, exact values including 258 and UINT64_MAX.
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM (SELECT b AS d FROM bit1_t UNION ALL SELECT b FROM bit9_t UNION ALL SELECT b FROM bit16_t UNION ALL SELECT b FROM bit64_t) AS u WHERE d IS NOT NULL ORDER BY num, width;
SELECT COUNT(*) AS null_rows FROM (SELECT b AS d FROM bit1_t UNION ALL SELECT b FROM bit9_t UNION ALL SELECT b FROM bit16_t UNION ALL SELECT b FROM bit64_t) AS u WHERE d IS NULL;

-- An empty BIT(64) branch still contributes its static width to the selection.
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM (SELECT b AS d FROM bit8_t UNION ALL SELECT b FROM bit64_empty UNION ALL SELECT b FROM bit16_t) AS u WHERE d IS NOT NULL ORDER BY num, width;
SELECT COUNT(*) AS null_rows FROM (SELECT b AS d FROM bit8_t UNION ALL SELECT b FROM bit64_empty UNION ALL SELECT b FROM bit16_t) AS u WHERE d IS NULL;

-- An untyped NULL branch is neutral: the set stays BIT(8).
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM (SELECT NULL AS d UNION ALL SELECT b FROM bit8_t) AS u WHERE d IS NOT NULL ORDER BY num, width;
SELECT COUNT(*) AS null_rows FROM (SELECT NULL AS d UNION ALL SELECT b FROM bit8_t) AS u WHERE d IS NULL;

-- All-NULL branches keep the historical result unchanged: the set has two
-- rows and none of them carries a non-NULL value.
SELECT COUNT(*) AS rows_out FROM (SELECT NULL AS d UNION ALL SELECT NULL AS d) AS u;
SELECT COUNT(d) AS d_count FROM (SELECT NULL AS d UNION ALL SELECT NULL AS d) AS u;

-- Mixed BIT/uint64 input is not a pure BIT set: it keeps the existing
-- coalesce resolution, where LENGTH counts decimal digits. The numeric
-- branch is an explicit unsigned domain, not an assumed literal type.
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM (SELECT b AS d FROM bit8_t UNION ALL SELECT CAST(258 AS UNSIGNED) AS d) AS u WHERE d IS NOT NULL ORDER BY num, width;

-- Plain COALESCE is untouched by the set-operation specialization.
SELECT CAST(COALESCE(b, 255) AS UNSIGNED) AS num FROM bit8_t ORDER BY num;

-- Outer predicate on the union result: with BIT(16) kept, all non-NULL rows
-- pass the two-byte length predicate.
SELECT COUNT(*) AS wide_rows FROM (SELECT b AS d FROM bit8_t UNION ALL SELECT b FROM bit16_t) AS u WHERE LENGTH(d) = 2;

-- VIEW reads materialize the same BIT(16) contract, and the stored column
-- declaration keeps the native bit type and width.
CREATE VIEW union_bit_view AS SELECT b AS d FROM bit8_t UNION ALL SELECT b FROM bit16_t;
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM union_bit_view WHERE d IS NOT NULL ORDER BY num, width;
SELECT COUNT(*) AS null_rows FROM union_bit_view WHERE d IS NULL;
SELECT column_name, data_type, column_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'union_bit_view' AND column_name = 'd' ORDER BY ordinal_position;

-- CTAS keeps BIT(16) in the saved schema: the persisted column declaration
-- keeps the native bit type and width, and the re-read returns identical rows.
CREATE TABLE union_bit_saved AS SELECT b AS d FROM bit8_t UNION ALL SELECT b FROM bit16_t;
SELECT COUNT(*) AS rows_saved FROM union_bit_saved;
SELECT column_name, data_type, column_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'union_bit_saved' AND column_name = 'd' ORDER BY ordinal_position;
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM union_bit_saved WHERE d IS NOT NULL ORDER BY num, width;

-- Rejected write does not break the session: SELECT 1 still works, the
-- same-connection pure-BIT query recovers, and the rejected row is absent.
INSERT INTO bit8_t VALUES (9, 256);
SELECT 1;
SELECT COUNT(*) AS rows_after_reject FROM bit8_t;
-- The same connection still materializes the pure-BIT set contract: the
-- mixed-width union re-reads the original rows with the BIT(16) common width.
SELECT CAST(d AS UNSIGNED) AS num, LENGTH(d) AS width FROM (SELECT b AS d FROM bit8_t UNION ALL SELECT b FROM bit16_t) AS u WHERE d IS NOT NULL ORDER BY num, width;

DROP DATABASE union_bit_width_regression;
