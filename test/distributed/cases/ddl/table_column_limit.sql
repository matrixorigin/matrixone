-- CREATE/CTAS/LIKE and final ALTER share the 4096 user-column quota.
DROP DATABASE IF EXISTS table_column_limit;
CREATE DATABASE table_column_limit;
USE table_column_limit;
SET @saved_group_concat_limit = @@session.group_concat_max_len;
SET session group_concat_max_len = 2000000;

-- Generate the real boundary without thousands of literal column definitions.
SET @columns = (SELECT group_concat(concat('c', result, ' int') ORDER BY result SEPARATOR ',') FROM generate_series(1,4095)g);
SET @ddl = concat('CREATE TABLE wide (', @columns, ')');
PREPARE make_wide FROM @ddl;
EXECUTE make_wide;
DEALLOCATE PREPARE make_wide;
ALTER TABLE wide ADD c4096 int;
INSERT INTO wide (c1,c4096) VALUES (1,77);
ALTER TABLE wide ADD rejected int;
SELECT count(*) AS columns_count FROM information_schema.columns WHERE table_schema = 'table_column_limit' AND table_name = 'wide';
SELECT c1,c4096 FROM wide;

-- COPY validates the final schema, so clause order does not reject replacement.
ALTER TABLE wide ADD replacement int, DROP c2;
SELECT count(*) AS columns_count FROM information_schema.columns WHERE table_schema = 'table_column_limit' AND table_name = 'wide';
SELECT c1,c4096,replacement FROM wide;
ALTER TABLE wide DROP replacement, ADD c2 int;

CREATE TABLE boundary_like LIKE wide;
SELECT count(*) AS columns_count FROM information_schema.columns WHERE table_schema = 'table_column_limit' AND table_name = 'boundary_like';
-- The explicit override is counted once; a distinct target-only column is extra.
CREATE TABLE boundary_ctas (c1 bigint) AS SELECT * FROM wide;
SELECT c1,c4096 FROM boundary_ctas;
CREATE TABLE rejected_ctas (extra int) AS SELECT * FROM wide;
SELECT count(*) AS rejected_tables FROM information_schema.tables WHERE table_schema = 'table_column_limit' AND table_name = 'rejected_ctas';

SET @columns = (SELECT group_concat(concat('c', result, ' int') ORDER BY result SEPARATOR ',') FROM generate_series(1,4097)g);
SET @ddl = concat('CREATE TABLE rejected_create (', @columns, ')');
PREPARE rejected FROM @ddl;
SELECT count(*) AS rejected_tables FROM information_schema.tables WHERE table_schema = 'table_column_limit' AND table_name = 'rejected_create';
SELECT c1,c4096 FROM wide;
SELECT count(*) AS copy_residue FROM information_schema.tables WHERE table_schema = 'table_column_limit' AND table_name LIKE '%_copy_%';
SELECT 1 AS service_alive;

SET session group_concat_max_len = @saved_group_concat_limit;
DROP DATABASE table_column_limit;
