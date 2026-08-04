DROP DATABASE IF EXISTS prepare_pagination_params;
CREATE DATABASE prepare_pagination_params;
USE prepare_pagination_params;

PREPARE numeric_reexecute FROM 'SELECT ? + 1 AS plus_one';
SET @numeric_param = 2;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = 2.5;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = 2;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = 12345678901234567890123456789012345678901234567890123456789012345;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = 0.123456789012345678901234567890;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = -12345678901234567890123456789012345678901234567890123456789012345;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = -0.123456789012345678901234567890;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = 12345678901234567890123456789012345678901234567890123456789012345;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = 1e10;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = 1e-10;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = 1e100;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = -1e10;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = '1e10';
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = '1e-10';
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = '-1e10';
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = ' 1e10 ';
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = '\t-1e10';
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = '1e-10 ';
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = '1e-10000';
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = '-1e-10000';
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = '2.5';
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = '9007199254740993';
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = 9223372036854775807;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = 2.5;
EXECUTE numeric_reexecute USING @numeric_param;
SET @numeric_param = 2;
EXECUTE numeric_reexecute USING @numeric_param;
DEALLOCATE PREPARE numeric_reexecute;

PREPARE bool_numeric FROM 'SELECT ? + 1 AS plus_one';
SET @bool_param = TRUE;
EXECUTE bool_numeric USING @bool_param;
DEALLOCATE PREPARE bool_numeric;

PREPARE numeric_compare FROM 'SELECT (? + 1) > 0 AS r';
SET @compare_param = 2;
EXECUTE numeric_compare USING @compare_param;
DEALLOCATE PREPARE numeric_compare;

PREPARE numeric_set FROM 'SET @set_result = ? + 1';
SET @set_param = 2;
EXECUTE numeric_set USING @set_param;
SELECT @set_result;
SET @set_param = 9223372036854775807;
EXECUTE numeric_set USING @set_param;
DEALLOCATE PREPARE numeric_set;

PREPARE numeric_ctas FROM 'CREATE TABLE prepared_numeric_ctas AS SELECT ? + 1 AS r';
SET @ctas_param = 2.5;
EXECUTE numeric_ctas USING @ctas_param;
SELECT * FROM prepared_numeric_ctas;
DEALLOCATE PREPARE numeric_ctas;

CREATE TABLE pagination_rows (id INT PRIMARY KEY);
INSERT INTO pagination_rows VALUES (1), (2), (3), (4);

PREPARE limit_param FROM 'SELECT id FROM pagination_rows ORDER BY id LIMIT ?';
SET @page_size = 2;
EXECUTE limit_param USING @page_size;
SET @page_size = '3';
EXECUTE limit_param USING @page_size;
SET @page_size = 3.0;
EXECUTE limit_param USING @page_size;
SET @page_size = 3;
EXECUTE limit_param USING @page_size;
SET @page_size = TRUE;
EXECUTE limit_param USING @page_size;
SET @page_size = -1;
EXECUTE limit_param USING @page_size;
DEALLOCATE PREPARE limit_param;

PREPARE ctas_limit FROM 'CREATE TABLE prepared_limit_ctas AS SELECT 1 LIMIT ?';
SET @page_size = '1';
EXECUTE ctas_limit USING @page_size;
DEALLOCATE PREPARE ctas_limit;

PREPARE offset_param FROM 'SELECT id FROM pagination_rows ORDER BY id LIMIT 2 OFFSET ?';
SET @page_offset = '1';
EXECUTE offset_param USING @page_offset;
SET @page_offset = 1;
EXECUTE offset_param USING @page_offset;
DEALLOCATE PREPARE offset_param;

PREPARE limit_offset_params FROM
    'SELECT id FROM pagination_rows ORDER BY id LIMIT ? OFFSET ?';
SET @page_size = 2;
SET @page_offset = '1';
EXECUTE limit_offset_params USING @page_size, @page_offset;
SET @page_size = '2';
SET @page_offset = 1;
EXECUTE limit_offset_params USING @page_size, @page_offset;
SET @page_size = 2;
EXECUTE limit_offset_params USING @page_size, @page_offset;
DEALLOCATE PREPARE limit_offset_params;

PREPARE ordinary_unsigned_cast FROM 'SELECT CAST(? AS UNSIGNED) AS converted';
SET @cast_param = '3';
EXECUTE ordinary_unsigned_cast USING @cast_param;
DEALLOCATE PREPARE ordinary_unsigned_cast;

DROP DATABASE prepare_pagination_params;
