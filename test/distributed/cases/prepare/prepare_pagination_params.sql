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
DEALLOCATE PREPARE numeric_reexecute;

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
DEALLOCATE PREPARE limit_param;

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
