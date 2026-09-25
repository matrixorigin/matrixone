-- @case
-- @desc: Prepared variadic comparisons use the SQL variable's runtime domain (#29317, #29318)
-- @label:bvt

SELECT GREATEST(2.0, 1, 2, '10'), LEAST(2.0, 1, 2, '10');
PREPARE p_extrema FROM 'SELECT GREATEST(?, ?, ?, ?), LEAST(?, ?, ?, ?)';
SET @n = 2.0, @a = 1, @b = 2, @s = '10';
EXECUTE p_extrema USING @n, @a, @b, @s, @n, @a, @b, @s;
SET @s = '02';
EXECUTE p_extrema USING @n, @a, @b, @s, @n, @a, @b, @s;
SET @s = NULL;
EXECUTE p_extrema USING @n, @a, @b, @s, @n, @a, @b, @s;
SET @s = '10';
EXECUTE p_extrema USING @n, @a, @b, @s, @n, @a, @b, @s;
DEALLOCATE PREPARE p_extrema;

-- A numeric literal peer has a provisional TEXT envelope at PREPARE.
SELECT GREATEST(2.0, 1), LEAST(2.0, 1);
PREPARE p_numeric_peer FROM 'SELECT GREATEST(?, 1), LEAST(?, 1)';
SET @n = 2.0;
EXECUTE p_numeric_peer USING @n, @n;
DEALLOCATE PREPARE p_numeric_peer;

SELECT FIELD(2.0, 1, 2, 'x', NULL);
PREPARE p_field FROM 'SELECT FIELD(?, ?, ?, ?, ?)';
SET @needle = 2.0, @first = 1, @second = 2, @text = 'x', @nil = NULL;
EXECUTE p_field USING @needle, @first, @second, @text, @nil;
SET @needle = 1.0;
EXECUTE p_field USING @needle, @first, @second, @text, @nil;
SET @needle = 2.0;
EXECUTE p_field USING @needle, @first, @second, @text, @nil;
DEALLOCATE PREPARE p_field;

SELECT FIELD(x'0062', x'61', x'0062'), FIELD(x'0062', x'62'), FIELD(x'41', x'61');
PREPARE p_binary FROM 'SELECT FIELD(?, ?, ?), FIELD(?, ?), FIELD(?, ?)';
SET @binary = x'0062', @binary_first = x'61', @binary_second = x'0062';
SET @short = x'62', @upper = x'41', @lower = x'61';
EXECUTE p_binary USING @binary, @binary_first, @binary_second, @binary, @short, @upper, @lower;
SET @binary = x'41';
EXECUTE p_binary USING @binary, @binary_first, @binary_second, @binary, @short, @upper, @lower;
DEALLOCATE PREPARE p_binary;

PREPARE p_cast FROM 'SELECT GREATEST(CAST(? AS CHAR), ?), FIELD(CAST(? AS CHAR), ?)';
SET @cast_num = 2, @cast_text = '10';
EXECUTE p_cast USING @cast_num, @cast_text, @cast_num, @cast_text;
DEALLOCATE PREPARE p_cast;
