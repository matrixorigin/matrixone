-- Prepared UNNEST accepts text JSON and path variables without caller casts.
PREPARE p_unnest_args FROM
  'SELECT u.seq,u.path,u.value FROM unnest(?,?,?) u ORDER BY u.seq';
SET @j = '{"a":[1,null,3]}', @p = '$.a', @o = FALSE;
EXECUTE p_unnest_args USING @j,@p,@o;
EXECUTE p_unnest_args USING @j,@p,@o;
SET @j = '{"b":[4]}', @p = '$.b';
EXECUTE p_unnest_args USING @j,@p,@o;
DEALLOCATE PREPARE p_unnest_args;

-- The same GENERATE_SERIES plan follows the current endpoint domain.
PREPARE p_series_args FROM
  'SELECT count(*) AS c,min(result) AS lo,max(result) AS hi FROM generate_series(?,?,?) g';
SET @lo = 1, @hi = 9, @st = 2;
EXECUTE p_series_args USING @lo,@hi,@st;
SET @lo = CAST(1 AS UNSIGNED), @hi = CAST(9 AS UNSIGNED), @st = CAST(2 AS UNSIGNED);
EXECUTE p_series_args USING @lo,@hi,@st;
SET @lo = 9, @hi = 1, @st = -2;
EXECUTE p_series_args USING @lo,@hi,@st;
SET @lo = '2020-01-01 00:00:00', @hi = '2020-01-03 00:00:00', @st = '1 day';
EXECUTE p_series_args USING @lo,@hi,@st;
SET @lo = CAST('2020-01-01 00:00:00' AS DATETIME), @hi = CAST('2020-01-03 00:00:00' AS DATETIME);
EXECUTE p_series_args USING @lo,@hi,@st;
SET @lo = 1, @hi = 9, @st = 2;
EXECUTE p_series_args USING @lo,@hi,@st;
DEALLOCATE PREPARE p_series_args;

-- A typed temporal literal still follows the placeholder's temporal path.
PREPARE p_series_mixed FROM
  'SELECT count(*) AS c,min(result) AS lo,max(result) AS hi FROM generate_series(?,''2020-01-03 00:00:00'',''1 day'') g';
SET @start = '2020-01-01 00:00:00';
EXECUTE p_series_mixed USING @start;
DEALLOCATE PREPARE p_series_mixed;

-- A numeric first argument fixes the domain of later placeholders at PREPARE.
PREPARE p_series_numeric FROM
  'SELECT count(*) AS c,min(result) AS lo,max(result) AS hi FROM generate_series(1,?,?) g';
SET @end = 9, @step = 2;
EXECUTE p_series_numeric USING @end,@step;
DEALLOCATE PREPARE p_series_numeric;

PREPARE p_series_cast FROM
  'SELECT count(*) AS c,min(result) AS lo,max(result) AS hi FROM generate_series(CAST(? AS SIGNED),?,?) g';
SET @start = 1, @end = 9, @step = 2;
EXECUTE p_series_cast USING @start,@end,@step;
DEALLOCATE PREPARE p_series_cast;

-- A known temporal first argument fixes the step placeholder's string domain.
PREPARE p_series_date FROM
  'SELECT count(*) AS c,min(result) AS lo,max(result) AS hi FROM generate_series(CAST(? AS DATETIME),?,?) g';
SET @start = '2020-01-01 00:00:00', @end = '2020-01-03 00:00:00', @step = '1 day';
EXECUTE p_series_date USING @start,@end,@step;
DEALLOCATE PREPARE p_series_date;
