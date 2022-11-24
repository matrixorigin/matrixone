SET @val = CAST(0 AS UNSIGNED);
select @val;

SET @val = CAST(0 AS bool);
select @val;

SET @val = CAST(0 AS varchar);
select @val;

SET @val = CAST(0 AS char);
select @val;

SET @val = CAST("2022-10-10" AS date);
select @val;
SET @val = CAST("2022-10-10" AS datetime);
select @val;
SET @val = CAST("2022-10-10" AS timestamp);
select @val;

SET @val = CAST('0' AS text);
select @val;

SET @val = CAST('0' AS blob);
select @val;
