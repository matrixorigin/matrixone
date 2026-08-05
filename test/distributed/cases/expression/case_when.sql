-- @suit

-- @case
-- @desc:test for case_when expression with constant operand
-- @label:bvt
select CASE "b" when "a" then 1 when "b" then 2 END;
select CASE "c" when "a" then 1 when "b" then 2 END;
select CASE "c" when "a" then 1 when "b" then 2 ELSE 3 END;
select CASE when 1=0 then "true" else "false" END;
select CASE 1 when 1 then "one" WHEN 2 then "two" ELSE "more" END;
select CASE 2.0 when 1 then "one" WHEN 2.0 then "two" ELSE "more" END;

select (CASE "two" when "one" then "1" WHEN "two" then "2" END) | 0;

select (CASE "two" when "one" then 1.00 WHEN "two" then 2.00 END) +0.0;
select case 1/0 when "a" then "true" else "false" END;
select case 1/0 when "a" then "true" END;

select (case 1/0 when "a" then "true" END) | 0;

select (case 1/0 when "a" then "true" END) + 0.0;
select case when 1>0 then "TRUE" else "FALSE" END;
select case when 1<0 then "TRUE" else "FALSE" END;
SELECT CAST(CASE WHEN 0 THEN '2001-01-01' END AS DATE);
SELECT CAST(CASE WHEN 0 THEN DATE'2001-01-01' END AS DATE);
select case 1.0 when 0.1 then "a" when 1.0 then "b" else "c" END;
select case 0.1 when 0.1 then "a" when 1.0 then "b" else "c" END;
select case 1 when 0.1 then "a" when 1.0 then "b" else "c" END;
select case 1.0 when 0.1 then "a" when 1 then "b" else "c" END;
select case 1.001 when 0.1 then "a" when 1 then "b" else "c" END;

-- @case
-- @desc:test for case_when expression with normal select
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1 (a varchar(10), PRIMARY KEY (a));
CREATE TABLE t2 (a varchar(10), b date, PRIMARY KEY(a));
INSERT INTO t1 VALUES ('test1');
INSERT INTO t2 VALUES
('test1','2016-12-13'),('test2','2016-12-14'),('test3','2016-12-15');
SELECT b, b = '20161213',
       CASE b WHEN '20161213' then 'found' ELSE 'not found' END FROM t2;


-- @case
-- @desc:test for case_when expression with group by
-- @label:bvt
drop table if exists t1;
create table t1 (a int);
insert into t1 values(1),(2),(3),(4);
select case a when 1 then 2 when 2 then 3 else 0 end as fcase, count(*) from t1 group by fcase;
select case a when 1 then "one" when 2 then "two" else "nothing" end as fcase, count(*) from t1 group by fcase;
drop table if exists t1;

-- @case
-- @desc:test for case_when expression with function
-- @label:bvt
create table t1 (`row` int not null, col int not null, val varchar(255) not null);
insert into t1 values (1,1,'orange'),(1,2,'large'),(2,1,'yellow'),(2,2,'medium'),(3,1,'green'),(3,2,'small');
select col,val, case when val="orange" then 1 when upper(val)="LARGE" then 2  else 3 end from t1;
select max(case col when 1 then val else null end) as color from t1 group by `row`;
drop table if exists t1;

create table t1(a float, b int default 3);
insert into t1 (a) values (2), (11), (8);
select min(a), min(case when 1=1 then a else NULL end),
  min(case when 1!=1 then NULL else a end)
from t1 where b=3 group by b;

drop table if exists  t1;
CREATE TABLE t1 (a INT, b INT);
INSERT INTO t1 VALUES (1,1),(2,1),(3,2),(4,2),(5,3),(6,3);
SELECT CASE WHEN AVG(a)>=0 THEN 'Positive' ELSE 'Negative' END FROM t1 GROUP BY b;

drop table if exists  t1;

-- @case
-- @desc:test for case_when expression with join
-- @label:bvt
drop table if exists  t1;
drop table if exists  t2;
create table t1 (a int, b bigint unsigned);
create table t2 (c int);
insert into t1 (a, b) values (1,4572794622775114594), (2,18196094287899841997),
  (3,11120436154190595086);
insert into t2 (c) values (1), (2), (3);
select t1.a, (case t1.a when 0 then 0 else t1.b end) d from t1
  join t2 on t1.a=t2.c order by d;
select t1.a, (case t1.a when 0 then 0 else t1.b end) d from t1
  join t2 on t1.a=t2.c where b=11120436154190595086 order by d;
drop table if exists small;
drop table if exists big;
CREATE TABLE small (id int not null,PRIMARY KEY (id));
CREATE TABLE big (id int not null,PRIMARY KEY (id));
INSERT INTO small VALUES (1), (2);
INSERT INTO big VALUES (1), (2), (3), (4);
SELECT big.*, dt.* FROM big LEFT JOIN (SELECT id as dt_id,
                           CASE id WHEN 0 THEN 0 ELSE 1 END AS simple,
                           CASE WHEN id=0 THEN NULL ELSE 1 END AS cond
                    FROM small) AS dt
     ON big.id=dt.dt_id;

drop table if exists small;
drop table if exists big;

-- @case
-- @desc:test for case_when expression with union
-- @label:bvt
SELECT 'case+union+test'
UNION
SELECT CASE '1' WHEN '2' THEN 'BUG' ELSE 'nobug' END
ORDER BY 1;

-- @case
-- @desc:test for case_when expression in where filter
-- @label:bvt
drop table t1;
CREATE TABLE t1(a int);
insert into t1 values(1),(1),(2),(1),(3),(2),(1);
SELECT 1 FROM t1 WHERE a=1 AND CASE 1 WHEN a THEN 1 ELSE 1 END;
DROP TABLE if exists t1;

-- @case
-- @desc:test for case_when expression with count()
-- @label:bvt
DROP TABLE if exists t1;
create table t1 (USR_ID int not null, MAX_REQ int not null);
insert into t1 values (1, 3);
select count(*) + MAX_REQ - MAX_REQ + MAX_REQ - MAX_REQ + MAX_REQ - MAX_REQ + MAX_REQ - MAX_REQ + MAX_REQ - MAX_REQ from t1 group by MAX_REQ;
select Case When Count(*) < MAX_REQ Then 1 Else 0 End from t1 where t1.USR_ID = 1 group by MAX_REQ;
DROP TABLE if exists t1;

select case when 1 in (1.0, 2.0, 3.0) then true else false end;

DROP TABLE if exists t1;
CREATE TABLE t1 (
                    id int NOT NULL AUTO_INCREMENT,
                    key_num int NOT NULL DEFAULT '0',
                    hiredate date NOT NULL,
                    PRIMARY KEY (id),
                    KEY key_num (key_num)
);

insert into t1 values
                   (1, 7369, '1980-12-17'),
                   (2, 7499, '1981-02-20'),
                   (3, 7521, '1981-02-22'),
                   (4, 7566, '1981-04-02'),
                   (5, 7654, '1981-09-28'),
                   (6, 7698, '1981-05-01'),
                   (7, 7782, '1981-06-09'),
                   (8, 7788, '0087-07-13'),
                   (9, 7839, '1981-11-17'),
                   (10, 7844, '1981-09-08'),
                   (11, 7876, '2007-07-13'),
                   (12, 7900, '1981-12-03'),
                   (13, 7980, '1987-07-13'),
                   (14, 7981, '2001-11-17'),
                   (15, 7982, '1951-11-08'),
                   (16, 7983, '1927-10-13'),
                   (17, 7984, '1671-12-09'),
                   (18, 7985, '1981-11-06'),
                   (19, 7986, '1771-12-06'),
                   (20, 7987, '1985-10-06');
select id, case when id < 5 then 0 when id < 10 then 1 when id < 15 then 2 when true then 3 else -1 end as xxx from t1;
DROP TABLE t1;
create table t1(a varchar(100));
insert into t1 values ("a");
select a, case when a="a" then 1 when upper(a)="b" then 2 end from t1;
drop table if exists t1;

-- @case
-- @desc:test for case_when expression with mixed decimal scales
-- @label:bvt
SELECT
  7.01970 * CAST(-58140.00 AS DECIMAL(23,2)) AS direct_mul,
  CASE WHEN 'USD' = 'RMB'
       THEN CAST(-58140.00 AS DECIMAL(23,2))
       ELSE 7.01970 * CAST(-58140.00 AS DECIMAL(23,2))
  END AS bug_case;

-- @case
-- @desc:test for case_when expression with then branch decimal cast
-- @label:bvt
SELECT
  CASE WHEN 'USD' = 'USD'
       THEN CAST(-58140.00 AS DECIMAL(23,2))
       ELSE 7.01970 * CAST(-58140.00 AS DECIMAL(23,2))
  END AS bug_case_then;

-- @case
-- @desc:test for iff expression with mixed decimal scales
-- @label:bvt
SELECT
  IFF('USD' = 'USD',
      CAST(-58140.00 AS DECIMAL(23,2)),
      7.01970 * CAST(-58140.00 AS DECIMAL(23,2))) AS bug_iff;

-- @case
-- @desc:test for case_when expression with decimal128 branches promoting to decimal256 result type
-- @label:bvt
SELECT
  CASE WHEN 1 = 1
       THEN CAST(1 AS DECIMAL(38,0))
       ELSE CAST(0 AS DECIMAL(38,20))
  END AS case_decimal256_then;
SELECT
  CASE WHEN 1 = 2
       THEN CAST(1 AS DECIMAL(38,0))
       ELSE CAST(0 AS DECIMAL(38,20))
  END AS case_decimal256_else;

-- @case
-- @desc:test for iff expression with decimal128 branches promoting to decimal256 result type
-- @label:bvt
SELECT
  IFF(1 = 1,
      CAST(1 AS DECIMAL(38,0)),
      CAST(0 AS DECIMAL(38,20))) AS iff_decimal256_true;
SELECT
  IFF(1 = 2,
      CAST(1 AS DECIMAL(38,0)),
      CAST(0 AS DECIMAL(38,20))) AS iff_decimal256_false;

-- @case
-- @desc:test for coalesce over decimal branches with different scales aligns scale/width
-- @label:bvt
SELECT 7.01970 * CAST(-58140.00 AS DECIMAL(23,2)) AS direct_mul;
SELECT COALESCE(
  CAST(NULL AS DECIMAL(23,2)),
  7.01970 * CAST(-58140.00 AS DECIMAL(23,2))
) AS coalesce_decimal_scale;
SELECT COALESCE(
  CAST(1.23 AS DECIMAL(23,2)),
  7.01970 * CAST(-58140.00 AS DECIMAL(23,2))
) AS coalesce_first_non_null;

-- @case
-- @desc:test for comparing a decimal256 case result with a decimal128 value
-- @label:bvt
SELECT (CASE WHEN 1 = 1 THEN CAST(1 AS DECIMAL(38,0))
             ELSE CAST(0 AS DECIMAL(38,20)) END)
     = CAST(1 AS DECIMAL(38,20)) AS decimal256_eq_decimal128;
SELECT (CASE WHEN 1 = 1 THEN CAST(5 AS DECIMAL(38,0))
             ELSE CAST(0 AS DECIMAL(38,20)) END)
     > CAST(1 AS DECIMAL(38,20)) AS decimal256_gt_decimal128;
SELECT (CASE WHEN 1 = 1 THEN CAST(5 AS DECIMAL(38,0))
             ELSE CAST(0 AS DECIMAL(38,20)) END)
     < CAST(1 AS DECIMAL(38,20)) AS decimal256_lt_decimal128;
SELECT (CASE WHEN 1 = 1 THEN CAST(5 AS DECIMAL(38,0))
             ELSE CAST(0 AS DECIMAL(38,20)) END)
     != CAST(1 AS DECIMAL(38,20)) AS decimal256_ne_decimal128;
SELECT (CASE WHEN 1 = 1 THEN CAST(5 AS DECIMAL(38,0))
             ELSE CAST(0 AS DECIMAL(38,20)) END)
     BETWEEN CAST(1 AS DECIMAL(38,20)) AND CAST(10 AS DECIMAL(38,20)) AS decimal256_between;

-- @case
-- @desc:test for coalesce promoting decimal branches to decimal256 when integral+scale overflows decimal128
-- @label:bvt
SELECT COALESCE(CAST(1 AS DECIMAL(38,0)), CAST(0.5 AS DECIMAL(30,30))) AS coalesce_promote_decimal256;
SELECT COALESCE(CAST(12345678901234567890123456789012345678 AS DECIMAL(38,0)), CAST(0.5 AS DECIMAL(30,30))) AS coalesce_promote_bignum;

-- @case
-- @desc:test for column-based coalesce over decimal branches with null and non-null rows
-- @label:bvt
drop table if exists t_coalesce_col;
create table t_coalesce_col (id int, a decimal(23,2), b decimal(38,7));
insert into t_coalesce_col values (1, null, 7.01970 * cast(-58140.00 as decimal(23,2))), (2, 1.23, 7.01970 * cast(-58140.00 as decimal(23,2)));
select id, coalesce(a, b) as col_coalesce from t_coalesce_col order by id;
drop table t_coalesce_col;

-- @case
-- @desc:test for coalesce over mixed integer and decimal branches
-- @label:bvt
drop table if exists t_coalesce_mix;
create table t_coalesce_mix (id int, i int, d decimal(20,5));
insert into t_coalesce_mix values (1, null, 1.50000), (2, 10, 2.50000);
select id, coalesce(i, d) as mix_coalesce from t_coalesce_mix order by id;
drop table t_coalesce_mix;
select coalesce(null, 1, cast(0.5 as decimal(10,5))) as mix_const_coalesce;

-- @case
-- @desc:test for coalesce over three or more decimal branches
-- @label:bvt
select coalesce(null, cast(1.23 as decimal(23,2)), cast(4.56780 as decimal(38,7))) as three_branch;
select coalesce(cast(null as decimal(23,2)), cast(null as decimal(20,5)), 7.01970 * cast(-58140.00 as decimal(23,2))) as three_branch_all_decimal;

-- @case
-- @desc:test for visible inferred decimal type of a coalesce result via view metadata
-- @label:bvt
drop view if exists v_coalesce_meta;
create view v_coalesce_meta as select coalesce(cast(null as decimal(23,2)), 7.01970 * cast(-58140.00 as decimal(23,2))) as c;
desc v_coalesce_meta;
drop view v_coalesce_meta;

-- @case
-- @desc:test control-flow view metadata across mixed type and nullability rules
-- @label:bvt
drop view if exists v_flow_metadata_safe;
create view v_flow_metadata_safe as
select if(1, '2', 3) as if_str_num,
       if(1, cast(1 as unsigned), cast(-1 as signed)) as if_unsigned_signed,
       case when 1 then cast('2024-01-01' as date) else cast('2024-01-02 03:04:05' as datetime) end as case_date_dt,
       case when 1 then _binary 'a' else 'bc' end as case_binary_char,
       ifnull(null, 9.5) as ifnull_decimal,
       nullif('01', 1) as nullif_mixed,
       coalesce(null, '8', 9) as coalesce_str_num,
       greatest(cast('2024-01-02' as date), cast('2023-12-31' as date)) as greatest_date;
desc v_flow_metadata_safe;
select column_name, column_type, is_nullable, character_maximum_length, numeric_precision, numeric_scale
from information_schema.columns
where table_schema = database() and table_name = 'v_flow_metadata_safe'
order by ordinal_position;
select * from v_flow_metadata_safe;
drop view v_flow_metadata_safe;

-- @case
-- @desc:test DATE to DATETIME CASE promotion in the ELSE branch
-- @label:bvt
drop view if exists v_case_temporal_else;
create view v_case_temporal_else as
select case when 0 then cast('2024-01-02 03:04:05' as datetime) else cast('2024-01-01' as date) end as case_date_dt_else;
desc v_case_temporal_else;
select * from v_case_temporal_else;
drop view v_case_temporal_else;

-- @case
-- @desc:test IF binary and character branch metadata
-- @label:bvt
drop view if exists v_if_binary_char;
create view v_if_binary_char as
select if(1, _binary 'a', 'bc') as if_binary_char;
desc v_if_binary_char;
select hex(if_binary_char) as if_binary_char_hex from v_if_binary_char;
drop view v_if_binary_char;

-- @case
-- @desc:test control-flow metadata and values with nullable and not-null columns
-- @label:bvt
drop table if exists t_flow_metadata;
create table t_flow_metadata (
    id int primary key,
    n_nullable int,
    n_notnull int not null,
    d_nullable date,
    d_notnull date not null
);
insert into t_flow_metadata values
    (1, null, 7, null, '2024-01-02'),
    (2, 5, 8, '2024-01-03', '2024-01-04');

drop view if exists v_flow_metadata_columns;
create view v_flow_metadata_columns as
select ifnull(null, 9.5) as ifnull_null_first,
       ifnull(n_nullable, 9.5) as ifnull_nullable_col,
       ifnull(n_notnull, 9.5) as ifnull_notnull_col,
       coalesce(null, '8', 9) as coalesce_null_first,
       coalesce(n_nullable, 9) as coalesce_nullable_col,
       coalesce(n_notnull, 9) as coalesce_notnull_col,
       greatest(cast('2024-01-02' as date), cast('2023-12-31' as date)) as greatest_const,
       greatest(d_nullable, cast('2023-12-31' as date)) as greatest_nullable_col,
       greatest(d_notnull, cast('2023-12-31' as date)) as greatest_notnull_col
from t_flow_metadata;
desc v_flow_metadata_columns;
select column_name, column_type, is_nullable, character_maximum_length, numeric_precision, numeric_scale
from information_schema.columns
where table_schema = database() and table_name = 'v_flow_metadata_columns'
order by ordinal_position;
select * from v_flow_metadata_columns order by ifnull_notnull_col;
drop view v_flow_metadata_columns;

select if(1, cast(18446744073709551615 as unsigned), cast(-1 as signed)) as if_uint64_signed_max,
       if(0, cast(18446744073709551615 as unsigned), cast(-1 as signed)) as if_uint64_signed_negative,
       case when 1 then cast(18446744073709551615 as unsigned) else cast(-1 as signed) end as case_uint64_signed_max;

-- NULL value arms are neutral when CASE finds a safe common type for signed
-- and unsigned integers.
select case
           when false then null
           when true then cast(18446744073709551615 as unsigned)
           else cast(-1 as signed)
       end as case_leading_null_uint64_max,
       case
           when false then cast(18446744073709551615 as unsigned)
           when false then null
           else cast(-1 as signed)
       end as case_middle_null_signed_negative,
       case
           when true then cast(18446744073709551615 as unsigned)
           when false then cast(-1 as signed)
           else null
       end as case_trailing_null_uint64_max;
select hex(case when 1 then _binary 'a' else 'bc' end) as case_binary_hex,
       hex(case when 0 then _binary 'a' else 'bc' end) as case_char_hex;

drop view if exists v_case_binary_utf8;
create view v_case_binary_utf8 as
select case when 1 then _binary 'a' else '中文' end as case_binary_utf8;
desc v_case_binary_utf8;
select hex(case when 0 then _binary 'a' else '中文' end) as case_utf8_hex;
drop view v_case_binary_utf8;

-- @case
-- @desc:test CASE binary metadata with NULL value branches
-- @label:bvt
drop view if exists v_case_binary_null;
create view v_case_binary_null as
select case when 1 then null else cast('a' as binary(4)) end as fixed_leading_null,
       case when 1 then cast('a' as binary(4)) else null end as fixed_trailing_null,
       case when 0 then cast('a' as binary(4)) when 1 then null else cast('b' as binary(4)) end as fixed_middle_null,
       case when 1 then null else cast('a' as varbinary(4)) end as var_leading_null,
       case when 1 then cast('a' as varbinary(4)) else null end as var_trailing_null,
       case when 0 then cast('a' as varbinary(4)) when 1 then null else cast('b' as varbinary(4)) end as var_middle_null;
desc v_case_binary_null;
drop view v_case_binary_null;

drop table t_flow_metadata;

-- @case
-- @desc:test conditional decimal literal and temporal string view metadata, including nested views
-- @label:bvt
drop table if exists t_conditional_literal_temporal;
create table t_conditional_literal_temporal (
    id int primary key,
    d decimal(8,2),
    dte date,
    dt datetime,
    ts timestamp(6)
);
insert into t_conditional_literal_temporal values
    (1, 12.50, '2024-01-01', '2024-01-01 01:02:03', '2024-01-01 01:02:03.123456');

drop view if exists v_conditional_literal_temporal;
create view v_conditional_literal_temporal as
select case when id = 1 then d else 0 end as d_case_literal,
       coalesce(dte, '2024-01-01') as dte_coalesce,
       coalesce(dt, '2024-01-01') as dt_coalesce,
       coalesce(ts, '2024-01-01') as ts_coalesce
from t_conditional_literal_temporal;
desc v_conditional_literal_temporal;

drop view if exists v_conditional_literal_temporal_nested;
create view v_conditional_literal_temporal_nested as
select d_case_literal, dte_coalesce, dt_coalesce, ts_coalesce
from v_conditional_literal_temporal;
desc v_conditional_literal_temporal_nested;

drop view v_conditional_literal_temporal_nested;
drop view v_conditional_literal_temporal;
drop table t_conditional_literal_temporal;

-- @case
-- @desc:test conditional string metadata does not narrow unknown TEXT/BLOB/FLOAT/DOUBLE branches
-- @label:bvt
drop table if exists t_conditional_unknown_width;
create table t_conditional_unknown_width (
    s text,
    b blob,
    f float,
    d double
);
insert into t_conditional_unknown_width values ('abcdef', 'abcdef', 123.456, 789.012);

drop view if exists v_conditional_unknown_width;
create view v_conditional_unknown_width as
select if(true, s, 3) as text_result,
       if(true, b, 3) as blob_result,
       if(false, 'x', f) as float_result,
       if(false, 'x', cast(d as double)) as double_result
from t_conditional_unknown_width;
desc v_conditional_unknown_width;
select length(if(true, s, 3)) as text_length,
       hex(if(true, b, 3)) as blob_hex,
       if(false, 'x', f) <> 'x' as float_value_preserved,
       if(false, 'x', cast(d as double)) <> 'x' as double_value_preserved
from t_conditional_unknown_width;

drop view v_conditional_unknown_width;
drop table t_conditional_unknown_width;

-- @case
-- @desc:test composed conditional VARCHAR bounds and UTF-8 values
-- @label:bvt
drop table if exists t_conditional_composed_width;
create table t_conditional_composed_width (d double, s varchar(2));
insert into t_conditional_composed_width values (123.456, '你好');
select length(case when false then 'x' when true then 1234567890123 else cast('2024-01-01' as date) end) as case_known_length,
       length(coalesce(cast(null as char(1)), 1234567890123, cast('2024-01-01' as date))) as coalesce_known_length,
       case when false then 'x' when true then d else cast('2024-01-01' as date) end <> 'x' as case_unknown_value_preserved,
       coalesce(cast(null as char(1)), d, cast('2024-01-01' as date)) <> 'x' as coalesce_unknown_value_preserved
from t_conditional_composed_width;
select if(true, s, 12) as unicode_if,
       case when true then s else 12 end as unicode_case,
       coalesce(s, 12) as unicode_coalesce
from t_conditional_composed_width;
drop table t_conditional_composed_width;
