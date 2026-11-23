-- Unary minus negates the numeric value of DATE/TIME/DATETIME/TIMESTAMP
-- operands (the same value as 0 - x), and unary plus keeps the operand.
drop database if exists unary_sign_temporal;
create database unary_sign_temporal;
use unary_sign_temporal;
set time_zone = '+00:00';

create table t (
    id int primary key,
    d date,
    dt datetime,
    dt3 datetime(3),
    ts timestamp,
    tm time,
    tm2 time(2)
);
insert into t values
    (1, '2024-06-18', '2024-06-18 01:02:03', '2024-06-18 01:02:03.250', '2024-06-18 01:02:03', '-01:02:03', '01:02:03.50'),
    (2, null, null, null, null, null, null);

select -cast('2024-06-18' as date), -cast('01:02:03' as time), -cast('2024-06-18 01:02:03' as datetime);
select id, -d, -dt, -dt3, -ts, -tm, -tm2 from t order by id;
select id, -d = 0 - d, -dt3 = 0 - dt3, -tm2 = 0 - tm2 from t order by id;
select id from t where -d < -20240000 order by id;
select id, +d, +dt3, +tm2 from t order by id;
select -(-d) = d + 0 from t where id = 1;

create table r as select -d a, -dt3 b, -tm2 c, +d e from t;
select column_name, column_type from information_schema.columns where table_schema = 'unary_sign_temporal' and table_name = 'r' order by ordinal_position;

prepare s from 'select -?';
set @d = cast('2024-06-18' as date);
execute s using @d;
deallocate prepare s;

drop database unary_sign_temporal;
