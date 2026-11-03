--- @metacmp(true)
-- expr +/- INTERVAL accepts the same operands as DATE_ADD/DATE_SUB:
-- every string type and prepared-statement parameter markers.
drop database if exists interval_op_str;
create database interval_op_str;
use interval_op_str;
create table s (id int primary key, c char(19), v varchar(19), t text, b blob, bn binary(10));
insert into s values (1, '2026-01-31', '2026-01-31', '2026-01-31 10:20:30', '2026-01-31', '2026-01-31'), (2, null, null, null, null, null), (3, 'bad', 'bad', 'bad', 'bad', 'bad');
select id, c + interval 1 month, v + interval 1 month, t + interval 1 month, b + interval 1 day, bn + interval 1 day from s order by id;
select id, interval 1 day + c, interval 1 day + t, c - interval 1 day, t - interval 1 hour from s order by id;
select id, c + interval 1 day = date_add(c, interval 1 day), t - interval 1 hour = date_sub(t, interval 1 hour) from s where id = 1;
select id from s where c + interval 1 day = '2026-02-01' order by id;
select cast('2026-01-31' as char(10)) + interval 1 day, cast('2026-01-31 23:00:00' as char(19)) - interval 1 hour, '' + interval 1 day;
set @d = '2026-02-28';
set @dt = '2024-02-29 13:14:15.123456';
set @bad = 'invalid';
set @n = 3;
set @x = null;
prepare p1 from 'select ? + interval 1 day, ? - interval 1 day, interval 1 month + ?';
execute p1 using @d, @d, @d;
execute p1 using @dt, @dt, @dt;
execute p1 using @x, @x, @x;
execute p1 using @bad, @bad, @bad;
prepare p_hour from 'select ? + interval 1 hour';
execute p_hour using @dt;
deallocate prepare p_hour;
prepare p2 from 'select ? + interval ? day';
execute p2 using @d, @n;
prepare p3 from 'select id from s where cast(v as date) < ? - interval 1 day order by id';
execute p3 using @d;
deallocate prepare p1;
deallocate prepare p2;
deallocate prepare p3;
drop database interval_op_str;
