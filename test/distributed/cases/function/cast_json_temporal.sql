-- CAST(json AS DATE/TIME/DATETIME/TIMESTAMP): a JSON string is read like the
-- same SQL string, JSON null is SQL NULL, other JSON values are rejected.
drop database if exists cast_json_temporal;
create database cast_json_temporal;
use cast_json_temporal;
select cast(cast('"2026-01-31"' as json) as date), cast(cast('"12:34:56.5"' as json) as time(1)), cast(cast('"2026-01-31 01:02:03.456"' as json) as datetime(3));
select cast(cast('"2026-01-31 01:02:03"' as json) as timestamp) = timestamp('2026-01-31 01:02:03');
select cast(json_extract('{"d":"2024-02-29"}', '$.d') as date), cast(json_extract('{"d":null}', '$.d') as date), cast(json_extract('{"a":1}', '$.d') as date);
select cast(cast('"not a date"' as json) as date), cast(cast('"2026-02-30"' as json) as date);
select cast(cast('"25:61:00"' as json) as time);
select cast('25:61:00' as time);
select cast(cast('20260131' as json) as date);
select cast(cast('true' as json) as time);
select cast(cast('["2026-01-31"]' as json) as datetime);
select cast(cast('{"d":"2026-01-31"}' as json) as date);
create table jt (id int primary key, j json);
insert into jt values (1, '{"d":"2026-01-31","t":"10:00:00","ts":"2026-01-31 10:20:30.5"}'), (2, '{"d":null}'), (3, '{}'), (4, null);
select id, cast(j->'$.d' as date), cast(j->'$.d' as datetime), cast(j->'$.t' as time), cast(j->'$.ts' as datetime(1)) from jt order by id;
select id from jt where cast(j->'$.d' as date) = '2026-01-31';
select id, date_add(cast(j->'$.d' as date), interval 1 day) from jt where id = 1;
create table jd (id int, d date, dt datetime, t time);
insert into jd select id, cast(j->'$.d' as date), cast(j->'$.ts' as datetime), cast(j->'$.t' as time) from jt;
select * from jd order by id;
drop database cast_json_temporal;
