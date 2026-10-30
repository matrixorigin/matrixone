-- IFNULL/IF/CASE with a JSON value next to text or numbers resolve to TEXT
-- like COALESCE; IF reads a JSON condition through JSON -> BOOL (issue #31544).
select ifnull(json_extract('{"a":null}', '$.a'), 'x');
select ifnull(json_extract('{"a":7}', '$.a'), 'x');
select ifnull(json_extract('{"a":1}', '$.zz'), 'x');
select ifnull(json_extract('{"a":"s"}', '$.a'), 0);
select if(json_extract('{"a":null}', '$.a'), 'y', 'n');
select if(json_extract('{"a":7}', '$.a'), 'y', 'n');
select if(json_extract('{"a":0}', '$.a'), 'y', 'n');
select if(json_extract('{"a":1}', '$.zz'), 'y', 'n');
select if(true, json_extract('{"a":[1,2]}', '$.a'), 'n'), if(false, json_extract('{"a":7}', '$.a'), 'n');
select case when 1 = 1 then json_extract('{"a":7}', '$.a') else 'n' end;
select coalesce(json_extract('{"a":null}', '$.a'), 'x'), coalesce(json_extract('{"a":1}', '$.zz'), 'x');
drop database if exists cond_json;
create database cond_json;
use cond_json;
create table t (id int primary key, j json);
insert into t values (1, '{"name":"a","n":1}'), (2, '{"n":null}'), (3, null);
select id, ifnull(json_extract(j, '$.name'), 'none'), if(json_extract(j, '$.n'), 'set', 'unset') from t order by id;
drop database cond_json;
