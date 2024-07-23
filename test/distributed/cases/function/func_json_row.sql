drop table if exists jrt;

-- create a table t for test json, with various data types.
create table jrt (
    id int,
    b bool, i int, bi bigint, f float, d double, 
    d64 decimal(10, 3), d128 decimal(30, 10),
    c1 char(1), c2 char(10), vc varchar(100), t text,
    td date, tt time, tdt datetime,
    js json
);

insert into jrt values 
(1, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vvvv', 'tttttttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(2, false, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'v\vvv', 'ttt"ttttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(3, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'v\\vvv', 'tttt""tttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(4, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'v\\\vvv', 'tttt"""tttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(5, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'v\\\\vvv', 'tttt""""tttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(6, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vv\nvv', 'tttt''tttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(7, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vv\n\nvv', 'tttt''''tttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(8, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'v{vvv', 'tttt}tttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(9, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'v{{vvv', 'ttt}ttttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(10, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vvvv', 'tttttttt[[[[', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(11, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vvvv', 'tttttttt]]]]', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(12, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vvvv', 'tttttttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(13, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vvvv', 'tttttttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(14, false, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vvvv', 'tttttttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(15, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vvvv', 'tttttttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(16, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vvvv', 'tttttttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(17, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vvvv', 'tttttttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(18, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vvvv', 'tttttttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(19, true, 1, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 'a', 'aaa', 'vvvv', 'tttttttt', '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '{"foo": "bar", "zoo": [1, 2, 3]}'),
(1000, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
;

select count(*) from jrt;

-- select
select id, cast(json_row(b, i, bi, f, d) as json) from jrt;
select id, cast(json_row(d64, d128, d64-d128, d128-d64) as json) from jrt;
select id, cast(json_row(c1, c2, vc, t) as json) from jrt;
select id, cast(json_row(td, tt, tdt) as json) from jrt;
select id, cast(json_row(id, js) as json) from jrt;
select id, cast(json_row(null, js) as json) from jrt;

-- aggregate
select 'agg';
select count(*), cast('[' || group_concat(json_row(id, bi, d, d128, t, js)) || ']' as json) from jrt;

-- group_concat bug
select b, count(*), '[' || group_concat(json_row(id)) || ']' as js from jrt group by b;
select b, '[' || json_row(id, bi, d, d128, t, js) || ']' as js from jrt where b = false;
select b, count(*), '[' || group_concat(json_row(id, bi, d, d128, t, js)) || ']' as js from jrt group by b;

-- esp, this one.
select b, '[' || group_concat(t, t, t, t, t) || ']' as js from jrt group by b; 

--
-- failure
--
select json_row(id, cast(vc as varbinary)) from jrt;
drop table jrt;



