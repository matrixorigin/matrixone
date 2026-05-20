-- test json_object function
select json_object();
select json_object('id', 87, 'name', 'carrot');
select json_object('a', 1, 'b', 'abc', 'c', null, 'd', true);
select json_object('int', 123, 'float', 1.5, 'bool', false);
select json_object('d64', cast(12345.67 as decimal(10,2)), 'd128', cast(9876.54321 as decimal(30,10)));
select json_object(cast('2021-02-01' as date), cast('11:11:11' as time));
select json_object('ts', cast('2022-01-01 01:02:03' as datetime));
select json_object('uid', cast('550e8400-e29b-41d4-a716-446655440000' as uuid));
select json_object('year', cast('2021' as year));
select json_object('bit1', cast(1 as bit), 'bit0', cast(0 as bit));
select json_object('binary', cast('hello' as binary(10)), 'varbin', cast('world' as varbinary));
select json_object('vec32', cast('[1.0,2.0,3.0]' as vecf32(3)), 'vec64', cast('[1.5,2.5,3.5]' as vecf64(3)));

-- nested JSON
select json_object('arr', json_array(1, 2, 3), 'obj', json_object('x', 1, 'y', 2));
select json_object('extracted', json_extract('{"a":1}', '$.a'));
select json_object('escaped', 'a\"b', 'slash', 'c\\d');

-- key overwrite (last wins)
select json_object('id', 1, 'id', 2);
select json_object('a', 1, 'a', 'replaced', 'a', null);

-- test with table
drop table if exists jot;
create table jot (
    id int,
    b bool, bi bigint, f float, d double,
    d64 decimal(10, 3), d128 decimal(30, 10), d256 decimal(50, 20),
    c1 char(1), c2 char(10), vc varchar(100), t text,
    bin binary(10), vbin varbinary,
    bitcol bit(4),
    td date, tt time, tdt datetime,
    ts timestamp,
    yr year,
    uid uuid,
    js json,
    vf32 vecf32(3),
    vf64 vecf64(3)
);

insert into jot values
(1, true, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359, 3.14159265358979323846,
 'a', 'aaa', 'vvvv', 'tttttttt',
 cast('hello' as binary(10)), cast('world' as varbinary), cast(b'0001' as bit(4)),
 '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '2022-01-01 01:02:03',
 2021,
 '550e8400-e29b-41d4-a716-446655440000',
 '{"a": 1, "b": [1, 2, 3], "c": {"d": "hello"}}',
 cast('[1.0,2.0,3.0]' as vecf32(3)), cast('[1.5,2.5,3.5]' as vecf64(3))),
(2, false, null, null, null, null, null, null,
 null, null, null, null,
 null, null, null,
 null, null, null, null,
 null,
 null,
 null,
 null, null),
(3, true, 2, 1.5, 2.5, 99.999, 0.1234567890, 1.23456789012345678901,
 'z', 'zzz', null, 'txt',
 cast('\x00\xFF' as varbinary), cast(null as binary), cast(b'0000' as bit(4)),
 '2021-12-31', '23:59:59', '2021-12-31 23:59:59', '2022-12-31 23:59:59',
 1999,
 '660e8400-e29b-41d4-a716-446655440001',
 '{"foo": "bar", "nested": {"x": null}, "zoo": [1, 2, 3]}',
 cast('[4.0,5.0,6.0]' as vecf32(3)), cast('[3.0,2.0,1.0]' as vecf64(3)))
;
select count(*) from jot;

-- per-type-group selects
select id, json_object('id', id, 'bool', b) from jot;
select id, json_object('bi', bi, 'd', d, 'f', f) from jot;
select id, json_object('d64', d64, 'd128', d128, 'd256', d256) from jot;
select id, json_object('c1', c1, 'vc', vc, 't', t) from jot;
select id, json_object('bin', bin, 'vbin', vbin) from jot;
select id, json_object('bit', bitcol) from jot;
select id, json_object('date', td, 'time', tt, 'dt', tdt) from jot;
select id, json_object('ts', ts) from jot;
select id, json_object('yr', yr) from jot;
select id, json_object('uid', uid) from jot;
select id, json_object('js', js) from jot;
select id, json_object('nullval', null) from jot;
select id, json_object('vf32', vf32, 'vf64', vf64) from jot;

-- all types combined
select id, json_object('id', id, 'b', b, 'bi', bi, 'f', f, 'd', d, 'd64', d64, 'd128', d128, 'd256', d256, 'c1', c1, 'vc', vc, 't', t, 'bin', bin, 'vbin', vbin, 'bit', bitcol, 'date', td, 'time', tt, 'dt', tdt, 'ts', ts, 'yr', yr, 'uid', uid, 'js', js, 'vf32', vf32, 'vf64', vf64) from jot;

-- empty
select json_object();

drop table jot;
