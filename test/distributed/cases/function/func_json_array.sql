-- test json_array function
select json_array();
select json_array(null);
select json_array(1, 2, 3);
select json_array(1, "abc", null, true);
select json_array(1.5, 2.5, 3.5);
select json_array(true, false, true);
select json_array("hello", "world");
select json_array(cast('2021-02-01' as date), cast('11:11:11' as time), cast('2021-02-01 11:11:11' as datetime));
select json_array(cast('2022-01-01 01:02:03' as datetime));
select json_array(cast(12345.67 as decimal(10,2)), cast(9876.54321 as decimal(30,10)));
select json_array(cast('550e8400-e29b-41d4-a716-446655440000' as uuid));
select json_array(json_extract('{"a":1}', '$.a'), json_extract('{"b":2}', '$.b'));
select json_array('a\"b', 'c\\d', 'e/f');
select json_array(cast('[1.0,2.0,3.0]' as vecf32(3)));
select json_array(cast('[1.5,2.5,3.5]' as vecf64(3)));

-- bit type
select json_array(cast(1 as bit), cast(0 as bit));

-- year type
select json_array(cast('2021' as year), cast('1999' as year));

-- enum type
drop table if exists jat_enum;
create table jat_enum (e enum('a', 'b', 'c'));
insert into jat_enum values ('a'), ('b'), ('c');
select json_array(e) from jat_enum;
drop table jat_enum;

-- binary types
select json_array(cast('hello' as binary(10)), cast('world' as varbinary));
select json_array(cast(null as binary), cast(null as varbinary));
select json_array(cast(x'000102ff' as varbinary));

-- test with full table
set time_zone = '+08:00';
drop table if exists jat;
create table jat (
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

insert into jat values
(
    1, 
    true, 
    11111111111111,
    0.1, 
    0.2222222,
    3.14, 
    3.14159265359, 
    3.14159265358979323846,
    'a',
    'aaa',
    'vvvv',
    'tttttttt',
    cast('hello' as binary(10)),
    cast('world' as varbinary), 
    cast(1 as bit(4)),
    '2021-02-01', 
    '11:11:11', 
    '2021-02-01 11:11:11', 
    '2022-01-01 01:02:03',
    2021,
    '550e8400-e29b-41d4-a716-446655440000',
    '{"a": 1, "b": [1, 2, 3], "c": {"d": "hello"}}',
    cast('[1.0,2.0,3.0]' as vecf32(3)), 
    cast('[1.5,2.5,3.5]' as vecf64(3))
),
(
    2, 
    false, 
    null,
    null, 
    null, 
    null, 
    null, 
    null,
    null, 
    null, 
    null, 
    null,
    null, 
    null, 
    null,
    null, 
    null, 
    null, 
    null,
    null,
    null,
    null,
    null, 
    null
 ),
(3, true, 2, 1.5, 2.5, 99.999, 0.1234567890, 1.23456789012345678901,
 'z', 'zzz', null, 'txt',
 cast('\x00\xFF' as varbinary), cast(null as binary), 
    cast(7 as bit(4)),
 '2021-12-31', '23:59:59', '2021-12-31 23:59:59', '2022-12-31 23:59:59',
 1999,
 '660e8400-e29b-41d4-a716-446655440001',
 '{"foo": "bar", "zoo": [1, 2, 3], "nested": {"x": null}}',
 cast('[4.0,5.0,6.0]' as vecf32(3)), cast('[3.0,2.0,1.0]' as vecf64(3)))
;

select count(*) from jat;

-- scalar selects per type group
select id, json_array(b, bi, f, d) from jat;
select id, json_array(d64, d128, d256) from jat;
select id, json_array(c1, c2, vc, t) from jat;
select id, json_array(bin, vbin) from jat;
select id, json_array(bitcol) from jat;
select id, json_array(td, tt, tdt) from jat;
select id, json_array(ts) from jat;
select id, json_array(yr) from jat;
select id, json_array(uid) from jat;
select id, json_array(id, js) from jat;
select id, json_array(null, js) from jat;
select id, json_array(vf32, vf64) from jat;

-- MySQL-compatible typed values retain their JSON representation.
select json_type(json_extract(json_array(cast('2024' as year)), '$[0]')), json_contains(json_array(cast('2024' as year)), '2024'), json_contains(json_array(cast('2024' as year)), '"2024"');
select json_unquote(json_extract(json_array(cast('04:05:06' as time(0)), cast('04:05:06.123456' as time(6))), '$[0]')), json_unquote(json_extract(json_array(cast('04:05:06' as time(0)), cast('04:05:06.123456' as time(6))), '$[1]'));
select json_array(st_geomfromtext('POINT(1 2)')) = json_array(convert(st_geomfromtext('POINT(1 2)'), json));
set time_zone = '+00:00';
select json_unquote(json_extract(json_array(ts), '$[0]')) from jat where id = 1;
set time_zone = '+08:00';
select json_unquote(json_extract(json_array(ts), '$[0]')) from jat where id = 1;

set @json_array_blob = x'00ff';
set @json_array_bit = 10;
prepare json_array_typed from 'select json_array(cast(? as blob), cast(? as bit(4)))';
execute json_array_typed using @json_array_blob, @json_array_bit;
deallocate prepare json_array_typed;

-- all types combined
select id, json_array(id, b, bi, f, d, d64, d128, d256, c1, vc, t, bin, vbin, bitcol, td, tt, tdt, ts, yr, uid, js, vf32, vf64) from jat;

-- empty array
select json_array();

drop table jat;
set time_zone = 'SYSTEM';
