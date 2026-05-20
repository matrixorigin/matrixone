-- test json_type function
select json_type('{"a": 1}');
select json_type('[1, 2, 3]');
select json_type('42');
select json_type('3.14');
select json_type('"hello"');
select json_type('true');
select json_type('false');
select json_type('null');
select json_type(null);

-- via json functions
select json_type(json_array(1, 2, 3));
select json_type(json_object('id', 1, 'name', 'carrot'));

-- typed scalars via json_array + json_extract
select json_type(json_extract(json_array(cast(3.14 as decimal(10,2))), '$[0]'));
select json_type(json_extract(json_array(cast('2021-02-01' as date)), '$[0]'));
select json_type(json_extract(json_array(cast('11:11:11' as time)), '$[0]'));
select json_type(json_extract(json_array(cast('2021-02-01 11:11:11' as datetime)), '$[0]'));
select json_type(json_extract(json_array(cast('hello' as binary(10))), '$[0]'));

-- test with table, all types
drop table if exists jtt;
create table jtt (
    id int,
    b bool, bi bigint, f float, d double,
    d64 decimal(10, 3), d128 decimal(30, 10),
    c1 char(1), c2 char(10), vc varchar(100), t text,
    bin binary(10), vbin varbinary,
    td date, tt time, tdt datetime,
    ts timestamp,
    yr year,
    uid uuid,
    js json,
    vf32 vecf32(3),
    e enum('a', 'b', 'c'),
    bitcol bit
);

insert into jtt values
(1, true, 11111111111111, 0.1, 0.2222222, 3.14, 3.14159265359,
 'a', 'aaa', 'vvvv', 'tttttttt',
 cast('hello' as binary(10)), cast('world' as varbinary),
 '2021-02-01', '11:11:11', '2021-02-01 11:11:11', '2022-01-01 01:02:03',
 2021,
 '550e8400-e29b-41d4-a716-446655440000',
 '{"a": 1, "b": [1, 2, 3], "c": {"d": "hello"}}',
 cast('[1.0,2.0,3.0]' as vecf32(3)), 'a', cast(1 as bit)),
(2, false, null, null, null, null, null,
 null, null, null, null,
 null, null,
 null, null, null, null,
 null, null, null, null, null, null, null)
;
select count(*) from jtt;

-- json_type on json column directly
select id, json_type(js) from jtt;
select id, json_type(null) from jtt;

-- typed scalars from table via json_array + json_extract
select json_type(json_extract(json_array(b), '$[0]')) from jtt where b is not null;
select json_type(json_extract(json_array(bi), '$[0]')) from jtt where bi is not null;
select json_type(json_extract(json_array(f), '$[0]')) from jtt where f is not null;
select json_type(json_extract(json_array(d), '$[0]')) from jtt where d is not null;
select json_type(json_extract(json_array(d64), '$[0]')) from jtt where d64 is not null;
select json_type(json_extract(json_array(d128), '$[0]')) from jtt where d128 is not null;
select json_type(json_extract(json_array(c1), '$[0]')) from jtt where c1 is not null;
select json_type(json_extract(json_array(td), '$[0]')) from jtt where td is not null;
select json_type(json_extract(json_array(tt), '$[0]')) from jtt where tt is not null;
select json_type(json_extract(json_array(tdt), '$[0]')) from jtt where tdt is not null;
select json_type(json_extract(json_array(ts), '$[0]')) from jtt where ts is not null;
select json_type(json_extract(json_array(uid), '$[0]')) from jtt where uid is not null;
select json_type(json_extract(json_array(yr), '$[0]')) from jtt where yr is not null;
select json_type(json_extract(json_array(bin), '$[0]')) from jtt where bin is not null;
select json_type(json_extract(json_array(vbin), '$[0]')) from jtt where vbin is not null;
select json_type(json_extract(json_array(vf32), '$[0]')) from jtt where vf32 is not null;
select json_type(json_extract(json_array(e), '$[0]')) from jtt where e is not null;
select json_type(json_extract(json_array(bitcol), '$[0]')) from jtt where bitcol is not null;

-- json_type on json_object value
select json_type(json_extract(json_object('id', id, 'name', vc), '$.id')) from jtt where vc is not null;
select json_type(json_extract(json_object('date', td, 'time', tt), '$.date')) from jtt where td is not null;
select json_type(json_extract(json_object('ts', ts), '$.ts')) from jtt where ts is not null;
select json_type(json_extract(json_object('uid', uid), '$.uid')) from jtt where uid is not null;

drop table jtt;

-- error: non-JSON argument should fail
-- select json_type(1);
-- select json_type(cast('2021-02-01' as date));
