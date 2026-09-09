-- Boundary values for variable-width text, binary payloads, JSON, and UUID.
drop database if exists udf_python_values_bvt;
create database udf_python_values_bvt;
use udf_python_values_bvt;

create function python_bvt_varchar (x varchar(64)) returns varchar(64) language python as 'def python_bvt_varchar(ctx, x): return x' handler 'python_bvt_varchar';
create function python_bvt_text (x text) returns text language python as 'def python_bvt_text(ctx, x): return x' handler 'python_bvt_text';
create function python_bvt_varbinary (x varbinary(8)) returns varbinary(8) language python as 'def python_bvt_varbinary(ctx, x): return x' handler 'python_bvt_varbinary';
create function python_bvt_blob (x blob) returns blob language python as 'def python_bvt_blob(ctx, x): return x' handler 'python_bvt_blob';
create function python_bvt_json (x json) returns json language python as 'def python_bvt_json(ctx, x): return None if x is None else __import__("json").dumps(__import__("json").loads(x), ensure_ascii=False, separators=(",", ":"))' handler 'python_bvt_json';
create function python_bvt_uuid (x uuid) returns uuid language python as 'def python_bvt_uuid(ctx, x): return x' handler 'python_bvt_uuid';

create table text_values (id int, value varchar(64), long_value text);
insert into text_values values
    (1, '', repeat('a', 64)),
    (2, '中😀', '数据库'),
    (3, repeat('v', 64), repeat('b', 64)),
    (4, null, null);

select id, python_bvt_varchar(value) as value,
       python_bvt_text(long_value) as long_value
from text_values order by id;

create table binary_values (id int, var_value varbinary(8), blob_value blob);
insert into binary_values values
    (1, unhex(''), unhex('')),
    (2, unhex('00010203ff'), unhex('00ff10')),
    (3, unhex('00010203ff060708'), unhex('00010203ff060708')),
    (4, null, null);

select id, hex(python_bvt_varbinary(var_value)) as var_value,
       hex(python_bvt_blob(blob_value)) as blob_value
from binary_values order by id;

create table json_values (id int, value json);
insert into json_values values
    (1, '{}'),
    (2, '{"a":1,"b":[true,null,"中"]}'),
    (3, '1'),
    (4, null);

select id, python_bvt_json(value) as value
from json_values order by id;

create table uuid_values (id int, value uuid);
insert into uuid_values values
    (1, cast('00000000-0000-0000-0000-000000000000' as uuid)),
    (2, cast('123e4567-e89b-12d3-a456-426614174000' as uuid)),
    (3, null);

select id, python_bvt_uuid(value) as value
from uuid_values order by id;

drop function python_bvt_varchar(varchar(64));
drop function python_bvt_text(text);
drop function python_bvt_varbinary(varbinary(8));
drop function python_bvt_blob(blob);
drop function python_bvt_json(json);
drop function python_bvt_uuid(uuid);
drop table text_values;
drop table binary_values;
drop table json_values;
drop table uuid_values;
drop database udf_python_values_bvt;
