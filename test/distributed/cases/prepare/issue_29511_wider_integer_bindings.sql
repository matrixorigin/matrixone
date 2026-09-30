-- @case
-- @desc: Wider and differently signed integer bindings use native key filters only in a proven safe domain.
-- @label:bvt

drop database if exists issue_29511_filters;
create database issue_29511_filters;
use issue_29511_filters;
create table lookup_key(id int primary key, u bigint unsigned);
insert into lookup_key select result, result from generate_series(1, 20000) g;

prepare signed_key from 'select count(*) from lookup_key where id = ?';
set @key = cast(12345 as signed);
explain force execute signed_key using @key;
-- @ignore:0
explain analyze force execute signed_key using @key;
execute signed_key using @key;
set @key = cast(4294967296 as signed);
explain force execute signed_key using @key;
execute signed_key using @key;
set @key = cast(-1 as signed);
explain force execute signed_key using @key;
execute signed_key using @key;
set @key = cast(12346 as signed);
execute signed_key using @key;
deallocate prepare signed_key;

prepare unsigned_key from 'select count(*) from lookup_key where u = ?';
set @key = cast(12345 as signed);
explain force execute unsigned_key using @key;
-- @ignore:0
explain analyze force execute unsigned_key using @key;
execute unsigned_key using @key;
set @key = cast(-1 as signed);
explain force execute unsigned_key using @key;
execute unsigned_key using @key;
set @key = cast(9007199254740993 as signed);
explain force execute unsigned_key using @key;
execute unsigned_key using @key;
set @key = cast(12346 as signed);
execute unsigned_key using @key;
deallocate prepare unsigned_key;

drop database issue_29511_filters;
