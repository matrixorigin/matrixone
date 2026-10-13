-- @case
-- @desc: Prepared integer comparisons keep native filters only for safe bound values.
-- @label:bvt

drop database if exists issue_29506_filters;
create database issue_29506_filters;
use issue_29506_filters;
create table keys32(id int primary key);
insert into keys32 select result from generate_series(1, 20000) g;

prepare equal_key from 'select count(*) from keys32 where id = ?';
set @key = '12345.0';
explain force execute equal_key using @key;
-- @ignore:0
explain analyze force execute equal_key using @key;
execute equal_key using @key;
set @key = cast(12345 as signed);
explain force execute equal_key using @key;
execute equal_key using @key;
set @key = cast(4294967296 as signed);
explain force execute equal_key using @key;
execute equal_key using @key;
set @key = '12345tail';
execute equal_key using @key;
deallocate prepare equal_key;

prepare between_keys from 'select count(*) from keys32 where id between ? and ?';
set @lo = '12344';
set @hi = '12346';
explain force execute between_keys using @lo, @hi;
execute between_keys using @lo, @hi;
deallocate prepare between_keys;

prepare in_keys from 'select count(*) from keys32 where id in (?, ?)';
explain force execute in_keys using @lo, @hi;
execute in_keys using @lo, @hi;
deallocate prepare in_keys;

create table keys64(id bigint primary key);
insert into keys64 values (9007199254740991), (9007199254740992), (9007199254740993);
prepare large_key from 'select group_concat(id order by id) from keys64 where id = ?';
set @key = '9007199254740992';
explain force execute large_key using @key;
execute large_key using @key;
set @key = '9007199254740991';
explain force execute large_key using @key;
execute large_key using @key;
set @key = '9007199254740993';
execute large_key using @key;
set @key = '9007199254740992.5';
execute large_key using @key;
deallocate prepare large_key;

-- Derived numeric parameters retain their casts and native key filtering.
prepare derived_key from 'select group_concat(id order by id) from keys64 where id=abs(cast(? as decimal(38,0)))';
set @key = '-9007199254740993';
explain force execute derived_key using @key;
execute derived_key using @key;
set @key = '9007199254740992.5';
execute derived_key using @key;
set @key = null;
execute derived_key using @key;
set @key = '9223372036854775808';
execute derived_key using @key;
set @key = '9007199254740993';
execute derived_key using @key;
deallocate prepare derived_key;

drop database issue_29506_filters;
