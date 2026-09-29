-- @case
-- @desc: Proven integral ROUND and TRUNCATE text bindings retain integer-key filtering.
-- @label:bvt

drop database if exists issue_29512_round_filter;
create database issue_29512_round_filter;
use issue_29512_round_filter;
create table lookup_key(id bigint primary key);
insert into lookup_key select result from generate_series(1, 20000) g;
insert into lookup_key values (-12345), (9007199254740991), (9007199254740992), (9007199254740993);

prepare round_key from 'select count(*) from lookup_key where id = round(?, 0)';
set @value = '12345.0';
explain force execute round_key using @value;
-- @ignore:0
explain analyze force execute round_key using @value;
execute round_key using @value;
set @value = '1.2345e4';
explain force execute round_key using @value;
execute round_key using @value;
set @value = '-12345.0';
execute round_key using @value;
set @value = '12345.5';
explain force execute round_key using @value;
execute round_key using @value;
set @value = '9007199254740992';
explain force execute round_key using @value;
execute round_key using @value;
set @value = null;
execute round_key using @value;
deallocate prepare round_key;

prepare scalar_round from 'select count(*) from lookup_key where id = round((select ?), 0)';
set @value = '12345.0';
explain force execute scalar_round using @value;
-- @ignore:0
explain analyze force execute scalar_round using @value;
execute scalar_round using @value;
deallocate prepare scalar_round;

prepare derived_round from 'select count(*) from lookup_key k join (select ? as v) x where k.id = round(x.v, 0)';
explain force execute derived_round using @value;
-- @ignore:0
explain analyze force execute derived_round using @value;
execute derived_round using @value;
deallocate prepare derived_round;

prepare truncate_key from 'select count(*) from lookup_key where id = truncate(?, 0)';
explain force execute truncate_key using @value;
-- @ignore:0
explain analyze force execute truncate_key using @value;
execute truncate_key using @value;
set @value = '12345.5';
execute truncate_key using @value;
deallocate prepare truncate_key;

drop database issue_29512_round_filter;
