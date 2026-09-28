-- @case
-- @desc: Rebinding a prepared integer-key predicate preserves rows and conversion warnings.
-- @label:bvt

drop database if exists issue_29429_prepared_filter;
create database issue_29429_prepared_filter;
use issue_29429_prepared_filter;

create table lookup_key(id bigint primary key, value varchar(12));
insert into lookup_key values (1, 'one'), (2, 'two');

prepare scan_key from 'select value from lookup_key where id = ?';
set @key = 1;
execute scan_key using @key;
show count(*) warnings;
set @key = 'invalid';
execute scan_key using @key;
show count(*) warnings;
set @key = null;
execute scan_key using @key;
show count(*) warnings;
set @key = 2;
execute scan_key using @key;
show count(*) warnings;
deallocate prepare scan_key;

prepare join_key from 'select a.value from lookup_key a join lookup_key b on a.id = b.id where a.id = ?';
set @key = 1;
execute join_key using @key;
show count(*) warnings;
set @key = 'invalid';
execute join_key using @key;
show count(*) warnings;
set @key = null;
execute join_key using @key;
show count(*) warnings;
set @key = 2;
execute join_key using @key;
show count(*) warnings;
deallocate prepare join_key;

drop database issue_29429_prepared_filter;
