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
set @issue_29429_saved_sql_mode = @@session.sql_mode;
set session sql_mode = concat_ws(',', nullif(@@session.sql_mode, ''), 'MYSQL_NUMERIC_COMPATIBILITY');
execute scan_key using @key;
show count(*) warnings;
set session sql_mode = @issue_29429_saved_sql_mode;
set @issue_29429_saved_sql_mode = null;
set @key = null;
execute scan_key using @key;
show count(*) warnings;
set @key = 2;
execute scan_key using @key;
show count(*) warnings;
deallocate prepare scan_key;

-- A scalar projection must keep the same numeric comparison domain as a direct marker.
prepare scan_scalar_key from 'select value from lookup_key where id = (select ?)';
set @key = '1.5';
execute scan_scalar_key using @key;
set @key = '1';
execute scan_scalar_key using @key;
set @key = null;
execute scan_scalar_key using @key;
set @key = '2';
execute scan_scalar_key using @key;
deallocate prepare scan_scalar_key;

prepare join_key from 'select a.value from lookup_key a join lookup_key b on a.id = b.id where a.id = ?';
set @key = 1;
execute join_key using @key;
show count(*) warnings;
set @key = 'invalid';
set @issue_29429_saved_sql_mode = @@session.sql_mode;
set session sql_mode = concat_ws(',', nullif(@@session.sql_mode, ''), 'MYSQL_NUMERIC_COMPATIBILITY');
execute join_key using @key;
show count(*) warnings;
set session sql_mode = @issue_29429_saved_sql_mode;
set @issue_29429_saved_sql_mode = null;
set @key = null;
execute join_key using @key;
show count(*) warnings;
set @key = 2;
execute join_key using @key;
show count(*) warnings;
deallocate prepare join_key;

create table lookup_composite(k1 int, k2 int, value varchar(12), primary key(k1, k2));
insert into lookup_composite values (1, 2, 'two'), (1, 3, 'three');
prepare composite_key from 'select value from lookup_composite where k1 = ? and k2 = ?';
prepare composite_scalar from 'select value from lookup_composite where k1 = ? and k2 + 0 = ?';
set @first = 1;
set @second = 2;
execute composite_key using @first, @second;
show count(*) warnings;
execute composite_scalar using @first, @second;
show count(*) warnings;
set @second = 'invalid';
set @issue_29429_saved_sql_mode = @@session.sql_mode;
set session sql_mode = concat_ws(',', nullif(@@session.sql_mode, ''), 'MYSQL_NUMERIC_COMPATIBILITY');
execute composite_key using @first, @second;
show count(*) warnings;
execute composite_scalar using @first, @second;
show count(*) warnings;
set session sql_mode = @issue_29429_saved_sql_mode;
set @issue_29429_saved_sql_mode = null;
set @second = null;
execute composite_key using @first, @second;
show count(*) warnings;
execute composite_scalar using @first, @second;
show count(*) warnings;
set @second = 3;
execute composite_key using @first, @second;
show count(*) warnings;
execute composite_scalar using @first, @second;
show count(*) warnings;
deallocate prepare composite_key;
deallocate prepare composite_scalar;

drop database issue_29429_prepared_filter;
