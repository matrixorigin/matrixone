drop database if exists test_diff_output_as_altered;
create database test_diff_output_as_altered;
use test_diff_output_as_altered;

-- Target-only columns retain their type and are nullable for base-side rows.
create table added_base(id int primary key, note varchar(20) not null);
insert into added_base values (1,'one'),(2,'two');
data branch create table added_tar from added_base;
alter table added_tar add column extra int not null default 7;
update added_tar set note = 'target' where id = 1;
update added_base set note = 'base' where id = 2;
data branch diff added_tar against added_base columns (note,extra) output as added_diff;
show create table added_diff;
select __mo_diff_source, __mo_diff_flag, id, note, extra from added_diff order by id;

-- A renamed column materializes under the target name for both sides.
create table rename_base(id int primary key, note varchar(20) not null);
insert into rename_base values (1,'one'),(2,'two');
data branch create table rename_tar from rename_base;
alter table rename_tar algorithm=instant, rename column note to label;
update rename_tar set label = 'target' where id = 1;
update rename_base set note = 'base' where id = 2;
data branch diff rename_tar against rename_base columns (label) output as rename_diff;
show create table rename_diff;
select __mo_diff_source, __mo_diff_flag, id, label from rename_diff order by id;

-- COPY ALTER with ADD FIRST reassigns physical column identities.
create table copy_base(id int primary key, amount decimal(9,2) not null);
insert into copy_base values (1,10.50),(2,20.00);
data branch create table copy_tar from copy_base;
alter table copy_tar algorithm=copy, add column extra int not null default 9 first;
update copy_tar set amount = 11.75 where id = 1;
update copy_base set amount = 21.25 where id = 2;
data branch diff copy_tar against copy_base columns (amount,extra);
data branch diff copy_tar against copy_base columns (amount,extra) output as copy_diff;
show create table copy_diff;
select __mo_diff_source, __mo_diff_flag, id, amount, extra from copy_diff order by id;

-- The default projection follows the target's visible column order.
data branch diff copy_tar against copy_base output as copy_full_diff;
show create table copy_full_diff;
select * from copy_full_diff order by id;

-- A base-side COPY ALTER reorders a common column.
create table base_copy_base(id int primary key, note varchar(20) not null, amount int not null);
insert into base_copy_base values (1,'one',10),(2,'two',20);
data branch create table base_copy_tar from base_copy_base;
alter table base_copy_base algorithm=copy, modify column note varchar(20) not null first;
data branch diff base_copy_tar against base_copy_base output as empty_reordered_diff;
show create table empty_reordered_diff;
select * from empty_reordered_diff;
update base_copy_tar set note = 'target', amount = 11 where id = 1;
update base_copy_base set note = 'base', amount = 21 where id = 2;
data branch diff base_copy_tar against base_copy_base output as base_copy_diff;
show create table base_copy_diff;
select * from base_copy_diff order by id;

-- Base-side rows written before COPY ALTER must omit dropped columns.
create table drop_base(id int primary key, x int, y int, amount decimal(9,2) not null);
insert into drop_base values (10,11,22,10.00);
data branch create table drop_tar from drop_base;
insert into drop_base values (20,77,88,20.00);
update drop_base set amount = 15.25 where id = 10;
alter table drop_base algorithm=copy, drop column x, drop column y;
data branch diff drop_tar against drop_base output as drop_diff;
show create table drop_diff;
select * from drop_diff order by id;

-- Identical COPY ALTER operations still materialize an empty result table.
create table empty_copy_base(id int primary key, amount int not null);
insert into empty_copy_base values (1,10);
data branch create table empty_copy_tar from empty_copy_base;
alter table empty_copy_base algorithm=copy, add column extra int not null default 9 first;
alter table empty_copy_tar algorithm=copy, add column extra int not null default 9 first;
data branch diff empty_copy_tar against empty_copy_base output as empty_copy_diff;
show create table empty_copy_diff;
select * from empty_copy_diff;

-- COPY ALTER preserves the common columns' types and values in the output.
set @previous_time_zone = @@session.time_zone;
set time_zone = '+00:00';
create table typed_copy_base(id int primary key, wall_time datetime(6), payload json, code binary(4), event_time timestamp(6));
insert into typed_copy_base values
    (1,'2024-01-01 01:02:03.010203','{"id":1}',x'01020304','2024-01-01 04:05:06.040506'),
    (2,'2024-01-02 01:02:03.010203','{"id":2}',x'05060708','2024-01-02 04:05:06.040506');
data branch create table typed_copy_tar from typed_copy_base;
alter table typed_copy_tar algorithm=copy, add column extra int not null default 9 first;
update typed_copy_tar set wall_time = '2024-02-01 12:34:56.123456', payload = '{"side":"target"}',
    code = x'00ff275c', event_time = '2024-02-01 06:07:08.654321' where id = 1;
update typed_copy_base set wall_time = '2024-02-02 12:34:56.654321', payload = '{"side":"base"}',
    code = x'61008000', event_time = '2024-02-02 06:07:08.123456' where id = 2;
data branch diff typed_copy_tar against typed_copy_base output as typed_copy_diff;
show create table typed_copy_diff;
select __mo_diff_source, __mo_diff_flag, extra, id, wall_time, payload, hex(code), event_time from typed_copy_diff order by id;

-- Both instants in the New York DST fall-back fold survive OUTPUT AS.
create table dst_base(id int primary key, ts timestamp(6));
data branch create table dst_tar from dst_base;
alter table dst_tar algorithm=copy, add column extra int not null default 9 first;
insert into dst_tar (id,ts) values (1,'2024-11-03 05:30:00.123456'),(2,'2024-11-03 06:30:00.123456');
set time_zone = 'America/New_York';
data branch diff dst_tar against dst_base output as dst_diff;
set time_zone = '+00:00';
select __mo_diff_source, __mo_diff_flag, id, ts from dst_diff order by id;

-- A local year-9999 timestamp can be in year 10000 in UTC.
set time_zone = '-05:00';
create table timestamp_max_base(id int primary key, ts timestamp(6));
data branch create table timestamp_max_tar from timestamp_max_base;
insert into timestamp_max_tar values (1,'2024-11-03 01:30:00.123456'),(2,'9999-12-31 23:59:59.999999');
alter table timestamp_max_tar algorithm=copy, add column extra int not null default 9 first;
data branch diff timestamp_max_tar against timestamp_max_base output as timestamp_max_diff;
show create table timestamp_max_diff;
select __mo_diff_source, __mo_diff_flag, extra, id, ts from timestamp_max_diff order by id;

-- Read the earliest storable non-zero instant in two positive offsets.
set time_zone = '+05:00';
create table timestamp_min_base(id int primary key, ts timestamp(6));
data branch create table timestamp_min_tar from timestamp_min_base;
insert into timestamp_min_tar values (1,'1970-01-01 05:00:01.000000');
data branch diff timestamp_min_tar against timestamp_min_base output as timestamp_min_diff;
select __mo_diff_source, __mo_diff_flag, id, ts from timestamp_min_diff;
set time_zone = '+06:00';
select id, ts from timestamp_min_diff;

-- The same negative offset can represent both ends of the row.
set time_zone = '-05:00';
create table timestamp_mixed_base(id int primary key, ts_min timestamp(6) not null, ts_max timestamp(6));
data branch create table timestamp_mixed_tar from timestamp_mixed_base;
insert into timestamp_mixed_tar values (1,'1969-12-31 19:00:01.000000','9999-12-31 23:59:59.999999');
set time_zone = '+05:00';
data branch diff timestamp_mixed_tar against timestamp_mixed_base output as timestamp_mixed_diff;
show create table timestamp_mixed_diff;
set time_zone = '-05:00';
select __mo_diff_source, __mo_diff_flag, id, ts_min, ts_max from timestamp_mixed_diff;
set time_zone = '+05:00';
-- Read the five-digit year as text so JDBC does not parse it as a timestamp.
select __mo_diff_source, __mo_diff_flag, id, ts_min, cast(ts_max as varchar(27)) as ts_max from timestamp_mixed_diff;
set time_zone = @previous_time_zone;

-- Unrelated tables resolve reordered columns by name and require matching decimal attributes.
create table b(id int primary key, pad decimal(9,2), v decimal(9,2) unsigned);
create table t(id int primary key, v decimal(9,2), pad decimal(9,2));
insert into t values (1, -1.25, 0);
data branch diff t against b columns (v) output as dec_unsigned_diff;
show tables like 'dec_unsigned_diff';

create table dec_ok_b(id int primary key, pad decimal(9,2), v decimal(9,2) unsigned);
create table dec_ok_t(id int primary key, v decimal(9,2) unsigned, pad decimal(9,2));
insert into dec_ok_b values (1, 0, 2.50);
insert into dec_ok_t values (2, 1.25, 0);
data branch diff dec_ok_t against dec_ok_b columns (v) output as dec_ok_diff;
show create table dec_ok_diff;
select __mo_diff_source, __mo_diff_flag, id, v from dec_ok_diff order by id;

-- The output metadata name must avoid Unicode fold collisions with source columns.
create table fold_base(id int primary key, `__mo_diff_ſource` int);
insert into fold_base values (1, 10);
data branch create table fold_tar from fold_base;
alter table fold_tar algorithm=copy, add column extra int not null default 9 first;
update fold_tar set `__mo_diff_ſource` = 20 where id = 1;
data branch diff fold_tar against fold_base output as fold_diff;
show create table fold_diff;
select * from fold_diff order by id;

drop database test_diff_output_as_altered;
