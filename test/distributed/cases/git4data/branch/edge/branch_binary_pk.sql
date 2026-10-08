-- DATA BRANCH must preserve and sort fixed- and variable-length binary primary keys.
drop database if exists branch_binary_pk;
create database branch_binary_pk;
use branch_binary_pk;

create table binary_base(k binary(4) primary key, v int);
insert into binary_base values
  (x'00', 10),
  (x'0061', 20),
  (x'61', 30),
  (x'ff', 40);
data branch create table binary_branch from binary_base;
select hex(k) as k, v from binary_branch order by k;
update binary_branch set v = 31 where hex(k) = '61000000';
select v as binary_base_v from binary_base where hex(k) = '61000000';
select v as binary_branch_v from binary_branch where hex(k) = '61000000';

create table varbinary_base(k varbinary(8) primary key, v int);
insert into varbinary_base values
  (x'00', 10),
  (x'0001', 20),
  (x'61', 30),
  (x'ff', 40);
data branch create table varbinary_branch from varbinary_base;
select hex(k) as k, v from varbinary_branch order by k;
update varbinary_branch set v = 31 where k = x'61';
select v as varbinary_base_v from varbinary_base where k = x'61';
select v as varbinary_branch_v from varbinary_branch where k = x'61';

-- SQL-created BINARY values retain their existing padding.
data branch merge binary_branch into binary_base when conflict accept;
data branch diff binary_branch against binary_base output count;
select hex(k) as k, v from binary_base order by k;

-- #26819: LOAD DATA's existing short BINARY bytes must survive SQL replay.
create table loaded_base(id int primary key, v int, b binary(255), key(v));
load data infile '$resources/load_data/branch_binary_short.csv' into table loaded_base
fields terminated by '|' lines terminated by '\n' parallel 'true';
select id, v, octet_length(b) as bytes, hex(b) as b from loaded_base order by id;
data branch create table loaded_branch from loaded_base;
update loaded_branch set v = v + 1;
data branch diff loaded_branch against loaded_base output count;
data branch merge loaded_branch into loaded_base when conflict accept;
data branch diff loaded_branch against loaded_base output count;
select id, v, octet_length(b) as bytes, hex(b) as b from loaded_base order by id;
select id from loaded_base where v = 11;
insert into loaded_branch select id + 2, v + 10, b from loaded_base;
delete from loaded_branch where id = 1;
data branch merge loaded_branch into loaded_base when conflict accept;
data branch diff loaded_branch against loaded_base output count;
select id, v, octet_length(b) as bytes, hex(b) as b from loaded_base order by id;

-- Typed tuple decoding preserves bytes, unlike an explicit BINARY cast.
select hex(serial_extract(serial(x'4142'), 0 as binary(4))) as decoded,
       hex(cast(x'4142' as binary(4))) as converted;
create table byte_base(id int primary key, b binary(255), v int);
insert into byte_base values
  (1, serial_extract(serial(x''), 0 as binary(255)), 10),
  (2, NULL, 20),
  (3, serial_extract(serial(x'00ff005c27'), 0 as binary(255)), 30),
  (4, serial_extract(serial(repeat('Z', 255)), 0 as binary(255)), 40);
data branch create table byte_branch from byte_base;
update byte_branch set v = v + 1;
data branch diff byte_branch against byte_base output as byte_diff;
select __mo_diff_flag, id, b is null as is_null, octet_length(b) as bytes,
       hex(b) = hex(serial_extract(serial(x'00ff005c27'), 0 as binary(255))) as exact_special
from byte_diff order by id;
data branch merge byte_branch into byte_base when conflict accept;
data branch diff byte_branch against byte_base output count;
select id, b is null as is_null, octet_length(b) as bytes, v from byte_base order by id;

-- PICK replays the same typed payload through its own SQL appender.
data branch create table byte_pick from byte_base;
update byte_branch set v = v + 1;
data branch pick byte_branch into byte_pick keys(1, 2, 3, 4);
data branch diff byte_branch against byte_pick output count;
select id, b is null as is_null, octet_length(b) as bytes, v from byte_pick order by id;

-- Without a primary key, replay also matches deletes by the exact row bytes.
create table no_pk_base(b binary(4), v int);
insert into no_pk_base values
  (serial_extract(serial(x'4142'), 0 as binary(4)), 10),
  (serial_extract(serial(x'414200'), 0 as binary(4)), 20);
data branch create table no_pk_branch from no_pk_base;
delete from no_pk_branch where hex(b) = '4142';
update no_pk_branch set v = 21;
data branch merge no_pk_branch into no_pk_base when conflict accept;
select hex(b) as b, v from no_pk_branch;
select hex(b) as b, v from no_pk_base;

-- Short keys with trailing NULs are distinct, including composite keys.
create table short_pk(k binary(4) primary key, v int);
insert into short_pk values
  (serial_extract(serial(x'4142'), 0 as binary(4)), 10),
  (serial_extract(serial(x'414200'), 0 as binary(4)), 20);
data branch create table short_pk_branch from short_pk;
update short_pk_branch set v = v + 1;
delete from short_pk_branch where hex(k) = '414200';
insert into short_pk_branch values (serial_extract(serial(x'ff'), 0 as binary(4)), 30);
data branch merge short_pk_branch into short_pk when conflict accept;
data branch diff short_pk_branch against short_pk output count;
select hex(k) as k, v from short_pk order by k;

create table composite_pk(id int, k binary(4), v int, primary key(id, k));
insert into composite_pk values
  (1, serial_extract(serial(x'4142'), 0 as binary(4)), 10),
  (1, serial_extract(serial(x'414200'), 0 as binary(4)), 20);
data branch create table composite_branch from composite_pk;
update composite_branch set v = v + 1;
data branch merge composite_branch into composite_pk when conflict accept;
data branch diff composite_branch against composite_pk output count;
select id, hex(k) as k, v from composite_pk order by id, k;

-- Historical replay uses the snapshot payload, not the subsequently edited source.
create table historical_base(id int primary key, k binary(4), v int);
insert into historical_base values
  (1, serial_extract(serial(x'4142'), 0 as binary(4)), 10),
  (2, serial_extract(serial(x'414200'), 0 as binary(4)), 20);
data branch create table historical_source from historical_base;
update historical_source set v = v + 1;
drop snapshot if exists branch_binary_history;
create snapshot branch_binary_history for table branch_binary_pk historical_source;
update historical_source set k = x'43', v = 99 where id = 1;
data branch create table historical_target from historical_base;
update historical_target set v = 0;
data branch merge historical_source{snapshot='branch_binary_history'} into historical_target when conflict accept;
data branch diff historical_source{snapshot='branch_binary_history'} against historical_target output count;
select id, hex(k) as k, v from historical_target order by id, k;
drop snapshot branch_binary_history;

-- A rejected conflicting merge leaves the short destination bytes unchanged.
data branch create table conflict_branch from short_pk;
update conflict_branch set v = 101 where hex(k) = '4142';
update short_pk set v = 202 where hex(k) = '4142';
data branch merge conflict_branch into short_pk;
select hex(k) as k, v from short_pk order by k;
data branch merge conflict_branch into short_pk when conflict accept;
data branch diff conflict_branch against short_pk output count;
select hex(k) as k, v from short_pk order by k;

drop database branch_binary_pk;
