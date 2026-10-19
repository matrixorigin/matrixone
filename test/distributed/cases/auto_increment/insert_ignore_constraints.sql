drop database if exists auto_increment_ignore_constraints;
create database auto_increment_ignore_constraints;
use auto_increment_ignore_constraints;

-- CHECK rejection and duplicate rejection share the accepted-row ID order.
create table checks_case(id bigint auto_increment primary key, uk int unique, v int check(v > 0));
insert ignore into checks_case values (NULL,1,-1),(NULL,2,1),(NULL,2,1),(NULL,3,1);
select id,uk from checks_case order by id;
select last_insert_id();
select id from checks_case where uk=3;
insert into checks_case(uk,v) values (99,1);
select id,uk from checks_case order by id;

-- A rejected middle row must not leave a positional gap.
truncate table checks_case;
insert ignore into checks_case values (NULL,1,1),(NULL,2,-1),(NULL,3,1);
select id,uk from checks_case order by id;
select last_insert_id();

-- An all-rejected statement must not insert rows or publish LAST_INSERT_ID.
truncate table checks_case;
insert ignore into checks_case values (NULL,1,-1),(NULL,2,-1);
select count(*) from checks_case;
select last_insert_id();
insert into checks_case(uk,v) values (3,1);
select id,uk from checks_case order by id;

-- A CHECK-rejected explicit ID must not advance or overflow eager allocation.
create table tiny_case(id tinyint auto_increment primary key, uk int unique, v int check(v>0));
insert ignore into tiny_case values (127,1,-1),(NULL,2,1);
select id,uk from tiny_case order by id;
select last_insert_id();
insert into tiny_case(uk,v) values (3,1);
select id,uk from tiny_case order by id;

-- Only accepted explicit IDs do not start a generated reservation.
create table manual_case(id bigint auto_increment primary key, v int check(v>0));
insert ignore into manual_case values (NULL,-1),(5,1);
insert into manual_case(v) values (2);
select id,v from manual_case order by id;

-- CHECK rejection compacts IDs when the table has no secondary UK.
create table check_only(id bigint auto_increment primary key, v int check(v>0));
insert ignore into check_only(v) values (-1),(1),(-1),(2);
select id,v from check_only order by id;
select last_insert_id();

-- FK rejection retains generated candidate provenance until arbitration.
create table parents(id int primary key);
insert into parents values (1);
create table fk_case(id bigint auto_increment primary key, uk int unique, p int,
foreign key(p) references parents(id));
insert ignore into fk_case values (NULL,1,9),(NULL,2,1),(NULL,2,1),(NULL,3,1);
select id,uk,p from fk_case order by id;
select last_insert_id();
insert into fk_case(uk,p) values (99,1);
select id,uk from fk_case order by id;

-- Unlike CHECK, all-FK rejection still consumes generated reservations.
truncate table fk_case;
insert ignore into fk_case(uk,p) values (1,9),(2,9);
insert into fk_case(uk,p) values (3,1);
select id,uk from fk_case order by id;

-- FK-only arbitration also preserves MATCH SIMPLE for NULL child values.
create table fk_only(id bigint auto_increment primary key, p int,
foreign key(p) references parents(id));
insert ignore into fk_only(p) values (9),(NULL),(1);
select id,p from fk_only order by id;
select last_insert_id();

-- Mixed filters must not let a rejected row publish a key.
create table mixed_case(id bigint auto_increment primary key, uk int unique,
v int check(v>0), p int, foreign key(p) references parents(id));
insert ignore into mixed_case values
(NULL,7,-1,1),(NULL,7,1,9),(NULL,7,1,1),(NULL,7,1,1),(NULL,8,1,1);
select id,uk from mixed_case order by id;
select last_insert_id();
select id from mixed_case where uk=8;
begin;
insert ignore into mixed_case values (NULL,9,-1,1),(NULL,10,1,1);
rollback;
select id,uk from mixed_case order by id;

-- Session series changes apply to retained candidates without arithmetic repair.
set @saved_increment = @@auto_increment_increment;
set @saved_offset = @@auto_increment_offset;
set auto_increment_increment=3;
set auto_increment_offset=2;
create table series_case(id bigint auto_increment primary key, uk int unique, v int check(v>0));
insert ignore into series_case values (NULL,1,-1),(NULL,2,1),(NULL,2,1),(NULL,3,1);
select id,uk from series_case order by id;
select last_insert_id();
insert into series_case(uk,v) values (99,1);
select id,uk from series_case order by id;
set auto_increment_increment=@saved_increment;
set auto_increment_offset=@saved_offset;

-- Unused reservations saturate at the destination integer boundary.
create table tiny_tail(id tinyint auto_increment primary key, v int check(v>0)) auto_increment=124;
insert ignore into tiny_tail(v) values (1),(-1),(-1),(-1),(-1);
select id,v from tiny_tail order by id;
select last_insert_id();
insert into tiny_tail(v) values (2);
select id,v from tiny_tail order by id;
create table medium_tail(id mediumint unsigned auto_increment primary key, v int check(v>0)) auto_increment=16777212;
insert ignore into medium_tail(v) values (1),(-1),(-1),(-1),(-1);
insert into medium_tail(v) values (2);
select id,v from medium_tail order by id;

-- A functional secondary index must publish the final accepted primary key.
create table expression_case(id bigint auto_increment primary key, uk int unique, v int check(v>0), index uk_abs((abs(uk))));
insert ignore into expression_case(uk,v) values (1,-1),(2,1),(2,1),(-3,1);
select id,uk from expression_case order by id;
select id,uk from expression_case force index(uk_abs) where abs(uk)=3;
select last_insert_id();

-- Prepared execution resets CHECK reservation state, including all-rejected runs.
create table prepared_case(id bigint auto_increment primary key, v int check(v>0)) auto_increment=124;
prepare check_stmt from 'insert ignore into prepared_case(v) values (?),(?)';
set @rejected=-1;
set @accepted=1;
execute check_stmt using @rejected,@accepted;
select id,v from prepared_case order by id;
execute check_stmt using @rejected,@rejected;
select last_insert_id();
execute check_stmt using @accepted,@accepted;
select id,v from prepared_case order by id;
select last_insert_id();
deallocate prepare check_stmt;

-- Current candidates may satisfy their own FK, without publishing rejected rows.
create table self_auto(id int auto_increment primary key, p int, v int check(v>0),
foreign key(p) references self_auto(id));
insert ignore into self_auto values (1,1,1),(2,1,1),(3,999,1);
select id,p from self_auto order by id;
truncate table self_auto;
insert ignore into self_auto values (NULL,1,1),(NULL,1,1);
select id,p from self_auto order by id;
truncate table self_auto;
insert ignore into self_auto values (1,1,-1),(2,1,1),(3,3,1),(4,3,1);
select id,p from self_auto order by id;

-- LOAD must enforce self references even when the table has no AUTO_INCREMENT.
create table self_load(id int primary key, p int, foreign key(p) references self_load(id));
load data inline format='csv', data='1,999' into table self_load fields terminated by ',';
select count(*) from self_load;
load data inline format='csv', data='1,999' ignore into table self_load fields terminated by ',';
select count(*) from self_load;
load data inline format='csv', data='1,1' ignore into table self_load fields terminated by ',';
insert ignore into self_load values (2,1),(3,999),(4,3);
select id,p from self_load order by id;

-- Changing FK checks must not make IGNORE scan unrelated existing orphan rows.
set foreign_key_checks=0;
load data inline format='csv', data='9,999' into table self_load fields terminated by ',';
set foreign_key_checks=1;
insert ignore into self_load values (5,5),(6,5);
select id,p from self_load order by id;

-- Native point keys without a scalar protobuf Literal carrier. Both self-FK
-- tuple binding and UNIQUE-index base-PK locators must preserve the exact type.
set @typed_parent=cast('00000000-0000-0000-0000-000000000001' as uuid);
set @typed_plain=cast('00000000-0000-0000-0000-000000000002' as uuid);
set @typed_accepted=cast('00000000-0000-0000-0000-000000000003' as uuid);
set @typed_missing=cast('00000000-0000-0000-0000-000000000099' as uuid);
create table typed_self_uuid(id uuid primary key,p uuid,foreign key(p) references typed_self_uuid(id));
insert into typed_self_uuid values(@typed_parent,NULL);
insert into typed_self_uuid values(@typed_plain,@typed_parent);
delete from typed_self_uuid where id=@typed_plain;
insert ignore into typed_self_uuid values(@typed_accepted,@typed_parent),(@typed_missing,@typed_plain);
select id,p from typed_self_uuid order by id;
create table typed_parent_uuid(id uuid primary key,k int unique);
insert into typed_parent_uuid values(@typed_parent,1),(@typed_plain,2);
create table typed_child_uuid(id int auto_increment primary key,u int unique,foreign key(id) references typed_parent_uuid(k));
insert into typed_child_uuid values(1,99);
truncate table typed_child_uuid;
insert ignore into typed_child_uuid(u) values(10),(10),(20),(30);
select id,u from typed_child_uuid order by id;

set @typed_parent=2020;
set @typed_plain=2021;
set @typed_accepted=2022;
set @typed_missing=2099;
create table typed_self_year(id year primary key,p year,foreign key(p) references typed_self_year(id));
insert into typed_self_year values(@typed_parent,NULL);
insert into typed_self_year values(@typed_plain,@typed_parent);
delete from typed_self_year where id=@typed_plain;
insert ignore into typed_self_year values(@typed_accepted,@typed_parent),(@typed_missing,@typed_plain);
select id,p from typed_self_year order by id;
create table typed_parent_year(id year primary key,k int unique);
insert into typed_parent_year values(@typed_parent,1),(@typed_plain,2);
create table typed_child_year(id int auto_increment primary key,u int unique,foreign key(id) references typed_parent_year(k));
insert into typed_child_year values(1,99);
truncate table typed_child_year;
insert ignore into typed_child_year(u) values(10),(10),(20),(30);
select id,u from typed_child_year order by id;

-- Values differ below Decimal128 precision; no float/string round-trip is valid.
set @typed_parent=cast('10000000000000000000000000000000000000000000000000000000000000001' as decimal(65,0));
set @typed_plain=cast('10000000000000000000000000000000000000000000000000000000000000002' as decimal(65,0));
set @typed_accepted=cast('10000000000000000000000000000000000000000000000000000000000000003' as decimal(65,0));
set @typed_missing=cast('10000000000000000000000000000000000000000000000000000000000000099' as decimal(65,0));
create table typed_self_decimal256(id decimal(65,0) primary key,p decimal(65,0),foreign key(p) references typed_self_decimal256(id));
insert into typed_self_decimal256 values(@typed_parent,NULL);
insert into typed_self_decimal256 values(@typed_plain,@typed_parent);
delete from typed_self_decimal256 where id=@typed_plain;
insert ignore into typed_self_decimal256 values(@typed_accepted,@typed_parent),(@typed_missing,@typed_plain);
select id,p from typed_self_decimal256 order by id;
create table typed_parent_decimal256(id decimal(65,0) primary key,k int unique);
insert into typed_parent_decimal256 values(@typed_parent,1),(@typed_plain,2);
create table typed_child_decimal256(id int auto_increment primary key,u int unique,foreign key(id) references typed_parent_decimal256(k));
insert into typed_child_decimal256 values(1,99);
truncate table typed_child_decimal256;
insert ignore into typed_child_decimal256(u) values(10),(10),(20),(30);
select id,u from typed_child_decimal256 order by id;

drop database auto_increment_ignore_constraints;
