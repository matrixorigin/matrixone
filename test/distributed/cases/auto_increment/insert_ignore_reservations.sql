drop database if exists auto_increment_ignore_reservations;
create database auto_increment_ignore_reservations;
use auto_increment_ignore_reservations;

-- Unknown input length uses growing reservations; CHECK runs before allocation.
create table src(n int primary key,v int);
insert into src values(1,-1),(2,1),(3,2),(4,3),(5,4);
create table bulk_check(id bigint auto_increment primary key,v int check(v>0));
insert ignore into bulk_check(v) select v from src order by n;
select id,v from bulk_check order by id;
insert into bulk_check(v) values(99);
select id from bulk_check where v=99;
truncate table bulk_check;
insert ignore into bulk_check(v) select -1 from src order by n;
insert into bulk_check(v) values(99);
select id from bulk_check;

-- Duplicate rejections repeatedly use one candidate, including a trailing batch.
create table bulk_unique(id bigint auto_increment primary key,v int unique);
insert ignore into bulk_unique(v) select 1 from src order by n;
insert into bulk_unique(v) values(2);
select id,v from bulk_unique order by id;

-- LOAD IGNORE and LOCAL enforce CHECK and use the bulk reservation policy.
create table load_check(id bigint auto_increment primary key,v int check(v>0));
load data infile '$resources/auto_increment/ignore_check.csv' ignore into table load_check fields terminated by ',' lines terminated by '\n' (v);
select id,v from load_check order by id;
insert into load_check(v) values(3);
select id,v from load_check order by id;
truncate table load_check;
load data local infile '$resources/auto_increment/ignore_check.csv' into table load_check fields terminated by ',' lines terminated by '\n' (v);
select id,v from load_check order by id;

-- Rejected explicit IDs do not advance the allocator, even at the type limit.
create table explicit_uk(id tinyint auto_increment primary key,uk int unique);
insert into explicit_uk values(1,1);
insert ignore into explicit_uk values(127,1),(NULL,2),(NULL,3);
select id,uk from explicit_uk order by id;
select last_insert_id();
insert into explicit_uk(uk) values(4);
select id,uk from explicit_uk order by id;

-- Accepted explicit values fence only subsequent candidates.
create table mixed_ids(id bigint auto_increment primary key,uk int unique);
insert ignore into mixed_ids values(NULL,1),(100,2),(NULL,3);
insert into mixed_ids(uk) values(4);
select id,uk from mixed_ids order by uk;
truncate table mixed_ids;
insert ignore into mixed_ids values(100,1),(NULL,2),(NULL,3);
insert into mixed_ids(uk) values(4);
select id,uk from mixed_ids order by uk;

create table parents(id bigint primary key);
insert into parents values(1);
create table explicit_fk(id bigint auto_increment primary key,p bigint,foreign key(p) references parents(id));
insert ignore into explicit_fk values(100,9),(NULL,1);
select id,p from explicit_fk;

-- A foreign key on the AUTO_INCREMENT column probes the final candidate.
create table candidate_parents(id bigint primary key);
insert into candidate_parents values(1);
create table candidate_fk(id bigint auto_increment primary key,v int,foreign key(id) references candidate_parents(id));
insert ignore into candidate_fk(v) values(11),(12),(13);
select id,v from candidate_fk;
truncate table candidate_fk;
delete from candidate_parents where id=1;
insert into candidate_parents values(2);
insert ignore into candidate_fk(v) values(11),(12),(13);
select count(*) from candidate_fk;
insert into candidate_parents values(4);
insert ignore into candidate_fk(v) values(14);
select id,v from candidate_fk;

-- Candidate-dependent composite FK must recheck the other child columns.
create table composite_parent(id bigint,p int,primary key(id,p));
insert into composite_parent values(1,2),(2,3);
create table composite_child(id bigint auto_increment primary key,p int,foreign key(id,p) references composite_parent(id,p));
insert ignore into composite_child(p) values(9),(2),(3);
select id,p from composite_child order by id;

-- Type saturation keeps unused reservation separate from a real terminal value.
create table near_terminal(id tinyint auto_increment primary key,v int check(v>0)) auto_increment=124;
prepare near_stmt from 'insert ignore into near_terminal(v) values (?),(?)';
set @bad=-1;
set @good=1;
execute near_stmt using @bad,@good;
execute near_stmt using @bad,@bad;
execute near_stmt using @good,@good;
select id,v from near_terminal;
deallocate prepare near_stmt;
create table terminal_pair(id tinyint auto_increment primary key,v int unique) auto_increment=126;
insert ignore into terminal_pair(v) values(1),(2);
select id,v from terminal_pair order by id;
create table unsigned_tail(id bigint unsigned auto_increment primary key,v int check(v>0)) auto_increment=18446744073709551610;
insert ignore into unsigned_tail(v) values(1),(-1),(-1),(-1),(-1),(-1),(-1),(-1),(-1),(-1),(-1);
insert into unsigned_tail(v) values(2);
select id,v from unsigned_tail;

-- Redundant unique keys still publish final generated primary keys.
create table redundant(id int auto_increment primary key,u int unique,v int check(v>0),unique key id_u(id,u));
insert ignore into redundant(u,v) values(1,-1),(2,1),(2,1),(3,1);
select id,u from redundant order by id;
select id,u from redundant force index(id_u) where id=2;
-- Self references see existing and earlier accepted parents, not the current row.
create table self_fk(id int auto_increment primary key,u int unique,foreign key(id) references self_fk(u));
set foreign_key_checks=0;
insert into self_fk values(10,11);
set foreign_key_checks=1;
insert ignore into self_fk(u) values(11),(12),(13),(20),(30);
select id,u from self_fk order by id;
select last_insert_id();
create table self_null(id int auto_increment primary key,p int,v int check(v>0),foreign key(p) references self_null(id));
insert ignore into self_null(p,v) values(null,1),(1,1),(9,1),(2,1);
select id,p from self_null order by id;

drop database auto_increment_ignore_reservations;
