drop database if exists fk_foreign_key_checks5;
create database fk_foreign_key_checks5;

drop database if exists fk_foreign_key_checks5_db0;
create database fk_foreign_key_checks5_db0;

drop database if exists fk_foreign_key_checks5_db1;
create database fk_foreign_key_checks5_db1;

create table fk_foreign_key_checks5_db0.t1(a int primary key);

create table fk_foreign_key_checks5_db1.t2(b int, constraint c1 foreign key (b) references fk_foreign_key_checks5_db0.t1(a));

--error
drop database fk_foreign_key_checks5_db0;

-- A rejected DROP inside a transaction must not retire a surviving table's
-- auto-increment state when the caller commits earlier successful work.
create table fk_foreign_key_checks5_db0.survivor(id int primary key auto_increment);
insert into fk_foreign_key_checks5_db0.survivor values (8);
set @survivor_id = (select rel_id from mo_catalog.mo_tables where account_id = 0 and relname = 'survivor' and reldatabase_id = (select dat_id from mo_catalog.mo_database where account_id = 0 and datname = 'fk_foreign_key_checks5_db0'));
begin;
insert into fk_foreign_key_checks5_db0.t1 values (1);
--error
drop database fk_foreign_key_checks5_db0;
commit;
select rel_id = @survivor_id as same_survivor from mo_catalog.mo_tables where account_id = 0 and rel_id = @survivor_id;
select a from fk_foreign_key_checks5_db0.t1;
insert into fk_foreign_key_checks5_db0.survivor values (null);
select id from fk_foreign_key_checks5_db0.survivor order by id;

drop database if exists fk_foreign_key_checks5_db2;
create database fk_foreign_key_checks5_db2;

create table fk_foreign_key_checks5_db2.t1(a int primary key);
create table fk_foreign_key_checks5_db2.t2(b int, constraint c1 foreign key (b) references fk_foreign_key_checks5_db2.t1(a));

--no error
drop database fk_foreign_key_checks5_db2;

drop table fk_foreign_key_checks5_db1.t2;

--no error
drop database fk_foreign_key_checks5_db0;

-- Checks disabled permits dropping a database referenced from another database.
create database fk_foreign_key_checks5_db0;
create table fk_foreign_key_checks5_db0.t1(a int primary key);
create table fk_foreign_key_checks5_db1.t2(b int, foreign key (b) references fk_foreign_key_checks5_db0.t1(a));
set foreign_key_checks=0;
drop database fk_foreign_key_checks5_db0;
set foreign_key_checks=1;
select count(*) as dropped_with_checks_off from mo_catalog.mo_database where account_id = 0 and datname = 'fk_foreign_key_checks5_db0';
drop table fk_foreign_key_checks5_db1.t2;

drop database if exists fk_foreign_key_checks5;
drop database if exists fk_foreign_key_checks5_db0;
drop database if exists fk_foreign_key_checks5_db1;
drop database if exists fk_foreign_key_checks5_db2;
