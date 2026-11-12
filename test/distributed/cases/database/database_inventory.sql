drop database if exists inventory_bvt;
create database inventory_bvt;
use inventory_bvt;
create table assertions(id int primary key, ok int);
create table payload(id int primary key, v int);
set @inventory_id = (select dat_id from mo_catalog.mo_database where account_id=current_account_id() and datname='inventory_bvt');
set @inventory_epoch = (select epoch from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id);

-- Application DML must not advance the directory token.
insert into payload values(1,10);
insert into assertions select 1, count(*)=1 and min(epoch)>0 and min(epoch)=@inventory_epoch from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id;

-- A real directory change advances the token; an IF NOT EXISTS no-op does not.
create table added(id int primary key);
insert into assertions select 2, epoch>@inventory_epoch from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id;
set @inventory_epoch = (select epoch from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id);
create table if not exists added(id int primary key);
insert into assertions select 3, epoch=@inventory_epoch from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id;

-- Directory writes and schema roll back together.
begin;
alter table payload add column rolled_back int default 7;
create table rollback_only(id int primary key);
rollback;
insert into assertions select 4, epoch=@inventory_epoch from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id;
insert into assertions select 5, count(*)=0 from information_schema.columns where table_schema='inventory_bvt' and table_name='payload' and column_name='rolled_back';
insert into assertions select 6, count(*)=0 from information_schema.tables where table_schema='inventory_bvt' and table_name='rollback_only';

-- Retirement is in the same owner, not a post-response catalog transaction.
begin;
drop database inventory_bvt;
rollback;
use inventory_bvt;
insert into assertions select 7, epoch=@inventory_epoch from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id;
insert into assertions select 8, count(*)=1 and min(v)=10 from payload;
select id,ok from assertions order by id;

-- COPY ALTER must publish and fence private auto-increment metadata.
create table copy_auto(id bigint primary key auto_increment, v int);
insert into copy_auto(v) values(3);
set @copy_epoch = (select epoch from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id);
alter table copy_auto add column marker int default 7, algorithm=copy;
insert into copy_auto(v) values(4);
insert into assertions select 11, count(*)=2 and count(distinct id)=2 and sum(v)=7 and min(marker)=7 and max(marker)=7 from copy_auto;
insert into assertions select 12, epoch>@copy_epoch from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id;

-- The same fencing rule applies to physical partition copies.
create table copy_partition(id int primary key, v int) partition by hash(id) partitions 2;
insert into copy_partition values(1,10),(2,20);
set @copy_epoch = (select epoch from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id);
alter table copy_partition add column marker int default 7, algorithm=copy;
insert into assertions select 13, count(*)=2 and sum(v)=30 and min(marker)=7 and max(marker)=7 from copy_partition;
insert into assertions select 14, epoch>@copy_epoch from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id;
select id,ok from assertions where id>=11 order by id;

-- A committed replacement has a new identity and cannot accept the old token.
drop database inventory_bvt;
create database inventory_bvt;
use inventory_bvt;
create table assertions(id int primary key, ok int);
insert into assertions select 9, count(*)=0 from mo_catalog.mo_database_inventory where account_id=current_account_id() and database_id=@inventory_id;
insert into assertions select 10, count(*)=1 and min(d.dat_id)<>@inventory_id and min(i.epoch)>0 from mo_catalog.mo_database d join mo_catalog.mo_database_inventory i on i.account_id=d.account_id and i.database_id=d.dat_id where d.account_id=current_account_id() and d.datname='inventory_bvt';
select id,ok from assertions order by id;
drop database inventory_bvt;
