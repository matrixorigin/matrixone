drop database if exists issue32003;
create database issue32003;
use issue32003;

create table m1 (k int primary key, v varchar(10));
insert into m1 values (1,'a'),(2,'b');
data branch create table m2 from m1;
insert into m2 values (3,'new');
update m2 set v='mod' where k=1;
data branch merge m2 into m1;
select k, v from m1 order by k;

delete from m1;
insert into m1 values (1,'x');
insert into m2 values (5,'postmerge');
data branch pick m2 into m1 keys (5);
select k, v from m1 order by k;
data branch pick m2 into m1 keys (3);
select k, v from m1 order by k;
data branch diff m2 against m1;

data branch delete table m2;

create table fake_dst (v varchar(10));
insert into fake_dst values ('padding'),('a'),('a');
delete from fake_dst where v='padding';
data branch create table fake_src from fake_dst;
truncate table fake_src;
truncate table fake_dst;
insert into fake_dst values ('a'),('a'),('a');
data branch merge fake_src into fake_dst;
select count(*) as remaining_copies from fake_dst where v='a';
data branch delete table fake_src;

create table mixed_dst (k int primary key, v varchar(10));
insert into mixed_dst values (1,'old'),(2,'old');
data branch create table mixed_src from mixed_dst;
delete from mixed_dst where k=1;
truncate table mixed_src;
insert into mixed_src values (1,'new'),(2,'new');
data branch pick mixed_src into mixed_dst keys(1,2) when conflict accept;
select k,v from mixed_dst order by k;

drop snapshot if exists issue32003_late_lo;
drop snapshot if exists issue32003_late_hi;
create table late_dst (k int primary key, v varchar(10));
insert into late_dst values (1,'old');
data branch create table late_src from late_dst;
create snapshot issue32003_late_lo for account sys;
truncate table late_src;
insert into late_src values (1,'new'),(2,'added');
create snapshot issue32003_late_hi for account sys;
alter table late_src add column note varchar(10) default 'default';
alter table late_dst add column note varchar(10) default 'default';
update late_dst set note='local' where k=1;
data branch pick late_src into late_dst between snapshot issue32003_late_lo and issue32003_late_hi when conflict accept;
select k,v,note from late_dst order by k;
drop snapshot issue32003_late_hi;
drop snapshot issue32003_late_lo;

create table dynamic_dst (k int primary key, v varchar(10));
insert into dynamic_dst values (1,'same');
data branch create table dynamic_src from dynamic_dst;
alter table dynamic_src algorithm=copy, add column ts timestamp(6) default current_timestamp(6);
alter table dynamic_dst algorithm=copy, add column ts timestamp(6) default current_timestamp(6);
truncate table dynamic_src;
insert into dynamic_src values (1,'same','2020-01-01 00:00:00');
-- @regex("DATA BRANCH cannot establish historical value for column", true)
data branch diff dynamic_src against dynamic_dst;
select k,v from dynamic_dst order by k;

-- A visible deletion cancels regardless of which hidden key was deleted.
create table bags_dst (v int);
insert into bags_dst values (8),(8),(8);
data branch create table bags_src from bags_dst;
truncate table bags_src;
insert into bags_src values (8),(8);
delete from bags_dst where v=8 limit 1;
data branch merge bags_src into bags_dst when conflict accept;
select v,count(*) as copies from bags_dst group by v;
-- A remaining deletion must use a live destination key, not an ancestor key.
create table fewer_dst (v int);
insert into fewer_dst values (8),(8),(8),(9);
data branch create table fewer_src from fewer_dst;
truncate table fewer_src;
insert into fewer_src values (8),(9);
delete from fewer_dst where v=8 limit 1;
data branch merge fewer_src into fewer_dst when conflict accept;
select v,count(*) as copies from fewer_dst group by v order by v;

create table fork_src (k int primary key, v int);
insert into fork_src values (1,10);
create snapshot issue32003_fork_lo for account sys;
insert into fork_src values (2,20);
data branch create table fork_dst from fork_src;
truncate table fork_src;
create snapshot issue32003_fork_hi for account sys;
data branch pick fork_src into fork_dst between snapshot issue32003_fork_lo and issue32003_fork_hi when conflict accept;
select count(*) as remaining from fork_dst;
drop snapshot issue32003_fork_hi;
drop snapshot issue32003_fork_lo;

create table upper_dst (k int primary key, v int);
data branch create table upper_src from upper_dst;
create snapshot issue32003_upper_lo for account sys;
truncate table upper_src;
insert into upper_src values (1,10);
create snapshot issue32003_upper_hi for account sys;
alter table upper_src add column note int;
alter table upper_dst add column note int;
insert into upper_dst values (1,10,42);
data branch pick upper_src into upper_dst between snapshot issue32003_upper_lo and issue32003_upper_hi when conflict fail;
select k,v,note from upper_dst order by k;
drop snapshot issue32003_upper_hi;
drop snapshot issue32003_upper_lo;

create table reinsert_root (k int primary key, v int);
insert into reinsert_root values (1,10);
data branch create table reinsert_clear from reinsert_root;
data branch create table reinsert_dml from reinsert_root;
data branch create table reinsert_dst from reinsert_root;
delete from reinsert_clear where k=1;
delete from reinsert_dml where k=1;
create snapshot issue32003_reinsert_lo for account sys;
truncate table reinsert_clear;
insert into reinsert_clear values (1,99);
insert into reinsert_dml values (1,99);
create snapshot issue32003_reinsert_hi for account sys;
update reinsert_dst set v=42 where k=1;
-- @regex("conflict: reinsert_clear INSERT and reinsert_dst INSERT", true)
data branch pick reinsert_clear into reinsert_dst between snapshot issue32003_reinsert_lo and issue32003_reinsert_hi when conflict fail;
select k,v from reinsert_dst order by k;
data branch pick reinsert_clear into reinsert_dst between snapshot issue32003_reinsert_lo and issue32003_reinsert_hi when conflict skip;
select k,v from reinsert_dst order by k;
data branch pick reinsert_clear into reinsert_dst between snapshot issue32003_reinsert_lo and issue32003_reinsert_hi when conflict accept;
select k,v from reinsert_dst order by k;
update reinsert_dst set v=42 where k=1;
data branch pick reinsert_dml into reinsert_dst between snapshot issue32003_reinsert_lo and issue32003_reinsert_hi when conflict skip;
select k,v from reinsert_dst order by k;
data branch pick reinsert_dml into reinsert_dst between snapshot issue32003_reinsert_lo and issue32003_reinsert_hi when conflict accept;
select k,v from reinsert_dst order by k;
drop snapshot issue32003_reinsert_hi;
drop snapshot issue32003_reinsert_lo;

-- Resolve column identity at the fork snapshot, not at the end of the physical generation.
create table swap_src (k int primary key, a int, b int);
insert into swap_src values (1,10,20),(2,30,40);
data branch create table swap_dst from swap_src;
alter table swap_src rename column a to tmpname;
alter table swap_src rename column b to a;
alter table swap_src rename column tmpname to b;
alter table swap_dst rename column a to tmpname;
alter table swap_dst rename column b to a;
alter table swap_dst rename column tmpname to b;
truncate table swap_src;
insert into swap_src(k,a,b) values (1,10,20),(2,40,30);
data branch diff swap_src against swap_dst;
data branch merge swap_src into swap_dst when conflict accept;
select k,a,b from swap_dst order by k;
create table rename_src (k int primary key, v int);
insert into rename_src values (1,10),(2,20);
data branch create table rename_dst from rename_src;
alter table rename_src rename column v to w;
truncate table rename_src;
insert into rename_src values (1,11);
data branch merge rename_src into rename_dst when conflict accept;
select k,v from rename_dst order by k;

-- Keep default introduction metadata at each sibling's physical fork.
create table layer_root (k int primary key, v int);
insert into layer_root values (1,10);
data branch create table layer_dst from layer_root;
truncate table layer_root;
insert into layer_root values (1,10);
data branch create table layer_src from layer_root;
alter table layer_src add column x int default 0, algorithm=copy;
alter table layer_dst add column x int default 0, algorithm=copy;
truncate table layer_src;
insert into layer_src values (1,10,33);
data branch pick layer_src into layer_dst keys(1) when conflict accept;
select k,v,x from layer_dst order by k;

-- An ephemeral intermediate relation does not hide a later clear boundary.
create table zero_root (k int primary key, v int);
insert into zero_root values (1,10),(2,20);
data branch create table zero_dst from zero_root;
begin;
data branch create table zero_mid from zero_root;
data branch create table zero_src from zero_mid;
drop table zero_mid;
commit;
select k,v from zero_src order by k;
create snapshot issue32003_zero_lo for account sys;
truncate table zero_src;
create snapshot issue32003_zero_hi for account sys;
data branch pick zero_src into zero_dst between snapshot issue32003_zero_lo and issue32003_zero_hi keys(1) when conflict accept;
select k,v from zero_dst order by k;
drop snapshot issue32003_zero_hi;
drop snapshot issue32003_zero_lo;

-- A bounded reinsert conflicts with a target deletion, not with an equal live row.
create table key_root (k int primary key, v int);
insert into key_root values (1,10),(2,20),(3,30);
data branch create table key_src from key_root;
data branch create table key_dst from key_root;
data branch create table key_clear from key_root;
delete from key_src where k>0;
create snapshot issue32003_key_lo for account sys;
truncate table key_src;
insert into key_src values (1,11),(2,22),(3,30),(4,40);
create snapshot issue32003_key_hi for account sys;
delete from key_dst where k=1;
truncate table key_clear;
-- @regex("conflict: key_src INSERT and key_dst INSERT", true)
data branch pick key_src into key_dst between snapshot issue32003_key_lo and issue32003_key_hi keys(1) when conflict fail;
data branch pick key_src into key_dst between snapshot issue32003_key_lo and issue32003_key_hi keys(1) when conflict skip;
select k,v from key_dst order by k;
data branch pick key_src into key_dst between snapshot issue32003_key_lo and issue32003_key_hi keys(1) when conflict accept;
-- @regex("conflict: key_src INSERT and key_dst INSERT", true)
data branch pick key_src into key_dst between snapshot issue32003_key_lo and issue32003_key_hi keys(2) when conflict fail;
data branch pick key_src into key_dst between snapshot issue32003_key_lo and issue32003_key_hi keys(2) when conflict skip;
select k,v from key_dst order by k;
data branch pick key_src into key_dst between snapshot issue32003_key_lo and issue32003_key_hi keys(2) when conflict accept;
data branch pick key_src into key_dst between snapshot issue32003_key_lo and issue32003_key_hi keys(3,4) when conflict fail;
select k,v from key_dst order by k;
-- @regex("conflict: key_src INSERT and key_clear INSERT", true)
data branch pick key_src into key_clear between snapshot issue32003_key_lo and issue32003_key_hi keys(1,4) when conflict fail;
select k,v from key_clear order by k;
data branch pick key_src into key_clear between snapshot issue32003_key_lo and issue32003_key_hi keys(1,4) when conflict skip;
select k,v from key_clear order by k;
data branch pick key_src into key_clear between snapshot issue32003_key_lo and issue32003_key_hi keys(1) when conflict accept;
select k,v from key_clear order by k;
drop snapshot issue32003_key_hi;
drop snapshot issue32003_key_lo;

-- Keep deletion evidence for a row inherited at a later target fork.
create table later_root (k int primary key, v int);
data branch create table later_src from later_root;
insert into later_src values (1,15);
delete from later_src where k=1;
insert into later_root values (1,10);
data branch create table later_dst from later_root;
insert into later_dst values (2,21);
delete from later_dst where k>0;
create snapshot issue32003_later_lo for account sys;
truncate table later_src;
insert into later_src values (1,20),(2,22);
create snapshot issue32003_later_hi for account sys;
data branch pick later_src into later_dst between snapshot issue32003_later_lo and issue32003_later_hi when conflict skip;
select k,v from later_dst order by k;
data branch pick later_src into later_dst between snapshot issue32003_later_lo and issue32003_later_hi when conflict accept;
select k,v from later_dst order by k;
drop snapshot issue32003_later_hi;
drop snapshot issue32003_later_lo;

-- Later ON UPDATE columns retain their values on updates, but use defaults on inserts.
create table touch_root (k int primary key, v int, color enum('red','green','blue') unique, tag enum('old','new'));
insert into touch_root values (1,10,'red','old'),(2,20,'green','old');
data branch create table touch_src from touch_root;
data branch create table touch_dst from touch_root;
create snapshot issue32003_touch_lo for account sys;
truncate table touch_src;
insert into touch_src values (1,11,'red','new'),(2,21,'blue','new'),(3,31,'green','new');
create snapshot issue32003_touch_hi for account sys;
alter table touch_src add column touched timestamp not null default '2000-01-01 00:00:00' on update current_timestamp;
alter table touch_dst add column touched timestamp not null default '2000-01-01 00:00:00' on update current_timestamp;
update touch_dst set touched='2001-01-01 00:00:00' where k in (1,2);
data branch pick touch_src into touch_dst between snapshot issue32003_touch_lo and issue32003_touch_hi when conflict accept;
select k,v from touch_dst where tag='new' and color=case k when 1 then 'red' when 2 then 'blue' when 3 then 'green' end order by k;
select k from touch_dst where (k in (1,2) and touched='2001-01-01 00:00:00') or (k=3 and touched='2000-01-01 00:00:00') order by k;
drop snapshot issue32003_touch_hi;
drop snapshot issue32003_touch_lo;
drop database issue32003;
