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
drop database issue32003;
