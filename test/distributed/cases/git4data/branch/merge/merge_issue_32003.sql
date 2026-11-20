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
drop database issue32003;
