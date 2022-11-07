--env prepare statement
drop table if exists time_01;
drop table if exists time_02;
drop table if exists time_03;
drop table if exists time_04;

-- time type format
create table time_01(t1 time,t2 time,t3 time);
insert into time_01 values("-838:59:59.0000","838:59:59.00","22:00:00");
insert into time_01 values("0:00:00.0000","0","0:00");
insert into time_01 values(null,NULL,null);
insert into time_01 values("23","1122","-1122");
insert into time_01 values("101412","4","-101219");
insert into time_01 values("24:59:09.932823","24:02:00.93282332424","24:20:34.00000000");
insert into time_01 values("2022-09-08 12:00:01","019","23403");
select * from time_01;

--invalid format
create table time_02(t1 time);
insert into time_02 values("200:60:60");
insert into time_02 values("1000:59:59");
insert into time_02 values("-1000:59:59");
insert into time_02 values("abc");
insert into time_02 values("中文");
insert into time_02 values(200);
-- @bvt:issue#6168
insert into time_02 values("");
select * from time_02;
-- @bvt:issue
-- filter and expression etc
insert into time_02 values("200:50:10");
select '-838:59:59.0000' from time_01 limit 1;
select '838:59:59.0000';
-- @bvt:issue#6168
select t1-t2,t1+t2 from time_01;
select t1+2,t1*2,t1/2,t1%2 from time_01;
-- @bvt:issue
select * from time_01 where t2 is not null;
select * from time_01 where t2 is null;
-- @bvt:issue#6168
select t1 from time_01 where t2>"23";
select t1 from time_01 where t1!="24:59:09.932823";
select * from time_01 where t1="24:59:09.932823" and t2>"24:01:00";
select * from time_01 where t2 between "23" and "24:59:09.932823";
select * from time_01 where t2 not between "23" and "24:59:09.932823";
select * from time_01 where t2 in("838:59:59.00","4");
select * from time_01 where t2 not in("838:59:59.00","4");
-- @bvt:issue
select count(t1) from time_01;
select count(t1),t2 from time_01 group by t2 order by t2;
select min(t1),max(t2) from time_01;
drop table time_02;
-- primary key default and compk
create table time_02 (t1 int,t2 time primary key,t3 varchar(25))partition by hash(t2)partitions 4;
create table time_03 (t1 int,t2 time primary key,t3 varchar(25));
-- @bvt:issue#6272
insert into time_03 values (30,"101412","yellow");
insert into time_03 values (40,"101412","apple");
select * from time_03;
-- @bvt:issue
drop table time_03;
create table time_03 (t1 int,t2 time,t3 varchar(25),t4 time default '110034',primary key(t1,t2));
insert into time_03(t1,t2,t3) values (30,"24:59:09.932823","yellow");
insert into time_03(t1,t2,t3) values (30,"24:59:09.932823","oooppppp");
insert into time_03(t1,t2,t3) values (31,"24:59:09.932823","postttttt");
insert into time_03(t1,t2,t3) values (32,NULL,"vinda");

--other function interactive
insert into time_03(t1,t2,t3) values (40,"37","gloooooooge");
select distinct t2 from time_03;
update time_03 set t4="220:00:00" where t1=30;
select * from time_03;
update time_03 set t4=NULL where t1=30;
select * from time_03;
delete from time_03 where t2 is not null;
select * from time_03;
insert into time_03 values (40,"37","gloooooooge","35:50");
truncate table time_03;
load data infile "$resources/external_table_file/time_ex_table.csv" into table time_03;
create external table time_ex_01(t1 int,t2 time,t3 varchar(25),t4 time)  infile{"filepath"='$resources/external_table_file/time_ex_table.csv'} fields terminated by ',' enclosed by '\"';
select * from time_ex_01;
select * from time_01 time1 join time_03 time3 on time1.t1=time3.t2;
select * from time_01 time1 left join time_03 time3 on time1.t1=time3.t2;
select * from time_01 time1 right join time_03 time3 on time1.t1=time3.t2;
select t1,t2 from time_01 union select t2,t4 from time_03;
select * from (select t1,t2 from time_01 intersect select t2,t4 from time_03) as t;
select t1,t2 from time_01 minus select t2,t4 from time_03;
select * from time_01 where t2 in (select t2 from time_03);
-- @bvt:issue#6168
select * from time_01 where t1 > (select t2 from time_03 where t1 = '10:14:12');
select t1,t2,t3 from time_01 where t1 < any(select t2 from time_03 where t1 = '10:14:12');
select t1,t2,t3 from time_01 where t1 >= all(select t2 from time_03 where t1 = '10:14:12');
select t1,t2,t3 from time_01 where t1 >= some(select t2 from time_03 where t1 = '10:14:12');
-- @bvt:issue
select * from time_01 where exists(select t2 from time_03);
select * from time_01 where not exists(select t2 from time_03);
create view time_view_01 as select * from time_01;
select * from time_view_01;

-- timediff
create table time_04 (t1 int,t2 time,t3 datetime,t4 timestamp);
insert into time_04 values (1,"344:59:09","2020-09-12","2021-09-22 10:01:23.903");
select * from time_04;
select timediff(t2,t3) from time_04;
-- @bvt:issue#6321
select timediff(t1,t2) from time_01;
-- @bvt:issue
select timediff(t1,NULL),timediff(NULL,t2)from time_01;
select timediff('20',NULL) , timediff('20','24:59:09.8453');
select timediff('12:00','24:59:09.8453'),timediff('-838:59:59.0000','-1122');
-- @bvt:issue#6321
select * from (select timediff(t1,t2) from time_01) as t;
-- @bvt:issue