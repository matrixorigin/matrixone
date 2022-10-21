
-- @suite
-- @setup
drop table if exists t1;
create table t1 (id int,ti tinyint unsigned,si smallint,bi bigint unsigned,fl float,dl double,de decimal,ch char(20),vch varchar(20),dd date,dt datetime);
insert into t1 values(1,1,4,3,1113.32,111332,1113.32,'hello','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(2,2,5,2,2252.05,225205,2252.05,'bye','sub query','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(3,6,6,3,3663.21,366321,3663.21,'hi','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(4,7,1,5,4715.22,471522,4715.22,'good morning','my subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(5,1,2,6,51.26,5126,51.26,'byebye',' is subquery?','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(6,3,2,1,632.1,6321,632.11,'good night','maybe subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(7,4,4,3,7443.11,744311,7443.11,'yes','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(8,7,5,8,8758.00,875800,8758.11,'nice to meet','just subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(9,8,4,9,9849.312,9849312,9849.312,'see you','subquery','2022-04-28','2022-04-28 22:40:11');

drop table if exists t2;
create table t2 (id int,ti tinyint unsigned,si smallint,bi bigint unsigned,fl float,dl double,de decimal,ch char(20),vch varchar(20),dd date,dt datetime);
insert into t2 values(1,1,4,3,1113.32,111332,1113.32,'hello','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(2,2,5,2,2252.05,225205,2252.05,'bye','sub query','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(3,6,6,3,3663.21,366321,3663.21,'hi','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(4,7,1,5,4715.22,471522,4715.22,'good morning','my subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(5,1,2,6,51.26,5126,51.26,'byebye',' is subquery?','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(6,3,2,1,632.1,6321,632.11,'good night','maybe subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(7,4,4,3,7443.11,744311,7443.11,'yes','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(8,7,5,8,8758.00,875800,8758.11,'nice to meet','just subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(9,8,4,9,9849.312,9849312,9849.312,'see you','subquery','2022-04-28','2022-04-28 22:40:11');

-- @case
-- @desc:test for from subquery
-- @label:bvt

select * from (select * from t1) sub where id > 4;
select ti as t,fl as f from (select * from t1) sub where dl <> 4;
select * from (select ti as t,fl as f from t1 where dl <> 4) sub;

select id,min(ti) from (select * from t1) sub group by id;
select * from (select id,min(ti) from (select * from t1) t1 group by id) sub;

create view v1 as select * from (select * from t1) sub where id > 4;
create view v2 as select ti as t,fl as f from (select * from t1) sub where dl <> 4;
create view v3 as select * from (select ti as t,fl as f from t1 where dl <> 4) sub;
create view v4 as select id,min(ti) from (select * from t1) sub group by id;
create view v5 as select * from (select id,min(ti) from (select * from t1) t1 group by id) sub;
select * from v1;
select * from v2;
select * from v3;
select * from v4;
select * from v5;

drop view v1;
drop view v2;
drop view v3;
drop view v4;
drop view v5;

--待确认
--select id,min(ti) from (select * from t1) sub order by id desc;
--select * from (select id,min(ti) from t1 sub order by id desc) sub;

select id,min(ti) from (select * from t1) sub group by id order by id desc;
select id,sum(ti) from (select * from t1) sub group by id;

select distinct(ti) from (select * from t1) sub;
select distinct(ti) from (select * from t1) sub where id <6;

create view v1 as select id,min(ti) from (select * from t1) sub group by id order by id desc;
create view v2 as select id,sum(ti) from (select * from t1) sub group by id;
create view v3 as select distinct(ti) from (select * from t1) sub;
create view v4 as select distinct(ti) from (select * from t1) sub where id <6;
select * from v1;
select * from v2;
select * from v3;
select * from v4;

drop view v1;
drop view v2;
drop view v3;
drop view v4;


-- mysql 不同，待确认
-- select distinct(ti),de from (select * from t1) sub where id < 6 order by ti asc;

select count(*) from (select * from t1) sub where id > 4 ;
select * from (select * from t1) sub where id > 1 limit 3;
select max(ti),min(si),avg(fl) from (select * from t1) sub where id < 4 || id > 5;
select max(ti)+10,min(si)-1,avg(fl) from (select * from t1) sub where id < 4 || id > 5;

select substr from (select * from t1) sub where id < 4 || id > 5;

select ti,-si from (select * from t1) sub order by -si desc;

select * from (select * from t1) sub where (ti=2 or si=3) and  (ch = 'bye' or vch = 'subquery');

select * from (select * from (select * from (select id,ti,si,de from (select * from t1 ) sub3 where fl <> 4.5 ) sub2 where ti > 1) sub1 where id >2 ) sub where id > 4;

select * from (select * from t1 where id > 100) sub ;

create view v1 as select count(*) from (select * from t1) sub where id > 4 ;
create view v2 as select * from (select * from t1) sub where id > 1 limit 3;
create view v3 as select max(ti),min(si),avg(fl) from (select * from t1) sub where id < 4 || id > 5;
create view v4 as select max(ti)+10,min(si)-1,avg(fl) from (select * from t1) sub where id < 4 || id > 5;
create view v5 as select substr from (select * from t1) sub where id < 4 || id > 5;
create view v6 as select ti,-si from (select * from t1) sub order by -si desc;
create view v7 as select * from (select * from t1) sub where (ti=2 or si=3) and  (ch = 'bye' or vch = 'subquery');
create view v8 as select * from (select * from (select * from (select id,ti,si,de from (select * from t1 ) sub3 where fl <> 4.5 ) sub2 where ti > 1) sub1 where id >2 ) sub where id > 4;
create view v9 as select * from (select * from t1 where id > 100) sub ;

select * from v1;
select * from v2;
select * from v3;
select * from v4;
select * from v5;
select * from v6;
select * from v7;
select * from v8;
select * from v9;

drop view v1;
drop view v2;
drop view v3;
drop view v4;
drop view v5;
drop view v6;
drop view v7;
drop view v8;
drop view v9;


-- @suite
-- @setup
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1 (libname1 varchar(21) not null primary key, city varchar(20));
create table t2 (isbn2 varchar(21) not null primary key, author varchar(20), title varchar(60));
create table t3 (isbn3 varchar(21) not null, libname3 varchar(21) not null, quantity int);
insert into t2 values ('001','Daffy','Aducklife');
insert into t2 values ('002','Bugs','Arabbitlife');
insert into t2 values ('003','Cowboy','Lifeontherange');
insert into t2 values ('000','Anonymous','Wannabuythisbook?');
insert into t2 values ('004','BestSeller','OneHeckuvabook');
insert into t2 values ('005','EveryoneBuys','Thisverybook');
insert into t2 values ('006','SanFran','Itisasanfranlifestyle');
insert into t2 values ('007','BerkAuthor','Cool.Berkley.the.book');
insert into t3 values('000','NewYorkPublicLibra',1);
insert into t3 values('001','NewYorkPublicLibra',2);
insert into t3 values('002','NewYorkPublicLibra',3);
insert into t3 values('003','NewYorkPublicLibra',4);
insert into t3 values('004','NewYorkPublicLibra',5);
insert into t3 values('005','NewYorkPublicLibra',6);
insert into t3 values('006','SanFransiscoPublic',5);
insert into t3 values('007','BerkeleyPublic1',3);
insert into t3 values('007','BerkeleyPublic2',3);
insert into t3 values('001','NYC Lib',8);
insert into t1 values ('NewYorkPublicLibra','NewYork');
insert into t1 values ('SanFransiscoPublic','SanFran');
insert into t1 values ('BerkeleyPublic1','Berkeley');
insert into t1 values ('BerkeleyPublic2','Berkeley');
insert into t1 values ('NYCLib','NewYork');
-- @case
-- @desc:test for from subquery with join
-- @label:bvt
select * from (select city,libname1,count(libname1) as a from t3 join t1 on libname1=libname3 join t2 on isbn3=isbn2 group by city,libname1) sub ;
create view v1 as select * from (select city,libname1,count(libname1) as a from t3 join t1 on libname1=libname3 join t2 on isbn3=isbn2 group by city,libname1) sub ;
select * from v1;
drop view v1;
