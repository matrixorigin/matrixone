-- test for from subquery with join
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create temporary table t1 (libname1 varchar(21) not null primary key, city varchar(20));
create temporary table t2 (isbn2 varchar(21) not null primary key, author varchar(20), title varchar(60));
create temporary table t3 (isbn3 varchar(21) not null, libname3 varchar(21) not null, quantity int);
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
select * from (select city,libname1,count(libname1) as a from t3 join t1 on libname1=libname3 join t2 on isbn3=isbn2 group by city,libname1) sub ;

-- test auto_increment as primary key
drop table if exists t1;
create temporary table t1(
a bigint primary key auto_increment,
b varchar(10)
);
insert into t1(b) values ('bbb');
insert into t1 values (1, 'ccc');
insert into t1 values (3, 'ccc');
insert into t1(b) values ('bbb1111');
select * from t1 order by a;

-- test keyword distinct
drop table if exists t1;
create temporary table t1(
a int,
b varchar(10)
);
insert into t1 values (111, 'a'),(110, 'a'),(100, 'a'),(000, 'b'),(001, 'b'),(011,'b');
select distinct b from t1;
select distinct b, a from t1;
select count(distinct a) from t1;
select sum(distinct a) from t1;
select avg(distinct a) from t1;
select min(distinct a) from t1;
select max(distinct a) from t1;
drop table t1;