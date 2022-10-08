#starsWith,endsWith函数在MySQL里没有，是模仿Clickhouse语义的函数，只能在MO里测试

#SELECT,data type
drop table if exists t1;
create table t1(a int,b varchar(100),c char(20));
insert into t1 values
(1,'Ananya Majumdar', 'IX'),
(2,'Anushka Samanta', 'X'),
(3,'Aniket Sharma', 'XI'),
(4,'Anik Das', 'X'),
(5,'Riya Jain', 'IX'),
(6,'Tapan Samanta', 'X');
select a,startswith(b,'An') from t1;
select a,b,c from t1 where startswith(b,'An')=1 and startswith(c,'I')=1;
drop table t1;

drop table if exists t1;
create table t1(a int,b varchar(100),c char(20));
insert into t1 values
(1,'Ananya Majumdar', 'XI'),
(2,'Anushka Samanta', 'X'),
(3,'Aniket Sharma', 'XI'),
(4,'Anik Das', 'X'),
(5,'Riya Jain', 'IX'),
(6,'Tapan Samanta', 'XI');
select a,endsWith(b,'a') from t1;
select a,b,c from t1 where endswith(b,'a')=1 and endswith(c,'I')=1;
drop table t1;


#NULL,data type
drop table if exists t1;
create table t1(a varchar(255),b char(255));
insert into t1 values(NULL, NULL);
insert into t1 values('a',null);
insert into t1 values(null, 'b');
select startswith(a,b) from t1;
drop table t1;

drop table if exists t1;
create table t1(a varchar(255),b char(255));
insert into t1 values(NULL, NULL);
insert into t1 values('a',null);
insert into t1 values(null, 'b');
select endsWith(a,b) from t1;
drop table t1;

select endsWith('a', 'a') from dual;
select endsWith('a', 'b') from dual;
select endsWith('aaaa', 'a') from dual;
select endsWith('xsdfsdf', 'fsdf') from dual;
select endsWith('xsdfsdf', 'fsdfx') from dual;


