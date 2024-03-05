drop database if exists fk_self_refer3;
create database fk_self_refer3;
use fk_self_refer3;

drop table if exists t1;
----create two FKs on a unique key and a primary key----
-- test only two FKs self refer in a table
create table t1(a int, b int,
                c int,
                d int, e int,
                f int, g int,
                primary key (a,b),
                unique key (a,c),
                constraint `c1` foreign key fk1(d,e) references t1(a,b),
                constraint `c2` foreign key fk2(f,g) references t1(a,c)
);
show create table t1;

--no error
insert into t1 values (1,2,1,1,2,1,1);

--no error
insert into t1 values (1,3,2,1,3,1,2);

--error. 5 does not exist in the column b
insert into t1 values (1,4,3,1,5,1,2);

--error. 4 does not exist in the column c
insert into t1 values (1,4,3,1,4,1,4);

--no error
insert into t1 values (1,4,3,1,4,1,3);

--error
update t1 set b = 5 where b = 4;

--error. 5 does not exist in the column b
update t1 set e = 5 where b = 4;

update t1 set e = NULL where b = 4;

--error. duplicate 3 in the column b
--update t1 set b = 3 where b = 4;

--no error.
update t1 set b = 5 where b = 4;

--error.
update t1 set c = 4 where b = 5;

--error
update t1 set g = 4 where b = 5;

--no error
update t1 set g = 2 where b = 5;

--error. duplicate 2 in the column c
--update t1 set c = 2 where b = 5;

--no error.
update t1 set c = 4 where b = 5;

--no error.
delete from t1 where b = 5;

--error
delete from t1 where b = 3;

update t1 set e = NULL where b = 3;

--error
delete from t1 where b = 3;

update t1 set g = NULL where b = 3;

--no error
delete from t1 where b = 3;

--error
delete from t1 where b = 2;

update t1 set e = NULL where b = 2;

--error
delete from t1 where b = 2;

update t1 set g = NULL where b = 2;

delete from t1 where b = 2;

select count(*) from t1;

drop table if exists p1;
create table p1(
    pa int,
    pb int,
    primary key (pa,pb)
);

drop table if exists q2;
create table q2(
   qa int,
   qb int,
   unique key (qa,qb)
);

drop table if exists t1;
----create two FKs on a unique key and a primary key----
----create tow FKs on two parent tables.
-- test only two FKs self refer in a table
create table t1(a int, b int,
                c int,
                d int, e int,
                f int, g int,
                h int, i int,
                j int, k int,
                primary key (a,b),
                unique key (a,c),
                constraint `c1` foreign key fk1(d,e) references t1(a,b),
                constraint `c2` foreign key fk2(f,h) references t1(a,c),
                constraint `c3` foreign key fk3(h,i) references p1(pa,pb),
                constraint `c4` foreign key fk4(h,k) references q2(qa,qb)
);
show create table t1;

--error. nothing parent tables
insert into t1 values (1,2,3,1,2,1,3,4,4,4,4);

insert into p1 values (4,4);

insert into q2 values (4,4);

--no error
insert into t1 values ( 1,2, 4, 1,2, 1,10, 4,4, 10,4);

--no error
insert into t1 values (1,3,3,1,2,1,10,NULL,NULL,NULL,4);

--error
insert into t1 values (
                              1,4,5,
                              1,3,
                              1,5,
                              5,5,
                              10,5);

insert into p1 values (5,5);

insert into q2 values (5,5);

--no error
insert into t1 values (
                          1,4,5,
                          1,3,
                          1,5,
                          5,5,
                          10,5);

--error. duplicate
--update t1 set c = 4 where b = 3;

--no error
update t1 set c = 6 where b = 3;

--no error
update t1 set c = NULL where b = 3;

--error
delete from t1 where c = 4;

--error
delete from t1 where c = 4;

--no error
update t1 set h = NULL where c = 4;

--error
delete from t1 where c = 4;

--no error
update t1 set d = NULL where c = 4;

--error
delete from t1 where c = 4;

--no error
update t1 set f = NULL,g = NULL where c = 4;

--error
delete from t1 where c = 4;

--no error
update t1 set i = NULL, j = NULL, k = NULL where c = 4;

--error
delete from t1 where c = 4;


update t1 set c = NULL where b = 2;

--error
delete from t1 where b = 2;


---delete or update rows on fk self table
drop table if exists p1;
create table t1(a int primary key,b int,constraint `c1` foreign key fk1(b) references t1(a));
show tables;
show create table t1;
insert into t1 values (1,1);
insert into t1 values (2,1);
insert into t1 values (3,2);

--error
delete A from t1 as A,  t1 as B where A.a = B.b;

--error
delete A,B from t1 as A,  t1 as B where A.a = B.b;

--error
update t1 as A,t1 as B set A.a = 4 where A.a = B.b;

--error
update t1 as A,t1 as B set A.a = 4, B.b = 3 where A.a = B.b;

--error
update t1 as A,t1 as B set A.a = 4, A.b = 3 where A.a = B.b;

--error
update t1 as A,t1 as B set B.a = 4 where A.a = B.b;

--no error
update t1 as A,t1 as B set A.a = 4 where A.a = 3;

--error
update t1 as A set A.a = 3, A.b = 3 where A.a = A.b;

--no error
insert into t1 values (3,3);

--error
update t1 as A set A.a = 4, A.b = 4 where A.a = A.b and A.a = 3;

--error
update t1 as A set A.b = 4, A.a = 4 where A.a = A.b and A.a = 3;

drop table if exists t1;
drop database if exists fk_self_refer3;