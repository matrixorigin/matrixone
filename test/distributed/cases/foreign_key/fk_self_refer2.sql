drop database if exists fk_self_refer2;
create database fk_self_refer2;
use fk_self_refer2;

drop table if exists t1;
----create two FKs on a primary key----
-- test only two FKs self refer in a table
create table t1(a int primary key,b int,c int,
                foreign key fk1(b) references t1(a),
                foreign key fk2(c) references t1(a)
);
show create table t1;

--should be error. 2,3 does no exists
insert into t1 values (1,2,3);

--no error
insert into t1 values (1,1,1);

--no error
insert into t1 values (2,2,1);

--no error
insert into t1 values (3,3,2);

--no error
insert into t1 values (4,3,1);

--error
insert into t1 values (5,6,1);

--no error
insert into t1 values (6,NULL,1);

--error
insert into t1 values (7,NULL,8);

--no error
insert into t1 values (8,NULL,NULL);

--error
delete from t1 where a = 1;

--error
delete from t1 where a = 2;

--error
delete from t1 where a = 3;

--no error
delete from t1 where a = 4;

--no error
delete from t1 where a = 6;

update t1 set b = 8 where a= 3;

--error
delete from t1 where a = 8;

--no error
delete from t1 where a = 3;

--no error
delete from t1 where a = 8;

-- error
update t1 set a = 3 where a = 2;

--no error
update t1 set b = NULL where a = 2;

--no error
delete from t1 where  a= 2;

--no error
update t1 set b = NULL where a = 1;

--error
delete from t1 where a = 1;

--no error
update t1 set c = NULL where a = 1;

--no error
delete from t1 where a = 1;

--0
select count(*) from t1;


drop table if exists t1;
----create two FKs on a unique key----
-- test only two FKs self refer in a table
create table t1(a int unique key,b int,c int,
                foreign key fk1(b) references t1(a),
                foreign key fk2(c) references t1(a)
);
show create table t1;

--no error
insert into t1 values (NULL,NULL,NULL);

--no error
insert into t1 values (1,1,1);

--no error
insert into t1 values (2,1,1);

--should no error. but mo report: ERROR 20101 (HY000): internal error: unexpected input batch for column expression
update t1 set a = NULL where a = 2;

--error
delete from t1 where c = 1;

drop database if exists fk_self_refer2;