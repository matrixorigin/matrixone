drop database if exists fk_self_refer2;
create database fk_self_refer2;
use fk_self_refer2;

drop table if exists t1;
----create two FKs on a primary key----
-- test only two FKs self refer in a table
create table t1(a int primary key,b int,c int,
                constraint `c1` foreign key fk1(b) references t1(a),
                constraint `c2` foreign key fk2(c) references t1(a)
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
                constraint `c1` foreign key fk1(b) references t1(a),
                constraint `c2` foreign key fk2(c) references t1(a)
);
show create table t1;

--no error
insert into t1 values (NULL,NULL,NULL);

--no error
insert into t1 values (1,1,1);

--no error
insert into t1 values (2,1,1);

--no error.
update t1 set a = NULL where a = 2;

--error
delete from t1 where c = 1;

--should be error. 1 duplicate
--insert into t1 values (1,2,3);

--no error
insert into t1 values (2,2,1);

--no error
insert into t1 values (3,3,2);

--no error
insert into t1 values (4,3,1);

--error. 6 does not exists
insert into t1 values (5,6,1);

--no error
insert into t1 values (6,NULL,1);

--error. 8 does not exists
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

update t1 set b = 8 where a = 3;

--error. 6 does not exist
update t1 set b = 6 where a = 3;

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
update t1 set a = 3 where a = 2;

--no error
delete from t1 where  a = 2;

delete from t1 where  a = 3;

--no error
update t1 set b = NULL where a = 1;

--error
delete from t1 where a = 1;

--no error
update t1 set c = NULL where a = 1;

--no error
delete from t1 where a = 1;

update t1 set b = null,c = NULL where a is null;

delete from t1 where a = 1;

delete from t1 where a is null;

--0
select count(*) from t1;


drop table if exists t1;
----create two FKs on a unique key and a primary key----
-- test only two FKs self refer in a table
create table t1(a int primary key,
                b int unique key,
                c int,
                constraint `c1` foreign key fk1(c) references t1(a),
                constraint `c2` foreign key fk2(c) references t1(b)
);
show create table t1;

--no error
insert into t1 values (1,1,1);

--error
insert into t1 values (2,2,3);

--error. 3 does not exist in the column b
insert into t1 values (3,2,3);

--no error
insert into t1 values (2,2,NULL);

--no error
insert into t1 values (3,3,2);

--error. 4 doest not exist in the column b
insert into t1 values (4,5,4);

--no error
insert into t1 values (4,5,NULL);

--no error
insert into t1 values (5,6,5);

--error
delete from t1 where a = 4;

--error. 7 does not exist in the column a
insert into t1 values (8,7,7);

--error
delete from t1 where a= 2;

--error
update t1 set b = NULL where a = 2;

--no error
delete from t1 where a = 3;

--no error
delete from t1 where a = 2;

--error
delete from t1 where a = 4;

--error
delete from t1 where a = 5;

--no error
update t1 set c = 1 where a = 5;

--no error
delete from t1 where a = 5;

--no error
delete from t1 where a = 4;

--error
delete from t1 where a = 1;

--error
update t1 set b = NULL where a = 1;

--error
update t1 set b = 2 where a = 1;

--no error
update t1 set c = NULL where a = 1;

--no error
update t1 set b = 2 where a = 1;

--no error
delete from t1 where a = 1;

--no error
select count(*) from t1;

drop table if exists t1;
drop database if exists fk_self_refer2;