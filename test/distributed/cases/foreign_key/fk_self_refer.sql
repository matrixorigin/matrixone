drop database if exists fk_self_refer;
create database fk_self_refer;
use fk_self_refer;

----create fk on a primary key----
-- test only one fk self refer in a table
drop table if exists t1;
create table t1(a int primary key,b int, constraint `c1` foreign key fk1(b) references t1(a));
show tables;
show create table t1;
insert into t1 values (1,1);
insert into t1 values (2,1);
insert into t1 values (3,2);

--error. no number 4 in column a
insert into t1 values (5,4);

--no error. same as mysql
insert into t1 values (4,NULL);

--no error. number 4 is in column a
insert into t1 values (5,4);

--error. number 4 is referred
delete from t1 where a= 4;

--delete the row a = 5.
delete from t1 where a= 5;

--no error. number 4 is not referred
delete from t1 where a= 4;

--no error. number 4 is not referred
insert into t1 values (4,4);

select * from t1;

--error. number 4 is referred by the b in same row
delete from t1 where a = 4;

--no error
update t1 set b = NULL where a= 4;

--no error. number 4 is not referred.
delete from t1 where a = 4;

drop table if exists t1;

--column b is not null
create table t1(a int primary key,b int not null, foreign key fk1(b) references t1(a));

insert into t1 values (4,4);

-- error. number is referred by the b in same row
delete from t1 where a= 4;

--error. b is not null
update t1 set b=NULL where a= 4;

--error. number 5 does not exists
update t1 set b=5 where a= 4;

--no error
insert into t1 values (3,4);

--no error
update t1 set b = 3 where a= 4;

--loop self reference
select * from t1;

--error. number 3 is referred
delete from t1 where a = 3;
--error. number 4 is referred
delete from t1 where a = 4;

--break loop self reference
update t1 set b = 4 where a =4;

--no error. number 3 is not referred
delete from t1 where a = 3;


----create fk on a unique key----
-- test only one fk self refer in a table
drop table if exists t1;
create table t1(a int unique key,b int,constraint `c1` foreign key fk1(b) references t1(a));
show tables;
show create table t1;
insert into t1 values (1,1);
insert into t1 values (2,1);
insert into t1 values (3,2);

--error. no number 4 in column a
insert into t1 values (5,4);

--no error. same as mysql
insert into t1 values (4,NULL);

--no error. number 4 is in column a
insert into t1 values (5,4);

--error. number 4 is referred
delete from t1 where a= 4;

--no error . delete the row a = 5.
delete from t1 where a= 5;

--no error. number 4 is not referred
delete from t1 where a= 4;

--no error. number 4 is not referred
insert into t1 values (4,4);

select * from t1;

--error. number 4 is referred by the b in same row
delete from t1 where a = 4;

--no error
update t1 set b = NULL where a= 4;

--no error. number 4 is not referred.
delete from t1 where a = 4;

--no error
update t1 set a = NULL where a = 3;

--no error
insert into t1 values (NULL,NULL);

--error
insert into t1 values (NULL,3);

--no error
insert into t1 values (NULL,2);


----create fk on a secondary key----
-- test only one fk self refer in a table
drop table if exists t1;
-- mo error. does not support fk on secondary key
create table t1(a int,b int,key (a), foreign key fk1(b) references t1(a));
-- show tables;
-- show create table t1;
-- insert into t1 values (1,1);
-- insert into t1 values (2,1);
-- insert into t1 values (3,2);
--
-- --error. no number 4 in column a
-- insert into t1 values (5,4);
--
-- --no error. same as mysql
-- insert into t1 values (4,NULL);
--
-- --no error. number 4 is in column a
-- insert into t1 values (5,4);
--
-- --error. number 4 is referred
-- delete from t1 where a= 4;
--
-- --no error . delete the row a = 5.
-- delete from t1 where a= 5;
--
-- --no error. number 4 is not referred
-- delete from t1 where a= 4;
--
-- --no error. number 4 is not referred
-- insert into t1 values (4,4);
--
-- select * from t1;
--
-- --error. number 4 is referred by the b in same row
-- delete from t1 where a = 4;
--
-- --no error
-- update t1 set b = NULL where a= 4;
--
-- --no error. number 4 is not referred.
-- delete from t1 where a = 4;
--
-- --no error in mysql
-- --error in mo. internal error: unexpected input batch for column expression
-- update t1 set a = NULL where a = 3;
--
-- --no error
-- insert into t1 values (NULL,NULL);
--
-- --error
-- insert into t1 values (NULL,3);
--
-- --no error
-- insert into t1 values (NULL,2);

drop table if exists t1;

drop database if exists fk_self_refer;