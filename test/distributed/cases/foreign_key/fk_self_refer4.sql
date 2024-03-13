drop database if exists fk_self_refer4;
create database fk_self_refer4;
use fk_self_refer4;

drop table if exists t1;
--alter table add fk
create table t1(a int primary key,b int);
show tables;
show create table t1;
insert into t1 values (1,2),(3,4),(5,6);

--error
alter table t1 add constraint fk1 foreign key (b) references t1(a);

delete from t1;
insert into t1 values (1,1),(2,3),(3,2);

--no error
alter table t1 add constraint fk1 foreign key (b) references t1(a);

--no error. uuid for constraint name
alter table t1 add foreign key (b) references t1(a);

--should be error. duplicate foreign key
alter table t1 add constraint fk1 foreign key (b) references t1(a);

insert into t1 values (4,4),(6,5),(5,6);

--error. violate foreign key
insert into t1 values (7,8);

--syntax error
--alter table t1 drop constraint fk1;

--no error
alter table t1 drop foreign key fk1;

--error. violate foreign key
insert into t1 values (7,8);

drop table if exists t1;
create table t2(a int);
insert into t2 values (1),(2),(3);

--no error
alter table t2 add constraint fk1 foreign key (a) references t1(a);

--error. duplicate fk1
alter table t2 add constraint fk1 foreign key (a) references t1(a);

--no error
show create table t2;

--error. violate foreign key
insert into t2 values (7);

--no error
insert into t2 values (6);

--no error
alter table t2 drop foreign key fk1;

show create table t2;

--no error
insert into t2 values (7);

--error. 7 is not in t1
alter table t2 add constraint fk1 foreign key (a) references t1(a);

--error. column self refer
alter table t2 add constraint fk1 foreign key (a) references t2(a);

delete from t2 where a = 7;

--no error
alter table t2 add constraint fk1 foreign key (a) references t1(a);

--error
update t2 set a = 7 where a = 6;

select * from t1;

--error. delete row (6,5)
delete from t1 where a = 6;

update t1 set b = NULL where a = 5;

select * from t1;

update t2 set a = NULL where a = 6;

select * from t2;

--no error. delete row (6,5)
delete from t1 where a = 6;

select * from t1;

--error. t1 referred by the t2
drop table t1;

--no error
drop table t2;

--no error. t1 has only self referred now.
drop table t1;

create table t1(a int primary key ,b int);

alter table t1 add constraint `fk1` foreign key (b) references t1(a);
alter table t1 add constraint `fk2` foreign key (b) references t1(a);
alter table t1 add constraint `fk3` foreign key (b) references t1(a);
alter table t1 add constraint `fk4` foreign key (b) references t1(a);
alter table t1 add constraint `fk5` foreign key (b) references t1(a);

show create table t1;


-- no error
insert into t1 values (1,4),(2,3),(3,2),(4,1),(5,5);

--error
delete from t1 where a = 4;

--error
delete from t1 where a = 5;


alter table t1 drop foreign key fk1;
alter table t1 drop foreign key fk2;
alter table t1 drop foreign key fk3;
alter table t1 drop foreign key fk4;

--error
delete from t1 where a = 4;

--error
delete from t1 where a = 5;

alter table t1 drop foreign key fk5;

--no error
delete from t1 where a = 4;

--no error
delete from t1 where a = 5;

--no error
delete from t1 where a = 1;

alter table t1 add constraint `fk1` foreign key (b) references t1(a);

--error fk2 does not exist
alter table t1 drop foreign key fk1, drop foreign key fk2, drop foreign key fk1;

--error duplicate fk1
alter table t1 add constraint fk1 foreign key (b) references t1(a), drop foreign key fk1, add constraint fk1 foreign key (b) references t1(a);

--no error
alter table t1 drop foreign key fk1, drop foreign key fk1, drop foreign key fk1;

--error fk1 does not exist
alter table t1 add constraint fk1 foreign key (b) references t1(a), drop foreign key fk1, add constraint fk1 foreign key (b) references t1(a);

--error. fk1 duplicate in new add constraint
alter table t1 add constraint fk1 foreign key (b) references t1(a), add constraint fk1 foreign key (b) references t1(a);

--no error
alter table t1 add constraint `fk1` foreign key (b) references t1(a);

--no error
alter table t1 drop constraint fk1;

drop database if exists fk_self_refer4;