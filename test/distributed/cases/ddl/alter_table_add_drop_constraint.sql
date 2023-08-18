------------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER, b CHAR(10), c date, d decimal(7,2), UNIQUE KEY(a, b));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);
select * from t1;

alter table t1 add primary key pk1(a, b);
desc t1;
select * from t1;

--ERROR 1068 (HY000): Multiple primary key defined
alter table t1 add constraint pk primary key pk1(c);
desc t1;
select * from t1;


alter table t1 drop primary key;
desc t1;
select * from t1;

--ERROR 20101 (HY000): internal error: Can't DROP Primary Key; check that column/key exists
alter table t1 drop primary key;
desc t1;
select * from t1;

alter table t1 add constraint pk primary key pk1(a);
desc t1;
select * from t1;
------------------------------------------------------------------------------------------