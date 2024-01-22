create database db7;
use db7;

drop table if exists t1;
CREATE TABLE t1(
col1 INT NOT NULL,
col2 DATE NOT NULL,
col3 VARCHAR(16) NOT NULL,
col4 INT NOT NULL,
PRIMARY KEY(col1)
);

insert into t1 values(1, '1980-12-17','Abby', 21);
insert into t1 values(2, '1981-02-20','Bob', 22);
insert into t1 values(3, '1981-02-22','Carol', 23);
insert into t1 values(4, '1981-04-02','Dora', 24);
insert into t1 values(5, '1981-09-28','bcvdf', 25);
insert into t1 values(6, '1981-05-01','green', 26);

begin;
alter table t1 add column col5 int default 0;
desc t1;
select * from t1;

insert into t1 values(7, '1989-09-28','bcvdfx', 25, 7);
insert into t1 values(8, '1991-05-01','fgreen', 26, 8);
select * from t1;

alter table t1 modify col5 VARCHAR(20);
desc t1;
select * from t1;
show tables;

commit;
desc t1;
select * from t1;
----------------------------------------------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(
col1 INT NOT NULL,
col2 DATE NOT NULL,
col3 VARCHAR(16) NOT NULL,
col4 INT NOT NULL,
PRIMARY KEY(col1)
);

insert into t1 values(1, '1980-12-17','Abby', 21);
insert into t1 values(2, '1981-02-20','Bob', 22);
insert into t1 values(3, '1981-02-22','Carol', 23);
insert into t1 values(4, '1981-04-02','Dora', 24);
insert into t1 values(5, '1981-09-28','bcvdf', 25);
insert into t1 values(6, '1981-05-01','green', 26);

begin;
alter table t1 add column col5 int default 0;
desc t1;
select * from t1;

insert into t1 values(7, '1989-09-28','bcvdfx', 25, 7);
insert into t1 values(8, '1991-05-01','fgreen', 26, 8);
select * from t1;

alter table t1 drop column col3;
desc t1;
select * from t1;
show tables;

commit;
desc t1;
select * from t1;
----------------------------------------------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(
col1 INT NOT NULL,
col2 DATE NOT NULL,
col3 VARCHAR(16) NOT NULL,
col4 INT NOT NULL,
PRIMARY KEY(col1)
);

insert into t1 values(1, '1980-12-17','Abby', 21);
insert into t1 values(2, '1981-02-20','Bob', 22);
insert into t1 values(3, '1982-02-22','Carol', 23);
insert into t1 values(4, '1983-04-02','Dora', 24);
insert into t1 values(5, '1984-09-28','bcvdf', 25);
insert into t1 values(6, '1985-05-01','green', 26);

begin;
alter table t1 add column col5 int default 0;
desc t1;
select * from t1;

insert into t1 values(7, '1989-09-28','bcvdfx', 25, 7);
insert into t1 values(8, '1991-05-01','fgreen', 26, 8);

alter table t1 modify col5 VARCHAR(20);
desc t1;
select * from t1;

alter table t1 rename column col3 to colx;
show index from t1;
desc t1;
select * from t1;

commit;
desc t1;
select * from t1;
----------------------------------------------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(
col1 INT NOT NULL,
col2 DATE NOT NULL,
col3 VARCHAR(16) NOT NULL,
col4 INT NOT NULL,
PRIMARY KEY(col1)
);

insert into t1 values(1, '1980-12-17','Abby', 21);
insert into t1 values(2, '1981-02-20','Bob', 22);
insert into t1 values(3, '1982-02-22','Carol', 23);
insert into t1 values(4, '1983-04-02','Dora', 24);
insert into t1 values(5, '1984-09-28','bcvdf', 25);
insert into t1 values(6, '1985-05-01','green', 26);

begin;
alter table t1 add column col5 int default 0;
desc t1;

insert into t1 values(7, '1989-09-28','bcvdfx', 25, 7);
insert into t1 values(8, '1991-05-01','fgreen', 26, 8);
select * from t1;

alter table t1 modify col5 VARCHAR(20);
desc t1;
select * from t1;

alter table t1 change col2 colx datetime;
show index from t1;
desc t1;
select * from t1;

commit;
desc t1;
select * from t1;
drop database db7;