drop table if exists f1;
drop table if exists c1;
create table f1(fa int primary key, fb int unique key);
create table c1 (ca int, cb int);
alter table c1 add constraint ffa foreign key f_a(ca) references f1(fa);
insert into f1 values (2,2);
insert into c1 values (1,1);
insert into c1 values (2,2);
select ca, cb from c1 order by ca;
alter table c1 drop foreign key ffa;
insert into c1 values (1,1);
select ca, cb from c1 order by ca;
drop table c1;
drop table f1;
create table f1(fa int primary key, fb int unique key);
create table c1 (ca int, cb int, constraint ffb foreign key f_a(cb) references f1(fb));
insert into f1 values (2,2);
insert into c1 values (2,1);
alter table c1 add constraint ffa foreign key f_a(ca) references f1(fa);
insert into c1 values (1,2);
alter table c1 drop foreign key ffb;
insert into c1 values (2,1);
insert into c1 values (1,2);
alter table c1 drop foreign key ffa;
insert into c1 values (1,2);
select ca, cb from c1 order by ca;
drop table c1;
drop table f1;


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

ALTER TABLE t1 ADD UNIQUE idx1 (col2, col3);
insert into t1 values(7, '1981-05-01','green', 26);
show index from t1;
select * from t1;
alter table t1 alter index idx1 invisible;
show index from t1;
alter table t1 alter index idx1 visible;
show index from t1;
ALTER TABLE t1 DROP INDEX idx1;
show index from t1;

ALTER TABLE t1 ADD UNIQUE INDEX idx2 (col2, col3);
show index from t1;
alter table t1 alter index idx2 invisible;
show index from t1;
ALTER TABLE t1 DROP INDEX idx2;
show index from t1;
drop table t1;


drop table if exists t2;
CREATE TABLE t2(
col1 INT NOT NULL,
col2 DATE NOT NULL,
col3 VARCHAR(16) NOT NULL,
col4 INT NOT NULL,
PRIMARY KEY(col1)
);

insert into t2 values(1, '1980-12-17','Abby', 21);
insert into t2 values(2, '1981-02-20','Bob', 22);
insert into t2 values(3, '1981-02-22','Carol', 23);
insert into t2 values(4, '1981-04-02','Dora', 24);
insert into t2 values(5, '1981-09-28','bcvdf', 25);
insert into t2 values(6, '1981-05-01','green', 26);

ALTER TABLE t2 ADD INDEX index1 (col2);
show index from t2;
alter table t2 alter index index1 invisible;
show index from t2;
select * from t2;
ALTER TABLE t2 DROP INDEX index1;
show index from t2;

ALTER TABLE t2 ADD INDEX index2 (col2,col3);
show index from t2;
alter table t2 alter index index2 invisible;
show index from t2;
ALTER TABLE t2 DROP INDEX index2;
show index from t2;
drop table t2;

drop table if exists t3;
CREATE TABLE t3(
col1 INT NOT NULL,
col2 DATE NOT NULL,
col3 VARCHAR(16) NOT NULL,
col4 INT NOT NULL,
PRIMARY KEY(col1, col2)
);

insert into t3 values(1, '1980-12-17','Abby', 21);
insert into t3 values(2, '1981-02-20','Bob', 22);
insert into t3 values(3, '1981-02-22','Carol', 23);
insert into t3 values(4, '1981-04-02','Dora', 24);
insert into t3 values(5, '1981-09-28','bcvdf', 25);
insert into t3 values(6, '1981-05-01','green', 26);

ALTER TABLE t3 ADD INDEX index1 (col2);
show index from t3;
alter table t3 alter index index1 invisible;
show index from t3;
select * from t3;
ALTER TABLE t3 DROP INDEX index1;
show index from t3;

ALTER TABLE t3 ADD UNIQUE INDEX index2 (col2,col3);
show index from t3;
alter table t3 alter index index2 invisible;
show index from t3;
ALTER TABLE t3 DROP INDEX index2;
show index from t3;

create unique index idx3 on t3(col2,col3);
show index from t3;
drop table t3;