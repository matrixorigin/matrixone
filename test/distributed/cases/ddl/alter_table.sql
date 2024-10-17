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
-- @pattern
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

drop table if exists t4;
CREATE TABLE t4(
col1 INT NOT NULL,
col2 DATE NOT NULL,
col3 VARCHAR(16) NOT NULL,
col4 int unsigned NOT NULL,
PRIMARY KEY(col1)
);

insert into t4 values(1, '1980-12-17','Abby', 21);
insert into t4 values(2, '1981-02-20','Bob', 22);
insert into t4 values(3, '1981-02-22','Carol', 23);
insert into t4 values(4, '1981-04-02','Dora', 24);
insert into t4 values(5, '1981-09-28','bcvdf', 25);
insert into t4 values(6, '1981-05-01','green', 26);

alter table t4 add constraint index (col3, col4);
alter table t4 add constraint index wwwww (col3, col4);
alter table t4 add constraint idx_6dotkott2kjsp8vw4d0m25fb7 index zxxxxx (col3);
show index from t4;
alter table t4 add index zxxxxx(col3);
show index from t4;
drop table t4;

drop table if exists t5;
CREATE TABLE t5(
col1 INT NOT NULL,
col2 DATE NOT NULL,
col3 VARCHAR(16) NOT NULL,
col4 int unsigned NOT NULL,
PRIMARY KEY(col1)
);

insert into t5 values(1, '1980-12-17','Abby', 21);
insert into t5 values(2, '1981-02-20','Bob', 22);
insert into t5 values(3, '1981-02-22','Carol', 23);
insert into t5 values(4, '1981-04-02','Dora', 24);
insert into t5 values(5, '1981-09-28','bcvdf', 25);
insert into t5 values(6, '1981-05-01','green', 26);

alter table t5 add constraint unique key (col3, col4);
alter table t5 add constraint unique key wwwww (col3, col4);
alter table t5 add constraint idx_6dotkott2kjsp8vw4d0m25fb7 unique key zxxxxx (col3);
show index from t5;
alter table t5 add unique key zxxxxx(col3);
show index from t5;
alter table t5 add constraint idx_6dotkott2kjsp8v unique key (col3);
alter table t5 add constraint idx_6dotkott2kjsp8v unique key (col4);
show index from t5;
drop table t5;

create table t5(a int);
alter table t5 comment = "comment_1";
show create table t5;
alter table t5 comment = "comment_2", comment = "comment_3";
show create table t5;
alter table t5 add column a int;
alter table t5 add column b tinyint, add column c smallint, add column d int, add column e bigint, add column f tinyint unsigned;
alter table t5 add column g smallint unsigned, add column h int unsigned, add column i bigint unsigned, add column j float, add column k double;
alter table t5 add column l varchar(255), add column m Date, add column n DateTime, add column o timestamp, add column p bool;
alter table t5 add column q decimal(5,2), add column r text;
show create table t5;
show columns from t5;
alter table t5 drop column b, drop column c, drop column d, drop column e, drop column f, drop column g, drop column h;
show columns from t5;
alter table t5 drop column i, drop column j, drop column k, drop column l, drop column m, drop column n, drop column o;
show columns from t5;
alter table t5 drop column p, drop column q, drop column r;
show columns from t5;
alter table t5 drop column a;
alter table t5 add column b int first, add column c int after b, add column d int first, add column f int after b;
show columns from t5;


drop table t5;
create table t5(a int primary key, b int, c int unique key);
alter table t5 drop column a;
alter table t5 drop column c;

drop table t5;
create table t5(a int, b int, primary key(a, b));
alter table t5 drop column a;

drop table t5;
create table t5(a int primary key, b int);
create table t6(b int, c int, constraint `c1` foreign key(b) references t5(a));
alter table t5 drop column b;
alter table t5 add column c int;
alter table t6 drop column b;
alter table t6 add column d int;
drop table t6;
drop table t5;

create table t5(a tinyint, b smallint, primary key(a))partition by hash(a) partitions 4;
alter table t5 add column c int;
alter table t5 drop column a;
drop table t5;

create table t5(a int, b int) cluster by a;
alter table t5 add column c int;
alter table t5 drop column a;
drop table t5;

drop table if exists t6;
create table t6(a int not null);
insert into t6 values(1),(2);
select * from t6;
alter table t6 add column b timestamp not null;
select * from t6;
alter table t6 add column c time not null;
select * from t6;
alter table t6 add column d datetime not null;
select * from t6;
alter table t6 add column e date not null;
select * from t6;
alter table t6 add column f datetime after a;
select * from t6;
drop table t6;


drop table if exists t7;
create table t7(a int not null);
insert into t7 values(1),(2);
select * from t7;
alter table t7 add column b int not null;
select * from t7;
alter table t7 add column c float not null;
select * from t7;
alter table t7 add column d int unsigned not null;
select * from t7;
alter table t7 add column e decimal(7,2) not null;
select * from t7;
alter table t7 add column f bool not null;
select * from t7;
alter table t7 add column g double after a;
select * from t7;
drop table t7;

drop table if exists t8;
create table t8(a int not null);
insert into t8 values(1),(2);
select * from t8;
alter table t8 add column b char(20) not null;
select * from t8;
alter table t8 add column c varchar(20) not null;
select * from t8;
alter table t8 add column d text not null;
select * from t8;
alter table t8 add column e binary(2) not null;
select * from t8;
alter table t8 add column f blob not null;
select * from t8;
alter table t8 add column g varchar(50) after a;
select * from t8;
alter table t8 add column h json not null;
select * from t8;
drop table t8;