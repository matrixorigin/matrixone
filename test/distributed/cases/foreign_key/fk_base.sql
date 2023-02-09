create table f1(b int, a int primary key);
create table f2(b int, aa varchar primary key);
create table c1 (a int, b int, foreign key f_a(a) references f1(c));
create table c1 (a int, b int, foreign key f_a(a) references f1(b));
create table c1 (a int, b int, foreign key f_a(a) references f2(aa));
create table c1 (a int, b int, foreign key f_a(a) references f1(a));
select * from f1;
select * from c1;
drop table f1;
truncate f1;
truncate c1;
drop table f1;
drop table c1;
drop table f1;
drop table f2;

create table f1(a int primary key, b int unique key);
create table c1 (a int, b int, foreign key f_a(a) references f1(a));
insert into f1 values (1,1), (2,2), (3,3);
insert into c1 values (1,1), (2,2), (3,3);
delete from f1 where a > 1;
drop table c1;
create table c1 (a int, b int, foreign key f_a(a) references f1(a) on delete set null);
insert into c1 values (1,1), (2,2), (3,3);
delete from f1 where a > 1;
select * from c1;
drop table c1;
insert into f1 values (2,2), (3,3);
create table c1 (a int, b int, foreign key f_a(a) references f1(a) on delete cascade);
insert into c1 values (1,1), (2,2), (3,3);
delete from f1 where a > 1;
select * from c1;
drop table c1;
drop table f1;

create table f1(a int primary key, b int unique key);
create table c1 (a int, b int, foreign key f_a(a) references f1(a));
insert into f1 values (1,1), (2,2), (3,3);
insert into c1 values (1,1), (2,2), (3,3);
update f1 set a = 11 where b = 1;
update c1 set a = 11 where b = 1;
update c1 set a = null where b = 1;
select * from c1 order by b;
update c1 set a = 3 where b = 2;
select * from c1 order by b;
drop table c1;
create table c1 (a int, b int, foreign key f_a(a) references f1(a) on update set null);
insert into c1 values (1,1), (2,2), (3,3);
update f1 set a=11 where a=1;
select * from c1 order by b;
drop table c1;
drop table f1;
create table f1(a int primary key, b int unique key);
create table c1 (a int, b int, foreign key f_a(a) references f1(a) on update cascade);
insert into f1 values (1,1), (2,2), (3,3);
insert into c1 values (1,1), (2,2), (3,3);
update f1 set a=0 where b>1;
update f1 set a=0 where b=1;
select * from c1 order by b;
drop table c1;
drop table f1;
--2.4 测试外键操作是否正常（on restrict、on casade、 set null三种情况）
------------FOREIGN KEY ON RESTRICT ON UPDATE RESTRICT（默认）----------------
drop table if exists t_dept;
CREATE TABLE t_dept
(
    id INT(11) PRIMARY KEY,
    name VARCHAR(22) NOT NULL,
    location VARCHAR(50)
);

INSERT INTO t_dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO t_dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO t_dept VALUES (30,'SALES','CHICAGO');
INSERT INTO t_dept VALUES (40,'OPERATIONS','BOSTON');


drop table if exists t_emp;
CREATE TABLE t_emp
(
    id INT(11) PRIMARY KEY,
    name VARCHAR(25),
    deptId INT(11),
    salary FLOAT,
    CONSTRAINT fk_emp_dept FOREIGN KEY(deptId) REFERENCES t_dept(id)
);

INSERT INTO t_emp VALUES (7369,'SMITH',20,1300.00);
INSERT INTO t_emp VALUES (7499,'ALLEN',30,1600.00);
INSERT INTO t_emp VALUES (7521,'WARD ',30,1250.00);
INSERT INTO t_emp VALUES (7566,'JONES',20,3475.00);
INSERT INTO t_emp VALUES (1234,'MEIXI',30,1250.00);
INSERT INTO t_emp VALUES (7698,'BLAKE',30,2850.00);
INSERT INTO t_emp VALUES (7782,'CLARK',10,2950.00);
INSERT INTO t_emp VALUES (7788,'SCOTT',20,3500.00);

update t_dept set id = 50 where name = 'ACCOUNTING';
delete from t_dept where name = 'ACCOUNTING';
update t_emp set deptId = 50 where salary <  1500;
update t_emp set deptId = null where salary <  1500;
select * from t_emp order by salary;
select * from t_dept;
drop table t_emp;
drop table t_dept;

------------FOREIGN KEY ON DELETE CASCADE ON UPDATE CASCADE----------------
drop table if exists t_dept1;
CREATE TABLE t_dept1
(
    id INT(11) PRIMARY KEY,
    name VARCHAR(22) NOT NULL,
    location VARCHAR(50)
);

INSERT INTO t_dept1 VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO t_dept1 VALUES (20,'RESEARCH','DALLAS');
INSERT INTO t_dept1 VALUES (30,'SALES','CHICAGO');
INSERT INTO t_dept1 VALUES (40,'OPERATIONS','BOSTON');


drop table if exists t_emp1;
CREATE TABLE t_emp1
(
    id INT(11) PRIMARY KEY,
    name VARCHAR(25),
    deptId INT(11),
    salary FLOAT,
    CONSTRAINT fk_emp_dept1 FOREIGN KEY(deptId) REFERENCES t_dept1(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO t_emp1 VALUES (7369,'SMITH',20,1300.00);
INSERT INTO t_emp1 VALUES (7499,'ALLEN',30,1600.00);
INSERT INTO t_emp1 VALUES (7521,'WARD ',30,1250.00);
INSERT INTO t_emp1 VALUES (7566,'JONES',20,3475.00);
INSERT INTO t_emp1 VALUES (1234,'MEIXI',30,1250.00);
INSERT INTO t_emp1 VALUES (7698,'BLAKE',30,2850.00);
INSERT INTO t_emp1 VALUES (7782,'CLARK',10,2950.00);
INSERT INTO t_emp1 VALUES (7788,'SCOTT',20,3500.00);

update t_dept1 set id = 50 where name = 'ACCOUNTING';
select * from t_dept1;
select * from t_emp1;

delete from t_dept1 where name = 'ACCOUNTING';
select * from t_dept1;
select * from t_emp1;

update t_emp1 set deptId = 50 where salary < 1500;
update t_emp1 set deptId = null where salary < 1500;

drop table t_emp1;
drop table t_dept1;

-----------FOREIGN KEY ON DELETE SET NULL ON UPDATE SET NULL-------------
drop table if exists t_dept2;
CREATE TABLE t_dept2
(
    id INT(11) PRIMARY KEY,
    name VARCHAR(22) NOT NULL,
    location VARCHAR(50)
);

INSERT INTO t_dept2 VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO t_dept2 VALUES (20,'RESEARCH','DALLAS');
INSERT INTO t_dept2 VALUES (30,'SALES','CHICAGO');
INSERT INTO t_dept2 VALUES (40,'OPERATIONS','BOSTON');

drop table if exists t_emp2;
CREATE TABLE t_emp2
(
    id INT(11) PRIMARY KEY,
    name VARCHAR(25),
    deptId INT(11),
    salary FLOAT,
    CONSTRAINT fk_emp_dept1 FOREIGN KEY(deptId) REFERENCES t_dept2(id) ON DELETE SET NULL ON UPDATE SET NULL
);

INSERT INTO t_emp2 VALUES (7369,'SMITH',20,1300.00);
INSERT INTO t_emp2 VALUES (7499,'ALLEN',30,1600.00);
INSERT INTO t_emp2 VALUES (7521,'WARD ',30,1250.00);
INSERT INTO t_emp2 VALUES (7566,'JONES',20,3475.00);
INSERT INTO t_emp2 VALUES (1234,'MEIXI',30,1250.00);
INSERT INTO t_emp2 VALUES (7698,'BLAKE',30,2850.00);
INSERT INTO t_emp2 VALUES (7782,'CLARK',10,2950.00);
INSERT INTO t_emp2 VALUES (7788,'SCOTT',20,3500.00);

update t_dept2 set id = 50 where name = 'ACCOUNTING';
select * from t_dept2;
select * from t_emp2;

delete from t_dept2 where name = 'ACCOUNTING';
select * from t_dept2;
select * from t_emp2;

update t_emp2 set deptId = 50 where salary < 1500;
update t_emp2 set deptId = null where salary < 1500;

drop table t_emp2;
drop table t_dept2;

create table f1(a int primary key, b int unique key);
create table f2(aa int primary key, bb int unique key);
create table c1 (aaa int, bbb int, foreign key f_a(aaa) references f1(a), foreign key f_b(bbb) references f2(aa));
insert into f1 values (1,1), (2,2), (3,3);
insert into f2 values (11,11), (22,22), (33,33);
insert into c1 values (1,11), (2,22), (3,33);
update c1 set aaa=2, bbb=12 where bbb=11;
update c1 set aaa=4, bbb=22 where bbb=11;
update c1 set aaa=2, bbb=33 where bbb=11;
select * from c1 order by bbb;

drop table c1;
drop table f2;
drop table f1;
create table f1(a int primary key, b int unique key);
create table c1 (a int, b int, foreign key f_a(a) references f1(a));
insert into f1 values (1,1), (2,2), (3,3);
insert into c1 values (11,11);
insert into c1 values (1,1),(11,11);
insert into c1 values (1,1);
drop table c1;
drop table f1;

create table f1(b int, a int primary key);
create table c1( a int primary key, b int unique key, c int not null, d int,foreign key(d) references f1(a));
insert into f1 values (1,1), (2,2), (3,3);
insert into c1 values(1,2,1,1);
insert into c1 values(2,2,1,1);
drop table c1;
drop table f1;

drop database if exists db1;
create database db1;
use db1;
create table f1(b int, a int primary key);
create table t1 (a int, b int);
create table t2(b int, a int unique);
truncate table f1;
drop database db1;
create database db1;
use db1;
show tables;
drop database db1;

---------Cross-tenant test---------
create account acc1 ADMIN_NAME 'root' IDENTIFIED BY '123456';
-- @session:id=1&user=acc1:root&password=123456
create database db2;
use db2;
create table f1(b int, a int primary key);
create table t1 (a int, b int);
create table t2(b int, a int unique);
truncate table f1;
drop database db2;
create database db2;
use db2;
show tables;
drop database db2;
-- @session
drop account if exists acc1;