drop database if exists db1;
create database db1;
use db1;
drop table if exists tb_empt6;
drop table if exists tb_dept1;
CREATE TABLE tb_dept1(id INT(11) PRIMARY KEY, name VARCHAR(22) NOT NULL,location VARCHAR(50) );
CREATE TABLE tb_emp6 (id INT(11) PRIMARY KEY,name VARCHAR(25),deptId INT(11),salary FLOAT,CONSTRAINT fk_emp_dept1 FOREIGN KEY(deptId) REFERENCES tb_dept1(id) );
desc tb_emp6;
show create table tb_emp6;

drop database if exists db2;
create database db2;
use db2;
drop table if exists t1;
drop table if exists t2;
create table t1(a int primary key, b varchar(5));
create table t2(a int ,b varchar(5), c int, constraint `c1` foreign key(c) references t1(a));
desc t2;
show create table t2;
