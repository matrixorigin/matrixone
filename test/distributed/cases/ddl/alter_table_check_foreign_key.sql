drop database if exists db8;
create database db8;
use db8;

drop table if exists dept;
create table dept(
                     deptno int unsigned auto_increment COMMENT '部门编号',
                     dname varchar(15) COMMENT '部门名称',
                     loc varchar(50)  COMMENT '部门所在位置',
                     primary key(deptno)
) COMMENT='部门表';

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');


drop table if exists emp;
create table emp(
                    empno int unsigned auto_increment COMMENT '雇员编号',
                    ename varchar(15) COMMENT '雇员姓名',
                    job varchar(10) COMMENT '雇员职位',
                    mgr int unsigned COMMENT '雇员对应的领导的编号',
                    hiredate date COMMENT '雇员的雇佣日期',
                    sal decimal(7,2) COMMENT '雇员的基本工资',
                    comm decimal(7,2) COMMENT '奖金',
                    deptno int unsigned COMMENT '所在部门',
                    primary key(empno),
                    FOREIGN KEY (deptno) REFERENCES dept(deptno)
);


INSERT INTO emp VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
INSERT INTO emp VALUES (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
INSERT INTO emp VALUES (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
INSERT INTO emp VALUES (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
INSERT INTO emp VALUES (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
INSERT INTO emp VALUES (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
INSERT INTO emp VALUES (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);
INSERT INTO emp VALUES (7788,'SCOTT','ANALYST',7566,'0087-07-13',3000,NULL,20);
INSERT INTO emp VALUES (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
INSERT INTO emp VALUES (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);
INSERT INTO emp VALUES (7876,'ADAMS','CLERK',7788,'0087-07-13',1100,NULL,20);
INSERT INTO emp VALUES (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30);
INSERT INTO emp VALUES (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20);
INSERT INTO emp VALUES (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

alter table emp modify deptno varchar(20);
--ERROR 3780 (HY000): Referencing column 'deptno' and referenced column 'deptno' in foreign key constraint 'emp_ibfk_1' are incompatible.
alter table emp modify deptno bigint;
--ERROR 3780 (HY000): Referencing column 'deptno' and referenced column 'deptno' in foreign key constraint 'emp_ibfk_1' are incompatible.

alter table emp modify deptno int unsigned;
--success
desc emp;

alter table emp modify deptno int unsigned default 100;
--success
desc emp;
--------------------------------------------------------------------------
alter table emp change deptno deptid varchar(20);
--ERROR 3780 (HY000): Referencing column 'deptid' and referenced column 'deptno' in foreign key constraint 'emp_ibfk_1' are incompatible.

alter table emp change deptno deptid bigint;
--ERROR 3780 (HY000): Referencing column 'deptid' and referenced column 'deptno' in foreign key constraint 'emp_ibfk_1' are incompatible.

alter table emp change deptno deptid int unsigned;
--success
desc emp;

--success
INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,100);
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails (`db1`.`emp`, CONSTRAINT `emp_ibfk_1` FOREIGN KEY (`deptno`) REFERENCES `dept` (`deptno`))
INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);
select * from emp;

drop table emp;
drop table dept;
------------------------------------------------------------------------------------------------------------------------
drop table if exists dept;
create table dept(
                     deptno int unsigned auto_increment COMMENT '部门编号',
                     dname varchar(15) COMMENT '部门名称',
                     loc varchar(50)  COMMENT '部门所在位置',
                     primary key(deptno)
) COMMENT='部门表';

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');

drop table if exists emp;
create table emp(
                    empno int unsigned auto_increment COMMENT '雇员编号',
                    ename varchar(15) COMMENT '雇员姓名',
                    job varchar(10) COMMENT '雇员职位',
                    mgr int unsigned COMMENT '雇员对应的领导的编号',
                    hiredate date COMMENT '雇员的雇佣日期',
                    sal decimal(7,2) COMMENT '雇员的基本工资',
                    comm decimal(7,2) COMMENT '奖金',
                    deptno int unsigned COMMENT '所在部门',
                    primary key(empno),
                    FOREIGN KEY (deptno) REFERENCES dept(deptno)
);


INSERT INTO emp VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
INSERT INTO emp VALUES (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
INSERT INTO emp VALUES (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
INSERT INTO emp VALUES (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
INSERT INTO emp VALUES (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
INSERT INTO emp VALUES (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
INSERT INTO emp VALUES (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);
INSERT INTO emp VALUES (7788,'SCOTT','ANALYST',7566,'0087-07-13',3000,NULL,20);
INSERT INTO emp VALUES (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
INSERT INTO emp VALUES (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);
INSERT INTO emp VALUES (7876,'ADAMS','CLERK',7788,'0087-07-13',1100,NULL,20);
INSERT INTO emp VALUES (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30);
INSERT INTO emp VALUES (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20);
INSERT INTO emp VALUES (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

alter table emp ALTER COLUMN deptno SET DEFAULT 10;
--success
desc emp;

alter table emp ALTER COLUMN deptno SET INVISIBLE;
--success
desc emp;

alter table emp ALTER COLUMN deptno drop default;
--success
desc emp;
select * from emp;

alter table emp rename column deptno to deptid;
--success
desc emp;
select * from emp;

--success
INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,100);
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails (`db1`.`emp`, CONSTRAINT `emp_ibfk_1` FOREIGN KEY (`deptno`) REFERENCES `dept` (`deptno`))
INSERT INTO emp VALUES (7990,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);
select * from emp;

drop table emp;
drop table dept;
------------------------------------------------------------------------------------------------------------------------
CREATE TABLE product (
        category INT NOT NULL,
        id INT NOT NULL,
        name VARCHAR(20) NOT NULL,
        price DECIMAL,
        PRIMARY KEY (category, id)
);

INSERT INTO product VALUES (10, 1, '小米手机', 2799.00);
INSERT INTO product VALUES (10, 2, '苹果手机', 6499.00);
INSERT INTO product VALUES (20, 3, 'Surface笔记本', 15388.00);
INSERT INTO product VALUES (30, 4, '特斯拉电动车',250000.00);


CREATE TABLE customer (
        id INT NOT NULL,
        name varchar(50) NOT NULL,
        PRIMARY KEY (id)
);

INSERT INTO customer VALUES (1, '小米');
INSERT INTO customer VALUES (2, '苹果');
INSERT INTO customer VALUES (3, '微软');
INSERT INTO customer VALUES (4, '特斯拉');

CREATE TABLE product_order (
        no INT NOT NULL AUTO_INCREMENT,
        product_category INT NOT NULL,
        product_id INT NOT NULL,
        customer_id INT NOT NULL,
        order_date datetime NOT NULL,
        PRIMARY KEY (no),
        INDEX(product_category, product_id),
        INDEX(customer_id),
        FOREIGN KEY (product_category, product_id) REFERENCES product (category, id) ON DELETE RESTRICT ON UPDATE CASCADE,
        FOREIGN KEY (customer_id) REFERENCES customer (id)
);
INSERT INTO product_order VALUES (1, 10, 1, 1, '2016-12-02 15:41:39');
INSERT INTO product_order VALUES (2, 10, 2, 2, '2016-12-01 15:42:42');
INSERT INTO product_order VALUES (3, 20, 3, 3, '2016-12-06 15:43:26');
INSERT INTO product_order VALUES (4, 30, 4, 3, '2016-12-31 15:43:26');

desc product_order;
select * from product_order;

alter table product_order change product_category product_kind varchar(20);
--ERROR 3780 (HY000): Referencing column 'product_kind' and referenced column 'category' in foreign key constraint 'product_order_ibfk_1' are incompatible.
alter table product_order change product_category product_kind bigint;
--ERROR 3780 (HY000): Referencing column 'deptid' and referenced column 'deptno' in foreign key constraint 'emp_ibfk_1' are incompatible.
alter table product_order modify product_id varchar(20);
--ERROR 3780 (HY000): Referencing column 'product_id' and referenced column 'id' in foreign key constraint 'product_order_ibfk_1' are incompatible.
alter table product_order modify product_id bigint;
--ERROR 3780 (HY000): Referencing column 'product_id' and referenced column 'id' in foreign key constraint 'product_order_ibfk_1' are incompatible.
alter table product_order change product_category product_kind int unsigned;
--ERROR 3780 (HY000): Referencing column 'product_kind' and referenced column 'category' in foreign key constraint 'product_order_ibfk_1' are incompatible.

alter table product_order change product_category product_kind int;
--success
desc product_order;
select * from product_order;

INSERT INTO product_order VALUES (4, 30, 5, 3, '2016-12-31 15:43:26');
--ERROR 1062 (23000): Duplicate entry '4' for key 'product_order.PRIMARY'
INSERT INTO product_order VALUES (5, 30, 5, 3, '2016-12-31 15:43:26');
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails (`db1`.`product_order`, CONSTRAINT `product_order_ibfk_1` FOREIGN KEY (`product_kind`, `product_id`) REFERENCES `product` (`category`, `id`) ON DELETE RESTRICT ON UPDATE CASCADE)

INSERT INTO product_order VALUES (5, 30, 4, 4, '2016-12-31 15:43:26');
--success
select *from product_order;

INSERT INTO product_order VALUES (6, 30, 4, 5, '2016-12-31 15:43:26');
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails (`db1`.`product_order`, CONSTRAINT `product_order_ibfk_2` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`id`))
drop table product_order;
drop table customer;
drop table product;

drop database if exists db8;