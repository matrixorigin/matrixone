DROP TABLE IF EXISTS dept;
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

DROP TABLE IF EXISTS emp;
create table emp(
    empno int unsigned auto_increment COMMENT '雇员编号',
    ename varchar(15) COMMENT '雇员姓名',
    job varchar(10) COMMENT '雇员职位',
    mgr int unsigned COMMENT '雇员对应的领导的编号',
    hiredate date COMMENT '雇员的雇佣日期',
    sal decimal(7,2) COMMENT '雇员的基本工资',
    comm decimal(7,2) COMMENT '奖金',
    deptno int unsigned COMMENT '所在部门',
    primary key(empno)
) COMMENT='雇员表';

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

--1.mysql：ERROR; mo: ERROR --ERROR 1052 (23000): Column 'deptno' in order clause is ambiguous
select t1.*, t2.deptno as deptno from emp t1 left join dept t2 on t1.deptno = t2.deptno order by deptno;

--2.mysql：ERROR; mo: ERROR --ERROR 1052 (23000): Column 'deptno' in order clause is ambiguous
select t1.*, t2.deptno from emp t1 left join dept t2 on t1.deptno = t2.deptno order by deptno;

--3.mysql：ERROR; mo: ERROR  --ERROR 1052 (23000): Column 'X1' in order clause is ambiguous
mysql> select empno X1, ename X1, sal X1 from emp where sal > 200 order by X1;

--4.mysql：ok; mo: ok;
select t2.dname, t1.* from emp t1 left join dept t2 on t1.deptno = t2.deptno order by deptno;

--5.mysql：ok; mo: ok
select t2.dname as deptname, t1.* from emp t1 left join dept t2 on t1.deptno = t2.deptno order by deptno;

--6.mysql：ok; mo: ok
select t2.dname as deptname, t1.* from emp t1 left join dept t2 on t1.deptno = t2.deptno where  '1' = '1' order by deptno;

--7.mysql：ok; mo: ok
select t2.dname as deptname, t1.*  from emp t1 left join dept t2 on t1.deptno = t2.deptno  where  '1' = '1' order by deptno, empno;

--8.mysql：ok; mo: ok
select t2.dname as deptname, t1.*
from emp t1 left join dept t2 on t1.deptno = t2.deptno where  '1' = '1'
group by t1.ename
order by deptno;

--9.mysql：ERROR; mo: ERROR
select t1.*, t2.loc, t2.deptno as deptno from emp t1 left join dept t2 on t1.deptno = t2.deptno order by deptno;

--10.mysql：ok; mo: ok
select t1.ename, t2.loc, t2.deptno as deptno from emp t1 left join dept t2 on t1.deptno = t2.deptno order by deptno;

--11. mysql：error; mo: error
select t1.ename, t2.loc from emp t1 left join dept t2 on t1.deptno = t2.deptno order by deptno;

--12. mysql: error, 别名不会参与order by表达式运算, mo: ok(bug)
select t1.ename, t2.loc, t2.deptno as deptno from emp t1 left join dept t2 on t1.deptno = t2.deptno order by deptno+33;

--13.mysql：error; mo: error
select t1.ename, t2.loc from emp t1 left join dept t2 on t1.deptno = t2.deptno order by deptno;

--14.mysql：ok, mo: error(bug,暂时选择报错)
-- mysql的别名是有作用域的， 通常selectList中子查询别名的优先级较高，即使主查询和子查询使用了相同的别名 ename，
-- 排序键冲突时MySQL会优先选择子查询别名，
select ename, (select ename from emp i1 where i1.empno = emp.mgr order by 1 limit 1) as ename from emp order by ename;

--15.mysql：ok, mo: error(bug,暂时选择报错) (同上)
select empno, (select ename from emp i1 where i1.empno = emp.mgr order by 1 limit 1) as ename, ename from emp order by ename;

--16.mysql: ok, mo: error(bug,暂时选择报错)
select empno, 20 as empno from emp order by empno;

--17..mysql: ok, mo: error(bug,暂时选择报错) (同上)
select empno,  space(50) as empno from emp order by empno;

--18.mysql:ok, mo: ok
select empno, ename, job, mgr, hiredate, sal, empno from emp where deptno != 20 order by empno;

--19.mysql:ok, mo: ok
select empno, ename, job, mgr, hiredate, sal, emp.empno from emp where deptno != 20 order by empno;

--20.mysql:ok, mo: ok
select t1.ename, t2.loc, t1.deptno, t2.deptno as deptno from emp t1 left join dept t2 on t1.deptno = t2.deptno order by t1.deptno;

--21.mysql:error, mo: error
select t1.ename, t2.loc, deptno, t2.deptno as deptno from emp t1 left join dept t2 on t1.deptno = t2.deptno order by deptno;

--22.mysql:error, mo: error
select t1.ename, t2.loc, t1.deptno, t2.deptno as deptno from emp t1 left join dept t2 on t1.deptno = t2.deptno order by deptno;


drop table if exists dept;
drop table if exists emp;
