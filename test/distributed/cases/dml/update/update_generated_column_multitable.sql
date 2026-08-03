-- @suit

-- @case
-- @desc:test for UPDATE with generated columns across multi-table and derived table sources
-- @label:bvt

DROP TABLE IF EXISTS emp_gcol;
DROP TABLE IF EXISTS dept_gcol;

CREATE TABLE emp_gcol (
    empno INT PRIMARY KEY,
    ename VARCHAR(20),
    comm INT,
    sal INT GENERATED ALWAYS AS (comm + 1) STORED,
    deptno INT
);

CREATE TABLE dept_gcol (
    deptno INT PRIMARY KEY,
    dname VARCHAR(20),
    loc VARCHAR(20),
    extra INT GENERATED ALWAYS AS (deptno + 100) STORED
);

INSERT INTO emp_gcol (empno, ename, comm, deptno) VALUES (1, 'alice', 10, 10);
INSERT INTO emp_gcol (empno, ename, comm, deptno) VALUES (2, 'bob', 20, 20);
INSERT INTO dept_gcol (deptno, dname, loc) VALUES (10, 'engineering', 'ny');
INSERT INTO dept_gcol (deptno, dname, loc) VALUES (20, 'sales', 'la');

-- Multi-table UPDATE with generated column on non-first table (dept)
UPDATE emp_gcol, dept_gcol SET emp_gcol.comm = 100, dept_gcol.loc = 'sf' WHERE emp_gcol.deptno = dept_gcol.deptno;
SELECT empno, comm, sal FROM emp_gcol ORDER BY empno;
SELECT deptno, loc, extra FROM dept_gcol ORDER BY deptno;

-- UPDATE...FROM with derived table source and generated column on target
UPDATE emp_gcol SET comm = 50 FROM (SELECT deptno FROM dept_gcol WHERE loc = 'sf') AS d WHERE emp_gcol.deptno = d.deptno;
SELECT empno, comm, sal FROM emp_gcol ORDER BY empno;

DROP TABLE IF EXISTS emp_gcol;
DROP TABLE IF EXISTS dept_gcol;
