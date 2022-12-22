// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"
)

// only use in developing
func TestSingleSQLQuery(t *testing.T) {
	//sql := "update emp set comm = 1200 where deptno = 10"
	//sql := "update dept set dname = 'XXX' where deptno = 10"
	//sql := "update emp set sal = sal + 500, comm = 1200 where deptno = 10"
	//sql := "update emp set empno = empno + 500, ename = 'LINJUNHONG' where deptno = 10"
	//sql := "update emp t1 set t1.sal = t1.sal + 500, t1.comm = 1200 where t1.deptno = 10"
	//sql := "update emp t1 set sal = sal + 500, comm = 1200 where t1.deptno = 10"
	//sql := "update emp t1 set t1.sal = sal + 500, comm = 1200 where deptno = 10"

	//sql := "update employees set ename = 'XXX', sal = sal + 2000 where empno = 7654"
	//sql := "update employees set ename = 'XXX' where empno = 7654"
	//sql := "update employees set ename = 'XXX', sal = sal + 2000 where deptno = 10"

	//sql := "update emp,dept set emp.sal = 4000, dept.loc = 'XXX' where emp.deptno = 20"
	//sql := "update emp t1,dept t2 set t1.sal = 4000, t2.loc = 'XXX' where t1.deptno = 20"
	//sql := "update emp t1,dept t2 set t1.ename = 'MEIXI', t2.loc = 'XXX' where t1.deptno = 20"
	//sql := "update emp t1, dept t2 set t1.ename = 'MEIXI', t1.empno = 1234, t2.loc = 'XXX' where t1.deptno = 20"
	//sql := "update emp,dept set emp.sal = 4000 where emp.deptno = 20"

	//sql := "update emp left join dept on emp.deptno = dept.deptno set emp.sal = 5000, dept.loc = 'YYY'"
	//sql := "update emp left join dept on emp.deptno = dept.deptno set emp.ename = 'MEIXI', dept.loc = 'YYY'"
	//sql := "update emp t1 left join dept t2 on t1.deptno = t2.deptno set t1.ename = 'MEIXI', t2.loc = 'YYY'"
	//sql := "update emp left join dept on emp.deptno = dept.deptno set emp.ename = 'MEIXI', dept.loc = 'YYY' where dept.deptno = 10"

	sql := "update emp, (select deptno from dept) as tx set emp.sal = 3333 where tx.deptno = emp.empno"

	mock := NewMockOptimizer()
	logicPlan, err := runOneStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, false, t)
}

// Single column unique index
func TestSingleTableDeleteSQL(t *testing.T) {
	mock := NewMockOptimizer()

	sqls := []string{
		"DELETE FROM emp where sal > 2000",
		"delete from emp t1 where t1.sal > 2000",
		"delete from emp where empno > 3000",
		"delete from emp where ename = 'SMITH'",
		"delete from dept where deptno = 10",
		"delete from dept where dname = 'RESEARCH'",
		"delete from dept where deptno = 10 order by dname limit 1",
		"delete from emp where deptno = 20 order by sal limit 2",
		"delete from emp where empno > 7800 order by empno limit 2",
	}

	runTestShouldPass(mock, t, sqls, false, false)
}

// Composite unique index
func TestCompositeUniqueIndexTableDeleteSQL(t *testing.T) {
	mock := NewMockOptimizer()

	sqls := []string{
		"delete from employees where sal > 2000",
		"delete from employees t1 where t1.sal > 2000",
		"delete from employees where empno > 3000",
		"delete from employees where ename = 'SMITH'",
		"delete from employees where empno = 7698",
		"delete from employees where empno = 7698 and ename = 'BLAKE'",
		"delete from employees where deptno = 20 order by sal limit 2",
		"delete from employees where empno > 7800 order by empno limit 2",
		"delete employees, dept from employees, dept where employees.deptno = dept.deptno and sal > 2000",
		"DELETE FROM employees, dept USING employees INNER JOIN dept WHERE employees.deptno = dept.deptno",
	}

	runTestShouldPass(mock, t, sqls, false, false)
}

// Single column unique index
func TestMultiTableDeleteSQL(t *testing.T) {
	mock := NewMockOptimizer()
	sqls := []string{
		"delete emp,dept from emp ,dept where emp.deptno = dept.deptno and emp.deptno = 10",
		"delete emp,dept from emp ,dept where emp.deptno = dept.deptno and sal > 2000",
		"delete t1,t2  from emp as t1,dept as t2 where t1.deptno = t2.deptno and t1.deptno = 10",
		"delete t1,dept from emp as t1,dept where t1.deptno = dept.deptno and t1.deptno = 10",
		"delete emp,dept from emp ,dept where emp.deptno = dept.deptno and empno > 7800",
		"delete emp,dept from emp ,dept where emp.deptno = dept.deptno and empno = 7839",
		"DELETE FROM emp, dept USING emp INNER JOIN dept WHERE emp.deptno = dept.deptno",
		"delete emp,dept from emp,dept where emp.deptno = dept.deptno",
		"delete emp from emp left join dept on emp.deptno = dept.deptno where dept.deptno = 20",
		"delete emp from emp left join dept on emp.deptno = dept.deptno where emp.sal > 2000",
		"delete t1 from emp as t1 left join dept as t2 on t1.deptno = t2.deptno where t1.sal > 2000",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

// Delete without index table
func TestWithoutIndexTableDeleteSQL(t *testing.T) {
	mock := NewMockOptimizer()

	sqls := []string{
		"delete from nation",
		"delete nation, nation2 from nation join nation2 on nation.n_name = nation2.n_name",
		"DELETE FROM NATION",
		"DELETE FROM NATION WHERE N_NATIONKEY > 10",
		"DELETE FROM NATION WHERE N_NATIONKEY > 10 ORDER BY N_NAME LIMIT 5",
		"DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20",
	}

	runTestShouldPass(mock, t, sqls, false, false)
}

func TestSingleTableUpdate(t *testing.T) {
	mock := NewMockOptimizer()

	sqls := []string{
		"update dept set dname = 'XXX' where deptno = 10",
		"update dept set deptno = '50' where loc = 'NEW YORK'",
		"update emp set comm = 1200 where deptno = 10",
		"update emp set sal = sal + 500, comm = 1200 where deptno = 10",
		"update emp set empno = empno + 500, ename = 'LINJUNHONG' where deptno = 10",
		"update employees set ename = 'XXX', sal = sal + 2000 where empno = 7654",
		"update employees set ename = 'XXX' where empno = 7654",
		"update employees set ename = 'XXX', sal = sal + 2000 where deptno = 10",
	}

	runTestShouldPass(mock, t, sqls, false, false)
}

func TestSingleTableWithAliasUpdate(t *testing.T) {
	mock := NewMockOptimizer()

	sqls := []string{
		"update emp t1 set t1.sal = t1.sal + 500, t1.comm = 1200 where t1.deptno = 10",
		"update emp t1 set sal = sal + 500, comm = 1200 where t1.deptno = 10",
		"update emp t1 set t1.sal = sal + 500, comm = 1200 where t1.deptno = 10",
		"update emp t1 set t1.sal = t1.sal + 500, comm = 1200 where t1.deptno = 10",
		"update emp t1 set t1.sal = sal + 500, comm = 1200 where deptno = 10",

		"update emp t1 set t1.empno = t1.empno + 500, t1.ename = 'LINJUNHONG' where t1.deptno = 10",
		"update emp t1 set empno = empno + 500, ename = 'LINJUNHONG' where t1.deptno = 10",
		"update emp t1 set empno = empno + 500, ename = 'LINJUNHONG' where deptno = 10",
		"update emp t1 set t1.empno = empno + 500, ename = 'LINJUNHONG' where t1.deptno = 10",
		"update emp t1 set t1.empno = t1.empno + 500, ename = 'LINJUNHONG' where t1.deptno = 10",
	}

	runTestShouldPass(mock, t, sqls, false, false)
}

func TestMultiTableUpdate(t *testing.T) {
	mock := NewMockOptimizer()

	sqls := []string{
		//1.-----------------
		"update emp,dept set emp.sal = 4000, dept.loc = 'XXX' where emp.deptno = 20",
		"update emp t1,dept t2 set t1.sal = 4000, t2.loc = 'XXX' where t1.deptno = 20",
		"update emp t1,dept t2 set t1.ename = 'MEIXI', t2.loc = 'XXX' where t1.deptno = 20",
		"update emp t1, dept t2 set t1.ename = 'MEIXI', t1.empno = 1234, t2.loc = 'XXX' where t1.deptno = 20",
		"update emp,dept set emp.sal = 4000 where emp.deptno = 20",

		//2.---------------- cross join
		"update emp t1, dept t2 set t1.sal = 5000, t2.loc = 'YYY' where t1.deptno = t2.deptno",
		"update emp t1, dept t2 set t1.empno = 5000, t1.ename = 'CLUO', t2.deptno = 50 where t1.deptno = t2.deptno",
		"update emp t1, dept t2 set empno = 5000, loc = 'YYY' where t1.deptno = t2.deptno",
		"update emp t1, dept t2 set t1.sal = 5000, loc = 'YYY' where t1.deptno = t2.deptno",

		//3.--------------- inner join
		"UPDATE emp t1 inner join dept t2 on t1.deptno = t2.deptno set t1.sal = t1.sal + 500",
		"UPDATE emp t1 inner join dept t2 on t1.deptno = t2.deptno set t1.sal = t1.sal + 500, ename = 'CLUO'",
		"UPDATE emp t1 inner join dept t2 on t1.deptno = t2.deptno set t1.sal = t1.sal + 500 where t2.dname = 'RESEARCH'",
		"UPDATE emp t1 inner join dept t2 on t1.deptno = t2.deptno set t1.empno = t2.deptno + 500 where t2.dname = 'RESEARCH'",

		//4.--------------- left join
		"update emp left join dept on emp.deptno = dept.deptno set emp.sal = 5000, dept.loc = 'YYY'",
		"update emp left join dept on emp.deptno = dept.deptno set emp.ename = 'MEIXI', dept.loc = 'YYY'",
		"update emp t1 left join dept t2 on t1.deptno = t2.deptno set t1.ename = 'MEIXI', t2.loc = 'YYY'",
		"update emp left join dept on emp.deptno = dept.deptno set emp.ename = 'MEIXI', dept.loc = 'YYY' where dept.deptno = 10",

		//5. ----------------sub query
		"update emp, (select deptno from dept) as tx set emp.sal = 3333 where tx.deptno = emp.empno",
	}

	runTestShouldPass(mock, t, sqls, false, false)
}
