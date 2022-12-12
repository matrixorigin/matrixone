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
	//sql := "delete from emp where empno > 3000"
	//sql := "delete from emp where ename = 'SMITH'"
	//sql := "delete from dept where deptno = 10"
	//sql := "delete from dept where dname = 'xxx'"
	//sql := "delete emp,dept from emp ,dept where emp.deptno = dept.deptno and emp.deptno = 10"
	sql := "delete from dept where dname = 'RESEARCH'"

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
		"delete employees, dept from employees, dept where employees.deptno = dept.deptno and sal > 2000",
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
	}
	runTestShouldPass(mock, t, sqls, false, false)
}
