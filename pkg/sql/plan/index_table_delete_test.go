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
	sql := "delete from emp where ename = 'SMITH'"
	//sql := "delete from dept where deptno = 10"

	mock := NewMockOptimizer()
	logicPlan, err := runOneStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, false, t)
}

func TestSingleTableDeleteSQL(t *testing.T) {
	mock := NewMockOptimizer()

	sqls := []string{
		"DELETE FROM emp where sal > 2000",
		"delete from emp t1 where t1.sal > 2000",
	}

	runTestShouldPass(mock, t, sqls, false, false)
}

func TestMultiTableDeleteSQL(t *testing.T) {
	mock := NewMockOptimizer()
	sqls := []string{
		"delete emp,dept from emp ,dept where emp.deptno = dept.deptno and emp.deptno = 10",
		"delete t1,t2  from emp as t1,dept as t2 where t1.deptno = t2.deptno and t1.deptno = 10",
		"delete t1,dept from emp as t1,dept where t1.deptno = dept.deptno and t1.deptno = 10",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}
