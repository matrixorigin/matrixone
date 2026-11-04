// Copyright 2026 Matrix Origin
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

	"github.com/stretchr/testify/require"
)

// Issue #30540: a foreign-key violation raised by INSERT-family statements
// used to surface as "internal error: Cannot add or update a child row ..."
// with the internal code 20101, because the plan embedded a 2-argument
// untyped assert(). The typed 3-argument assert reports
// ErrFKNoReferencedRow2, which maps to MySQL errno 1452
// (ER_NO_REFERENCED_ROW_2) — the code a MySQL client expects.
func TestBindInsertForeignKeyUsesTypedAssert(t *testing.T) {
	prepareEmpDept := func(mock *MockOptimizer) {
		emp := mock.ctxt.tables["emp"]
		dept := mock.ctxt.tables["dept"]
		require.NotEmpty(t, emp.Fkeys)
		emp.TblId = 88887
		emp.Fkeys[0].ForeignTbl = dept.TblId
		emp.Fkeys[0].ForeignCols = []uint64{0}
		dept.RefChildTbls = []uint64{emp.TblId}
		mock.ctxt.id2name[emp.TblId] = "emp"
	}

	t.Run("plain insert", func(t *testing.T) {
		mock := NewMockOptimizer(true, newPlanTestProcess(t))
		prepareEmpDept(mock)

		logicPlan, err := runOneStmt(mock, t, "INSERT INTO emp(deptno) VALUES (2)")
		require.NoError(t, err)
		require.True(t, updateFkPlanContainsTypedAssert(
			logicPlan.GetQuery(),
			foreignKeyNoReferencedRowAssert,
		))
	})

	t.Run("insert on duplicate key update", func(t *testing.T) {
		mock := NewMockOptimizer(true, newPlanTestProcess(t))
		prepareEmpDept(mock)

		logicPlan, err := runOneStmt(mock, t,
			"INSERT INTO emp(empno, deptno) VALUES (1, 2) ON DUPLICATE KEY UPDATE deptno = 2")
		require.NoError(t, err)
		require.True(t, updateFkPlanContainsTypedAssert(
			logicPlan.GetQuery(),
			foreignKeyNoReferencedRowAssert,
		))
	})

	t.Run("replace", func(t *testing.T) {
		mock := NewMockOptimizer(true, newPlanTestProcess(t))
		prepareEmpDept(mock)

		logicPlan, err := runOneStmt(mock, t, "REPLACE INTO emp(empno, deptno) VALUES (1, 2)")
		require.NoError(t, err)
		require.True(t, updateFkPlanContainsTypedAssert(
			logicPlan.GetQuery(),
			foreignKeyNoReferencedRowAssert,
		))
	})
}
