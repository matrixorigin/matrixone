// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// collectAssignmentFunctionExprs gathers every function plan expression whose
// overload carries one of the given names, walking projection lists and the
// VALUES rowset data of every query node.
func collectAssignmentFunctionExprs(queryPlan *plan.Plan, names ...string) []*plan.Expr {
	wanted := make(map[string]bool, len(names))
	for _, name := range names {
		wanted[name] = true
	}
	var out []*plan.Expr
	var walk func(expr *plan.Expr)
	walk = func(expr *plan.Expr) {
		if expr == nil {
			return
		}
		if fn := expr.GetF(); fn != nil {
			if wanted[fn.Func.GetObjName()] {
				out = append(out, expr)
			}
			for _, arg := range fn.Args {
				walk(arg)
			}
		}
		if list := expr.GetList(); list != nil {
			for _, arg := range list.List {
				walk(arg)
			}
		}
	}
	query := queryPlan.GetQuery()
	if query == nil {
		return nil
	}
	for _, node := range query.Nodes {
		for _, expr := range node.ProjectList {
			walk(expr)
		}
		if node.RowsetData != nil {
			for _, col := range node.RowsetData.Cols {
				for _, row := range col.Data {
					if row != nil {
						walk(row.Expr)
					}
				}
			}
		}
	}
	return out
}

func buildUnsignedDecimalNullInsertPlan(t *testing.T, sql string) *plan.Plan {
	t.Helper()
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(true, proc)
	withUnsignedDecimalAuthoringFloor(t, proc)
	newUnsignedDecimalMockTable(ctx)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	return built
}

// findNullRowsetAssignment returns the VALUES-rowset assignment expression for
// the attributed DECIMAL column: the plain cast wrapper over the scalar NULL.
func findNullRowsetAssignment(t *testing.T, queryPlan *plan.Plan) *plan.Expr {
	t.Helper()
	query := queryPlan.GetQuery()
	require.NotNil(t, query)
	for _, node := range query.Nodes {
		if node.RowsetData == nil {
			continue
		}
		for _, col := range node.RowsetData.Cols {
			for _, row := range col.Data {
				if row == nil || row.Expr == nil {
					continue
				}
				expr := row.Expr
				if fn := expr.GetF(); fn != nil &&
					types.T(expr.Typ.Id).IsDecimal() &&
					expr.Typ.NumericAttributes&types.NumericAttributeUnsigned != 0 {
					return expr
				}
			}
		}
	}
	t.Fatalf("no attributed DECIMAL rowset assignment found: %s", queryPlan.String())
	return nil
}

// TestUnsignedDecimalValuesNullAssignment pins the scalar-NULL assignment
// boundary (round-26 review F1). VALUES(NULL) sources bind target-free as
// T_any; an admission node composed around them used to return T_any, and the
// executor's result allocation has no ANY case (a panic). The shared
// NULL-aware composition degrades a null-identity source to the plain cast
// wrapper at the rowset layer on BOTH the ordinary and the IGNORE axis; the
// projection layer keeps its policy admission over the typed scan column, and
// NO admission anywhere carries the unallocatable ANY type. The executor-level
// assertions construct the executor over the actual planned admission node and
// evaluate the actual planned rowset expression, so the allocatable planned
// result type is proven from the plan itself rather than assumed.
func TestUnsignedDecimalValuesNullAssignment(t *testing.T) {
	admissionNames := []string{
		unsignedDecimalAdmissionName,
		unsignedDecimalAdmissionStrictName,
		unsignedDecimalAdmissionIgnoreName,
	}

	assertAdmissionShapesAllocatable := func(t *testing.T, sql string, wantCount int, wantNames ...string) []*plan.Expr {
		built := buildUnsignedDecimalNullInsertPlan(t, sql)
		admissions := collectAssignmentFunctionExprs(built, admissionNames...)
		require.Len(t, admissions, wantCount,
			"unexpected admission nodes in %q: %s", sql, built.String())
		for _, name := range wantNames {
			require.NotEmpty(t, collectAssignmentFunctionExprs(built, name),
				"the statement policy's admission entry %s must appear in %q: %s", name, sql, built.String())
		}
		for _, admission := range admissions {
			require.NotZero(t, admission.Typ.Id,
				"an admission node must never carry the ANY type (the executor cannot allocate it): %s", built.String())
			// Executor-level assertion over the actual planned node: result
			// allocation from the planned type must succeed.
			proc := newPlanTestProcess(t)
			withUnsignedDecimalAuthoringFloor(t, proc)
			executor, err := colexec.NewExpressionExecutor(proc, admission)
			require.NoError(t, err)
			executor.Free()
		}
		return admissions
	}

	assertRowsetTypedNull := func(t *testing.T, sql string) {
		built := buildUnsignedDecimalNullInsertPlan(t, sql)
		expr := findNullRowsetAssignment(t, built)
		proc := newPlanTestProcess(t)
		withUnsignedDecimalAuthoringFloor(t, proc)
		result, free, err := colexec.GetReadonlyResultFromExpression(
			proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
		require.NoError(t, err,
			"executor construction over the planned null rowset assignment must not panic or fail")
		defer free()
		require.Equal(t, types.T_decimal64, result.GetType().Oid,
			"the planned rowset result type is the attributed DECIMAL target")
		require.Equal(t, int32(2), result.GetType().Scale)
		require.True(t, result.GetNulls().Contains(0),
			"casting NULL yields a typed NULL, never an error or a zero")
	}

	t.Run("ordinary values null: scalar null degrades; typed policy admission stands", func(t *testing.T) {
		admissions := assertAdmissionShapesAllocatable(t,
			"insert into ud_ins(a) values (null)", 1, unsignedDecimalAdmissionName)
		require.Equal(t, types.T_decimal64, types.T(admissions[0].Typ.Id),
			"the projection admission polices the typed scan column, not the ANY source")
		assertRowsetTypedNull(t, "insert into ud_ins(a) values (null)")
	})

	t.Run("insert ignore values null degrades both layers without losing the ignore entry", func(t *testing.T) {
		admissions := assertAdmissionShapesAllocatable(t,
			"insert ignore into ud_ins(a) values (null)", 1, unsignedDecimalAdmissionIgnoreName)
		require.Equal(t, types.T_decimal64, types.T(admissions[0].Typ.Id))
		assertRowsetTypedNull(t, "insert ignore into ud_ins(a) values (null)")
	})

	t.Run("null and literal coexist in one statement; every admission is typed", func(t *testing.T) {
		admissions := assertAdmissionShapesAllocatable(t,
			"insert into ud_ins(a) values (null),(1.5)", 2, unsignedDecimalAdmissionName)
		for _, admission := range admissions {
			require.NotZero(t, admission.Typ.Id)
		}
		assertRowsetTypedNull(t, "insert into ud_ins(a) values (null),(1.5)")
	})
}
