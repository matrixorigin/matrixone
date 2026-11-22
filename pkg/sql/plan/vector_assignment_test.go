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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestVectorAssignmentChecksActualDimension(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float32, types.T_array_float64, types.T_array_bf16,
		types.T_array_float16, types.T_array_int8, types.T_array_uint8} {
		t.Run(oid.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			typ := types.New(oid, 2, 0)
			pt := planpb.Type{Id: int32(oid), Width: 2}
			source := &Expr{Typ: pt, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}
			for _, ddl := range []bool{false, true} {
				var expr *Expr
				var err error
				if ddl {
					expr, err = makePlan2AssignmentCastExpr(proc.Ctx, DeepCopyExpr(source), pt)
				} else {
					expr, err = forceAssignmentCastExpr(proc.Ctx, DeepCopyExpr(source), pt)
				}
				require.NoError(t, err)
				require.NotNil(t, expr.GetF(), "相同元数据不能省略赋值检查")
				exec, err := colexec.NewExpressionExecutor(proc, expr)
				require.NoError(t, err)
				t.Cleanup(func() { exec.Free() })
				bat := batch.NewWithSize(1)
				t.Cleanup(func() { bat.Clean(proc.Mp()) })
				bat.Vecs[0] = vector.NewVec(typ, proc.Mp())
				require.NoError(t, vector.AppendBytes(bat.Vecs[0], make([]byte, 3*typ.GetArrayElementSize()), false, proc.Mp()))
				bat.SetRowCount(1)
				_, err = exec.Eval(proc, []*batch.Batch{bat}, nil)
				require.ErrorContains(t, err, "expected vector dimension 2 != actual dimension 3")
			}
			expr, err := forceCastExpr2(proc.Ctx, DeepCopyExpr(source), typ, &Expr{Typ: pt, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}})
			require.NoError(t, err)
			require.NotNil(t, expr.GetF())

			// 动态目标不新增冗余赋值复制；显式转换优化也保持原契约。
			pt.Width = types.MaxArrayDimension
			source.Typ = pt
			expr, err = forceAssignmentCastExpr(proc.Ctx, source, pt)
			require.NoError(t, err)
			require.Same(t, source, expr)
		})
	}
}

func TestVectorGeneratedColumnAssignment(t *testing.T) {
	proc := testutil.NewProcess(t)
	stmt, err := mysql.ParseOne(proc.Ctx, "create table t (a vecf32(2), g vecf32(2) as (greatest(a,a)) stored)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	col := stmt.(*tree.CreateTable).Defs[1].(*tree.ColumnTableDef)
	pt := planpb.Type{Id: int32(types.T_array_float32), Width: 2}
	gen, err := buildGeneratedExpr(proc.Ctx, col, pt, []*ColDef{{Name: "a", Typ: pt}}, proc)
	require.NoError(t, err)
	require.Equal(t, "cast", gen.Expr.GetF().Func.ObjName)
	require.Equal(t, "greatest", gen.Expr.GetF().Args[0].GetF().Func.ObjName)

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	legacy := gen.Expr.GetF().Args[0]
	for _, ignore := range []bool{false, true} {
		checked, err := builder.applyGeneratedColumnAssignmentCast(DeepCopyExpr(legacy), ignore)
		require.NoError(t, err)
		require.Equal(t, "cast", checked.GetF().Func.ObjName)
		require.Equal(t, "greatest", checked.GetF().Args[0].GetF().Func.ObjName)
		again, err := builder.applyGeneratedColumnAssignmentCast(checked, ignore)
		require.NoError(t, err)
		require.Same(t, checked, again, "已有的赋值检查不重复复制向量")
		exec, err := colexec.NewExpressionExecutor(proc, checked)
		require.NoError(t, err)
		t.Cleanup(func() { exec.Free() })
		bat := batch.NewWithSize(1)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		bat.Vecs[0] = vector.NewVec(types.New(types.T_array_float32, 2, 0), proc.Mp())
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], types.ArrayToBytes([]float32{1, 2, 3}), false, proc.Mp()))
		bat.SetRowCount(1)
		_, err = exec.Eval(proc, []*batch.Batch{bat}, nil)
		require.ErrorContains(t, err, "expected vector dimension 2 != actual dimension 3")
	}
}

func BenchmarkVectorAssignmentDimensionCheck(b *testing.B) {
	for _, width := range []int32{2, 768} {
		b.Run(fmt.Sprintf("width=%d", width), func(b *testing.B) {
			proc := testutil.NewProcess(b)
			typ := types.New(types.T_array_float32, width, 0)
			pt := planpb.Type{Id: int32(typ.Oid), Width: width}
			source := &Expr{Typ: pt, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}
			checked, err := forceAssignmentCastExpr(proc.Ctx, source, pt)
			require.NoError(b, err)
			exec, err := colexec.NewExpressionExecutor(proc, checked)
			require.NoError(b, err)
			defer exec.Free()
			bat := batch.NewWithSize(1)
			defer bat.Clean(proc.Mp())
			bat.Vecs[0] = vector.NewVec(typ, proc.Mp())
			payload := make([]byte, int(width)*typ.GetArrayElementSize())
			for range 1024 {
				require.NoError(b, vector.AppendBytes(bat.Vecs[0], payload, false, proc.Mp()))
			}
			bat.SetRowCount(1024)
			inputs := []*batch.Batch{bat}
			_, err = exec.Eval(proc, inputs, nil)
			require.NoError(b, err)
			b.ReportAllocs()
			b.SetBytes(int64(len(payload) * 1024))
			b.ResetTimer()
			for range b.N {
				if _, err := exec.Eval(proc, inputs, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestVectorSelectionSQLMetadata(t *testing.T) {
	for _, test := range []struct {
		sql string
		oid types.T
	}{
		{"coalesce(cast(null as vecf32(2)),cast('[1e300,-1e300]' as vecf64(2)))", types.T_array_float64},
		{"coalesce(cast('[1e300,-1e300]' as vecf64(2)),cast(null as vecf32(2)))", types.T_array_float64},
		{"sqrt(cast('[1,4]' as vecf32(2)))", types.T_array_float64},
		{"coalesce(null,cast('[1,2]' as vecint8(2)),cast('[3,4]' as vecint8(2)))", types.T_array_int8},
	} {
		t.Run(test.sql, func(t *testing.T) {
			pl, err := buildMySQLDMLCompatibilityPlan(t, "select "+test.sql)
			require.NoError(t, err)
			q := pl.GetQuery()
			result := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			require.Equal(t, int32(test.oid), result.Typ.Id)
			require.Equal(t, int32(2), result.Typ.Width)
		})
	}
	for _, name := range []string{"coalesce", "least", "greatest"} {
		_, err := buildMySQLDMLCompatibilityPlan(t, fmt.Sprintf("select %s(cast('[1,2]' as vecf32(2)),cast('[1,2,3]' as vecf32(3)))", name))
		require.Error(t, err)
	}
}

func TestArithmeticNullCommonTypeCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"coalesce left", "select coalesce(null + 1, cast('[1,2]' as vecf32(2)))"},
		{"coalesce right", "select coalesce(cast('[1,2]' as vecf32(2)), null + 1)"},
		{"ifnull", "select ifnull(null + 1, cast('[1,2]' as vecf32(2)))"},
		{"if", "select if(false, null + 1, cast('[1,2]' as vecf32(2)))"},
		{"case", "select case when false then null + 1 else cast('[1,2]' as vecf32(2)) end"},
		{"greatest", "select greatest(null + 1, cast('[1,2]' as vecf32(2)))"},
		{"least", "select least(null + 1, cast('[1,2]' as vecf32(2)))"},
		{"union", "select null + 1 union all select cast('[1,2]' as vecf32(2))"},
		{"derived union", "select x from (select null + 1 as x) d union all select cast('[1,2]' as vecf32(2))"},
		{"values", "select * from (values row(null + 1), row(cast('[1,2]' as vecf32(2)))) a"},
		{"scalar", "select coalesce((select null + 1), cast('[1,2]' as vecf32(2)))"},
		{"scalar derived", "select coalesce((select x from (select null + 1 as x) d), cast('[1,2]' as vecf32(2)))"},
		{"scalar correlated output", "select coalesce((select d.x), d.v) from (select null + 1 as x, cast('[1,2]' as vecf32(2)) as v) d"},
		{"scalar deep correlated output", "select coalesce((select (select d.x)), d.v) from (select null + 1 as x, cast('[1,2]' as vecf32(2)) as v) d"},
		{"scalar union", "select (select null + 1 from (select 1 union all select 2) q) union all select cast('[1,2]' as vecf32(2))"},
		{"correlated scalar", "select (select coalesce(d.x, d.v)) from (select null + 1 as x, cast('[1,2]' as vecf32(2)) as v) d"},
		{"deep correlated scalar", "select (select (select if(false, d.x, d.v))) from (select null + 1 as x, cast('[1,2]' as vecf32(2)) as v) d"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pl, err := buildMySQLDMLCompatibilityPlan(t, tc.sql)
			require.NoError(t, err)
			q := pl.GetQuery()
			result := q.Nodes[q.Steps[q.MainOutputStep()]].ProjectList[0]
			require.Equal(t, int32(types.T_array_float32), result.Typ.Id)
			require.Equal(t, int32(2), result.Typ.Width)
		})
	}

	for _, sql := range []string{
		"select coalesce(cast(null as signed), cast('[1,2]' as vecf32(2)))",
		"select coalesce((select cast(null as signed)), cast('[1,2]' as vecf32(2)))",
		"select coalesce(1 / 0, cast('[1,2]' as vecf32(2)))",
		"select coalesce(null + 1, cast('[1,2]' as vecf32(2)), cast('[1,2,3]' as vecf32(3)))",
	} {
		t.Run("invalid/"+sql, func(t *testing.T) {
			_, err := buildMySQLDMLCompatibilityPlan(t, sql)
			require.Error(t, err)
		})
	}
}

func TestArithmeticNullCommonTypeMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, withArithmetic, withBare string
	}{
		{"text width", "select coalesce(null + 1, 'x')", "select coalesce(null, 'x')"},
		{"date", "select if(false, null + 1, cast('2024-01-01' as date))", "select if(false, null, cast('2024-01-01' as date))"},
		{"decimal", "select if(false, null + 1, cast(12.34 as decimal(10,2)))", "select if(false, null, cast(12.34 as decimal(10,2)))"},
		{"binary", "select coalesce(null + 1, cast('x' as binary(4)))", "select coalesce(null, cast('x' as binary(4)))"},
		{"collation", "select coalesce(null + 1, cast('x' as char(3)) collate utf8mb4_unicode_ci)", "select coalesce(null, cast('x' as char(3)) collate utf8mb4_unicode_ci)"},
		{"all-null conditional", "select coalesce(null + 1, null + 2)", "select coalesce(null, null)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := buildMySQLDMLCompatibilityPlan(t, tc.withArithmetic)
			require.NoError(t, err)
			control, err := buildMySQLDMLCompatibilityPlan(t, tc.withBare)
			require.NoError(t, err)
			actualQuery, controlQuery := actual.GetQuery(), control.GetQuery()
			actualType := actualQuery.Nodes[actualQuery.Steps[len(actualQuery.Steps)-1]].ProjectList[0].Typ
			controlType := controlQuery.Nodes[controlQuery.Steps[len(controlQuery.Steps)-1]].ProjectList[0].Typ
			require.Equal(t, controlType, actualType)
		})
	}

	pl, err := buildMySQLDMLCompatibilityPlan(t, "select null + 1, coalesce(null + 1, cast('[1,2]' as vecf32(2)))")
	require.NoError(t, err)
	q := pl.GetQuery()
	results := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList
	require.Equal(t, int32(types.T_int64), results[0].Typ.Id, "standalone arithmetic keeps its concrete type")
	require.Equal(t, int32(types.T_array_float32), results[1].Typ.Id)
	require.Equal(t, int32(2), results[1].Typ.Width)
}
