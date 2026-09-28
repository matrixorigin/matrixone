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
				bat.Vecs[0] = vector.NewVec(typ)
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

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
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
		bat.Vecs[0] = vector.NewVec(types.New(types.T_array_float32, 2, 0))
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
			bat.Vecs[0] = vector.NewVec(typ)
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
