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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestSetColumnComparisonUsesMemberValues(t *testing.T) {
	tests := []struct {
		name, rightMembers, expr string
		want                     []bool
		wantNull                 bool
	}{
		{"different order", "b,a", "n_name = n_regionkey", []bool{true, true, false, false, true, false}, true},
		{"different order reversed operands", "b,a", "n_regionkey = n_name", []bool{true, true, false, false, true, false}, true},
		{"different order not equal", "b,a", "n_name <> n_regionkey", []bool{false, false, true, true, false, true}, true},
		{"same order", "a,b", "n_name = n_regionkey", []bool{false, false, true, true, true, true}, true},
		{"numeric column", "b,a", "n_name = n_nationkey", []bool{true, true, true, true, true, true}, true},
		{"null-safe equality", "b,a", "n_name <=> n_regionkey", []bool{true, true, false, false, true, false, true}, false},
		{"unary numeric SET", "b,a", "n_name = +n_regionkey", []bool{false, false, true, true, true, true}, true},
		{"numeric cast SET", "b,a", "n_name = cast(n_regionkey as unsigned)", []bool{false, false, true, true, true, true}, true},
		{"numeric literal", "b,a", "n_name = 1", []bool{true, false, true, false, false, false}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			mock := NewMockOptimizer(false, proc)
			mock.ctxt.tables["nation"].Cols[1].Typ = planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b"}
			mock.ctxt.tables["nation"].Cols[2].Typ = planpb.Type{Id: int32(types.T_uint64), Enumvalues: tc.rightMembers}
			pl, err := runOneExprStmt(mock, t, "select "+tc.expr+" from nation")
			require.NoError(t, err)
			expr := DeepCopyExpr(pl.GetQuery().Nodes[1].ProjectList[0])
			setComparisonInputRelPos(expr)
			input := batch.NewWithSize(3)
			defer input.Clean(proc.Mp())
			input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			input.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
			input.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
			left := []uint64{1, 2, 1, 2, 0, 3, 0}
			right := []uint64{2, 1, 1, 2, 0, 3, 1}
			numeric := []int32{1, 2, 1, 2, 0, 3, 0}
			for i := range left {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], numeric[i], false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(input.Vecs[1], left[i], i == 6, proc.Mp()))
				require.NoError(t, vector.AppendFixed(input.Vecs[2], right[i], i == 6, proc.Mp()))
			}
			input.SetRowCount(len(left))
			exec, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer exec.Free()
			result, err := exec.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			for i, want := range tc.want {
				require.False(t, result.GetNulls().Contains(uint64(i)), "row %d", i)
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result, i), "row %d", i)
			}
			require.Equal(t, tc.wantNull, result.GetNulls().Contains(6))
		})
	}
}

// Bind the optimized expression's columns to the corresponding input vectors.
func setComparisonInputRelPos(expr *planpb.Expr) {
	if col := expr.GetCol(); col != nil {
		col.RelPos = 0
		switch {
		case strings.HasSuffix(col.Name, ".n_nationkey"):
			col.ColPos = 0
		case strings.HasSuffix(col.Name, ".n_name"):
			col.ColPos = 1
		case strings.HasSuffix(col.Name, ".n_regionkey"):
			col.ColPos = 2
		default:
			panic("unexpected column: " + col.Name)
		}
	}
	if f := expr.GetF(); f != nil {
		for _, arg := range f.Args {
			setComparisonInputRelPos(arg)
		}
	}
}

func TestSetColumnJoinUsesMemberValues(t *testing.T) {
	proc := newPlanTestProcess(t)
	mock := NewMockOptimizer(false, proc)
	mock.ctxt.tables["nation"].Cols[1].Typ = planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b"}
	mock.ctxt.tables["nation"].Cols[2].Typ = planpb.Type{Id: int32(types.T_uint64), Enumvalues: "b,a"}
	pl, err := runOneExprStmt(mock, t, "select l.n_nationkey, r.n_nationkey from nation l join nation r on l.n_name = r.n_regionkey")
	require.NoError(t, err)
	var on []*planpb.Expr
	for _, node := range pl.GetQuery().Nodes {
		if node.NodeType == planpb.Node_JOIN {
			on = append(on, node.OnList...)
		}
	}
	require.NotEmpty(t, on)
	for _, predicate := range on {
		equality := predicate.GetF()
		require.NotNil(t, equality)
		require.Equal(t, "=", equality.Func.ObjName)
		require.Len(t, equality.Args, 2)
		require.True(t, containsEnumOrSetDisplayValue(equality.Args[0]))
		require.True(t, containsEnumOrSetDisplayValue(equality.Args[1]))
	}
}
