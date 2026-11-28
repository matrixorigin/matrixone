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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestGeneratedOutgoingFKChecksUseSQLEquality(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	floatType := Type{Id: int32(types.T_float64)}
	intType := Type{Id: int32(types.T_int32)}
	builder := NewQueryBuilder(plan.Query_UPDATE, NewMockCompilerContext(true), false, false)
	builder.qry.Nodes = []*Node{{
		NodeType: plan.Node_PROJECT,
		ProjectList: []*Expr{
			GetColExpr(floatType, 0, 0), GetColExpr(intType, 0, 1), GetColExpr(floatType, 0, 2),
		},
		Stats: &plan.Stats{Outcnt: 4},
	}}
	table := &TableDef{
		Cols: []*ColDef{
			{Name: "g", ColId: 1, Typ: floatType},
			{Name: "tenant", ColId: 2, Typ: intType},
		},
		Fkeys: []*plan.ForeignKeyDef{{Cols: []uint64{1, 2}}},
	}
	nodeID, err := appendGeneratedOutgoingFKChecks(builder, nil, 0, 0, table, []int{0}, []int{2, 1})
	require.NoError(t, err)
	filters := builder.qry.Nodes[nodeID].FilterList
	require.Len(t, filters, 1)
	assertion := filters[0].GetF()
	require.NotNil(t, assertion)
	require.Equal(t, "assert", assertion.Func.ObjName)

	rows := batch.NewWithSize(3)
	rows.Vecs[0] = vector.NewVec(types.T_float64.ToType())
	rows.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	rows.Vecs[2] = vector.NewVec(types.T_float64.ToType())
	defer rows.Clean(proc.Mp())
	require.NoError(t, vector.AppendFixedList(rows.Vecs[0], []float64{0, 0, 1, 0}, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(rows.Vecs[1], []int32{3, 3, 3, 0}, []bool{false, false, false, true}, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(rows.Vecs[2], []float64{math.Copysign(0, -1), 1, 0, 1}, []bool{false, false, true, false}, proc.Mp()))
	rows.SetRowCount(4)

	// Signed zero is equal, a different non-NULL tuple is not, and a NULL
	// in either final component exempts the tuple from a parent match.
	allowed, free, err := colexec.GetReadonlyResultFromExpression(proc, assertion.Args[0], []*batch.Batch{rows})
	require.NoError(t, err)
	defer free()
	require.False(t, allowed.GetNulls().Any())
	require.Equal(t, []bool{true, false, true, true}, vector.MustFixedColNoTypeCheck[bool](allowed))
}

func TestGeneratedFKParentMustBeOutsideActionClosure(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	ctx.id2name = map[uint64]string{2: "outside", 3: "child", 4: "descendant", 5: "cycle_a", 6: "cycle_b"}
	ctx.tables = map[string]*TableDef{
		"outside":    {TblId: 2},
		"child":      {TblId: 3, Fkeys: []*plan.ForeignKeyDef{{ForeignTbl: 1}}},
		"descendant": {TblId: 4, Fkeys: []*plan.ForeignKeyDef{{ForeignTbl: 3}}},
		"cycle_a":    {TblId: 5, Fkeys: []*plan.ForeignKeyDef{{ForeignTbl: 6}}},
		"cycle_b":    {TblId: 6, Fkeys: []*plan.ForeignKeyDef{{ForeignTbl: 5}}},
	}
	builder := NewQueryBuilder(plan.Query_UPDATE, ctx, false, false)
	bindCtx := NewBindContext(builder, nil)
	for _, tc := range []struct {
		name         string
		root, parent uint64
		stable       bool
	}{
		{"independent", 1, 2, true},
		{"root", 1, 1, false},
		{"descendant", 1, 4, false},
		{"independent cycle", 1, 5, true},
		{"self reference", 1, 0, false},
		{"ambiguous root", 0, 2, false},
		{"missing definition", 1, 7, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stable, err := generatedFKParentIsStable(builder, bindCtx, tc.root, tc.parent)
			require.NoError(t, err)
			require.Equal(t, tc.stable, stable)
		})
	}
}

func TestReduceSinkScanKeepsFKMarkJoinInputs(t *testing.T) {
	q := &Query{
		Nodes: []*Node{
			{NodeType: plan.Node_LOCK_OP},
			{NodeType: plan.Node_SINK, Children: []int32{0}},
			{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}},
			{NodeType: plan.Node_TABLE_SCAN},
			{NodeType: plan.Node_JOIN, JoinType: plan.Node_MARK, Children: []int32{2, 3}},
		},
		Steps: []int32{1, 4},
	}
	reduceSinkSinkScanNodes(q)
	require.Equal(t, []int32{1, 4}, q.Steps)
	require.Equal(t, []int32{2, 3}, q.Nodes[4].Children)
	require.Equal(t, []int32{0}, q.Nodes[2].SourceStep)
}
