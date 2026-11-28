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
