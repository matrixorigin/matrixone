// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestGroupingDistinctValueKeys(t *testing.T) {
	for _, typ := range []plan.Type{
		{Id: int32(types.T_enum), Enumvalues: "b,a,c"},
		{Id: int32(types.T_uint64), Enumvalues: "b,a,c"},
		{Id: int32(types.T_varchar), Width: 32},
		{Id: int32(types.T_varbinary), Width: 32},
		{Id: int32(types.T_binary), Width: 4},
		{Id: int32(types.T_int32)},
		{Id: int32(types.T_int64)},
		{Id: int32(types.T_decimal128), Width: 20, Scale: 4},
		{Id: int32(types.T_decimal256), Width: 50, Scale: 8},
		{Id: int32(types.T_char), Width: 4},
		{Id: int32(types.T_text)},
		{Id: int32(types.T_json)},
		{Id: int32(types.T_uuid)},
		{Id: int32(types.T_timestamp), Scale: 6},
		{Id: int32(types.T_float32), Width: 8, Scale: 2},
		{Id: int32(types.T_geometry)},
		{Id: int32(types.T_geometry32)},
	} {
		for _, suffix := range []string{" with rollup", ""} {
			name := types.T(typ.Id).String() + typ.Enumvalues + suffix
			t.Run(name, func(t *testing.T) {
				mock := NewMockOptimizer(false)
				mock.ctxt.tables["nation"] = DeepCopyTableDef(mock.ctxt.tables["nation"], true)
				for _, col := range mock.ctxt.tables["nation"].Cols {
					if col.Name == "n_name" {
						col.Typ = typ
					}
				}
				p, err := buildSingleStmt(mock, t,
					"select distinct n_name k from nation group by n_name"+suffix+" order by k")
				require.NoError(t, err)
				q := p.GetQuery()
				var hasUnion func(int32) bool
				hasUnion = func(id int32) bool {
					n := q.Nodes[id]
					if n.NodeType == plan.Node_UNION_ALL {
						return true
					}
					for _, child := range n.Children {
						if hasUnion(child) {
							return true
						}
					}
					return false
				}
				found := 0
				for _, n := range q.Nodes {
					if n.NodeType != plan.Node_AGG || !hasUnion(n.NodeId) {
						continue
					}
					found++
					require.Len(t, n.GroupBy, 1, "only visible output participates in DISTINCT")
					key := n.GroupBy[0]
					require.NotNil(t, key.GetF(), "global DISTINCT must materialize SQL values, not grouping provenance")
					require.Equal(t, "values", key.GetF().Func.ObjName)
					require.Len(t, key.GetF().Args, 1)
					require.NotNil(t, key.GetF().Args[0].GetCol())
					require.Equal(t, key.GetF().Args[0].Typ, key.Typ, "normalization preserves the exact result type")
					require.False(t, key.Typ.NotNullable)
					if makeTypeByPlan2Expr(key).IsVarlen() {
						checkGroupingDistinctStringValues(t, key)
					}
					if typ.Id == int32(types.T_float32) {
						checkGroupingDistinctFloatValues(t, key)
					}
				}
				if suffix != "" {
					require.Equal(t, 1, found, "test must reach the optimized global DISTINCT")
				} else {
					require.Zero(t, found, "ordinary GROUP BY has no grouping transport")
				}
			})
		}
	}
}

func checkGroupingDistinctFloatValues(t *testing.T, key *plan.Expr) {
	t.Helper()
	proc := testutil.NewProcess(t)
	input := vector.NewVec(makeTypeByPlan2Expr(key))
	t.Cleanup(func() { input.Free(proc.Mp()) })
	// FLOAT(8,2) can store this after rounding an accepted 999999.99 input.
	for _, isNull := range []bool{true, true, false, false} {
		require.NoError(t, vector.AppendFixed(input, float32(1000000), isNull, proc.Mp()))
	}
	input.GetGrouping().Add(1)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = input
	bat.SetRowCount(4)
	expr := DeepCopyExpr(key)
	expr.GetF().Args[0].GetCol().RelPos = 0
	expr.GetF().Args[0].GetCol().ColPos = 0
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	for i := 0; i < 2; i++ {
		output, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err, "materializing stored values must not repeat assignment validation")
		require.Equal(t, input.GetType(), output.GetType())
		require.Equal(t, float32(1000000), vector.GetFixedAtNoTypeCheck[float32](output, 2))
		require.False(t, output.HasGrouping())
		require.True(t, output.GetNulls().Contains(0))
		require.True(t, output.GetNulls().Contains(1))
		require.True(t, input.GetGrouping().Contains(1))
	}
}

func checkGroupingDistinctStringValues(t *testing.T, key *plan.Expr) {
	t.Helper()
	proc := testutil.NewProcess(t)
	input := vector.NewVec(makeTypeByPlan2Expr(key))
	t.Cleanup(func() { input.Free(proc.Mp()) })
	value := []byte("a")
	if key.Typ.Id == int32(types.T_binary) {
		value = make([]byte, key.Typ.Width)
		copy(value, "a")
	}
	if key.Typ.Id == int32(types.T_json) {
		json, err := types.ParseStringToByteJson(`"a"`)
		require.NoError(t, err)
		value, err = json.Marshal()
		require.NoError(t, err)
	}
	if key.Typ.Id == int32(types.T_geometry) || key.Typ.Id == int32(types.T_geometry32) {
		geometry, err := geo.ParseWKT("LINESTRING(0 0,1 1,2 2,3 3)")
		require.NoError(t, err)
		value = geo.WriteWKB(geometry)
		if key.Typ.Id == int32(types.T_geometry32) {
			value = geo.WriteWKBFloat32(geometry)
		}
		proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
			if name == "max_points_in_geometry" {
				return int64(3), nil
			}
			return nil, nil
		})
	}
	for _, isNull := range []bool{true, true, false, false} {
		require.NoError(t, vector.AppendBytes(input, value, isNull, proc.Mp()))
	}
	input.GetGrouping().Add(1)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = input
	bat.SetRowCount(4)
	expr := DeepCopyExpr(key)
	expr.GetF().Args[0].GetCol().RelPos = 0
	expr.GetF().Args[0].GetCol().ColPos = 0
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	for i := 0; i < 2; i++ {
		output, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Equal(t, input.GetType(), output.GetType())
		require.False(t, output.HasGrouping(), "DISTINCT compares SQL NULLs, not their origin")
		require.True(t, output.GetNulls().Contains(0))
		require.True(t, output.GetNulls().Contains(1))
		require.Equal(t, value, output.GetBytesAt(2))
		require.True(t, input.GetGrouping().Contains(1), "normalization cannot mutate shared input")
		func() {
			m, err := hashmap.NewStrHashMap(true, proc.Mp())
			require.NoError(t, err)
			defer m.Free()
			groups, _, err := m.NewIterator().Insert(0, 4, []*vector.Vector{output})
			require.NoError(t, err)
			require.Equal(t, groups[0], groups[1], "actual and subtotal NULL must be one group")
			require.Equal(t, groups[2], groups[3], "equal non-NULL values must still deduplicate")
			require.NotEqual(t, groups[0], groups[2])
		}()
	}
}
