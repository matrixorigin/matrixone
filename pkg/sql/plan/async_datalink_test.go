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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAsyncDatalinkColumnsAndFinalBinding(t *testing.T) {
	table := &TableDef{Name: "docs", Cols: []*planpb.ColDef{{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}}, {Name: "path", Typ: planpb.Type{Id: int32(types.T_datalink)}}}, Indexes: []*planpb.IndexDef{
		{IndexAlgo: "fulltext2", Parts: []string{"path"}},
		{IndexAlgo: "fulltext2", Parts: []string{"path"}},
	}}
	columns, err := asyncDatalinkColumns(table)
	require.NoError(t, err)
	require.Equal(t, []string{"path"}, columns)
	table.Indexes = nil // modern DML deliberately strips the original definitions.
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	builder := NewQueryBuilder(planpb.Query_SELECT, mock.CurrentContext(), false, true)
	ctx := NewBindContext(builder, nil)
	builder.qry.Nodes = []*planpb.Node{{NodeType: planpb.Node_PROJECT, Stats: &planpb.Stats{Outcnt: 1, Selectivity: 1}, BindingTags: []int32{42}, ProjectList: []*planpb.Expr{makePlan2StringConstExprWithType("already computed")}}}
	id, err := appendAsyncDatalinkAdmission(builder, ctx, table, columns, 0, 42, func(name string) (int32, bool) { return 7, name == "path" }, makePlan2BoolConstExprWithType(false))
	require.NoError(t, err)
	node := builder.qry.Nodes[id]
	require.True(t, node.FilterIsBarrier)
	require.Equal(t, planpb.Node_FILTER, node.NodeType)
	args := node.FilterList[0].GetF().Args
	require.Equal(t, int32(42), args[0].GetCol().RelPos)
	require.Equal(t, int32(7), args[0].GetCol().ColPos)
	require.Equal(t, int32(types.T_bool), args[1].Typ.Id)
	same, err := appendAsyncDatalinkAdmission(builder, ctx, table, nil, 0, 42, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int32(0), same)
	_, err = appendAsyncDatalinkAdmission(builder, ctx, table, columns, 0, 42, func(string) (int32, bool) { return 0, false }, nil)
	require.ErrorContains(t, err, "missing final Datalink column")
	for _, algo := range []struct {
		name, params string
		want         bool
	}{{"fulltext", "", false}, {"fulltext", `{"async":"true"}`, true}, {"fulltext2", "", true}} {
		table.Indexes = []*planpb.IndexDef{{IndexAlgo: algo.name, IndexAlgoParams: algo.params, Parts: []string{"path"}}}
		got, err := asyncDatalinkColumns(table)
		require.NoError(t, err)
		require.Equal(t, algo.want, len(got) > 0, algo.name)
	}
	table.Cols[1].Typ.Id = int32(types.T_text)
	got, err := asyncDatalinkColumns(table)
	require.NoError(t, err)
	require.Empty(t, got)
}
