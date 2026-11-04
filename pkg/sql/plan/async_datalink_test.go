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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAsyncDatalinkUpdateFinalAssignments(t *testing.T) {
	for _, tc := range []struct {
		name            string
		auto, generated bool
		assignment      string
		checks          int
	}{
		{"carried sources", false, false, "val=val+1", 0},
		{"explicit same value", false, false, "updated_at=updated_at", 1},
		{"automatic source", true, false, "val=val+1", 1},
		{"generated automatic source", true, true, "val=val+1", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockOptimizer(false, newPlanTestProcess(t)).CurrentContext().(*MockCompilerContext)
			table := ctx.tables["t_on_update_gen"]
			table.Cols[2].Typ = planpb.Type{Id: int32(types.T_datalink)}
			table.Cols[3].Typ = planpb.Type{Id: int32(types.T_datalink)}
			table.Cols[2].OnUpdate = nil
			table.Cols[3].GeneratedCol = nil
			if tc.auto {
				auto, err := forceCastExpr(ctx.GetContext(), makePlan2StringConstExprWithType("file:///tmp/owned.txt"), table.Cols[2].Typ)
				require.NoError(t, err)
				table.Cols[2].OnUpdate = &planpb.OnUpdate{Expr: auto, OriginString: "'file:///tmp/owned.txt'"}
			}
			if tc.generated {
				table.Cols[3].GeneratedCol = &planpb.GeneratedCol{Expr: &planpb.Expr{Typ: table.Cols[2].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 2, Name: "updated_at"}}}, IsStored: true, OriginString: "updated_at"}
			}
			table.Indexes = []*planpb.IndexDef{{IndexAlgo: "fulltext2", IndexName: "ft", Parts: []string{"updated_at", "g"}}}
			stmts, err := mysql.Parse(ctx.GetContext(), "update constraint_test.t_on_update_gen set "+tc.assignment, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			p, err := BuildPlan(ctx, stmts[0], false)
			require.NoError(t, err)
			checks := 0
			for _, node := range p.GetQuery().Nodes {
				for _, filter := range node.FilterList {
					require.NoError(t, planpb.VisitExprTree(filter, func(e *planpb.Expr) error {
						if f := e.GetF(); f != nil && f.Func.GetObjName() == "_check_async_datalink_source" {
							checks++
						}
						return nil
					}))
				}
			}
			require.Equal(t, tc.checks, checks)
		})
	}
}

func TestAsyncDatalinkUpdateColumns(t *testing.T) {
	ctx := &DMLContext{
		tableDefs:            []*planpb.TableDef{{TblId: 1}, {TblId: 2}},
		aliases:              []string{"d", "other"},
		updateCol2Expr:       []map[string]tree.Expr{{"status": nil}, {"status": nil}},
		asyncDatalinkColumns: map[int][]string{0: {"p1", "p2"}},
	}
	for _, tc := range []struct {
		name  string
		final map[string]int32
		want  []string
	}{
		{"ordinary column", map[string]int32{"d.status": 0}, nil},
		{"one assigned source", map[string]int32{"d.p2": 7}, []string{"p2"}},
		{"both assigned sources", map[string]int32{"d.p1": 8, "d.p2": 7}, []string{"p1", "p2"}},
		{"distinct target alias", map[string]int32{"other.p1": 7}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, asyncDatalinkUpdateColumns(ctx, 0, tc.final))
			require.Equal(t, []string{"p1", "p2"}, ctx.asyncDatalinkColumns[0], "do not overwrite captured names")
		})
	}
	// Repeated physical targets can recompute values after merging aliases,
	// including UPDATE IGNORE. Preserve the full final-value check there.
	ctx.tableDefs[1].TblId = 1
	require.Equal(t, []string{"p1", "p2"}, asyncDatalinkUpdateColumns(ctx, 0, nil))
	ctx.updateCol2Expr[1] = nil // a read-only alias cannot introduce a value.
	require.Empty(t, asyncDatalinkUpdateColumns(ctx, 0, nil))
	require.Empty(t, asyncDatalinkUpdateColumns(ctx, 1, nil))
}

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
