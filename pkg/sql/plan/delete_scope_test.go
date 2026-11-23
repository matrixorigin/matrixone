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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDeleteIndexKeyRepresentations(t *testing.T) {
	ctx := NewMockOptimizer(false).CurrentContext()
	builder := NewQueryBuilder(planpb.Query_DELETE, ctx, false, true)
	table := &planpb.TableDef{Cols: []*planpb.ColDef{
		{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
		{Name: "e", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "z,aa"}},
		{Name: "s", Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: ",a"}},
		{Name: "n", Typ: planpb.Type{Id: int32(types.T_uint64)}},
	}, Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}}
	positions := map[string]int32{"id": 0, "e": 1, "s": 2, "n": 3}
	for _, tc := range []struct {
		name   string
		parts  []string
		unique bool
		fn     string
	}{
		{"enum secondary", []string{"e", "id"}, false, "serial_full"},
		{"set secondary", []string{"s", "id"}, false, "serial_full"},
		{"enum composite unique", []string{"n", "e"}, true, "serial"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index := &planpb.IndexDef{Parts: tc.parts, Unique: tc.unique}
			require.True(t, needsDeleteIndexKeyVariants(table, index))
			stored, display, err := builder.deleteIndexKeys(table, index, positions, 17, true)
			require.NoError(t, err)
			require.Equal(t, tc.fn, stored.GetF().Func.ObjName)
			if !tc.unique {
				require.Equal(t, "case", display.GetF().Func.ObjName)
				display = display.GetF().Args[2]
			}
			require.Equal(t, tc.fn, display.GetF().Func.ObjName)
			for i, name := range tc.parts {
				if name != "e" && name != "s" {
					require.NotNil(t, stored.GetF().Args[i].GetCol())
					require.NotNil(t, display.GetF().Args[i].GetCol())
					continue
				}
				require.NotNil(t, stored.GetF().Args[i].GetCol(), "stored key uses the physical column")
				require.Equal(t, int32(types.T_varchar), display.GetF().Args[i].Typ.Id)
				require.NotNil(t, display.GetF().Args[i].GetF(), "backfill key uses the displayed value")
			}
		})
	}
	require.False(t, needsDeleteIndexKeyVariants(table, &planpb.IndexDef{Parts: []string{"n", "id"}}))
	require.False(t, needsDeleteIndexKeyVariants(table, &planpb.IndexDef{Parts: []string{"e"}, Unique: true}))
	require.True(t, canBindStoredDeleteRows([]*planpb.TableDef{table}))
	legacy := *table
	legacy.Pkey = &planpb.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"e", "id"}}
	require.False(t, canBindStoredDeleteRows([]*planpb.TableDef{&legacy}))
	owner, err := builder.deleteIndexOwner(table, positions, 17)
	require.NoError(t, err)
	require.Equal(t, int32(0), owner.GetCol().ColPos)
	for _, typ := range []types.T{types.T_float32, types.T_float64} {
		floatTable := *table
		floatTable.Cols = append([]*planpb.ColDef(nil), table.Cols...)
		floatPK := *table.Cols[0]
		floatPK.Typ.Id = int32(typ)
		floatTable.Cols[0] = &floatPK
		owner, err = builder.deleteIndexOwner(&floatTable, positions, 17)
		require.NoError(t, err)
		require.Equal(t, "serial", owner.GetF().Func.ObjName)
		require.Equal(t, int32(0), owner.GetF().Args[0].GetCol().ColPos)
	}
	_, _, err = builder.deleteIndexKeys(table, &planpb.IndexDef{Parts: []string{"missing", catalog.Row_ID}}, positions, 17, true)
	require.Error(t, err)
}

func TestReduceSinkPreservesReturningStep(t *testing.T) {
	query := &planpb.Query{
		HasReturning:  true,
		ReturningStep: 2,
		Steps:         []int32{1, 3, 4},
		Nodes: []*planpb.Node{
			{NodeType: planpb.Node_VALUE_SCAN},
			{NodeType: planpb.Node_SINK, Children: []int32{0}},
			{NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{0}},
			{NodeType: planpb.Node_PROJECT, Children: []int32{2}},
			{NodeType: planpb.Node_PROJECT, Children: []int32{0}},
		},
	}
	reduceSinkSinkScanNodes(query)
	require.Equal(t, []int32{3, 4}, query.Steps)
	require.Equal(t, int32(1), query.ReturningStep)
	require.Equal(t, int32(4), query.Steps[query.ReturningStep])
}

func TestIsUnrestrictedDelete(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		targetCount int
		want        bool
	}{
		{name: "plain", sql: "delete from t", targetCount: 1, want: true},
		{name: "order only", sql: "delete from t order by a", targetCount: 1, want: true},
		{name: "where", sql: "delete from t where true", targetCount: 1},
		{name: "limit zero", sql: "delete from t limit 0", targetCount: 1},
		{name: "join without where", sql: "delete t from t join s on t.a = s.a", targetCount: 1},
		{name: "using without where", sql: "delete from t using t join s on t.a = s.a", targetCount: 1},
		{name: "partition", sql: "delete from t partition (p0)", targetCount: 1},
		{name: "multiple targets", sql: "delete t, s from t join s on t.a = s.a", targetCount: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			deleteStmt, ok := stmt.(*tree.Delete)
			require.True(t, ok)
			require.Equal(t, test.want, isUnrestrictedDelete(deleteStmt, test.targetCount))
		})
	}

	require.False(t, isUnrestrictedDelete(nil, 1))
}

func TestDeleteCTETargetScope(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, tc := range []struct {
			name, sql string
			wantError string
		}{
			{"alias shadows CTE", "with x as (select 1 as id) delete x from nation as x where x.n_nationkey=1", ""},
			{"joined source without WHERE", "with x as (select 99 as id), q as (select 1 as id) delete x from nation as x join q on x.n_nationkey=q.id", ""},
			{"apply source", "delete nation from nation cross apply generate_series(nation.n_nationkey,nation.n_nationkey) as g where nation.n_nationkey=1", ""},
			{"qualified explicit alias baseline", "delete ignored.x from nation as x where x.n_nationkey=1", ""},
			{"qualified explicit alias with CTE", "with x as (select 99 as id) delete ignored.x from nation as x where x.n_nationkey=1", ""},
			{"qualified explicit alias same spelling baseline", "delete ignored.nation from nation as nation where nation.n_nationkey=1", ""},
			{"qualified explicit alias same spelling with CTE", "with nation as (select 99 as id) delete ignored.nation from tpch.nation as nation where nation.n_nationkey=1", ""},
			{"unrelated CTE", "with q as (select 1 as id) delete x from nation as x where x.n_nationkey=1", ""},
			{"qualified source shadows CTE", "with nation as (select 1 as id) delete x from tpch.nation as x where x.n_nationkey=1", ""},
			{"qualified unaliased source", "with nation as (select 1 as id) delete nation from tpch.nation where nation.n_nationkey=1", ""},
			{"qualified target", "with nation as (select 1 as id) delete from tpch.nation where n_nationkey=1", ""},
			{"CTE target", "with nation as (select 1 as n_nationkey) delete from nation where n_nationkey=1", "cannot delete from CTE"},
			{"aliased CTE target", "with nation as (select 1 as n_nationkey) delete x from nation as x where x.n_nationkey=1", "cannot delete from CTE"},
			{"qualified target mismatches source", "with q as (select 1) delete other.nation from tpch.nation where nation.n_nationkey=1", "does not match its source"},
			{"qualified target reads CTE", "with nation as (select 1 as n_nationkey) delete tpch.nation from nation where nation.n_nationkey=1", "cannot delete from CTE"},
			{"target missing from USING", "delete from nation using region", "missing FROM-clause entry for table 'nation'"},
			{"target hidden by alias", "delete nation from nation as x where x.n_nationkey=1", "missing FROM-clause entry for table 'nation'"},
			{"second target missing", "delete x, missing from nation as x join region as r on x.n_regionkey=r.r_regionkey", "missing FROM-clause entry for table 'missing'"},
			{"derived source", "delete nation from (select 1 as n_nationkey) as nation", "missing FROM-clause entry for table 'nation'"},
		} {
			route := "modern/"
			if legacy {
				route = "legacy/"
			}
			t.Run(route+tc.name, func(t *testing.T) {
				mock := NewMockOptimizer(true)
				stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				var p *Plan
				if legacy {
					p, err = buildDelete(stmt.(*tree.Delete), mock.CurrentContext(), false)
				} else {
					p, err = BuildPlan(mock.CurrentContext(), stmt, false)
				}
				if tc.wantError != "" {
					require.ErrorContains(t, err, tc.wantError)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
					return
				}
				require.NoError(t, err)
				var targets []string
				for _, node := range p.GetQuery().Nodes {
					if node.NodeType == planpb.Node_DELETE {
						require.False(t, node.DeleteCtx.CanTruncate, "a filtered source must not truncate the target")
						targets = append(targets, node.DeleteCtx.TableDef.Name)
					}
					for _, update := range node.UpdateCtxList {
						targets = append(targets, update.TableDef.Name)
					}
				}
				require.Equal(t, []string{"nation"}, targets)
			})
		}
	}
}
