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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func scalarVectorPlanFixture(t *testing.T, sql string) *plan.Query {
	t.Helper()
	return scalarVectorPlanFixtureForIndex(t, sql, false)
}

func scalarVectorPlanFixtureForIndex(t *testing.T, sql string, hnsw bool) *plan.Query {
	t.Helper()
	ctx := newVectorJoinMockCtx()
	table := newVectorJoinTableDef(false, false)
	table.Name = "scalar_vector_items"
	table.TblId = 901
	table.Cols[0].Typ.Width = 255
	table.Cols[0].Typ.NotNullable = true
	table.Cols[1].Typ = plan.Type{Id: int32(types.T_array_float64), Width: 3}
	index := newVectorJoinIvfIndex()
	if hnsw {
		table.Cols[0].Typ = plan.Type{Id: int32(types.T_int64), NotNullable: true}
		table.Cols[1].Typ = plan.Type{Id: int32(types.T_array_float32), Width: 3}
		index = newVectorJoinHnswIndex()
	}
	for _, def := range index.IndexDefs {
		def.TableExist = true
		if !hnsw {
			def.IndexAlgoParams = `{"op_type":"vector_l2_ops","lists":"1"}`
		}
		table.Indexes = append(table.Indexes, def)
		ctx.tables[def.IndexTableName] = &plan.TableDef{Name: def.IndexTableName, TblId: uint64(910 + len(table.Indexes))}
		ctx.objects[def.IndexTableName] = &plan.ObjectRef{SchemaName: "tpch", ObjName: def.IndexTableName, Obj: int64(910 + len(table.Indexes))}
	}
	ctx.tables[table.Name] = table
	ctx.objects[table.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
	provider := DeepCopyTableDef(table, true)
	provider.Name, provider.TblId, provider.Indexes = "scalar_vector_provider", 902, nil
	ctx.tables[provider.Name] = provider
	ctx.objects[provider.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: provider.Name, Obj: int64(provider.TblId)}
	statements, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	defer statements[0].Free()
	p, err := BuildPlan(ctx, statements[0], false)
	require.NoError(t, err)
	if prepare := p.GetDcl().GetPrepare(); prepare != nil {
		return prepare.Plan.GetQuery()
	}
	return p.GetQuery()
}

func scalarVectorPlanHasIndex(q *plan.Query) bool {
	seen := make(map[int32]bool)
	var visit func(int32) bool
	visit = func(id int32) bool {
		if seen[id] {
			return false
		}
		seen[id] = true
		node := q.Nodes[id]
		if node.NodeType == plan.Node_VECTOR_INDEX_SCAN {
			return true
		}
		for _, child := range node.Children {
			if visit(child) {
				return true
			}
		}
		return false
	}
	for _, root := range q.Steps {
		if visit(root) {
			return true
		}
	}
	return false
}

func TestScalarVectorQueryIndexPublicPlan(t *testing.T) {
	t.Run("prepared_limit", func(t *testing.T) {
		q := scalarVectorPlanFixture(t, `prepare p from 'select id from scalar_vector_items order by l2_distance(v,
			(select v from scalar_vector_items ref where ref.id=?)) limit ?'`)
		require.True(t, scalarVectorPlanHasIndex(q))
		p := &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), p, []any{"ref", uint64(2)})
		require.NoError(t, err)
		for _, candidate := range []*plan.Plan{p, filled} {
			cols := GetResultColumnsFromPlan(candidate)
			require.Len(t, cols, 1)
			require.Equal(t, int32(types.T_varchar), cols[0].Typ.Id, "result metadata must come from a result branch, not the provider")
			require.Equal(t, "id", cols[0].OriginName)
		}
	})
	t.Run("literal_control", func(t *testing.T) {
		q := scalarVectorPlanFixture(t, `select id from scalar_vector_items order by l2_distance(v, '[1,2,3]') limit 10`)
		require.True(t, scalarVectorPlanHasIndex(q), "the fixture must expose an eligible vector index")
	})
	t.Run("primary_key_scalar", func(t *testing.T) {
		q := scalarVectorPlanFixture(t, `select id from scalar_vector_items order by l2_distance(v,
			(select v from scalar_vector_items ref where ref.id='ref')) limit 10`)
		require.True(t, scalarVectorPlanHasIndex(q), "a scalar query vector must reach the index access path")
		root := q.Nodes[q.Steps[0]]
		require.Equal(t, plan.Node_VECTOR_QUERY_TOP, root.NodeType)
		require.Len(t, root.Children, 3)
		require.Equal(t, int32(1), root.ProjectList[0].GetCol().RelPos)
		copy := DeepCopyQuery(q)
		require.Equal(t, root.VectorQuerySourceId, copy.Nodes[copy.Steps[0]].VectorQuerySourceId)
		wire, err := q.Marshal()
		require.NoError(t, err)
		var decoded plan.Query
		require.NoError(t, decoded.Unmarshal(wire))
		require.Equal(t, root.VectorQuerySourceId, decoded.Nodes[decoded.Steps[0]].VectorQuerySourceId)
	})
}

func TestScalarVectorQueryHnswPreparedProvider(t *testing.T) {
	q := scalarVectorPlanFixtureForIndex(t, `prepare p from 'select id from scalar_vector_items order by l2_distance(v,
		(select v from scalar_vector_provider ref where ref.id=?)) limit ?'`, true)
	require.Equal(t, plan.Node_VECTOR_QUERY_TOP, q.Nodes[q.Steps[0]].NodeType)
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
	for _, params := range [][]any{{int64(1), uint64(2)}, {int64(2), uint64(0)}, {int64(3), uint64(1)}} {
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), p, params)
		require.NoError(t, err)
		for _, candidate := range []*plan.Plan{p, filled} {
			cols := GetResultColumnsFromPlan(candidate)
			require.Len(t, cols, 1)
			require.Equal(t, int32(types.T_int64), cols[0].Typ.Id)
			found := false
			for _, node := range candidate.GetQuery().Nodes {
				if node.NodeType != plan.Node_FUNCTION_SCAN {
					continue
				}
				found = true
				require.Len(t, node.Children, 1)
				for _, expr := range node.ProjectList {
					require.Equal(t, node.TableDef.Cols[expr.GetCol().ColPos].Typ, expr.Typ,
						"a function scan projects its own result schema, not its query-vector input")
				}
			}
			require.True(t, found)
		}
	}
}

func TestScalarVectorQueryConservativeBoundaries(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"multirow", `select id from scalar_vector_items order by l2_distance(v,(select v from scalar_vector_items ref where ref.id>'a')) limit 2`},
		{"correlated", `select t.id from scalar_vector_items t order by l2_distance(t.v,(select v from scalar_vector_items ref where ref.id=t.id)) limit 2`},
		{"two_order_keys", `select id from scalar_vector_items order by l2_distance(v,(select v from scalar_vector_items ref where ref.id='ref')),id limit 2`},
		{"found_rows", `select sql_calc_found_rows id from scalar_vector_items order by l2_distance(v,(select v from scalar_vector_items ref where ref.id='ref')) limit 2`},
		{"force_exact", `select id from scalar_vector_items order by l2_distance(v,(select v from scalar_vector_items ref where ref.id='ref')) limit 2 by rank with option 'mode=force'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := scalarVectorPlanFixture(t, tc.sql)
			require.False(t, scalarVectorPlanHasIndex(q))
			var visit func(int32)
			visit = func(id int32) {
				n := q.Nodes[id]
				require.NotEqual(t, plan.Node_VECTOR_QUERY_TOP, n.NodeType)
				for _, child := range n.Children {
					visit(child)
				}
			}
			visit(q.Steps[0])
		})
	}
}
