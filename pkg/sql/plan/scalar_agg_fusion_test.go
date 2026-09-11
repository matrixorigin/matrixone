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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestFuseScalarAggregatesPlan(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		scans int
	}{
		{"same filtered input", `select
		 (select count(*) from lineitem a where a.l_quantity > 1),
		 (select avg(b.l_extendedprice) from lineitem b where b.l_quantity > 1),
		 (select avg(c.l_discount) from lineitem c where c.l_quantity > 1)
		 from nation where n_nationkey = 1`, 1},
		{"different filters", `select
		 (select count(*) from lineitem where l_quantity > 1),
		 (select avg(l_extendedprice) from lineitem where l_quantity > 2)`, 2},
		{"having can remove scalar row", `select
		 (select count(*) from lineitem having count(*) > 0),
		 (select avg(l_extendedprice) from lineitem)`, 2},
		{"grouped subquery", `select
		 (select count(*) from lineitem group by l_orderkey),
		 (select avg(l_extendedprice) from lineitem)`, 2},
		{"distinct has a separate state contract", `select
		 (select count(distinct l_orderkey) from lineitem),
		 (select avg(l_extendedprice) from lineitem)`, 2},
		{"fallible aggregate argument", `select
		 (select count(*) from lineitem),
		 (select sum(1 / l_quantity) from lineitem)`, 2},
		{"volatile predicate", `select
		 (select count(*) from lineitem where rand() > 0.5),
		 (select avg(l_extendedprice) from lineitem where rand() > 0.5)`, 2},
		{"correlated subquery", `select
		 (select count(*) from lineitem where l_orderkey = n_nationkey),
		 (select avg(l_extendedprice) from lineitem where l_orderkey = n_nationkey)
		 from nation`, 2},
		{"join condition observes both scalar rows", `select a.c, b.s from
		 (select count(*) c from lineitem) a left join
		 (select sum(l_extendedprice) s from lineitem) b on a.c = b.s`, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false), t, tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.scans, countReachableTableScans(p.GetQuery(), "lineitem"))
		})
	}
}

func TestFuseScalarAggregatesBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*planpb.Node, *planpb.Node)
	}{
		{"different snapshot", func(_, scan *planpb.Node) { scan.ScanSnapshot = &planpb.Snapshot{} }},
		{"limited input", func(_, scan *planpb.Node) { scan.Limit = makePlan2Int64ConstExprWithType(1) }},
		{"zero limit on aggregate", func(agg, _ *planpb.Node) { agg.Limit = makePlan2Int64ConstExprWithType(0) }},
		{"offset removes singleton", func(agg, _ *planpb.Node) { agg.Offset = makePlan2Int64ConstExprWithType(1) }},
		{"sample is not identical input", func(_, scan *planpb.Node) { scan.SampleFunc = &planpb.SampleFuncSpec{} }},
		{"different table", func(_, scan *planpb.Node) { scan.ObjRef.Obj++ }},
		{"external relation", func(_, scan *planpb.Node) { scan.TableDef.TableType = "e" }},
		{"unbound argument", func(agg, _ *planpb.Node) { agg.AggList[0].GetF().Args[0].GetCol().RelPos = 100 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := scalarFusionTestBuilder()
			tc.mutate(builder.qry.Nodes[3], builder.qry.Nodes[2])
			root, changed := builder.fuseScalarAggregates(4)
			require.False(t, changed)
			require.Equal(t, int32(4), root)
			require.Len(t, builder.qry.Nodes[1].AggList, 1)
		})
	}
}

func TestFuseScalarAggregatesTransitiveOutputRemap(t *testing.T) {
	builder := scalarFusionTestBuilder()
	query := builder.qry
	// (A CROSS B) is itself the right child of C CROSS (A CROSS B).
	scan := DeepCopyNode(query.Nodes[0])
	scan.NodeId = 5
	scan.BindingTags = []int32{30}
	agg := DeepCopyNode(query.Nodes[1])
	agg.NodeId = 6
	agg.Children = []int32{5}
	agg.BindingTags = []int32{31, 32}
	agg.AggList[0].GetF().Args[0].GetCol().RelPos = 30
	query.Nodes = append(query.Nodes, scan, agg,
		&planpb.Node{NodeId: 7, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER, Children: []int32{6, 4}},
		&planpb.Node{NodeId: 8, NodeType: planpb.Node_PROJECT, Children: []int32{7}, ProjectList: []*planpb.Expr{
			GetColExpr(agg.AggList[0].Typ, 32, 0),
			GetColExpr(agg.AggList[0].Typ, 12, 0),
			GetColExpr(agg.AggList[0].Typ, 22, 0),
		}})
	root, changed := builder.fuseScalarAggregates(8)
	require.True(t, changed)
	require.Equal(t, int32(8), root)
	require.Equal(t, []int32{6}, query.Nodes[8].Children)
	require.Len(t, agg.AggList, 3)
	for i, expr := range query.Nodes[8].ProjectList {
		require.Equal(t, int32(32), expr.GetCol().RelPos)
		require.Equal(t, int32(i), expr.GetCol().ColPos)
		require.Equal(t, int32(30), agg.AggList[i].GetF().Args[0].GetCol().RelPos)
	}
	_, changed = builder.fuseScalarAggregates(root)
	require.False(t, changed, "the pass must be idempotent")
}

func scalarFusionTestBuilder() *QueryBuilder {
	query := &planpb.Query{}
	for _, tag := range []int32{10, 20} {
		scanID := int32(len(query.Nodes))
		typ := planpb.Type{Id: int32(types.T_int32)}
		query.Nodes = append(query.Nodes, &planpb.Node{
			NodeId: scanID, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{tag},
			ObjRef:   &planpb.ObjectRef{Obj: 1, ObjName: "t"},
			TableDef: &planpb.TableDef{Name: "t", Cols: []*planpb.ColDef{{Name: "v", Typ: typ}}},
		}, &planpb.Node{
			NodeId: scanID + 1, NodeType: planpb.Node_AGG, Children: []int32{scanID},
			BindingTags: []int32{tag + 1, tag + 2},
			AggList:     []*planpb.Expr{distinctAggTestExpr(function.SUM, false, typ, GetColExpr(typ, tag, 0))},
		})
	}
	query.Nodes = append(query.Nodes, &planpb.Node{
		NodeId: 4, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER, Children: []int32{1, 3},
	})
	return &QueryBuilder{qry: query}
}
