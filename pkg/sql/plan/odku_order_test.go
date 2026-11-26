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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestNoODKUConflictProof(t *testing.T) {
	first := &plan.IndexDef{Unique: true}
	second := &plan.IndexDef{Unique: true}
	ordinary := &plan.IndexDef{}
	require.False(t, noODKUConflict(false, nil, nil, nil), "a real primary key can conflict")
	require.True(t, noODKUConflict(true, nil, nil, nil))
	require.True(t, noODKUConflict(true, []*plan.IndexDef{ordinary, first}, []*plan.IndexDef{first}, []bool{true}))
	require.False(t, noODKUConflict(true, []*plan.IndexDef{first}, []*plan.IndexDef{first}, []bool{false}), "a dynamic key is not proven NULL")
	require.False(t, noODKUConflict(true, []*plan.IndexDef{first}, []*plan.IndexDef{first}, nil), "a missing proof must fail closed")
	require.False(t, noODKUConflict(true, []*plan.IndexDef{first, second}, []*plan.IndexDef{first}, []bool{true}), "a filtered unique index is not absent")
}

func TestODKUMetadataProjectionRewrite(t *testing.T) {
	ref := func(tag, pos int32) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: pos}}}
	}
	c := &plan.OdkuActionCtx{
		SourceColumns: []*plan.Expr{ref(7, 0)}, OldColumns: []*plan.Expr{ref(7, 1)}, SourceOrdinal: ref(7, 2),
		UpdateExprs: []*plan.Expr{ref(1, 0)}, InsertChecks: []*plan.Expr{ref(0, 0)}, UpdateChecks: []*plan.Expr{ref(0, 1)},
		InsertCheckConstraintCount: 1,
		Constraints:                []*plan.OdkuConstraint{{InputKey: ref(7, 3), PriorTarget: ref(7, 4), CurrentKey: ref(0, 0)}},
		ForeignKeyChecks:           []*plan.OdkuForeignKeyCheck{{Columns: []plan.ColRef{{RelPos: 0, ColPos: 1}}, Eligibility: ref(1, 1)}},
	}
	node := &plan.Node{PreInsertUkCtx: &plan.PreInsertUkCtx{OrderedOdku: true, OdkuActions: c}}
	replacements := make(map[[2]int32]*plan.Expr)
	for i := range 5 {
		replacements[[2]int32{7, int32(i)}] = ref(8, int32(i+10))
	}
	// Runtime row expressions use rel0/current and rel1/incoming, regardless of
	// whether those numbers also happen to be relational binding tags.
	replacements[[2]int32{1, 0}] = ref(8, 99)
	replacements[[2]int32{0, 0}] = ref(8, 99)
	replaceColumnsForNode(node, replacements)
	for i, e := range odkuSourceExpressions(c) {
		// Source, original image, constraint key/target, then ordinal.
		expected := []int32{10, 11, 13, 14, 12}[i]
		require.Equal(t, int32(8), e.GetCol().RelPos)
		require.Equal(t, expected, e.GetCol().ColPos)
	}
	require.Equal(t, int32(0), c.UpdateExprs[0].GetCol().ColPos)
	require.Equal(t, int32(1), c.UpdateExprs[0].GetCol().RelPos)
	require.Equal(t, int32(0), c.Constraints[0].CurrentKey.GetCol().RelPos)
	require.Equal(t, int32(0), c.Constraints[0].CurrentKey.GetCol().ColPos)
	require.Equal(t, int32(0), c.InsertChecks[0].GetCol().ColPos)
	require.Equal(t, int32(1), c.UpdateChecks[0].GetCol().ColPos)
	require.Equal(t, int32(1), c.ForeignKeyChecks[0].Columns[0].ColPos)
	require.Equal(t, int32(1), c.ForeignKeyChecks[0].Eligibility.GetCol().ColPos)
	clone := DeepCopyPreInsertUkCtx(node.PreInsertUkCtx).OdkuActions
	require.Equal(t, uint32(1), clone.InsertCheckConstraintCount)
	clone.SourceOrdinal.GetCol().ColPos = 99
	clone.ForeignKeyChecks[0].Columns[0].ColPos = 99
	clone.UpdateExprs[0].GetCol().ColPos = 99
	require.Equal(t, int32(12), c.SourceOrdinal.GetCol().ColPos)
	require.Equal(t, int32(1), c.ForeignKeyChecks[0].Columns[0].ColPos)
	require.Equal(t, int32(0), c.UpdateExprs[0].GetCol().ColPos)
}

func TestODKUValidationSurvivesOptimization(t *testing.T) {
	mock := NewMockOptimizer(true)
	for _, table := range mock.ctxt.tables {
		if table.Name2ColIndex == nil {
			table.Name2ColIndex = make(map[string]int32)
			for i, c := range table.Cols {
				table.Name2ColIndex[c.Name] = int32(i)
			}
		}
	}
	p, e := runOneStmt(mock, t, "insert into emp(empno,deptno) values (1,999),(1,10) on duplicate key update deptno=values(deptno),comm=0")
	require.NoError(t, e)
	q := p.GetQuery()
	require.NotNil(t, q)
	barriers := 0
	actionOwner := false
	for _, n := range q.Nodes {
		if c := n.PreInsertUkCtx.GetOdkuActions(); c != nil {
			actionOwner = true
			require.GreaterOrEqual(t, c.ActionFinalColumn, int32(0))
			require.NotEmpty(t, c.ForeignKeyChecks)
			require.Equal(t, int32(types.T_bool), c.SourceColumns[c.ActionFinalColumn].Typ.Id)
		}
		if !n.FilterIsBarrier {
			continue
		}
		barriers++
		require.Equal(t, plan.Node_FILTER, n.NodeType)
		require.False(t, n.IsEnd)
		require.NotEmpty(t, n.FilterList)
		for _, a := range n.FilterList {
			require.NotNil(t, a.GetF())
			require.Equal(t, "assert", a.GetF().Func.ObjName)
			require.Len(t, a.GetF().Args, 3)
		}
		hasSelector := false
		for _, parent := range q.Nodes {
			if parent.NodeType == plan.Node_FILTER && !parent.FilterIsBarrier && len(parent.Children) == 1 && parent.Children[0] == n.NodeId {
				hasSelector = true
			}
		}
		require.True(t, hasSelector, "validation must execute before its separate final/no-op selector")
		require.True(t, DeepCopyNode(n).FilterIsBarrier)
	}
	require.True(t, actionOwner)
	require.Equal(t, 2, barriers)
	encoded, e := q.Marshal()
	require.NoError(t, e)
	decoded := &plan.Query{}
	require.NoError(t, decoded.Unmarshal(encoded))
	count := 0
	for _, n := range decoded.Nodes {
		if n.FilterIsBarrier {
			count++
		}
	}
	require.Equal(t, barriers, count)
}

func TestODKUStaticSourceBound(t *testing.T) {
	source := func() *plan.Node {
		return &plan.Node{NodeType: plan.Node_VALUE_SCAN, TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "v"}}}, RowsetData: &plan.RowsetData{RowCount: 1, Cols: []*plan.ColData{{Data: []*plan.RowsetExpr{{Expr: makePlan2Int64ConstExprWithType(1)}}}}}}
	}
	for _, kind := range []plan.Node_NodeType{plan.Node_PROJECT, plan.Node_PRE_INSERT, plan.Node_FILTER} {
		b := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{source(), {NodeType: kind, Children: []int32{0}}}}}
		require.True(t, b.odkuSourceAtMostOne(1))
	}
	for _, mutate := range []func(*plan.Node){
		func(n *plan.Node) { n.RowsetData.RowCount = 2 },
		func(n *plan.Node) {
			n.RowsetData.Cols[0].Data = append(n.RowsetData.Cols[0].Data, &plan.RowsetExpr{Expr: makePlan2Int64ConstExprWithType(2)})
		},
		func(n *plan.Node) { n.RowsetData.Cols[0] = nil },
		func(n *plan.Node) { n.RowsetData.Cols[0].Data[0].Expr = nil },
		func(n *plan.Node) { n.TableDef.Cols = nil },
		func(n *plan.Node) { n.TableDef = nil },
		func(n *plan.Node) { n.NodeType = plan.Node_TABLE_SCAN; n.Stats = &plan.Stats{Outcnt: 1} },
		func(n *plan.Node) { n.NodeType = plan.Node_FUNCTION_SCAN },
		func(n *plan.Node) { n.NodeType = plan.Node_SINK_SCAN },
		func(n *plan.Node) { n.NodeType = plan.Node_JOIN },
	} {
		n := source()
		mutate(n)
		b := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{n}}}
		require.False(t, b.odkuSourceAtMostOne(0))
	}
	b := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{{NodeType: plan.Node_PROJECT, Children: []int32{0}}}}}
	require.False(t, b.odkuSourceAtMostOne(0))
	require.False(t, b.odkuSourceAtMostOne(-1))
	require.False(t, b.odkuSourceAtMostOne(2))
}

func TestODKUSingleSourceKeepsPhysicalOrdinal(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		single bool
	}{
		{"insert into dept values(1,'A','B') on duplicate key update loc=values(loc)", true},
		{"insert into dept values(1,'A','B'),(2,'C','D') on duplicate key update loc=values(loc)", false},
		{"insert into dept select * from dept limit 1 on duplicate key update loc=values(loc)", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			p, e := runOneStmt(mock, t, tc.sql)
			require.NoError(t, e)
			for _, q := range []*plan.Query{p.GetQuery(), DeepCopyPlan(p).GetQuery()} {
				window, sorts := 0, 0
				found := false
				for _, n := range q.Nodes {
					if n.NodeType == plan.Node_WINDOW {
						window++
					}
					if n.NodeType == plan.Node_SORT {
						sorts++
					}
					if c := n.PreInsertUkCtx.GetOdkuActions(); c != nil {
						found = true
						child := q.Nodes[n.Children[0]]
						require.Equal(t, plan.Node_LOCK_OP, child.NodeType)
						require.NotNil(t, c.SourceOrdinal.GetCol())
						require.Equal(t, int32(0), c.SourceOrdinal.GetCol().RelPos)
						pos := c.SourceOrdinal.GetCol().ColPos
						require.GreaterOrEqual(t, pos, int32(0))
						require.Less(t, int(pos), len(child.ProjectList))
						require.Equal(t, int32(types.T_int64), child.ProjectList[pos].Typ.Id)
						require.Equal(t, int32(types.T_int64), c.SourceOrdinal.Typ.Id)
					}
					require.False(t, n.NodeType == plan.Node_JOIN && n.JoinType == plan.Node_DEDUP && n.OnDuplicateAction == plan.Node_UPDATE, "the action owner must not be followed by grouped replay")
				}
				require.True(t, found)
				if tc.single {
					require.Zero(t, window)
					require.Zero(t, sorts)
				} else {
					require.Positive(t, window)
					require.Positive(t, sorts)
				}
				data, e := q.Marshal()
				require.NoError(t, e)
				var copy plan.Query
				require.NoError(t, copy.Unmarshal(data))
				for _, n := range copy.Nodes {
					if c := n.PreInsertUkCtx.GetOdkuActions(); c != nil {
						require.NotNil(t, c.SourceOrdinal.GetCol())
						require.Equal(t, int32(types.T_int64), c.SourceOrdinal.Typ.Id)
					}
				}
			}
		})
	}
}

func TestODKUPhaseOrderPreservesExplicitAndGeneratedDependencies(t *testing.T) {
	ref := func(pos int32) *plan.Expr { return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}} }
	table := &plan.TableDef{Name2ColIndex: map[string]int32{"a": 0, "automatic": 1, "independent": 2, "dependent": 3, "chain": 4, "physical": 5}, Cols: []*plan.ColDef{
		{Name: "a"}, {Name: "automatic"},
		{Name: "independent", GeneratedCol: &plan.GeneratedCol{Expr: ref(0)}},
		{Name: "dependent", GeneratedCol: &plan.GeneratedCol{Expr: ref(1)}},
		{Name: "chain", GeneratedCol: &plan.GeneratedCol{Expr: ref(3)}},
		{Name: "physical", Hidden: true},
	}}
	names := []string{"a", "a", "automatic", "independent", "dependent", "chain", "physical"}
	exprs := make([]*plan.Expr, len(names))
	for i := range exprs {
		exprs[i] = ref(int32(i))
	}
	ordered, values, prefix, columns := orderODKUUpdatePhases(table, names, exprs, 2, 1, map[string]bool{"automatic": true})
	require.Equal(t, []string{"a", "a", "independent", "automatic", "dependent", "chain", "physical"}, ordered)
	require.Equal(t, 3, prefix)
	require.Equal(t, []int32{0, 2}, columns)
	require.Equal(t, []*plan.Expr{exprs[0], exprs[1], exprs[3], exprs[2], exprs[4], exprs[5], exprs[6]}, values)
}
