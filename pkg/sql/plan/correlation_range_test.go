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
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestRangeScalarCompleteRelation(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"guarded row operand", `SELECT CASE WHEN o.N_NATIONKEY<0 THEN (o.N_NATIONKEY,(SELECT j.N_NATIONKEY FROM NATION j WHERE j.N_NATIONKEY<=o.N_NATIONKEY))=(SELECT COUNT(*),MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_NATIONKEY<=o.N_NATIONKEY) ELSE 0 END FROM NATION o`},
		{"guarded expression", `SELECT CASE WHEN o.N_NATIONKEY<0 THEN (SELECT SUM(i.N_NATIONKEY)+1 FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) ELSE 0 END FROM NATION o`},
		{"volatile result", `SELECT (SELECT MAX(i.N_NATIONKEY)+RAND() FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM NATION o`},
		{"correlated result", `SELECT (SELECT MAX(i.N_NATIONKEY)+o.N_NATIONKEY FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM NATION o`},
		{"wrapped expression", `SELECT (WITH q AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) SELECT m+1 FROM q) FROM NATION o`},
		{"row expression", `SELECT (o.N_NATIONKEY,1)=(SELECT MAX(i.N_NATIONKEY)+1,COUNT(*) FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM NATION o`},
		{"having", `SELECT (SELECT MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY HAVING MAX(i.N_NATIONKEY)>=5) FROM NATION o`},
		{"distinct", `SELECT (SELECT DISTINCT MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM NATION o`},
		{"grouped top one", `SELECT (SELECT MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY GROUP BY i.N_REGIONKEY ORDER BY i.N_REGIONKEY DESC LIMIT 1) FROM NATION o`},
		{"grouped cardinality", `SELECT (SELECT COUNT(*) FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY GROUP BY i.N_REGIONKEY HAVING i.N_REGIONKEY>0) FROM NATION o`},
		{"derived outer", `SELECT (SELECT MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM (SELECT N_REGIONKEY FROM NATION) o`},
		{"distinct outer", `WITH q AS (SELECT DISTINCT N_REGIONKEY FROM NATION) SELECT (SELECT COUNT(DISTINCT i.N_NATIONKEY) FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM q o`},
		{"two outer bindings", `SELECT (SELECT MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY AND i.N_NATIONKEY<=x.N_NATIONKEY) FROM NATION o JOIN NATION x ON x.N_NATIONKEY=o.N_NATIONKEY`},
		{"union inner", `SELECT (SELECT COUNT(*) FROM (SELECT N_REGIONKEY FROM NATION UNION ALL SELECT N_REGIONKEY FROM NATION) i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM NATION o`},
		{"union outer", `WITH q AS (SELECT N_REGIONKEY FROM NATION UNION ALL SELECT N_REGIONKEY FROM NATION) SELECT (SELECT MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM q o`},
		{"derived inner", `SELECT (SELECT MAX(i.N_NATIONKEY) FROM (SELECT N_NATIONKEY,N_REGIONKEY FROM NATION) i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM NATION o`},
		{"non aggregate top one", `SELECT (SELECT i.N_NATIONKEY FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY ORDER BY i.N_NATIONKEY DESC LIMIT 1) FROM NATION o`},
		{"recursive grouped", `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) SELECT (SELECT COUNT(*) FROM r i WHERE i.n<=o.n GROUP BY i.n HAVING i.n>0) FROM r o`},
		{"unselected cardinality", `SELECT CASE WHEN o.N_NATIONKEY<0 THEN (SELECT COUNT(*) FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY GROUP BY i.N_NATIONKEY) ELSE 0 END FROM NATION o`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			q := p.GetQuery()
			require.NotNil(t, q.Correlation)
			require.NoError(t, q.ValidateCorrelation())
			require.NotEmpty(t, q.Correlation.Programs)
			if tc.name == "two outer bindings" {
				require.Len(t, q.Correlation.Programs[0].InputTypes, 2)
			}
			if tc.name == "guarded row operand" {
				found := false
				for _, node := range q.Nodes {
					require.NoError(t, planpb.VisitExpressionsInOwner(node, func(expr *planpb.Expr) error {
						return planpb.VisitExprTree(expr, func(e *planpb.Expr) error {
							if call := e.GetDependent(); call != nil && len(call.Lhs) == 2 && call.Lhs[1].GetDependent() != nil {
								found = true
							}
							return nil
						})
					}))
				}
				require.True(t, found, "comparison operand must remain inside the demand expression")
			}
		})
	}
}

func TestRangeScalarKeepsExistingFastPaths(t *testing.T) {
	for _, aggregate := range []string{
		"MAX(i.N_NATIONKEY)", "COUNT(*)", "COUNT(DISTINCT i.N_NATIONKEY)", "SUM(1)",
		"MAX(i.N_NATIONKEY)+1", "COALESCE(MAX(i.N_NATIONKEY),-1)", "CAST(MAX(i.N_NATIONKEY) AS SIGNED)+1",
		"CASE WHEN MAX(i.N_NATIONKEY) IS NULL THEN -1 ELSE MAX(i.N_NATIONKEY) END",
		"COUNT(*)+COUNT(DISTINCT i.N_NATIONKEY)", "COUNT(*)+COALESCE(SUM(i.N_NATIONKEY),0)",
	} {
		for _, predicate := range []string{"<=", "="} {
			t.Run(aggregate+predicate, func(t *testing.T) {
				sql := fmt.Sprintf(`SELECT (SELECT %s FROM NATION i WHERE i.N_REGIONKEY%s o.N_REGIONKEY) FROM NATION o`, aggregate, predicate)
				p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
				require.NoError(t, err)
				require.Nil(t, p.GetQuery().Correlation)
			})
		}
	}
	for _, sql := range []string{
		`WITH q AS (SELECT * FROM NATION) SELECT (SELECT MAX(i.N_NATIONKEY) FROM q i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) FROM q o`,
		`SELECT (WITH q AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_REGIONKEY<=o.N_REGIONKEY) SELECT m FROM q) FROM NATION o`,
		`SELECT (SELECT i.N_NATIONKEY FROM NATION i WHERE i.N_REGIONKEY=o.N_REGIONKEY ORDER BY i.N_NATIONKEY DESC LIMIT 1) FROM NATION o`,
		`SELECT (WITH q AS (SELECT i.N_REGIONKEY,MAX(i.N_NATIONKEY) AS m FROM NATION i GROUP BY i.N_REGIONKEY) SELECT m FROM q WHERE q.N_REGIONKEY<o.N_REGIONKEY) FROM NATION o`,
	} {
		p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
		require.NoError(t, err)
		require.Nil(t, p.GetQuery().Correlation, sql)
	}
}

func TestRangeScalarAdmissionIsReadOnly(t *testing.T) {
	addOne := func(b *QueryBuilder, _ *planpb.SubqueryRef, _ *BindContext) {
		expr, err := BindFuncExprImplByPlanExpr(b.GetContext(), "+", []*planpb.Expr{
			b.qry.Nodes[4].ProjectList[0], makePlan2Int64ConstExprWithType(1),
		})
		require.NoError(t, err)
		b.qry.Nodes[4].ProjectList[0] = expr
	}
	for _, tc := range []struct {
		name      string
		change    func(*QueryBuilder, *planpb.SubqueryRef, *BindContext)
		dependent bool
	}{
		{"native fast path", func(*QueryBuilder, *planpb.SubqueryRef, *BindContext) {}, false},
		{"computed result", func(b *QueryBuilder, _ *planpb.SubqueryRef, _ *BindContext) {
			b.qry.Nodes[4].ProjectList[0] = makePlan2Int64ConstExprWithType(1)
		}, true},
		{"aggregate expression", addOne, false},
		{"guarded expression", addOne, true},
		{"explicit group", func(b *QueryBuilder, _ *planpb.SubqueryRef, _ *BindContext) {
			b.qry.Nodes[3].GroupBy = []*planpb.Expr{b.qry.Nodes[3].AggList[0].GetF().Args[0]}
		}, true},
		{"having", func(b *QueryBuilder, _ *planpb.SubqueryRef, _ *BindContext) {
			b.qry.Nodes[3].FilterList = []*planpb.Expr{newSubqueryBoolConst(true)}
		}, true},
		{"limit zero", func(b *QueryBuilder, _ *planpb.SubqueryRef, _ *BindContext) {
			b.qry.Nodes[4].Limit = makePlan2Uint64ConstExprWithType(0)
		}, true},
		{"offset", func(b *QueryBuilder, _ *planpb.SubqueryRef, _ *BindContext) {
			b.qry.Nodes[4].Offset = makePlan2Uint64ConstExprWithType(1)
		}, true},
		{"rank", func(b *QueryBuilder, _ *planpb.SubqueryRef, _ *BindContext) {
			b.qry.Nodes[4].RankOption = &planpb.RankOption{Mode: "force"}
		}, true},
		{"multiple bindings", func(_ *QueryBuilder, _ *planpb.SubqueryRef, c *BindContext) {
			c.bindings = append(c.bindings, c.bindings[0])
		}, true},
		{"unsupported input expression", func(b *QueryBuilder, _ *planpb.SubqueryRef, _ *BindContext) {
			b.qry.Nodes[3].AggList[0].GetF().Args = nil
		}, true},
		{"quantified control", func(_ *QueryBuilder, s *planpb.SubqueryRef, _ *BindContext) {
			s.Typ = planpb.SubqueryRef_EXISTS
		}, false},
		{"CTE private marker", func(b *QueryBuilder, _ *planpb.SubqueryRef, _ *BindContext) {
			typ := b.qry.Nodes[1].TableDef.Cols[0].Typ
			b.qry.Nodes = append(b.qry.Nodes, &planpb.Node{NodeId: 5, NodeType: planpb.Node_PROJECT,
				Children: []int32{1}, BindingTags: []int32{21}, ProjectList: []*planpb.Expr{GetColExpr(typ, 20, 0)}})
			b.qry.Nodes[2].Children[0] = 5
			b.qry.Nodes[2].FilterList[0].GetF().Args[0].GetCol().RelPos = 21
			b.qry.Nodes[3].AggList[0].GetF().Args[0].GetCol().RelPos = 21
			b.localCTERoots = map[int32]bool{5: true}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
			integer := planpb.Type{Id: int32(types.T_int32)}
			rowID := planpb.Type{Id: int32(types.T_Rowid), NotNullable: true}
			scan := func(id, tag int32) *planpb.Node {
				return &planpb.Node{NodeId: id, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{tag},
					TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "k", Typ: integer}, {Name: catalog.Row_ID, Typ: rowID, Hidden: true}},
						Name2ColIndex: map[string]int32{catalog.Row_ID: 1}}}
			}
			pred, err := BindFuncExprImplByPlanExpr(b.GetContext(), "<=", []*planpb.Expr{
				GetColExpr(integer, 20, 0), {Typ: integer, Expr: &planpb.Expr_Corr{Corr: &planpb.CorrColRef{RelPos: 10, Depth: 1}}},
			})
			require.NoError(t, err)
			agg, err := BindFuncExprImplByPlanExpr(b.GetContext(), "max", []*planpb.Expr{GetColExpr(integer, 20, 0)})
			require.NoError(t, err)
			b.qry.Nodes = []*planpb.Node{scan(0, 10), scan(1, 20),
				{NodeId: 2, NodeType: planpb.Node_FILTER, Children: []int32{1}, FilterList: []*planpb.Expr{pred}},
				{NodeId: 3, NodeType: planpb.Node_AGG, Children: []int32{2}, BindingTags: []int32{30, 31}, AggList: []*planpb.Expr{agg}},
				{NodeId: 4, NodeType: planpb.Node_PROJECT, Children: []int32{3}, BindingTags: []int32{40}, ProjectList: []*planpb.Expr{GetColExpr(integer, 31, 0)}},
			}
			subCtx := &BindContext{aggregateTag: 31, aggregates: []*planpb.Expr{agg}, results: b.qry.Nodes[4].ProjectList}
			b.ctxByNode = []*BindContext{nil, nil, subCtx, subCtx, subCtx}
			ctx := &BindContext{bindings: []*Binding{{tag: 10, nodeId: 0, cols: []string{"k", catalog.Row_ID}, colIsHidden: []bool{false, true}}}}
			sub := &planpb.SubqueryRef{Typ: planpb.SubqueryRef_SCALAR, NodeId: 4, RowSize: 1}
			tc.change(b, sub, ctx)
			before := proto.Clone(b.qry).(*planpb.Query)
			require.Equal(t, tc.dependent, b.rangeScalarNeedsDependent(0, sub, ctx, tc.name == "guarded expression"))
			require.True(t, proto.Equal(before, b.qry), "admission changed the lexical relation")
		})
	}
}
