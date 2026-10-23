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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCorrelatedAggregateForwardsInlineCTEIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		columns   int
	}{
		{"scalar only", `WITH q AS (SELECT * FROM NATION) SELECT b.N_NATIONKEY,
   (SELECT MAX(x.N_REGIONKEY) FROM q x WHERE x.N_NATIONKEY<=b.N_NATIONKEY) FROM q b`, 2},
		{"marker before scalar", `WITH q AS (SELECT * FROM NATION) SELECT b.N_NATIONKEY,
   EXISTS(SELECT 1 FROM q x WHERE x.N_NATIONKEY=b.N_REGIONKEY),
   (SELECT MAX(x.N_REGIONKEY) FROM q x WHERE x.N_NATIONKEY<=b.N_NATIONKEY) FROM q b`, 3},
		{"marker after scalar", `WITH q AS (SELECT * FROM NATION) SELECT b.N_NATIONKEY,
   (SELECT MAX(x.N_REGIONKEY) FROM q x WHERE x.N_NATIONKEY<=b.N_NATIONKEY),
   EXISTS(SELECT 1 FROM q x WHERE x.N_NATIONKEY=b.N_REGIONKEY) FROM q b`, 3},
		{"consecutive scalars", `WITH q AS (SELECT * FROM NATION) SELECT b.N_NATIONKEY,
   (SELECT MAX(x.N_REGIONKEY) FROM q x WHERE x.N_NATIONKEY<=b.N_NATIONKEY),
   (SELECT COUNT(*) FROM q x WHERE x.N_NATIONKEY<b.N_NATIONKEY) FROM q b`, 3},
		{"renamed reordered columns", `WITH q(v,k) AS (SELECT N_REGIONKEY,N_NATIONKEY FROM NATION)
   SELECT b.k,(SELECT MAX(x.v) FROM q x WHERE x.k<=b.k) FROM q b`, 2},
		{"filtered inner CTE", `WITH q AS (SELECT * FROM NATION WHERE N_NATIONKEY<0)
			SELECT b.N_NATIONKEY,(SELECT MAX(x.N_REGIONKEY) FROM q x WHERE x.N_NATIONKEY<=b.N_NATIONKEY) FROM NATION b`, 2},
		{"nested transparent CTEs", `WITH a AS (SELECT * FROM NATION),q AS(SELECT * FROM a)
   SELECT b.N_NATIONKEY,(SELECT MAX(x.N_REGIONKEY) FROM q x WHERE x.N_NATIONKEY<=b.N_NATIONKEY) FROM q b`, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			q := p.GetQuery()
			assertReachablePlanHasNoCorrelatedExpr(t, q)
			require.Len(t, q.Headings, tc.columns, "native identity must stay private")
			for _, n := range q.Nodes {
				require.NotEqual(t, planpb.Node_WINDOW, n.NodeType, "do not enumerate identities")
			}
		})
	}
}

func TestCorrelatedAggregateCTEIdentityRejectsUnprovenSources(t *testing.T) {
	for _, tc := range []struct{ name, source string }{
		{"duplicate native identity", `SELECT N_NATIONKEY,N_REGIONKEY FROM NATION UNION ALL SELECT N_NATIONKEY,N_REGIONKEY FROM NATION`},
		{"volatile source filter", `SELECT N_NATIONKEY,N_REGIONKEY FROM NATION WHERE rand()>0`},
		{"computed projection", `SELECT N_NATIONKEY,ABS(N_REGIONKEY) AS N_REGIONKEY FROM NATION`},
		{"distinct", `SELECT DISTINCT N_NATIONKEY,N_REGIONKEY FROM NATION`},
		{"grouped", `SELECT N_NATIONKEY,MAX(N_REGIONKEY) AS N_REGIONKEY FROM NATION GROUP BY N_NATIONKEY`},
		{"multiplying join", `SELECT a.N_NATIONKEY,a.N_REGIONKEY FROM NATION a CROSS JOIN NATION b`},
		{"pagination", `SELECT N_NATIONKEY,N_REGIONKEY FROM NATION LIMIT 2`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t,
				`WITH q AS (`+tc.source+`) SELECT b.N_NATIONKEY,
     (SELECT MAX(x.N_REGIONKEY) FROM NATION x WHERE x.N_NATIONKEY<=b.N_NATIONKEY) FROM q b`)
			require.ErrorContains(t, err, "cannot be safely decorrelated")
		})
	}
}

func TestScalarCTEIdentityAdmissionDoesNotPublishPartialChanges(t *testing.T) {
	integer := planpb.Type{Id: int32(types.T_int32)}
	rowID := planpb.Type{Id: int32(types.T_Rowid), NotNullable: true}
	scan := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{10}, TableDef: &planpb.TableDef{
		Cols:          []*planpb.ColDef{{Name: "k", Typ: integer}, {Name: "v", Typ: integer}, {Name: catalog.Row_ID, Typ: rowID, Hidden: true}},
		Name2ColIndex: map[string]int32{catalog.Row_ID: 2},
	}}
	project := &planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{0}, BindingTags: []int32{20},
		ProjectList: []*planpb.Expr{GetColExpr(integer, 10, 0), GetColExpr(integer, 10, 1)}}
	computed := &planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{1}, BindingTags: []int32{30},
		ProjectList: []*planpb.Expr{makePlan2Int64ConstExprWithType(1)}}
	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{scan, project, computed}}, localCTERoots: map[int32]bool{1: true, 2: true}}
	before := make([]*planpb.Expr, len(project.ProjectList))
	for i, expr := range project.ProjectList {
		before[i] = DeepCopyExpr(expr)
	}
	require.Nil(t, builder.analyzeScalarCTERowSource(2, 1))
	require.Equal(t, before, project.ProjectList, "rejecting the outer projection must not widen its admitted child")
	source := builder.analyzeScalarCTERowSource(1, 2)
	require.NotNil(t, source)
	require.Equal(t, before, project.ProjectList, "analysis must remain read-only until the aggregate owner admits the rewrite")
	require.Equal(t, int32(2), source.marker.GetCol().ColPos)
	source.publish()
	require.Len(t, project.ProjectList, 3)
	require.Equal(t, before, project.ProjectList[:2])
	require.Nil(t, builder.findReachableRowIDColRef(1), "unrelated row-marker proofs must retain their stricter projection boundary")
}
