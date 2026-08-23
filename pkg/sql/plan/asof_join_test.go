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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBuildAsofJoin(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t,
		"select l.k, r.ts from "+
			"(select 1 k, cast('2026-01-01 10:00:00' as timestamp) ts) l "+
			"asof left join "+
			"(select 99 unused, 1 k, cast('2026-01-01 09:59:00' as timestamp) ts) r "+
			"on l.k = r.k and l.ts >= r.ts tolerance interval 2 minute")
	require.NoError(t, err)

	var join *planpb.Node
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_ASOF_LEFT {
			join = node
			break
		}
	}
	require.NotNil(t, join)
	require.Equal(t, int32(1), join.AsofRightCol)
	// equality key, temporal predecessor predicate, and tolerance lower bound
	require.Len(t, join.OnList, 3)

	commuted, err := runOneStmt(NewMockOptimizer(false), t,
		"select * from "+
			"(select 1 k, cast('2026-01-01' as timestamp) ts) AS l "+
			"asof join (select 1 k, cast('2026-01-01' as timestamp) ts) r "+
			"on r.k = l.k and r.ts <= l.ts")
	require.NoError(t, err)
	require.NotNil(t, commuted)
}

func TestBuildAsofPreservesLegacyImplicitAliasJoin(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t,
		"select asof.k from "+
			"(select 1 k, cast('2026-01-01' as timestamp) ts) asof "+
			"join (select 1 k, cast('2026-01-02' as timestamp) ts) u "+
			"on asof.k = u.k and asof.ts > u.ts")
	require.NoError(t, err)

	var join *planpb.Node
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == planpb.Node_JOIN {
			join = node
			break
		}
	}
	require.NotNil(t, join)
	require.Equal(t, planpb.Node_INNER, join.JoinType)
}

func TestBuildAsofJoinRejectsInvalidContracts(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "missing equality key",
			sql: "select * from (select cast('2026-01-01' as timestamp) ts) AS l " +
				"asof join (select cast('2026-01-01' as timestamp) ts) r on l.ts >= r.ts",
			want: "at least one equality key",
		},
		{
			name: "forward lookup",
			sql: "select * from (select 1 k, cast('2026-01-01' as timestamp) ts) AS l " +
				"asof join (select 1 k, cast('2026-01-01' as timestamp) ts) r " +
				"on l.k = r.k and l.ts <= r.ts",
			want: "must look backward",
		},
		{
			name: "two temporal predicates",
			sql: "select * from (select 1 k, cast('2026-01-01' as timestamp) ts) AS l " +
				"asof join (select 1 k, cast('2026-01-01' as timestamp) ts) r " +
				"on l.k = r.k and l.ts >= r.ts and l.ts > r.ts",
			want: "exactly one temporal inequality",
		},
		{
			name: "missing temporal predicate",
			sql: "select * from (select 1 k) AS l asof join (select 1 k) r " +
				"on l.k = r.k",
			want: "exactly one temporal inequality",
		},
		{
			name: "predicate stays on left input",
			sql: "select * from (select 1 k, cast('2026-01-01' as timestamp) ts) AS l " +
				"asof join (select 1 k, cast('2026-01-01' as timestamp) ts) r " +
				"on l.k = r.k and l.ts >= l.ts",
			want: "must compare the left and right inputs",
		},
		{
			name: "temporal operand is not a column",
			sql: "select * from (select 1 k, cast('2026-01-01' as timestamp) ts) AS l " +
				"asof join (select 1 k, cast('2026-01-01' as timestamp) ts) r " +
				"on l.k = r.k and date_add(l.ts, interval 1 second) >= r.ts",
			want: "temporal operands must be columns",
		},
		{
			name: "temporal type coercion is not a column",
			sql: "select * from (select 1 k, cast('2026-01-01' as date) ts) AS l " +
				"asof join (select 1 k, cast('2026-01-01' as timestamp) ts) r " +
				"on l.k = r.k and l.ts >= r.ts",
			want: "temporal operands must be columns",
		},
		{
			name: "nonconstant tolerance",
			sql: "select * from (select 1 k, cast('2026-01-01' as timestamp) ts) AS l " +
				"asof join (select 1 k, cast('2026-01-01' as timestamp) ts) r " +
				"on l.k = r.k and l.ts >= r.ts tolerance interval l.k minute",
			want: "TOLERANCE must be a constant interval",
		},
		{
			name: "using clause",
			sql:  "select * from (select 1 k) AS l asof join (select 1 k) r using (k)",
			want: "requires an ON clause",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestRefreshAsofRightColAfterRemap(t *testing.T) {
	col := func(rel, pos int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rel, ColPos: pos}}}
	}
	condition := func(op string, left, right *planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: op}, Args: []*planpb.Expr{left, right},
		}}}
	}

	node := &planpb.Node{OnList: []*planpb.Expr{
		{},
		condition("=", col(0, 0), col(1, 0)),
		condition(">=", col(0, 1), col(1, 7)),
	}}
	require.NoError(t, refreshAsofRightColAfterRemap(node))
	require.Equal(t, int32(7), node.AsofRightCol)

	node.OnList = []*planpb.Expr{condition("<=", col(1, 4), col(0, 1))}
	require.NoError(t, refreshAsofRightColAfterRemap(node))
	require.Equal(t, int32(4), node.AsofRightCol)

	node.OnList = []*planpb.Expr{condition("=", col(0, 0), col(1, 0))}
	require.ErrorContains(t, refreshAsofRightColAfterRemap(node), "was lost")
}
