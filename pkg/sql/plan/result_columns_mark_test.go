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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestGetResultColumnsFromPlanPreservesMarkJoinBooleanProvenance(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	const stored = planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED
	const computed = planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED
	const in = "role_id in (select n_nationkey from nation)"
	const any = "role_id = any (select n_nationkey from nation)"
	const source = " from mo_catalog.mo_role_privs"
	for _, tc := range []struct {
		name string
		sql  string
		want []planpb.ResultColumnKind
	}{
		{"in", "select " + in + source, []planpb.ResultColumnKind{computed}},
		{"any", "select " + any + source, []planpb.ResultColumnKind{computed}},
		{"distinct", "select distinct " + in + source, []planpb.ResultColumnKind{computed}},
		{"stored sibling first", "select with_grant_option, " + in + source, []planpb.ResultColumnKind{stored, computed}},
		{"stored sibling last", "select " + in + ", with_grant_option" + source, []planpb.ResultColumnKind{computed, stored}},
		{"distinct siblings", "select distinct with_grant_option, " + in + source, []planpb.ResultColumnKind{stored, computed}},
		{"derived", "select p from (select " + in + " as p" + source + ") d", []planpb.ResultColumnKind{computed}},
		{"cte", "with d as (select " + in + " as p" + source + ") select p from d", []planpb.ResultColumnKind{computed}},
		{"two marks", "select " + in + ", with_grant_option, " + any + source, []planpb.ResultColumnKind{computed, stored, computed}},
		{"grouped stored sibling", "select a.v, a.n in (select n_nationkey from nation) from " +
			"(select with_grant_option v, role_id n from mo_catalog.mo_role_privs group by with_grant_option, role_id) a",
			[]planpb.ResultColumnKind{stored, computed}},
		{"window over marks", "select first_value(flag) over (), first_value(p) over () from " +
			"(select with_grant_option flag, " + in + " as p" + source + ") d", []planpb.ResultColumnKind{stored, computed}},
		{"not in control", "select role_id not in (select n_nationkey from nation)" + source, []planpb.ResultColumnKind{computed}},
		{"exists control", "select exists (select n_nationkey from nation)" + source, []planpb.ResultColumnKind{computed}},
		{"literal in control", "select role_id in (7,9)" + source, []planpb.ResultColumnKind{computed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			columns := GetResultColumnsFromPlan(p)
			require.Len(t, columns, len(tc.want))
			for i, want := range tc.want {
				require.Equal(t, int32(types.T_bool), columns[i].Typ.Id)
				require.Equal(t, want, columns[i].ResultColumnKind, "column %d: %s", i, tc.sql)
				if want == computed {
					require.Empty(t, columns[i].OriginName)
					require.False(t, columns[i].Primary)
					require.False(t, columns[i].Unique)
				}
			}
		})
	}
}

func TestResultColumnMarkJoinOwnsOnlyItsMarker(t *testing.T) {
	typ := planpb.Type{Id: int32(types.T_bool)}
	col := func(rel, pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rel, ColPos: pos}}}
	}
	query := &planpb.Query{Nodes: []*planpb.Node{
		{NodeType: planpb.Node_TABLE_SCAN, ProjectList: []*planpb.Expr{col(0, 0)}},
		{
			NodeType: planpb.Node_AGG, Children: []int32{0},
			ProjectList: []*planpb.Expr{col(-1, 0)}, GroupBy: []*planpb.Expr{col(0, 0)},
		},
		{NodeType: planpb.Node_TABLE_SCAN, ProjectList: []*planpb.Expr{col(0, 0)}},
		{
			NodeType: planpb.Node_JOIN, JoinType: planpb.Node_MARK, Children: []int32{1, 2},
			ProjectList: []*planpb.Expr{col(0, 0), col(-1, 0)},
		},
	}}
	join := query.Nodes[3]
	// Both the child's grouping slot and the MARK node's own synthetic output
	// use (-1,0). Classify ownership before remapping into the child.
	require.Equal(t, planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED,
		newResultColumnKinds(query).kind(3, join.ProjectList[0]))
	require.Equal(t, planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED,
		newResultColumnKinds(query).kind(3, join.ProjectList[1]))
	require.False(t, newResultColumnKinds(query).computed(3, col(-1, 1)))
	join.BindingTags = []int32{42}
	join.ProjectList = nil
	require.True(t, newResultColumnKinds(query).computed(3, col(42, 0)),
		"the pre-remapping binding tag also identifies the MARK node's own result")
	require.False(t, newResultColumnKinds(query).computed(3, col(42, 1)))
	join.JoinType = planpb.Node_INNER
	require.False(t, newResultColumnKinds(query).computed(3, col(-1, 0)),
		"an unresolved negative reference on an ordinary JOIN is not a MARK output")
}
