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
	"fmt"
	"sort"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func buildPreparedAggregatePlan(t *testing.T, sql string) *planpb.Prepare {
	t.Helper()
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, fmt.Sprintf("prepare stmt1 from '%s'", sql))
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.NotNil(t, prepare.GetPlan().GetQuery())
	return prepare
}

func collectParamPositions(expr *planpb.Expr, positions map[int32]struct{}) {
	if expr == nil {
		return
	}
	if param := expr.GetP(); param != nil {
		positions[param.Pos] = struct{}{}
		return
	}
	if function := expr.GetF(); function != nil {
		for _, arg := range function.Args {
			collectParamPositions(arg, positions)
		}
		return
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			collectParamPositions(item, positions)
		}
	}
}

func preparedParamPositions(prepare *planpb.Prepare) []int32 {
	positions := make(map[int32]struct{})
	for _, node := range prepare.GetPlan().GetQuery().Nodes {
		for _, exprs := range [][]*planpb.Expr{node.ProjectList, node.AggList, node.GroupBy} {
			for _, expr := range exprs {
				collectParamPositions(expr, positions)
			}
		}
	}
	result := make([]int32, 0, len(positions))
	for pos := range positions {
		result = append(result, pos)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func planListContainsParamPos(exprs []*planpb.Expr, pos int32) bool {
	for _, expr := range exprs {
		positions := make(map[int32]struct{})
		collectParamPositions(expr, positions)
		if _, ok := positions[pos]; ok {
			return true
		}
	}
	return false
}

func TestPreparedAggregateParametersAreDiscoveredAndExecutable(t *testing.T) {
	for _, function := range []string{"min", "max", "count", "group_concat"} {
		t.Run(function, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, fmt.Sprintf("select %s(?) from nation", function))
			require.Len(t, prepare.ParamTypes, 1)
			require.Equal(t, []int32{0}, preparedParamPositions(prepare))

			_, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{int64(1)})
			require.NoError(t, err)
			_, err = FillValuesOfParamsInPlan(context.Background(), prepare.Plan, nil)
			require.ErrorContains(t, err, "prepare params")
		})
	}
}

func TestPreparedProjectionAndGroupMarkersStayIndependent(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select ? as k, sum(n_nationkey) from nation group by ?")
	require.Len(t, prepare.ParamTypes, 2)
	require.Equal(t, []int32{0, 1}, preparedParamPositions(prepare))

	var projectHasFirst, groupHasSecond bool
	for _, node := range prepare.Plan.GetQuery().Nodes {
		projectHasFirst = projectHasFirst || planListContainsParamPos(node.ProjectList, 0)
		groupHasSecond = groupHasSecond || planListContainsParamPos(node.GroupBy, 1)
	}
	require.True(t, projectHasFirst)
	require.True(t, groupHasSecond)
}

func TestPreparedNestedGroupMarkerStaysIndependent(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select (? + 0) as k, sum(n_nationkey) from nation group by (? + 0)")
	require.Len(t, prepare.ParamTypes, 2)
	require.Equal(t, []int32{0, 1}, preparedParamPositions(prepare))
}

func TestPreparedEqualLookingAggregatesStayIndependent(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select min(?), min(?) from nation")
	require.Len(t, prepare.ParamTypes, 2)
	require.Equal(t, []int32{0, 1}, preparedParamPositions(prepare))
}

func TestPreparedNestedAggregateMarkersStayIndependent(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select min(abs(?)), min(abs(?)) from nation")
	require.Len(t, prepare.ParamTypes, 2)
	require.Equal(t, []int32{0, 1}, preparedParamPositions(prepare))
}

func TestWindowExpressionKeysRetainParameterOffsets(t *testing.T) {
	windowSpec := func() *tree.WindowSpec {
		return &tree.WindowSpec{
			HasFrame: true,
			Frame: &tree.FrameClause{
				Type:  tree.Rows,
				Start: &tree.FrameBound{Type: tree.CurrentRow},
				End:   &tree.FrameBound{Type: tree.CurrentRow},
			},
		}
	}
	first := testWindowFuncExpr("min", tree.FUNC_TYPE_DEFAULT, windowSpec(), testScalarFuncExpr("abs", tree.NewParamExpr(0)))
	second := testWindowFuncExpr("min", tree.FUNC_TYPE_DEFAULT, windowSpec(), testScalarFuncExpr("abs", tree.NewParamExpr(1)))

	require.NotEqual(t, windowExprAstKey(first), windowExprAstKey(second))
}
