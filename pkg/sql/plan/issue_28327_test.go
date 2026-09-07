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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func issue28327Plan(t *testing.T) (*QueryBuilder, int32, *plan.Node, *plan.IndexDef) {
	t.Helper()

	builder := NewQueryBuilder(plan.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)
	tableDef := makeFullTextJoinTestTableDef("ft_28327", true)
	registerFullTextJoinRegularIndexTable(builder, tableDef.Indexes[0].IndexTableName)
	tag := builder.genNewBindTag()
	scan := makeFullTextJoinTestScan(tableDef, tag, nil)
	scanID := builder.appendNode(scan, ctx)
	return builder, scanID, scan, tableDef.Indexes[0]
}

func issue28327Match(tableDef *plan.TableDef, tag int32, pattern string) *plan.Expr {
	return makeFullTextMatchExpr(pattern, 0, tableDef, tag, []int32{2, 3})
}

func TestIssue28327OptionalFullTextStreamsAnchorAtBase(t *testing.T) {
	builder, scanID, scan, indexDef := issue28327Plan(t)
	alpha := issue28327Match(scan.TableDef, scan.BindingTags[0], "alpha")
	beta := issue28327Match(scan.TableDef, scan.BindingTags[0], "beta")
	proj := &plan.Node{ProjectList: []*plan.Expr{alpha, beta}}

	rootID, _, _, served, err := builder.applyJoinFullTextIndices(
		scanID, proj, scan, makePlan2Uint64ConstExprWithType(2), nil,
		nil, nil, []int32{0, 1}, []*plan.IndexDef{indexDef, indexDef},
		nil, nil, map[int32]int32{}, map[[2]int32]int{}, map[[2]int32]*plan.Expr{})
	require.NoError(t, err)

	root := builder.qry.Nodes[rootID]
	require.Equal(t, plan.Node_JOIN, root.NodeType)
	require.Equal(t, plan.Node_LEFT, root.JoinType)
	first := builder.qry.Nodes[root.Children[0]]
	require.Equal(t, plan.Node_JOIN, first.NodeType)
	require.Equal(t, plan.Node_LEFT, first.JoinType)
	require.Equal(t, scanID, first.Children[0], "every optional stream must join from the base result")
	for _, join := range []*plan.Node{first, root} {
		require.Equal(t, scan.BindingTags[0], join.OnList[0].GetF().Args[0].GetCol().RelPos,
			"a later stream cannot join through an earlier nullable score stream")
	}

	require.Len(t, served, 2)
	for _, match := range []*plan.Expr{alpha, beta} {
		score := builder.servedFullTextScore(match.GetF(), served)
		require.NotNil(t, score, "each optional MATCH must have a NULL-safe score expression")
		require.NotNil(t, score.GetF())
		require.Equal(t, "coalesce", score.GetF().Func.ObjName)
	}
	for _, fn := range collectFullTextFunctionScans(builder, rootID) {
		require.Nil(t, fn.Limit, "optional streams must not be early-truncated")
	}
}
