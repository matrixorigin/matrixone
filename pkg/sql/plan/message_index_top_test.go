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
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestTopValueThroughIndexJoin(t *testing.T) {
	for _, tc := range []struct {
		name, barrier                                                           string
		project, shared, sharedStep, rightKey, shuffledParent, nullable, direct bool
		want                                                                    bool
		flag                                                                    planpb.OrderBySpec_OrderByFlag
	}{
		{name: "probe", want: true},
		{name: "row_preserving_project", project: true, want: true},
		{name: "nullable_first_key", nullable: true, want: true},
		{name: "nullable_direct_scan", nullable: true, direct: true, want: true},
		{name: "nullable_desc_defaults_last", nullable: true, flag: planpb.OrderBySpec_DESC, want: true},
		{name: "nullable_asc_explicit_last", nullable: true, flag: planpb.OrderBySpec_ASC | planpb.OrderBySpec_NULLS_LAST, want: true},
		{name: "nullable_desc_explicit_first", nullable: true, flag: planpb.OrderBySpec_DESC | planpb.OrderBySpec_NULLS_FIRST, want: true},
		{name: "nullable_desc_direct_scan", nullable: true, direct: true, flag: planpb.OrderBySpec_DESC, want: true},
		{name: "nonnullable_direct_scan", direct: true, want: true},
		{name: "project_limit", project: true, barrier: "project_limit"},
		{name: "project_filter", project: true, barrier: "project_filter"},
		{name: "join_limit", barrier: "join_limit"},
		{name: "join_offset", barrier: "join_offset"},
		{name: "scan_limit", barrier: "scan_limit"},
		{name: "index_column", rightKey: true},
		{name: "shared_join", shared: true},
		{name: "shared_across_steps", sharedStep: true},
		{name: "shuffled_parent_keeps_runtime_filter", shuffledParent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
			typ := planpb.Type{Id: int32(types.T_int64), NotNullable: !tc.nullable}
			scanTag, idxTag, projectTag := int32(101), int32(102), int32(103)
			scan := &planpb.Node{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{scanTag},
				TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "v", Typ: typ}}}}
			index := &planpb.Node{NodeId: 1, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{idxTag},
				TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "key", Typ: typ}}}}
			project := &planpb.Node{NodeId: 2, NodeType: planpb.Node_PROJECT, Children: []int32{0},
				BindingTags: []int32{projectTag}, ProjectList: []*planpb.Expr{GetColExpr(typ, scanTag, 0)}}
			probe := int32(0)
			sortExpr := GetColExpr(typ, scanTag, 0)
			if tc.project {
				probe = 2
				sortExpr = GetColExpr(typ, projectTag, 0)
			}
			if tc.rightKey {
				sortExpr = GetColExpr(typ, idxTag, 0)
			}
			join := &planpb.Node{NodeId: 3, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INDEX,
				Children: []int32{probe, 1}}
			top := &planpb.Node{NodeId: 4, NodeType: planpb.Node_SORT, Children: []int32{3},
				OrderBy: []*planpb.OrderBySpec{{Expr: sortExpr, Flag: tc.flag}}, Limit: makePlan2Uint64ConstExprWithType(20)}
			union := &planpb.Node{NodeId: 5, NodeType: planpb.Node_UNION_ALL, Children: []int32{4, 3}}
			shuffled := &planpb.Node{NodeId: 6, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
				Children: []int32{3, 1}, Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{Shuffle: true}},
				RuntimeFilterBuildList: []*planpb.RuntimeFilterSpec{{Tag: 9}}}
			b.qry.Nodes = []*planpb.Node{scan, index, project, join, top, union, shuffled}
			if tc.shuffledParent {
				top.Children[0] = 6
			}
			if tc.direct {
				top.Children[0] = 0
			}
			switch tc.barrier {
			case "project_limit":
				project.Limit = makePlan2Uint64ConstExprWithType(1)
			case "project_filter":
				project.FilterList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
			case "join_limit":
				join.Limit = makePlan2Uint64ConstExprWithType(1)
			case "join_offset":
				join.Offset = makePlan2Uint64ConstExprWithType(1)
			case "scan_limit":
				scan.Limit = makePlan2Uint64ConstExprWithType(1)
			}
			root := int32(4)
			if tc.shared {
				root = 5
			}
			if tc.sharedStep {
				b.qry.Steps = []int32{4, 3}
			}
			b.handleMessageFromTopToScan(root)
			if !tc.want {
				require.Empty(t, top.SendMsgList)
				require.Empty(t, scan.RecvMsgList)
				if tc.shuffledParent {
					require.True(t, shuffled.Stats.HashmapStats.Shuffle)
					require.Len(t, shuffled.RuntimeFilterBuildList, 1)
				}
				return
			}
			require.Len(t, top.SendMsgList, 1)
			require.Equal(t, int32(message.MsgTopValue), top.SendMsgList[0].MsgType)
			require.Equal(t, top.SendMsgList, scan.RecvMsgList)
			require.Empty(t, index.RecvMsgList)
			require.Len(t, scan.OrderBy, 1)
			require.Equal(t, scanTag, scan.OrderBy[0].Expr.GetCol().RelPos)
			require.Nil(t, scan.IndexReaderParam, "INDEX JOIN cannot prove a static reader limit")
		})
	}
}
