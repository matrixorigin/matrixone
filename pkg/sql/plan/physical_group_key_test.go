// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"slices"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCompactPhysicalGroupKeys(t *testing.T) {
	for _, changed := range [][]int{nil, {0}, {1}, {2}, {0, 2}} {
		visible := make([]*pb.Expr, 3, 6)
		for i := range visible {
			visible[i] = GetColExpr(types.T_varchar.ToType().PlanType(), 1, int32(i))
		}
		physical := DeepCopyExprList(visible)
		visible[0].GetCol().Name = "display.column"
		physical[0].GetCol().Name = "column"
		original := DeepCopyExprList(visible)
		want := []int32{0, 1, 2}
		for j, i := range changed {
			physical[i].Typ.Width++
			want[i] = int32(3 + j)
		}
		groups, keys, err := CompactPhysicalGroupKeys(visible, physical)
		require.NoError(t, err)
		require.Len(t, groups, 3+len(changed))
		require.True(t, proto.Equal(&pb.Node{GroupBy: original}, &pb.Node{GroupBy: groups[:3]}))
		if len(changed) == 0 {
			require.Empty(t, keys)
		} else {
			slices.Sort(want)
			require.Equal(t, want, keys)
		}
		for _, i := range changed {
			require.True(t, proto.Equal(physical[i], groups[3+slices.Index(changed, i)]))
			require.NotSame(t, physical[i], groups[3+slices.Index(changed, i)])
		}
		require.Nil(t, visible[:cap(visible)][3], "hidden keys must not overwrite the visible backing array")
	}
	_, _, err := CompactPhysicalGroupKeys([]*pb.Expr{{}}, []*pb.Expr{{}, {}})
	require.Error(t, err)
	_, _, err = CompactPhysicalGroupKeys([]*pb.Expr{{}}, []*pb.Expr{nil})
	require.Error(t, err)
}

// Display labels are the only ignored root-column fields. Distinct identity,
// prepared binding, and future protobuf metadata must retain their own slot.
func TestPhysicalGroupKeyIdentityBoundaries(t *testing.T) {
	for name, change := range map[string]func(*pb.Expr){
		"relation":       func(e *pb.Expr) { e.GetCol().RelPos++ },
		"ordinal":        func(e *pb.Expr) { e.GetCol().ColPos++ },
		"prepared":       func(e *pb.Expr) { e.PreparedNumeric = &pb.PreparedNumericMetadata{} },
		"expr_unknown":   func(e *pb.Expr) { e.XXX_unrecognized = []byte{0xa0, 0x06, 0x01} },
		"column_unknown": func(e *pb.Expr) { e.GetCol().XXX_unrecognized = []byte{0xa0, 0x06, 0x01} },
	} {
		t.Run(name, func(t *testing.T) {
			visible := GetColExpr(types.T_varchar.ToType().PlanType(), 1, 0)
			physical := DeepCopyExpr(visible)
			change(physical)
			before, err := proto.Marshal(physical)
			require.NoError(t, err)
			groups, keys, err := CompactPhysicalGroupKeys([]*pb.Expr{visible}, []*pb.Expr{physical})
			require.NoError(t, err)
			require.Len(t, groups, 2)
			require.Equal(t, []int32{1}, keys)
			after, err := proto.Marshal(physical)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestMixedPadSpaceGroupKeyRetention(t *testing.T) {
	value := "coalesce(cast(n_name as char(8)),cast(n_comment as varchar(8)))"
	for _, sql := range []string{
		"select n_regionkey," + value + ",n_comment,count(*) from nation group by n_regionkey," + value + ",n_comment",
		"select distinct n_regionkey," + value + ",n_comment from nation",
		"select n_regionkey," + value + ",n_comment,count(distinct n_name) from nation group by n_regionkey," + value + ",n_comment",
	} {
		t.Run(sql, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
			require.NoError(t, err)
			found := false
			for _, n := range p.GetQuery().Nodes {
				if n.NodeType == pb.Node_AGG && len(n.GroupByHashKey) == 3 {
					require.Len(t, n.GroupBy, 4)
					require.Equal(t, []int32{0, 2, 3}, n.GroupByHashKey)
					require.True(t, n.GroupBy[1].Typ.PadSpace)
					require.False(t, n.GroupBy[3].Typ.PadSpace)
					require.Empty(t, n.PhysicalEqualityKeyList, "consumed DISTINCT-only expressions must not remain on AGG")
					found = true
				}
			}
			require.True(t, found, "expected a compact mixed-domain aggregate")
		})
	}
}
