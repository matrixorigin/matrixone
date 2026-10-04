// Copyright 2021 - 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func twBoundary(name string) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: name}},
	}
}

func twAgg(name string) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{Func: &plan.ObjectRef{ObjName: name}},
		},
	}
}

func TestBuildTimeWindowLayout(t *testing.T) {
	cases := []struct {
		name        string
		aggList     []*plan.Expr
		partitionBy int
		wantSlot    []int32
		wantAggIdx  []int32
		wantWStart  int32
		wantWEnd    int32
		wantPartSlt []int32
		wantColCnt  int32
	}{
		{
			name:       "aggregates only",
			aggList:    []*plan.Expr{twAgg("max"), twAgg("min")},
			wantSlot:   []int32{0, 1},
			wantAggIdx: []int32{0, 1},
			wantWStart: TimeWindowSlotNone,
			wantWEnd:   TimeWindowSlotNone,
			wantColCnt: 2,
		},
		{
			// Boundaries trail the aggregates no matter where they sit in
			// AggList; the binder puts _wstart first for `select _wstart, max(v)`.
			name:       "boundaries follow aggregates regardless of AggList order",
			aggList:    []*plan.Expr{twBoundary(TimeWindowStart), twBoundary(TimeWindowEnd), twAgg("max")},
			wantSlot:   []int32{1, 2, 0},
			wantAggIdx: []int32{2},
			wantWStart: 1,
			wantWEnd:   2,
			wantColCnt: 3,
		},
		{
			name:       "wend without wstart takes the first boundary slot",
			aggList:    []*plan.Expr{twBoundary(TimeWindowEnd), twAgg("max")},
			wantSlot:   []int32{1, 0},
			wantAggIdx: []int32{0 + 1},
			wantWStart: TimeWindowSlotNone,
			wantWEnd:   1,
			wantColCnt: 2,
		},
		{
			// The operator carries one flag per boundary, not one column per
			// reference, so repeats collapse onto the same slot.
			name:       "repeated wstart references share one slot",
			aggList:    []*plan.Expr{twBoundary(TimeWindowStart), twAgg("max"), twBoundary(TimeWindowStart)},
			wantSlot:   []int32{1, 0, 1},
			wantAggIdx: []int32{1},
			wantWStart: 1,
			wantWEnd:   TimeWindowSlotNone,
			wantColCnt: 2,
		},
		{
			name:       "boundary only",
			aggList:    []*plan.Expr{twBoundary(TimeWindowStart)},
			wantSlot:   []int32{0},
			wantAggIdx: nil,
			wantWStart: 0,
			wantWEnd:   TimeWindowSlotNone,
			wantColCnt: 1,
		},
		{
			name:        "partition keys come last",
			aggList:     []*plan.Expr{twBoundary(TimeWindowStart), twAgg("max")},
			partitionBy: 2,
			wantSlot:    []int32{1, 0},
			wantAggIdx:  []int32{1},
			wantWStart:  1,
			wantWEnd:    TimeWindowSlotNone,
			wantPartSlt: []int32{2, 3},
			wantColCnt:  4,
		},
		{
			name:       "empty",
			aggList:    nil,
			wantSlot:   []int32{},
			wantAggIdx: nil,
			wantWStart: TimeWindowSlotNone,
			wantWEnd:   TimeWindowSlotNone,
			wantColCnt: 0,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			node := &plan.Node{AggList: c.aggList}
			for i := 0; i < c.partitionBy; i++ {
				node.TimeWindowPartitionBy = append(node.TimeWindowPartitionBy, twAgg("col"))
			}

			layout := BuildTimeWindowLayout(node)
			require.Equal(t, c.wantSlot, layout.Slot)
			require.Equal(t, c.wantAggIdx, layout.AggIdx)
			require.Equal(t, c.wantWStart, layout.WStartSlot)
			require.Equal(t, c.wantWEnd, layout.WEndSlot)
			require.Equal(t, c.wantPartSlt, layout.PartitionSlot)
			require.Equal(t, c.wantColCnt, layout.ColCnt)
		})
	}
}

// The compiler builds the operator's columns from AggIdx and the boundary
// flags while the planner addresses them through Slot. If those two views ever
// disagree the projection reads someone else's column, which is exactly the
// drift this type exists to prevent.
func TestBuildTimeWindowLayoutSlotsAreConsistent(t *testing.T) {
	node := &plan.Node{
		AggList: []*plan.Expr{
			twBoundary(TimeWindowEnd),
			twAgg("max"),
			twBoundary(TimeWindowStart),
			twAgg("min"),
		},
		TimeWindowPartitionBy: []*plan.Expr{twAgg("col")},
	}
	layout := BuildTimeWindowLayout(node)

	// Aggregates occupy a dense prefix, in AggList order.
	for want, aggIdx := range layout.AggIdx {
		require.Equal(t, int32(want), layout.Slot[aggIdx])
	}
	require.Equal(t, []int32{1, 3}, layout.AggIdx)

	// Boundaries sit immediately after, and every reference agrees with the
	// authoritative boundary slot.
	require.Equal(t, int32(2), layout.WStartSlot)
	require.Equal(t, int32(3), layout.WEndSlot)
	require.Equal(t, layout.WStartSlot, layout.Slot[2])
	require.Equal(t, layout.WEndSlot, layout.Slot[0])

	// Partition keys take the tail, leaving the aggregates a prefix for FILL.
	require.Equal(t, []int32{4}, layout.PartitionSlot)
	require.Equal(t, int32(5), layout.ColCnt)

	for _, slot := range layout.Slot {
		require.Less(t, slot, layout.ColCnt)
		require.GreaterOrEqual(t, slot, int32(0))
	}
}
