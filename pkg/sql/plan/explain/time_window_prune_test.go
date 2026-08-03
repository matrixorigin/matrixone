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

package explain

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// timeWindowNode returns the single TIME_WINDOW node of a built plan.
func timeWindowNode(t *testing.T, sql string) *plan.Node {
	t.Helper()
	logicPlan, err := buildOneStmt(plan2.NewMockOptimizer(false), t, sql)
	require.NoError(t, err)

	var found *plan.Node
	for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
		if node.NodeType == plan.Node_TIME_WINDOW {
			require.Nil(t, found, "expected exactly one TIME_WINDOW node")
			found = node
		}
	}
	require.NotNil(t, found)
	return found
}

func fillNode(t *testing.T, sql string) *plan.Node {
	t.Helper()
	logicPlan, err := buildOneStmt(plan2.NewMockOptimizer(false), t, sql)
	require.NoError(t, err)

	for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
		if node.NodeType == plan.Node_FILL {
			return node
		}
	}
	t.Fatal("no FILL node found")
	return nil
}

// projectedSlots lists the operator output positions a node's ProjectList
// reads, in projection order.
func projectedSlots(node *plan.Node) []int32 {
	slots := make([]int32, 0, len(node.ProjectList))
	for _, expr := range node.ProjectList {
		if col := expr.GetCol(); col != nil {
			slots = append(slots, col.ColPos)
		}
	}
	return slots
}

const twTable = "constraint_test.t_on_update"

// A TIME_WINDOW ProjectList addresses the operator's output batch, which is
// laid out from the (pruned) AggList. Every projected slot must therefore be
// inside the layout the operator will actually build.
func requireSlotsWithinLayout(t *testing.T, node *plan.Node) {
	t.Helper()
	layout := plan2.BuildTimeWindowLayout(node)
	for _, slot := range projectedSlots(node) {
		require.Less(t, slot, layout.ColCnt,
			"projected slot %d is outside the operator's %d output columns", slot, layout.ColCnt)
	}
}

func TestTimeWindowPruneOperatorShape(t *testing.T) {
	t.Run("discarded aggregates are pruned and slots compact", func(t *testing.T) {
		// Only min(val) survives, so it must move to slot 0 rather than keep
		// max(val)'s original position. Reading slot 0 while max(val) still
		// occupied it returned max's values under the min alias.
		node := timeWindowNode(t,
			"select c from (select _wstart as a, max(val) as b, min(val) as c from "+twTable+
				" interval(updated_at, 5, second)) x")

		require.Len(t, node.AggList, 1)
		require.Equal(t, "min", node.AggList[0].GetF().Func.ObjName)
		require.Equal(t, []int32{0}, projectedSlots(node))
		requireSlotsWithinLayout(t, node)
	})

	t.Run("boundary output survives when aggregates are pruned", func(t *testing.T) {
		// _wstart is the only consumed output; it must be addressed at the
		// boundary slot, not at slot 0 where an aggregate used to sit.
		node := timeWindowNode(t,
			"select a from (select _wstart as a, max(val) as b, min(val) as c from "+twTable+
				" interval(updated_at, 5, second)) x")

		require.Len(t, node.AggList, 1)
		layout := plan2.BuildTimeWindowLayout(node)
		require.Equal(t, []int32{layout.WStartSlot}, projectedSlots(node))
		requireSlotsWithinLayout(t, node)
	})

	t.Run("wend survives when wstart is pruned", func(t *testing.T) {
		// Pruning _wstart must not abandon the rest of the remapping: _wend
		// used to be skipped entirely, leaving the parent unable to resolve it.
		node := timeWindowNode(t,
			"select d from (select _wstart as a, _wend as d, max(val) as b from "+twTable+
				" interval(updated_at, 5, second)) x")

		require.Len(t, node.AggList, 1)
		layout := plan2.BuildTimeWindowLayout(node)
		require.Equal(t, plan2.TimeWindowSlotNone, layout.WStartSlot)
		require.Equal(t, []int32{layout.WEndSlot}, projectedSlots(node))
		requireSlotsWithinLayout(t, node)
	})

	t.Run("aggregates stay a prefix ahead of the boundaries", func(t *testing.T) {
		node := timeWindowNode(t,
			"select _wstart, _wend, max(val) from "+twTable+
				" interval(updated_at, 10, minute) sliding(5, minute)")

		// Projection order must be [aggregate, _wstart, _wend]: FILL and the
		// operator both read this batch positionally.
		require.Equal(t, []int32{0, 1, 2}, projectedSlots(node))
		requireSlotsWithinLayout(t, node)
	})

	t.Run("everything discarded keeps a boundary row carrier", func(t *testing.T) {
		// The window count is still the row count, and the operator sizes its
		// batch off Vecs[0], so one column has to survive -- preferably the
		// boundary, which runs no aggregate.
		node := timeWindowNode(t,
			"select 1 from (select _wstart as a, max(val) as b from "+twTable+
				" interval(updated_at, 5, second)) x")

		require.Len(t, node.AggList, 1)
		isBoundary := node.AggList[0].GetCol() != nil
		require.True(t, isBoundary, "expected the boundary to be kept as the carrier")
		layout := plan2.BuildTimeWindowLayout(node)
		require.Equal(t, int32(1), layout.ColCnt)
	})

	t.Run("everything discarded without a boundary keeps an aggregate carrier", func(t *testing.T) {
		node := timeWindowNode(t,
			"select 1 from (select max(val) as b from "+twTable+
				" interval(updated_at, 5, second)) x")

		require.Len(t, node.AggList, 1)
		layout := plan2.BuildTimeWindowLayout(node)
		require.Equal(t, int32(1), layout.ColCnt)
	})

	t.Run("pruning a window aggregate keeps its side effect running below", func(t *testing.T) {
		// The binder rewrites a time-window aggregate's argument to a plain
		// reference to the child AGG's result, so sleep(0) lives in the AGG
		// node. Pruning max(...) here is safe only because AGG retains a
		// side-effecting aggregate regardless of who reads it.
		sql := "select a from (select _wstart as a, max(sleep(0)) as b from " + twTable +
			" interval(updated_at, 5, second)) x"

		tw := timeWindowNode(t, sql)
		require.Len(t, tw.AggList, 1, "the window's max(col) reference itself is inert and prunable")
		requireSlotsWithinLayout(t, tw)

		logicPlan, err := buildOneStmt(plan2.NewMockOptimizer(false), t, sql)
		require.NoError(t, err)
		aggFound := false
		for _, node := range reachablePlanNodes(logicPlan.GetQuery()) {
			if node.NodeType != plan.Node_AGG {
				continue
			}
			aggFound = true
			require.Len(t, node.AggList, 1, "the side-effecting aggregate must survive")
		}
		require.True(t, aggFound, "the sleep(0) call must still be evaluated somewhere")
	})

	t.Run("fill columns are pruned in lockstep with the window", func(t *testing.T) {
		// constructFill derives ColLen from this AggList and the operator fills
		// Vecs[0..ColLen) of the window's projected batch. If the window drops
		// max(val) but FILL still claims two columns, it runs off the batch.
		sql := "select c from (select _wstart as a, _wend as d, max(val) as b, min(val) as c from " +
			twTable + " interval(updated_at, 5, second) fill(prev)) x"

		tw := timeWindowNode(t, sql)
		fill := fillNode(t, sql)

		require.Len(t, tw.AggList, 1)
		require.Len(t, fill.AggList, 1)
		require.Equal(t, "min", fill.AggList[0].GetF().Func.ObjName)

		// FILL's ColLen must not exceed the aggregate prefix the window emits.
		layout := plan2.BuildTimeWindowLayout(tw)
		require.LessOrEqual(t, len(fill.AggList), len(layout.AggIdx))
		requireSlotsWithinLayout(t, tw)
	})

	t.Run("fill(linear) values are pruned with their columns", func(t *testing.T) {
		// fill(linear) builds FillVal out of references to the window's
		// aggregates. Counting those references as consumers would keep every
		// aggregate alive and defeat pruning entirely.
		sql := "select c from (select _wstart as a, max(val) as b, min(val) as c from " +
			twTable + " interval(updated_at, 5, second) fill(linear)) x"

		tw := timeWindowNode(t, sql)
		fill := fillNode(t, sql)

		require.Len(t, tw.AggList, 1)
		require.Len(t, fill.AggList, 1)
		require.Len(t, fill.FillVal, 1)
	})
}

func TestTimeWindowPartitionShape(t *testing.T) {
	t.Run("group by keys become partition keys ordered ahead of the timestamp", func(t *testing.T) {
		node := timeWindowNode(t,
			"select _wstart, id, max(val) from "+twTable+" group by id interval(updated_at, 5, second)")

		require.Len(t, node.TimeWindowPartitionBy, 1)
		// The operator finds boundaries by watching consecutive rows, so the
		// partition key has to sort ahead of the window's timestamp.
		require.Len(t, node.OrderBy, 2)
		requireSlotsWithinLayout(t, node)

		// The partition key is carried in the tail slots, leaving the
		// aggregates a prefix.
		layout := plan2.BuildTimeWindowLayout(node)
		require.Equal(t, []int32{2}, layout.PartitionSlot)
		require.Equal(t, int32(3), layout.ColCnt)
	})

	t.Run("partition keys survive even when the outer query discards them", func(t *testing.T) {
		// Dropping a partition key would merge groups that must stay apart, so
		// it is retained for grouping even with its output unused.
		node := timeWindowNode(t,
			"select b from (select _wstart as a, id, max(val) as b from "+twTable+
				" group by id interval(updated_at, 5, second)) x")

		require.Len(t, node.TimeWindowPartitionBy, 1)
		requireSlotsWithinLayout(t, node)
	})

	t.Run("no group by means no partitioning", func(t *testing.T) {
		node := timeWindowNode(t,
			"select _wstart, max(val) from "+twTable+" interval(updated_at, 5, second)")

		require.Empty(t, node.TimeWindowPartitionBy)
		require.Len(t, node.OrderBy, 1)
		requireSlotsWithinLayout(t, node)
	})
}
