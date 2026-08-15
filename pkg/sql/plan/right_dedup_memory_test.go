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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestDisableMemoryUnsafeRightDedupUsesCombinedMapSize(t *testing.T) {
	const combinedMapBytes = 64*1024 + 128*1024

	t.Run("combined maps fit", func(t *testing.T) {
		builder, joins := makeChainedRightDedupBuilder(combinedMapBytes)

		builder.disableMemoryUnsafeRightDedup(4)

		require.True(t, joins[0].IsRightJoin)
		require.True(t, joins[1].IsRightJoin)
	})

	t.Run("combined maps exceed budget", func(t *testing.T) {
		builder, joins := makeChainedRightDedupBuilder(combinedMapBytes - 1)

		builder.disableMemoryUnsafeRightDedup(4)

		require.False(t, joins[0].IsRightJoin)
		require.False(t, joins[1].IsRightJoin)
	})
}

func TestDisableMemoryUnsafeRightDedupHonorsRowThreshold(t *testing.T) {
	t.Run("combined keys fit", func(t *testing.T) {
		builder, joins := makeChainedRightDedupBuilder(2201)

		builder.disableMemoryUnsafeRightDedup(4)

		require.True(t, joins[0].IsRightJoin)
		require.True(t, joins[1].IsRightJoin)
	})

	t.Run("combined keys reach threshold", func(t *testing.T) {
		builder, joins := makeChainedRightDedupBuilder(2200)

		builder.disableMemoryUnsafeRightDedup(4)

		require.False(t, joins[0].IsRightJoin)
		require.False(t, joins[1].IsRightJoin)
	})
}

func TestDisableMemoryUnsafeRightDedupRejectsUnknownCardinality(t *testing.T) {
	tests := []struct {
		name  string
		stats *planpb.Stats
	}{
		{name: "non-finite", stats: &planpb.Stats{Outcnt: math.NaN()}},
		{name: "default sentinel", stats: DefaultStats()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder, joins := makeChainedRightDedupBuilder(1 << 30)
			builder.qry.Nodes[0].Stats = test.stats

			builder.disableMemoryUnsafeRightDedup(4)

			require.False(t, joins[0].IsRightJoin)
			require.False(t, joins[1].IsRightJoin)
		})
	}
}

func TestDisableMemoryUnsafeRightDedupSharesBudgetWithInputUniqueCandidates(t *testing.T) {
	const sharedBudget = 150 * 1024
	builder, joins := makeChainedRightDedupBuilder(sharedBudget)
	joins[0].DedupInputKeysUnique = true
	// Raise the lookup-only target to the next int-map growth step. Both
	// classes then fit 150 KiB independently, while their resident sum does not.
	builder.qry.Nodes[1].Stats = &planpb.Stats{Outcnt: 1100}
	inputUniqueBytes, inputUniqueRows, inputUniqueUnsafe := builder.rightDedupMemoryTotals(
		joins[:1], true, math.MaxUint64,
	)
	ordinaryBytes, ordinaryRows, ordinaryUnsafe := builder.rightDedupMemoryTotals(
		joins[1:], false, math.MaxUint64,
	)
	require.False(t, inputUniqueUnsafe)
	require.False(t, ordinaryUnsafe)
	require.LessOrEqual(t, inputUniqueBytes, uint64(sharedBudget))
	require.LessOrEqual(t, ordinaryBytes, uint64(sharedBudget))
	require.Greater(t, inputUniqueBytes+ordinaryBytes, uint64(sharedBudget),
		"each class fits independently, but their resident maps exceed the shared budget")

	builder.disableMemoryUnsafeRightDedup(4)

	require.True(t, joins[0].IsRightJoin, "the flagged PK candidate only retains its target map")
	require.False(t, joins[1].IsRightJoin, "the ordinary candidate must include the retained lookup-only map")
	require.Equal(t, uint64(1100), inputUniqueRows)
	require.Equal(t, uint64(1100), ordinaryRows)
}

func TestDisableMemoryUnsafeRightDedupCombinesInputUniqueRows(t *testing.T) {
	const sharedRowBudget = 1150
	builder, joins := makeChainedRightDedupBuilder(sharedRowBudget)
	joins[0].DedupInputKeysUnique = true

	builder.disableMemoryUnsafeRightDedup(4)

	require.True(t, joins[0].IsRightJoin)
	require.False(t, joins[1].IsRightJoin,
		"100 lookup-only rows plus 1100 ordinary rows reach the shared row limit")
}

func TestDisableMemoryUnsafeRightDedupUnknownClassDoesNotPoisonRevertedPeer(t *testing.T) {
	t.Run("unknown input-unique class", func(t *testing.T) {
		builder, joins := makeChainedRightDedupBuilder(1 << 30)
		joins[0].DedupInputKeysUnique = true
		builder.qry.Nodes[1].Stats = &planpb.Stats{Outcnt: math.NaN()}

		builder.disableMemoryUnsafeRightDedup(4)

		require.False(t, joins[0].IsRightJoin)
		require.True(t, joins[1].IsRightJoin)
	})

	t.Run("unknown ordinary class", func(t *testing.T) {
		builder, joins := makeChainedRightDedupBuilder(1 << 30)
		joins[0].DedupInputKeysUnique = true
		builder.qry.Nodes[3].Stats = &planpb.Stats{Outcnt: math.Inf(1)}

		builder.disableMemoryUnsafeRightDedup(4)

		require.True(t, joins[0].IsRightJoin)
		require.False(t, joins[1].IsRightJoin)
	})
}

func TestAddRightDedupMemoryTotalsRejectsOverflow(t *testing.T) {
	_, _, ok := addRightDedupMemoryTotals(math.MaxUint64-1, 1, 2, 1, math.MaxUint64)
	require.False(t, ok)

	_, _, ok = addRightDedupMemoryTotals(1, math.MaxUint64, 1, 1, math.MaxUint64)
	require.False(t, ok)
}

func makeChainedRightDedupBuilder(joinSpillMem int64) (*QueryBuilder, []*planpb.Node) {
	source := &planpb.Node{NodeType: planpb.Node_VALUE_SCAN, Stats: &planpb.Stats{Outcnt: 1000}}
	targetPK := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, Stats: &planpb.Stats{Outcnt: 100}}
	pkDedup := &planpb.Node{
		NodeType:          planpb.Node_JOIN,
		JoinType:          planpb.Node_DEDUP,
		Children:          []int32{1, 0},
		OnList:            []*planpb.Expr{makeRightDedupEquality(types.T_int64)},
		OnDuplicateAction: planpb.Node_FAIL,
		IsRightJoin:       true,
		Stats:             &planpb.Stats{Outcnt: 100}, // stale pre-swap RIGHT DEDUP estimate
	}
	targetUnique := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, Stats: &planpb.Stats{Outcnt: 100}}
	uniqueDedup := &planpb.Node{
		NodeType:          planpb.Node_JOIN,
		JoinType:          planpb.Node_DEDUP,
		Children:          []int32{3, 2},
		OnList:            []*planpb.Expr{makeRightDedupEquality(types.T_varchar)},
		OnDuplicateAction: planpb.Node_FAIL,
		IsRightJoin:       true,
		Stats:             &planpb.Stats{Outcnt: 100},
	}
	return &QueryBuilder{
		qry:          &planpb.Query{Nodes: []*planpb.Node{source, targetPK, pkDedup, targetUnique, uniqueDedup}},
		joinSpillMem: joinSpillMem,
	}, []*planpb.Node{pkDedup, uniqueDedup}
}

func makeRightDedupEquality(typ types.T) *planpb.Expr {
	planType := planpb.Type{Id: int32(typ)}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{F: &planpb.Function{Args: []*planpb.Expr{
			{Typ: planType, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}},
			{Typ: planType, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0}}},
		}}},
	}
}
