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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestDetermineShuffleForJoinPreservesOnlySameConditionRangeAfterRemap(t *testing.T) {
	t.Run("same condition keeps range through real remap", func(t *testing.T) {
		builder, join, aggregate := makeShuffleJoinRealRemapFixture(t)
		aggregate.Stats.HashmapStats.ShuffleColIdx = 1
		join.Stats.HashmapStats = &plan.HashMapStats{
			HashmapSize: threshHoldForHashShuffle - 1,
			Shuffle:     true, ShuffleColIdx: 1,
			ShuffleType:           plan.ShuffleType_Range,
			ShuffleTypeForMultiCN: plan.ShuffleTypeForMultiCN_Hybrid,
			ShuffleMethod:         plan.ShuffleMethod_Reuse,
			ShuffleColMin:         10, ShuffleColMax: 1_000_000,
			Ranges: []float64{100, 1_000}, Nullcnt: 7,
		}

		_, err := builder.remapAllColRefs(
			join.NodeId,
			0,
			make(map[[2]int32]int),
			make(map[[2]int32]bool),
			make(map[[2]int32]int),
		)
		require.NoError(t, err)
		// tempOptimizeForDML resets join cardinality flags after createQuery's
		// remap, while the earlier condition identity and range derivation remain.
		resetHashMapStats(join.Stats)
		join.Stats.HashmapStats.HashmapSize = threshHoldForHashShuffle - 1
		// Neither join key can reuse the aggregate's current third-key partition.
		aggregate.Stats.HashmapStats.ShuffleColIdx = 2

		determineShuffleForJoinWithColRefMode(join, builder, true)

		require.True(t, join.Stats.HashmapStats.Shuffle)
		require.Equal(t, int32(1), join.Stats.HashmapStats.ShuffleColIdx)
		require.Equal(t, plan.ShuffleType_Range, join.Stats.HashmapStats.ShuffleType)
		require.Equal(t, plan.ShuffleTypeForMultiCN_Simple, join.Stats.HashmapStats.ShuffleTypeForMultiCN)
		require.Equal(t, plan.ShuffleMethod_Normal, join.Stats.HashmapStats.ShuffleMethod)
		require.Equal(t, int64(10), join.Stats.HashmapStats.ShuffleColMin)
		require.Equal(t, int64(1_000_000), join.Stats.HashmapStats.ShuffleColMax)
		require.Equal(t, []float64{100, 1_000}, join.Stats.HashmapStats.Ranges)
		require.Equal(t, int64(7), join.Stats.HashmapStats.Nullcnt)
	})

	t.Run("later column cannot inherit earlier strategy", func(t *testing.T) {
		join := &plan.Node{
			NodeType: plan.Node_JOIN,
			JoinType: plan.Node_INNER,
			Children: []int32{0, 1},
			OnList: []*plan.Expr{
				makeShuffleJoinEquality(t, types.T_int64, 64, 0, 1, 0),
				makeShuffleJoinEquality(t, types.T_int64, 100_000, 0, 1, 1),
			},
			Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
				HashmapSize: 3_000_000,
				Shuffle:     true, ShuffleColIdx: 0,
				ShuffleType:           plan.ShuffleType_Range,
				ShuffleTypeForMultiCN: plan.ShuffleTypeForMultiCN_Hybrid,
				ShuffleMethod:         plan.ShuffleMethod_Reuse,
				ShuffleColMin:         10, ShuffleColMax: 1_000_000,
				Ranges: []float64{100, 1_000}, Nullcnt: 7,
			}},
		}
		builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{
			makeShuffleJoinTestChild(10, 10_000_000),
			makeShuffleJoinTestChild(20, 3_000_000),
		}}}

		determineShuffleForJoinWithColRefMode(join, builder, true)

		require.True(t, join.Stats.HashmapStats.Shuffle)
		require.Equal(t, int32(1), join.Stats.HashmapStats.ShuffleColIdx)
		require.Equal(t, plan.ShuffleType_Hash, join.Stats.HashmapStats.ShuffleType)
		require.Equal(t, plan.ShuffleTypeForMultiCN_Simple, join.Stats.HashmapStats.ShuffleTypeForMultiCN)
		require.Equal(t, plan.ShuffleMethod_Normal, join.Stats.HashmapStats.ShuffleMethod)
		require.Zero(t, join.Stats.HashmapStats.ShuffleColMin)
		require.Zero(t, join.Stats.HashmapStats.ShuffleColMax)
		require.Nil(t, join.Stats.HashmapStats.Ranges)
		require.Zero(t, join.Stats.HashmapStats.Nullcnt)
	})

	t.Run("expression cannot inherit column range strategy", func(t *testing.T) {
		join := &plan.Node{
			NodeType: plan.Node_JOIN,
			JoinType: plan.Node_INNER,
			Children: []int32{0, 1},
			OnList: []*plan.Expr{
				makeShuffleJoinSerialEquality(t, 100_000, 0, 1, 0),
			},
			Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
				HashmapSize: threshHoldForHashShuffle - 1,
				Shuffle:     true, ShuffleColIdx: 0,
				ShuffleType:   plan.ShuffleType_Range,
				ShuffleMethod: plan.ShuffleMethod_Reuse,
				ShuffleColMin: 10, ShuffleColMax: 1_000_000,
				Ranges: []float64{100, 1_000}, Nullcnt: 7,
			}},
		}
		builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{
			makeShuffleJoinTestChild(10, 10_000_000),
			makeShuffleJoinTestChild(20, 3_000_000),
		}}}

		determineShuffleForJoinWithColRefMode(join, builder, true)

		require.False(t, join.Stats.HashmapStats.Shuffle)
		require.Equal(t, plan.ShuffleType_Hash, join.Stats.HashmapStats.ShuffleType)
		require.Equal(t, plan.ShuffleMethod_Normal, join.Stats.HashmapStats.ShuffleMethod)
		require.Nil(t, join.Stats.HashmapStats.Ranges)
	})
}
