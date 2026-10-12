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

package readutil

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func filterTestObject(t *testing.T, id byte) objectio.ObjectStats {
	t.Helper()
	objID := types.Objectid{id}
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, true, true)
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 2))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, objectio.BlockMaxRows+1))
	return *stats
}

func TestFilterObjectsUncommittedOwnership(t *testing.T) {
	uncommitted := filterTestObject(t, 11)
	extra := filterTestObject(t, 22)
	committed := filterTestObject(t, 33)
	node := &plan.Node{Stats: plan2.DefaultStats()}
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash

	for _, cnCount := range []int32{1, 2, 3} {
		t.Run(fmt.Sprintf("cn-count=%d", cnCount), func(t *testing.T) {
			owner := int32(plan2.SimpleCharHashToRange(uncommitted.ObjectName().ObjectId()[:], uint64(cnCount)))
			localCN := (owner + 1) % cnCount
			seen := make(map[types.Blockid]int)
			for cn := int32(0); cn < cnCount; cn++ {
				rsp := &engine.RangesShuffleParam{Node: node, CNCNT: cnCount, CNIDX: cn, IsLocalCN: cn == localCN}
				var workspace []objectio.ObjectStats
				if cn == localCN {
					workspace = []objectio.ObjectStats{uncommitted}
					if cnCount > 1 {
						require.True(t, plan2.ShouldSkipObjByShuffle(rsp, &uncommitted), "must exercise a different object's bucket")
					}
				}
				nextCalled := false
				next := func() (objectio.ObjectStats, error) {
					if nextCalled {
						return objectio.ZeroObjectStats, ErrNoMore
					}
					nextCalled = true
					return committed, nil
				}
				var blocks objectio.BlockInfoSlice
				_, _, _, _, _, _, _, _, err := FilterObjects(
					context.Background(), engine.RangesParam{Rsp: rsp}, nil, nil, nil, nil, nil,
					next, workspace, []objectio.ObjectStats{extra}, &blocks, false, nil, nil,
				)
				require.NoError(t, err)
				var want []types.Blockid
				for i, obj := range []objectio.ObjectStats{uncommitted, extra, committed} {
					if (i == 0 && cn != localCN) || (i != 0 && plan2.ShouldSkipObjByShuffle(rsp, &obj)) {
						continue
					}
					for blk := uint16(0); blk < 2; blk++ {
						want = append(want, obj.ConstructBlockInfo(blk).BlockID)
					}
				}
				var got []types.Blockid
				for i := 0; i < blocks.Len(); i++ {
					id := blocks.Get(i).BlockID
					got = append(got, id)
					seen[id]++
				}
				require.Equal(t, want, got)
			}
			require.Len(t, seen, 6)
			for _, count := range seen {
				require.Equal(t, 1, count, "every block must have exactly one reader")
			}
		})
	}
}

func TestFilterObjectsUncommittedStillFiltered(t *testing.T) {
	obj := filterTestObject(t, 11)
	node := &plan.Node{Stats: plan2.DefaultStats()}
	owner := int32(plan2.SimpleCharHashToRange(obj.ObjectName().ObjectId()[:], 2))
	rsp := &engine.RangesShuffleParam{Node: node, CNCNT: 2, CNIDX: 1 - owner, IsLocalCN: true}
	require.True(t, plan2.ShouldSkipObjByShuffle(rsp, &obj))
	wantErr := errors.New("filter failed")
	for _, tc := range []struct {
		name string
		keep bool
		err  error
	}{
		{name: "match", keep: true},
		{name: "reject"},
		{name: "error", err: wantErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			filter := func(stats *objectio.ObjectStats) (bool, error) {
				calls++
				require.Equal(t, obj, *stats)
				return tc.keep, tc.err
			}
			var blocks objectio.BlockInfoSlice
			_, _, _, _, _, _, _, _, err := FilterObjects(
				context.Background(), engine.RangesParam{Rsp: rsp}, filter, nil, nil, nil, nil,
				nil, []objectio.ObjectStats{obj}, nil, &blocks, false, nil, nil,
			)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, 1, calls)
			want := 0
			if tc.keep {
				want = 2
			}
			require.Equal(t, want, blocks.Len())
		})
	}
}
