// Copyright 2022 Matrix Origin
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

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectLocationMarshalAndUnmarshal(t *testing.T) {
	var loc ObjectLocation
	for i := 0; i < len(loc); i++ {
		loc[i] = byte(i)
	}

	data, err := loc.Marshal()
	require.NoError(t, err)

	var ret ObjectLocation
	err = ret.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, loc, ret)
}

func mockStatsList(t *testing.T, statsCnt int) (statsList []objectio.ObjectStats) {
	for idx := 0; idx < statsCnt; idx++ {
		stats := objectio.NewObjectStats()
		blkCnt := rand.Uint32()%100 + 1
		require.Nil(t, objectio.SetObjectStatsBlkCnt(stats, blkCnt))
		require.Nil(t, objectio.SetObjectStatsRowCnt(stats, BlockMaxRows*(blkCnt-1)+BlockMaxRows*6/10))
		require.Nil(t, objectio.SetObjectStatsObjectName(stats, objectio.BuildObjectName(objectio.NewSegmentid(), uint16(blkCnt))))
		require.Nil(t, objectio.SetObjectStatsExtent(stats, objectio.NewExtent(0, 0, 0, 0)))
		require.Nil(t, objectio.SetObjectStatsSortKeyZoneMap(stats, index.NewZM(types.T_bool, 1)))

		statsList = append(statsList, *stats)
	}

	return
}

func TestNewStatsBlkIter(t *testing.T) {
	stats := mockStatsList(t, 1)[0]
	blks := UnfoldBlkInfoFromObjStats(&stats)

	iter := NewStatsBlkIter(&stats)
	for iter.Next() {
		id := iter.Sequence()
		actual := iter.Entry()
		require.True(t, blks[id] == *actual)
	}
}

func TestForeachBlkInObjStatsList(t *testing.T) {
	statsList := mockStatsList(t, 100)

	count := 0
	ForeachBlkInObjStatsList(false, func(blk *BlockInfo) bool {
		count++
		return false
	}, statsList...)

	require.Equal(t, count, 1)

	count = 0
	ForeachBlkInObjStatsList(true, func(blk *BlockInfo) bool {
		count++
		return false
	}, statsList...)

	require.Equal(t, count, len(statsList))

	count = 0
	ForeachBlkInObjStatsList(true, func(blk *BlockInfo) bool {
		count++
		return true
	}, statsList...)

	objectio.ForeachObjectStats(func(stats *objectio.ObjectStats) bool {
		count -= int(stats.BlkCnt())
		return true
	}, statsList...)

	require.Equal(t, count, 0)

	count = 0
	ForeachBlkInObjStatsList(false, func(blk *BlockInfo) bool {
		count++
		return true
	}, statsList...)

	objectio.ForeachObjectStats(func(stats *objectio.ObjectStats) bool {
		count -= int(stats.BlkCnt())
		return true
	}, statsList...)

	require.Equal(t, count, 0)
}
