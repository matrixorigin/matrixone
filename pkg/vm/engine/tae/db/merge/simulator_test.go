// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestBasicSimulator(t *testing.T) {
	player := NewSimPlayer()
	player.sexec.SetLogEnabled(true)
	player.ResetPace(100*time.Millisecond, 120*time.Second)

	player.Start()

	const K = 1024
	sid := objectio.NewSegmentid

	// add 4 lv0 small objects
	data := []SData{
		{
			stats: newTestObjectStats(t, 100, 200, 24*K, 50, 0, sid(), 0),
		},
		{
			stats: newTestObjectStats(t, 100, 200, 43*K, 50, 0, sid(), 0),
		},
		{
			stats: newTestObjectStats(t, 50, 300, 20*K, 50, 0, sid(), 0),
		},
		{
			stats: newTestObjectStats(t, 150, 400, 20*K, 50, 0, sid(), 0),
		},
	}

	tombstones := []STombstone{
		{
			SData: SData{
				stats: newTestObjectStats(t, 0, 0, 30*K, 20, 0, sid(), 0),
			},
			distro: map[objectio.ObjectId]int{
				data[1].stats.ObjectLocation().ObjectId(): 14,
				data[2].stats.ObjectLocation().ObjectId(): 6,
			},
		},
		{
			SData: SData{
				stats: newTestObjectStats(t, 0, 0, 50*K, 40, 0, sid(), 0),
			},
			distro: map[objectio.ObjectId]int{
				data[0].stats.ObjectLocation().ObjectId(): 8,
				data[3].stats.ObjectLocation().ObjectId(): 2,
				objectio.NewObjectid():                    20,
			},
		},
		{
			SData: SData{
				stats: newTestObjectStats(t, 0, 0, 3*K, 1, 0, sid(), 0),
			},
			distro: map[objectio.ObjectId]int{
				data[0].stats.ObjectLocation().ObjectId(): 1,
			},
		},

		{
			SData: SData{
				stats: newTestObjectStats(t, 0, 0, 3*K, 1, 0, sid(), 0),
			},
			distro: map[objectio.ObjectId]int{
				data[0].stats.ObjectLocation().ObjectId(): 1,
			},
		},
	}

	for _, data := range data {
		player.AddData(data)
	}
	for _, tombstone := range tombstones {
		player.AddTombstone(tombstone)
	}

	player.AddTombstoneByDesc(STombstoneDesc{
		SData: SData{
			stats: newTestObjectStats(t, 0, 0, 512*K, 300, 0, sid(), 0),
		},
		desc: []catalog.LevelDist{
			{
				Lv:               0,
				ObjCnt:           2,
				ObjCntProportion: 0.5,
				DelAvg:           0.1,
				DelVar:           0.001,
			},
		},
	})

	time.Sleep(3 * time.Second)
	player.Stop()
	t.Logf("report: %v", player.ReportString())

}

func constantCount(zms []index.ZM) int {
	constantZMCount := 0
	for _, zm := range zms {
		if IsConstantZM(zm) {
			constantZMCount++
		}
	}
	return constantZMCount
}

// make coverage checker happy
func TestIterBailout(t *testing.T) {
	const K = 1024
	sid := objectio.NewSegmentid

	sdata := []SData{
		{
			stats: newTestObjectStats(t, 100, 200, 24*K, 50, 0, sid(), 0),
		},
		{
			stats: newTestObjectStats(t, 100, 200, 43*K, 50, 0, sid(), 0),
		},
	}
	stombstones := []STombstone{
		{
			SData: sdata[0],
		},
		{
			SData: sdata[1],
		},
	}

	{
		it := iterSDAsStats(sdata)
		it(func(stats *objectio.ObjectStats) bool {
			t.Logf("stats: %v", stats)
			return false
		})
	}

	{
		it := iterSTAsStats(stombstones)
		it(func(stats *objectio.ObjectStats) bool {
			t.Logf("stats: %v", stats)
			return false
		})
	}

	stable := &STable{
		data:      [8]map[objectio.ObjectId]SData{},
		tombstone: make(map[objectio.ObjectId]STombstone),
	}
	stable.data[0] = make(map[objectio.ObjectId]SData)
	stable.data[0][sdata[0].stats.ObjectLocation().ObjectId()] = sdata[0]
	stable.data[0][sdata[1].stats.ObjectLocation().ObjectId()] = sdata[1]
	stable.tombstone[sdata[0].stats.ObjectLocation().ObjectId()] = stombstones[0]
	stable.tombstone[sdata[1].stats.ObjectLocation().ObjectId()] = stombstones[1]

	{
		it := stable.IterDataItem()
		it(func(item catalog.MergeDataItem) bool {
			t.Logf("item: %v", item)
			return false
		})
	}

	{
		it := stable.IterTombstoneItem()
		it(func(item catalog.MergeTombstoneItem) bool {
			t.Logf("item: %v", item)
			return false
		})
	}

}

func TestSplitZM(t *testing.T) {
	zm := index.NewZM(types.T_int32, 0)
	zm.Update(int32(1))
	zm.Update(int32(20))
	{
		zmSplit := splitZM(zm, []int{1, 1, 1})
		require.Equal(t, 2, constantCount(zmSplit))
	}
	{
		zmSplit := splitZM(zm, []int{100, 100, 100})
		require.Equal(t, 0, constantCount(zmSplit))
	}
}

func TestUpdateStringTypeZM(t *testing.T) {
	zm := index.NewZM(types.T_varchar, 0)
	zm.Update([]byte("12345"))
	zm.Update([]byte("12346"))
	{
		zmSplit := splitZM(zm, []int{1, 1, 1})
		require.Equal(t, 2, constantCount(zmSplit))
	}

	{
		zmSplit := splitZM(zm, []int{100, 100, 100})
		require.Equal(t, 0, constantCount(zmSplit))
	}
}
