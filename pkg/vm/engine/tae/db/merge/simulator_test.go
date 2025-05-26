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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"iter"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestBasicSimulator(t *testing.T) {
	player := NewSimPlayer()
	player.sexec.SetLogEnabled(true)
	player.ResetPace(100*time.Millisecond, 120*time.Second)

	player.Start()
	defer player.Stop()

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

	time.Sleep(3 * time.Second)
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

func addEventByLine(
	lines iter.Seq[string],
	baseTs int64,
) (sdata []SData, stombstones []STombstoneDesc) {
	var line map[string]any
	firstSeqTime := int64(0)
	for lineStr := range lines {
		json.Unmarshal([]byte(lineStr), &line)
		ts, _ := timestamp.ParseTimestamp(line["createTime"].(string))
		if firstSeqTime == 0 {
			firstSeqTime = ts.PhysicalTime
		}
		statsbs, _ := base64.StdEncoding.DecodeString(line["stats"].(string))
		stat := objectio.ObjectStats(statsbs)
		isTombstone := line["isTombstone"].(bool)
		createTime := types.BuildTS(baseTs+ts.PhysicalTime-firstSeqTime, 0)

		if !isTombstone {
			sdata = append(sdata, SData{
				stats:      &stat,
				createTime: createTime,
			})
		} else {
			desc := make([]catalog.LevelDist, 8)
			for i := range 8 {
				keyPrefix := fmt.Sprintf("l%d", i)
				dist := catalog.LevelDist{
					Lv: i,
				}
				dist.ObjCnt = int(line[keyPrefix+"ObjCnt"].(float64))
				dist.ObjCntProportion = line[keyPrefix+"ObjCntProportion"].(float64)
				dist.DelAvg = line[keyPrefix+"DelAvg"].(float64)
				dist.DelVar = line[keyPrefix+"DelVar"].(float64)
				desc[i] = dist
			}
			stombstones = append(stombstones, STombstoneDesc{
				SData: SData{stats: &stat, createTime: createTime},
				desc:  desc,
			})
		}
	}
	sort.Slice(sdata, func(i, j int) bool {
		return sdata[i].createTime.Physical() < sdata[j].createTime.Physical()
	})
	sort.Slice(stombstones, func(i, j int) bool {
		return stombstones[i].createTime.Physical() < stombstones[j].createTime.Physical()
	})
	return
}

func ExtractFromLocalLog(
	filename string,
	beginTime time.Time,
) (sdata []SData, stombstones []STombstoneDesc, err error) {
	ctx := context.Background()
	content, err := os.ReadFile(filename)
	if err != nil {
		err = moerr.NewInternalErrorf(ctx, "Failed to read file: %v", err)
		return
	}
	baseTs := beginTime.UTC().UnixNano()
	lines := strings.Split(string(content), "\n")
	sdata, stombstones = addEventByLine(func(yield func(string) bool) {
		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			yield(line)
		}
	}, baseTs)
	return
}

func ExtractFromLokiExport(
	filename string,
	beginTime time.Time,
) (sdata []SData, stombstones []STombstoneDesc, err error) {
	ctx := context.Background()
	content, err := os.ReadFile(filename)
	if err != nil {
		err = moerr.NewInternalErrorf(ctx, "Failed to read file: %v", err)
		return
	}
	var data []map[string]any
	json.Unmarshal(content, &data)
	baseTs := beginTime.UTC().UnixNano()
	sdata, stombstones = addEventByLine(func(yield func(string) bool) {
		for i := range data {
			yield(data[i]["line"].(string))
		}
	}, baseTs)
	return
}

func TestSimulatorOnLocalTpcc100(t *testing.T) {
	// t.Skip("turn on if experiment is needed")
	player := NewSimPlayer()
	player.ResetPace(10*time.Millisecond, 4*time.Second)
	// player.sexec.SetLogEnabled(true)
	filename := "/root/matrixone/zmtest/local-tpcc100.json"
	sdata, stombdesc, err := ExtractFromLocalLog(filename, player.sclock.Now())
	require.NoError(t, err)
	require.NotNil(t, stombdesc)

	player.SetEventSource(sdata, stombdesc)

	player.Start()
	defer player.Stop()

	player.WaitEventSourceExhaustedAndHoldFor(80 * time.Minute)

	t.Logf("report: %v", player.ReportString())
}

func TestSimulatorOnStatementInfo(t *testing.T) {
	t.Skip("turn on if experiment is needed")
	player := NewSimPlayer()
	player.ResetPace(10*time.Millisecond, 3*time.Second)

	filename := "/root/matrixone/zmtest/statement-info.json"
	sdata, stombdesc, err := ExtractFromLokiExport(filename, player.sclock.Now())
	require.NoError(t, err)
	require.Nil(t, stombdesc)

	player.SetEventSource(sdata, stombdesc)
	player.Start()
	defer player.Stop()

	time.Sleep(20 * time.Second)
	t.Logf("first: %s, last: %s",
		sdata[0].createTime.ToTimestamp().ToStdTime(),
		sdata[len(sdata)-1].createTime.ToTimestamp().ToStdTime(),
	)

	t.Logf("report: %v", player.ReportString())
}
