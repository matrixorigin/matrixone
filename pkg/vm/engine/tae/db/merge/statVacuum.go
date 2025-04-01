// Copyright 2024 Matrix Origin
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
	"container/heap"
	"context"
	"fmt"
	"iter"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

var (
	DefaultVacuumOpts = &VacuumOpts{
		EnableDetail: true,
		HollowTopK:   10,
	}

	DefaultTombstoneOpts = &TombstoneOpts{
		L1Size:  8 * common.Const1MBytes,
		L1Count: 4,
		L2Size:  100 * common.Const1MBytes,
		L2Count: 2,
	}
)

type TombstoneOpts struct {
	L1Size  uint32
	L1Count int
	L2Size  uint32
	L2Count int
}

func (o *TombstoneOpts) WithL1(size uint32, count int) *TombstoneOpts {
	o.L1Size = size
	o.L1Count = count
	return o
}

func (o *TombstoneOpts) WithL2(size uint32, count int) *TombstoneOpts {
	o.L2Size = size
	o.L2Count = count
	return o
}

func (o *TombstoneOpts) Clone() *TombstoneOpts {
	return &TombstoneOpts{
		L1Size:  o.L1Size,
		L1Count: o.L1Count,
		L2Size:  o.L2Size,
		L2Count: o.L2Count,
	}
}

type VacuumOpts struct {
	EnableDetail bool
	HollowTopK   int
}

func NewVacuumOpts() *VacuumOpts {
	return &VacuumOpts{
		EnableDetail: DefaultVacuumOpts.EnableDetail,
		HollowTopK:   DefaultVacuumOpts.HollowTopK,
	}
}

func (o *VacuumOpts) WithEnableDetail(enable bool) *VacuumOpts {
	o.EnableDetail = enable
	return o
}

func (o *VacuumOpts) WithHollowTopK(k int) *VacuumOpts {
	o.HollowTopK = k
	return o
}

func (o *VacuumOpts) Clone() *VacuumOpts {
	return &VacuumOpts{
		EnableDetail: o.EnableDetail,
		HollowTopK:   o.HollowTopK,
	}
}

type ObjectStatsItem interface {
	GetCreatedAt() types.TS
	GetObjectStats() *objectio.ObjectStats
}

type VacuumStats struct {
	// basic
	TotalSize     uint64
	TotalRows     uint64
	HistoSize     [8]int
	HistoCreateAt [4]int // <1m, 1m-10m, 10m-30m, >30m

	// more
	HistoVacuumPercent [5]int // <20%, 20%-40%, 40%-60%, 60%-80%, >80%
	DataVacuumPercent  float64
	DelVacuumPercent   float64
	TopHollow          itemSet
}

func (s *VacuumStats) String() string {
	ret := fmt.Sprintf("TotalSize: %s, TotalRows: %d, HistoSize: %v, HistoCreateAt: %v",
		common.HumanReadableBytes(int(s.TotalSize)), s.TotalRows, s.HistoSize, s.HistoCreateAt)
	if s.DataVacuumPercent > 0 {
		ret += fmt.Sprintf("\n\t HistoVacuumPercent: %v, DataVacuumPercent: %.2f%%, DelVacuumPercent: %.2f%%", s.HistoVacuumPercent, s.DataVacuumPercent*100, s.DelVacuumPercent*100)
		for i, l := 0, s.TopHollow.Len(); i < l; i++ {
			item := heap.Pop(&s.TopHollow).(mItem)
			ret += fmt.Sprintf("\n\t TopHollow[%d]: %s, %s, %d", i, item.obj.ObjectShortName().ShortString(), common.HumanReadableBytes(int(item.obj.OriginSize())), item.vacuumScore)
		}
	}
	return ret
}

func timeLevelSince(t types.TS) int {
	ago := time.Since(t.ToTimestamp().ToStdTime())
	if ago < 1*time.Minute {
		return 0
	}
	if ago < 10*time.Minute {
		return 1
	}
	if ago < 30*time.Minute {
		return 2
	}
	return 3
}

func percentLevel(percent int) int {
	return min(percent/20, 4)
}

func GatherTombstoneTasks(ctx context.Context, tombstoneStats iter.Seq[*objectio.ObjectStats], opts *TombstoneOpts) (ret []mergeTask) {
	small := make([]*objectio.ObjectStats, 0, opts.L1Count)
	big := make([]*objectio.ObjectStats, 0, opts.L2Count)
	for stat := range tombstoneStats {
		if stat.OriginSize() < opts.L1Size {
			small = append(small, stat)
		} else if stat.OriginSize() < opts.L2Size {
			big = append(big, stat)
		}
	}

	if len(small) >= opts.L1Count {
		ret = append(ret, mergeTask{
			objs:        small,
			note:        "l1 small tombstone",
			kind:        taskHostDN,
			isTombstone: true,
		})
	}

	if len(big) >= opts.L2Count {
		ret = append(ret, mergeTask{
			objs:        big,
			note:        "l2 big tombstone",
			kind:        taskHostDN,
			isTombstone: true,
		})
	}
	return
}

func CalculateVacuumStats(ctx context.Context, tbl *catalog.TableEntry, opts *VacuumOpts) (*VacuumStats, error) {

	var (
		// For VacuumPercent
		bufferBatch   *containers.Batch
		objDelCounter map[types.Objectid]int
		// metas         []objectio.ObjectDataMeta

		ret = &VacuumStats{}
	)

	reader := txnbase.MockTxnReaderWithNow()
	tombstoneIt := tbl.MakeTombstoneVisibleObjectIt(reader)
	defer tombstoneIt.Release()

	// collect stats
	for tombstoneIt.Next() {
		item := tombstoneIt.Item()
		if !ObjectValid(item) {
			continue
		}
		stat := item.GetObjectStats()
		ret.TotalSize += uint64(stat.OriginSize())
		ret.TotalRows += uint64(stat.Rows())
		ret.HistoSize[sizeLevel(stat.OriginSize(), len(ret.HistoSize)-1)]++
		ret.HistoCreateAt[timeLevelSince(item.GetCreatedAt())]++

		if !opts.EnableDetail {
			continue
		}

		// IO read
		obj := item.GetObjectData()
		for blk := range item.BlockCnt() {
			if bufferBatch == nil {
				bufferBatch = containers.BuildBatchWithPool(
					[]string{objectio.TombstoneAttr_Rowid_Attr},
					[]types.Type{types.T_Rowid.ToType()},
					8192,
					obj.GetRuntime().VectorPool.Transient,
				)
				objDelCounter = make(map[types.Objectid]int)
				defer bufferBatch.Close()
			}
			if err := obj.Scan(
				ctx,
				&bufferBatch,
				reader,
				item.GetSchema(),
				uint16(blk),
				[]int{0}, // only the rowid column
				common.MergeAllocator,
			); err != nil {
				return nil, err
			}

			if err := containers.ForeachVector(bufferBatch.Vecs[0], func(v types.Rowid, _ bool, _ int) error {
				objDelCounter[*v.BorrowObjectID()]++
				return nil
			}, nil); err != nil {
				return nil, err
			}

			bufferBatch.Reset()
		}
	}

	// detail is enabled, calculate more stats
	if len(objDelCounter) > 0 {
		var topHollow itemSet
		var topk = opts.HollowTopK
		dataIt := tbl.MakeDataVisibleObjectIt(reader)
		defer dataIt.Release()
		totalDataRows := uint64(0)
		hittedDelRows := uint64(0)
		for dataIt.Next() {
			item := dataIt.Item()
			if !ObjectValid(item) {
				continue
			}
			totalDataRows += uint64(item.Rows())
			del := objDelCounter[*item.ID()]
			if del == 0 {
				// no delete on this object, skip
				continue
			}
			hittedDelRows += uint64(del)
			delPercent := del * 100 / int(item.Rows())
			// normalize the size by the max object size
			ratio := float64(item.OriginSize()) / float64(common.DefaultMaxOsizeObjBytes)
			percent := int(math.Round(float64(delPercent) * ratio))
			ret.HistoVacuumPercent[percentLevel(percent)]++
			if topk > 0 {
				heap.Push(&topHollow, mItem{vacuumScore: percent, obj: item})
				if topHollow.Len() > topk {
					heap.Pop(&topHollow)
				}
			}
		}
		if totalDataRows > 0 {
			ret.DataVacuumPercent = float64(hittedDelRows) / float64(totalDataRows)
		}
		if ret.TotalRows > 0 {
			ret.DelVacuumPercent = float64(ret.TotalRows-hittedDelRows) / float64(ret.TotalRows)
		}
		ret.TopHollow = topHollow
	}

	return ret, nil
}

// region: utils

type mItem struct {
	vacuumScore int
	obj         *catalog.ObjectEntry
}

type itemSet []mItem

func (is itemSet) Len() int { return len(is) }

func (is itemSet) Less(i, j int) bool {
	return is[i].vacuumScore < is[j].vacuumScore
}

func (is itemSet) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

func (is *itemSet) Push(x any) {
	item := x.(mItem)
	*is = append(*is, item)
}

func (is *itemSet) Pop() any {
	old := *is
	n := len(old)
	item := old[n-1]
	// old[n-1] = nil // avoid memory leak
	*is = old[0 : n-1]
	return item
}

func (is *itemSet) Clear() {
	old := *is
	*is = old[:0]
}

// endregion
