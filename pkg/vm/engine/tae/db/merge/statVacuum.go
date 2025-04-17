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
	"slices"
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
		CheckBigOnly: true,

		HollowTopK: 10,

		StartScore: 60,
		EndScore:   10,
		Duration:   1 * time.Hour,
	}

	DefaultTombstoneOpts = &TombstoneOpts{
		OneShot: false,
		L1Size:  8 * common.Const1MBytes,
		L1Count: 4,
		L2Size:  common.DefaultMaxOsizeObjBytes,
		L2Count: 2,
	}
)

type TombstoneOpts struct {
	OneShot bool
	L1Size  uint32
	L1Count int
	L2Size  uint32
	L2Count int
}

func (o *TombstoneOpts) String() string {
	return fmt.Sprintf("TombstoneOpts{OneShot: %t, L1S: %s, L1C: %d, L2S: %s, L2C: %d}",
		o.OneShot,
		common.HumanReadableBytes(int(o.L1Size)),
		o.L1Count,
		common.HumanReadableBytes(int(o.L2Size)),
		o.L2Count,
	)
}

func (o *TombstoneOpts) Clone() *TombstoneOpts {
	return &TombstoneOpts{
		OneShot: o.OneShot,
		L1Size:  o.L1Size,
		L1Count: o.L1Count,
		L2Size:  o.L2Size,
		L2Count: o.L2Count,
	}
}

func NewTombstoneOpts() *TombstoneOpts {
	return DefaultTombstoneOpts.Clone()
}

func (o *TombstoneOpts) WithL1(size uint32, count int) *TombstoneOpts {
	o.L1Size = size
	o.L1Count = count
	return o
}

func (o *TombstoneOpts) WithL2Count(count int) *TombstoneOpts {
	o.L2Count = count
	return o
}

func (o *TombstoneOpts) WithOneShot(oneShot bool) *TombstoneOpts {
	o.OneShot = oneShot
	return o
}

type VacuumOpts struct {
	EnableDetail bool
	CheckBigOnly bool
	HollowTopK   int

	StartScore int
	EndScore   int
	Duration   time.Duration
}

func (o *VacuumOpts) String() string {
	return fmt.Sprintf("VacuumOpts{Detail: %t, BigOnly: %t, TopK: %d, S: %d, E: %d, D: %v}",
		o.EnableDetail, o.CheckBigOnly, o.HollowTopK, o.StartScore, o.EndScore, o.Duration)
}

func (o *VacuumOpts) Clone() *VacuumOpts {
	return &VacuumOpts{
		EnableDetail: o.EnableDetail,
		CheckBigOnly: o.CheckBigOnly,
		HollowTopK:   o.HollowTopK,

		StartScore: o.StartScore,
		EndScore:   o.EndScore,
		Duration:   o.Duration,
	}
}

func NewVacuumOpts() *VacuumOpts {
	return DefaultVacuumOpts.Clone()
}

func (o *VacuumOpts) WithStartScore(start int) *VacuumOpts {
	o.StartScore = start
	return o
}

func (o *VacuumOpts) WithEndScore(end int) *VacuumOpts {
	o.EndScore = end
	return o
}

func (o *VacuumOpts) WithDuration(duration time.Duration) *VacuumOpts {
	o.Duration = duration
	return o
}

func (o *VacuumOpts) WithEnableDetail(enable bool) *VacuumOpts {
	o.EnableDetail = enable
	return o
}

func (o *VacuumOpts) WithHollowTopK(k int) *VacuumOpts {
	o.HollowTopK = k
	return o
}

func (o *VacuumOpts) WithCheckBigOnly(check bool) *VacuumOpts {
	o.CheckBigOnly = check
	return o
}

func (o *VacuumOpts) CalcScore(maxCreateAgo time.Duration) int {
	if maxCreateAgo > o.Duration {
		return o.EndScore
	}
	return o.StartScore + (o.StartScore-o.EndScore)*int(maxCreateAgo)/int(o.Duration)
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
	MaxCreateAgo  time.Duration

	DataVacuumScoreToCompact int

	// more
	HistoVacuumScore  [5]int // <20%, 20%-40%, 40%-60%, 60%-80%, >80%
	DataVacuumPercent float64
	DelVacuumPercent  float64
	TopHollow         itemSet
}

func (s *VacuumStats) String() string {
	ret := fmt.Sprintf("TotalSize: %s, TotalRows: %d, HistoSize: %v, HistoCreateAt: %v",
		common.HumanReadableBytes(int(s.TotalSize)), s.TotalRows, s.HistoSize, s.HistoCreateAt)
	if s.DataVacuumPercent > 0 {
		ret += fmt.Sprintf(
			"\n\t HistoVacuumScore: %v, DataVacuumPercent: %.2f%%, "+
				"DelVacuumPercent: %.2f%%, DataVacuumScoreToCompact: %d",
			s.HistoVacuumScore, s.DataVacuumPercent*100,
			s.DelVacuumPercent*100, s.DataVacuumScoreToCompact)
		for i, l := 0, s.TopHollow.Len(); i < l; i++ {
			item := heap.Pop(&s.TopHollow).(mItem)
			ret += fmt.Sprintf("\n\t TopHollow[%d]: %s, %s, %v, %d", i,
				item.obj.ObjectShortName().ShortString(),
				common.HumanReadableBytes(int(item.obj.OriginSize())),
				item.obj.GetLevel(),
				item.vacuumScore,
			)
		}
	}
	return ret
}

func timeLevelSince(ago time.Duration) int {
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

func scoreLevel(score int) int {
	return min(score/20, 4)
}

func GatherTombstoneTasks(ctx context.Context,
	tombstoneStats iter.Seq[*objectio.ObjectStats],
	opts *TombstoneOpts,
) (ret []mergeTask) {
	if opts.OneShot {
		targets := slices.Collect(tombstoneStats)
		if len(targets) > 0 {
			ret = append(ret, mergeTask{
				objs:        targets,
				note:        "one shot vacuum",
				kind:        taskHostDN,
				isTombstone: true,
			})
		}
		return
	}
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

func GatherCompactTasks(ctx context.Context,
	stats *VacuumStats,
	rc *resourceController,
) (ret []mergeTask) {
	for i, l := 0, stats.TopHollow.Len(); i < l; i++ {
		item := heap.Pop(&stats.TopHollow).(mItem)
		if item.vacuumScore >= stats.DataVacuumScoreToCompact {
			lv := item.obj.GetLevel()
			if lv > 1 {
				// make it down -1 level after compact
				lv -= 2
			} else {
				// merge it from scratch
				lv = 0
			}
			note := fmt.Sprintf("compact task %v/%v",
				item.vacuumScore, stats.DataVacuumScoreToCompact)
			// do not check the resource, leave it to mainLoop
			ret = append(ret, mergeTask{
				objs:        []*objectio.ObjectStats{item.obj.GetObjectStats()},
				kind:        taskHostDN,
				isTombstone: false,
				level:       int8(lv),
				note:        note,
				oSize:       int(item.obj.OriginSize()),
			})
		}
	}
	return ret
}

func CalculateVacuumStats(ctx context.Context,
	tbl *catalog.TableEntry,
	opts *VacuumOpts,
) (*VacuumStats, error) {

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

		if opts.CheckBigOnly && item.GetObjectStats().OriginSize() < common.DefaultMaxOsizeObjBytes {
			continue
		}

		stat := item.GetObjectStats()
		ret.TotalSize += uint64(stat.OriginSize())
		ret.TotalRows += uint64(stat.Rows())
		ret.HistoSize[sizeLevel(stat.OriginSize(), len(ret.HistoSize)-1)]++
		createAgo := time.Since(item.GetCreatedAt().ToTimestamp().ToStdTime())
		ret.HistoCreateAt[timeLevelSince(createAgo)]++
		if createAgo > ret.MaxCreateAgo {
			ret.MaxCreateAgo = createAgo
		}

		if !opts.EnableDetail {
			continue
		}

		// IO read
		obj := item.GetObjectData()
		each := func(v types.Rowid, _ bool, _ int) error {
			objDelCounter[*v.BorrowObjectID()]++
			return nil
		}
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
			err := containers.ForeachVector(bufferBatch.Vecs[0], each, nil)
			if err != nil {
				return nil, err
			}

			bufferBatch.Reset()
		}
	}

	ret.DataVacuumScoreToCompact = opts.CalcScore(ret.MaxCreateAgo)

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
			if opts.CheckBigOnly && item.OriginSize() < common.DefaultMaxOsizeObjBytes {
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
			score := objectVacuumScore(item, delPercent)
			ret.HistoVacuumScore[scoreLevel(score)]++
			if topk > 0 {
				heap.Push(&topHollow, mItem{vacuumScore: score, obj: item})
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

func objectVacuumScore(obj *catalog.ObjectEntry, delPercent int) int {
	sizeRatio := float64(obj.OriginSize()) / float64(common.DefaultMaxOsizeObjBytes)
	lvRatio := 0.5 * float64(obj.GetLevel()+1) / float64(8)
	final := (sizeRatio + lvRatio) / 2.0 // each 50% weight
	score := float64(delPercent) * final
	return int(math.Round(score))
}

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
