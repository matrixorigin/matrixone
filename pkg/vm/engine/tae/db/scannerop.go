// Copyright 2021 Matrix Origin
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

package db

import (
	"container/heap"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/shirou/gopsutil/v3/mem"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type ScannerOp interface {
	catalog.Processor
	PreExecute() error
	PostExecute() error
}

const (
	constMergeWaitDuration  = 1 * time.Minute
	constMergeMinBlks       = 5
	constHeapCapacity       = 300
	const1GBytes            = 1 << 30
	constMergeExpansionRate = 8
	constKeyMergeWaitFactor = "MO_MERGE_WAIT" // smaller value means shorter wait, cost much more io
)

func EnvOrDefaultFloat(key string, defaultValue float64) float64 {
	val, ok := os.LookupEnv(key)
	if !ok {
		return defaultValue
	}
	i, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return defaultValue
	}
	return i
}

// min heap item
type mItem[T any] struct {
	row   int
	entry T
}

type itemSet[T any] []*mItem[T]

func (is itemSet[T]) Len() int { return len(is) }

func (is itemSet[T]) Less(i, j int) bool {
	// max heap
	return is[i].row > is[j].row
}

func (is itemSet[T]) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

func (is *itemSet[T]) Push(x any) {
	item := x.(*mItem[T])
	*is = append(*is, item)
}

func (is *itemSet[T]) Pop() any {
	old := *is
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*is = old[0 : n-1]
	return item
}

func (is *itemSet[T]) Clear() {
	old := *is
	*is = old[:0]
}

// heapBuilder founds out blocks to be merged via maintaining a min heap holding
// up to default 300 items.
type heapBuilder[T any] struct {
	items itemSet[T]
	cap   int
}

func (h *heapBuilder[T]) reset() {
	h.items.Clear()
}

func (h *heapBuilder[T]) push(item *mItem[T]) {
	heap.Push(&h.items, item)
	if h.items.Len() > h.cap {
		heap.Pop(&h.items)
	}
}

// copy out the items in the heap
func (h *heapBuilder[T]) finish() []T {
	ret := make([]T, h.items.Len())
	for i, item := range h.items {
		ret[i] = item.entry
	}
	return ret
}

// deletableSegBuilder founds deletable segemnts of a table.
// if a segment has no any non-dropped blocks, it can be deleted. except the
// segment has the max segment id, appender may creates block in it.
type deletableSegBuilder struct {
	segHasNonDropBlk     bool
	segRowCnt, segRowDel int
	segIsSorted          bool
	isCreating           bool
	maxSegId             uint64
	segCandids           []*catalog.SegmentEntry // appendable
	nsegCandids          []*catalog.SegmentEntry // non-appendable
}

func (d *deletableSegBuilder) reset() {
	d.resetForNewSeg()
	d.maxSegId = 0
	d.segCandids = d.segCandids[:0]
	d.nsegCandids = d.nsegCandids[:0]
}

func (d *deletableSegBuilder) resetForNewSeg() {
	d.segHasNonDropBlk = false
	d.segIsSorted = false
	d.isCreating = false
	d.segRowCnt = 0
	d.segRowDel = 0
}

// call this when a non dropped block was found when iterating blocks of a segment,
// which make the builder skip this segment
func (d *deletableSegBuilder) hintNonDropBlock() {
	d.segHasNonDropBlk = true
}

func (d *deletableSegBuilder) push(entry *catalog.SegmentEntry) {
	isAppendable := entry.IsAppendable()
	if isAppendable && d.maxSegId < entry.SortHint {
		d.maxSegId = entry.SortHint
	}
	if d.segHasNonDropBlk {
		return
	}
	// all blocks has been dropped
	if isAppendable {
		d.segCandids = append(d.segCandids, entry)
	} else {
		d.nsegCandids = append(d.nsegCandids, entry)
	}
}

// copy out segment entries expect the one with max segment id.
func (d *deletableSegBuilder) finish() []*catalog.SegmentEntry {
	sort.Slice(d.segCandids, func(i, j int) bool { return d.segCandids[i].SortHint < d.segCandids[j].SortHint })
	if last := len(d.segCandids) - 1; last >= 0 && d.segCandids[last].SortHint == d.maxSegId {
		d.segCandids = d.segCandids[:last]
	}
	if len(d.segCandids) == 0 && len(d.nsegCandids) == 0 {
		return nil
	}
	ret := make([]*catalog.SegmentEntry, len(d.segCandids)+len(d.nsegCandids))
	copy(ret[:len(d.segCandids)], d.segCandids)
	copy(ret[len(d.segCandids):], d.nsegCandids)
	if cnt := len(d.nsegCandids); cnt != 0 {
		logutil.Info("Mergeblocks deletable nseg", zap.Int("cnt", cnt))
	}
	return ret
}

type activeTaskStats map[uint64]struct {
	blk      int
	estBytes int
}

// mergeLimiter consider update rate and time to decide to merge or not.
type mergeLimiter struct {
	objectMinRows       int
	maxRowsForBlk       int
	tableName           string
	estimateRowSize     int
	memAvail            int
	memSpare            int // 15% of total memory
	activeMergeBlkCount int32
	activeEstimateBytes int64
	taskConsume         struct {
		sync.Mutex
		m activeTaskStats
	}
	mergeWaitFactor float64
}

func (ml *mergeLimiter) AddActiveTask(id uint64, n, mergedRows int) {
	size := ml.calcMergeConsume(n, mergedRows)
	atomic.AddInt64(&ml.activeEstimateBytes, int64(size))
	atomic.AddInt32(&ml.activeMergeBlkCount, int32(n))
	ml.taskConsume.Lock()
	if ml.taskConsume.m == nil {
		ml.taskConsume.m = make(activeTaskStats)
	}
	ml.taskConsume.m[id] = struct {
		blk      int
		estBytes int
	}{n, size}
	ml.taskConsume.Unlock()
}

func (ml *mergeLimiter) OnExecDone(v any) {
	task := v.(tasks.MScopedTask)

	ml.taskConsume.Lock()
	stat := ml.taskConsume.m[task.ID()]
	delete(ml.taskConsume.m, task.ID())
	ml.taskConsume.Unlock()

	atomic.AddInt32(&ml.activeMergeBlkCount, -int32(stat.blk))
	atomic.AddInt64(&ml.activeEstimateBytes, -int64(stat.estBytes))
}

func (ml *mergeLimiter) checkMemAvail(blks, mergedRows int) bool {
	if ml.maxRowsForBlk == 0 {
		return false
	}

	merging := int(atomic.LoadInt64(&ml.activeEstimateBytes))
	left := ml.memAvail - ml.calcMergeConsume(blks, mergedRows) - merging

	return left > ml.memSpare
}

func (ml *mergeLimiter) calcMergeConsume(blks, mergedRows int) int {
	// by test exprience, full 8192 rows batch will expand to 8x memory comsupation.
	// the ExpansionRate will be moderated by the actual row number after applying deletes
	rate := float64(constMergeExpansionRate*mergedRows) / float64(blks*ml.maxRowsForBlk)
	if rate < 1.5 {
		rate = 1.5
	}
	return int(float64(blks*ml.maxRowsForBlk*ml.estimateRowSize) * rate)
}

func (ml *mergeLimiter) determineGapIntentFactor(rowsGap float64) float64 {
	return rowsGap / float64(ml.objectMinRows) * 5 * ml.mergeWaitFactor
}

type MergeTaskBuilder struct {
	db *DB
	*catalog.LoopProcessor
	tableRowCnt      int
	tableDelete      int
	tid              uint64
	limiter          *mergeLimiter
	delSegBuilder    *deletableSegBuilder
	sortedSegBuilder *heapBuilder[*catalog.SegmentEntry]
}

func newMergeTaskBuiler(db *DB) *MergeTaskBuilder {
	op := &MergeTaskBuilder{
		db:            db,
		LoopProcessor: new(catalog.LoopProcessor),
		limiter:       &mergeLimiter{},
		delSegBuilder: &deletableSegBuilder{
			segCandids:  make([]*catalog.SegmentEntry, 0),
			nsegCandids: make([]*catalog.SegmentEntry, 0),
		},
		sortedSegBuilder: &heapBuilder[*catalog.SegmentEntry]{
			items: make(itemSet[*catalog.SegmentEntry], 0, 2),
			cap:   2,
		},
	}

	op.TableFn = op.onTable
	op.BlockFn = op.onBlock
	op.SegmentFn = op.onSegment
	op.PostSegmentFn = op.onPostSegment
	op.PostTableFn = op.onPostTable
	return op
}

func (s *MergeTaskBuilder) checkSortedSegs(segs []*catalog.SegmentEntry) (
	mblks []*catalog.BlockEntry, msegs []*catalog.SegmentEntry, rows int,
) {
	if len(segs) < 2 {
		return
	}
	s1, s2 := segs[0], segs[1]
	r1 := s1.Stat.Rows - s1.Stat.Dels
	r2 := s2.Stat.Rows - s2.Stat.Dels
	logutil.Infof(
		"mergeblocks ======== %v %v | %v %v | %v %v",
		s1.SortHint, s2.SortHint, r1, r2, s1.Stat.MergeIntent, s2.Stat.MergeIntent,
	)
	// skip big segment which is over 200 blks
	if r1 > s.limiter.objectMinRows*40 || r2 > s.limiter.objectMinRows*40 {
		return
	}

	// push back schedule for big gap
	if gap := math.Abs(float64(r1 - r2)); gap > float64(s.limiter.objectMinRows) {
		// bump intention
		s1.Stat.MergeIntent++
		s2.Stat.MergeIntent++
		waitIntent := s.limiter.determineGapIntentFactor(gap)
		if float64(s1.Stat.MergeIntent) < waitIntent ||
			float64(s2.Stat.MergeIntent) < waitIntent {
			return
		}
	}

	msegs = segs[:2]
	mblks = make([]*catalog.BlockEntry, 0, len(msegs)*constMergeMinBlks)
	for _, seg := range msegs {
		blkit := seg.MakeBlockIt(true)
		for ; blkit.Valid(); blkit.Next() {
			entry := blkit.Get().GetPayload()
			if !entry.IsActive() {
				continue
			}
			entry.RLock()
			if entry.IsCommitted() &&
				catalog.ActiveWithNoTxnFilter(entry.BaseEntryImpl) {
				mblks = append(mblks, entry)
			}
			entry.RUnlock()
		}
	}

	rows = r1 + r2
	logutil.Infof("mergeblocks merge %v-%v, sorted %d and %d rows",
		s.tid, s.limiter.tableName, r1, r2)
	return
}

func (s *MergeTaskBuilder) trySchedMergeTask() {
	if s.tid == 0 {
		return
	}
	// deletable segs
	delSegs := s.delSegBuilder.finish()
	hasDelSeg := len(delSegs) > 0

	hasMergeObjects := false

	mergedBlks, msegs, mergedRows := s.checkSortedSegs(s.sortedSegBuilder.finish())
	blkCnt := len(mergedBlks)
	if blkCnt > 0 && s.limiter.checkMemAvail(blkCnt, mergedRows) {
		delSegs = append(delSegs, msegs...)
		hasMergeObjects = true
	}

	if !hasDelSeg && !hasMergeObjects {
		return
	}

	segScopes := make([]common.ID, len(delSegs))
	for i, s := range delSegs {
		segScopes[i] = *s.AsCommonID()
	}

	// remove stale segments only
	if hasDelSeg && !hasMergeObjects {
		factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
			return jobs.NewDelSegTask(ctx, txn, delSegs), nil
		}
		_, err := s.db.Runtime.Scheduler.ScheduleMultiScopedTxnTask(nil, tasks.DataCompactionTask, segScopes, factory)
		if err != nil {
			logutil.Infof("[Mergeblocks] Schedule del seg errinfo=%v", err)
			return
		}
		logutil.Infof("[Mergeblocks] Scheduled | %d-%s del %d seg",
			s.tid, s.limiter.tableName, len(delSegs))
		return
	}

	// remove stale segments and mrege objects

	scopes := make([]common.ID, blkCnt)
	for i, blk := range mergedBlks {
		scopes[i] = *blk.AsCommonID()
	}
	scopes = append(scopes, segScopes...)

	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return jobs.NewMergeBlocksTask(ctx, txn, mergedBlks, delSegs, nil, s.db.Runtime)
	}
	task, err := s.db.Runtime.Scheduler.ScheduleMultiScopedTxnTask(nil, tasks.DataCompactionTask, scopes, factory)
	if err != nil {
		if err != tasks.ErrScheduleScopeConflict {
			logutil.Infof("[Mergeblocks] Schedule error info=%v", err)
		}
	} else {
		// reset flag of sorted
		for _, seg := range delSegs {
			seg.Stat.SameDelsStreak = 0
			seg.Stat.MergeIntent = 0
		}
		n := len(scopes)
		s.limiter.AddActiveTask(task.ID(), blkCnt, mergedRows)
		task.AddObserver(s.limiter)
		if n > constMergeMinBlks {
			n = constMergeMinBlks
		}
		logutil.Infof("[Mergeblocks] Scheduled | %d-%s Scopes=[%d],[%d]%s",
			s.tid, s.limiter.tableName,
			len(segScopes), len(scopes),
			common.BlockIDArraryString(scopes[:n]))
	}
}

func (s *MergeTaskBuilder) resetForTable(entry *catalog.TableEntry) {
	s.tableRowCnt = 0
	s.tableDelete = 0
	s.tid = 0
	if entry != nil {
		s.tid = entry.ID
		schema := entry.GetLastestSchema()
		s.limiter.maxRowsForBlk = int(schema.BlockMaxRows)
		s.limiter.objectMinRows = determineObjectMinRows(
			int(schema.SegmentMaxBlocks), int(schema.BlockMaxRows))
		s.limiter.tableName = schema.Name
		if rowsize := entry.Stats.GetEstimateRowSize(); rowsize != 0 {
			s.limiter.estimateRowSize = rowsize
		} else {
			s.limiter.estimateRowSize = schema.EstimateRowSize()
		}
	}
	s.delSegBuilder.reset()
	s.sortedSegBuilder.reset()
}

func determineObjectMinRows(segMaxBlks, blkMaxRows int) int {
	// the max rows of a full object
	objectFullRows := segMaxBlks * blkMaxRows
	// we want every object has at least 5 blks rows
	objectMinRows := constMergeMinBlks * blkMaxRows
	if objectFullRows < objectMinRows { // for small config in unit test
		return objectFullRows
	}
	return objectMinRows
}

// PreExecute is called before each loop, refresh and print stats
func (s *MergeTaskBuilder) PreExecute() error {

	// fresh mem use
	if stats, err := mem.VirtualMemory(); err == nil {
		s.limiter.memAvail = int(stats.Available)
		if s.limiter.memSpare == 0 {
			s.limiter.memSpare = int(float32(stats.Total) * 0.15)
		}
	}

	if f := EnvOrDefaultFloat(constKeyMergeWaitFactor, 1.0); f < 0.1 {
		s.limiter.mergeWaitFactor = 0.1
	} else {
		s.limiter.mergeWaitFactor = f
	}
	return nil
}
func (s *MergeTaskBuilder) PostExecute() error {
	if cnt := atomic.LoadInt32(&s.limiter.activeMergeBlkCount); cnt > 0 {
		mergem := float32(atomic.LoadInt64(&s.limiter.activeEstimateBytes)) / const1GBytes
		logutil.Infof(
			"Mergeblocks avail mem: %dG, active mergeing size: %.2fG, current active blk: %d",
			s.limiter.memAvail/const1GBytes, mergem, cnt)
	}
	logutil.Infof("mergeblocks ------------------------------------")
	return nil
}

func (s *MergeTaskBuilder) onTable(tableEntry *catalog.TableEntry) (err error) {
	if !tableEntry.IsActive() {
		err = moerr.GetOkStopCurrRecur()
	}
	s.resetForTable(tableEntry)
	return
}

func (s *MergeTaskBuilder) onPostTable(tableEntry *catalog.TableEntry) (err error) {
	// base on the info of tableEntry, we can decide whether to merge or not
	s.trySchedMergeTask()
	return
}

func (s *MergeTaskBuilder) onSegment(segmentEntry *catalog.SegmentEntry) (err error) {
	if !segmentEntry.IsActive() {
		return moerr.GetOkStopCurrRecur()
	}

	segmentEntry.RLock()
	defer segmentEntry.RUnlock()

	// Skip uncommitted entries
	if !segmentEntry.IsCommitted() ||
		!catalog.ActiveWithNoTxnFilter(segmentEntry.BaseEntryImpl) {
		return moerr.GetOkStopCurrRecur()
	}

	s.delSegBuilder.resetForNewSeg()
	s.delSegBuilder.segIsSorted = segmentEntry.IsSortedLocked()
	return
}

func (s *MergeTaskBuilder) onPostSegment(seg *catalog.SegmentEntry) (err error) {
	s.delSegBuilder.push(seg)

	if !seg.IsSorted() || s.delSegBuilder.isCreating {
		return nil
	}

	// for sorted segments, we have to see if it is qualified to be merged
	seg.Stat.Rows = s.delSegBuilder.segRowCnt
	if seg.Stat.Dels == s.delSegBuilder.segRowDel {
		// SameDelsStreak is used to tell how frequently the segment is modified
		seg.Stat.SameDelsStreak++
	} else {
		seg.Stat.SameDelsStreak = 0
	}
	seg.Stat.Dels = s.delSegBuilder.segRowDel
	rowsLeftOnSeg := s.delSegBuilder.segRowCnt - s.delSegBuilder.segRowDel
	// it has too few rows, merge it
	iscandidate := func() bool {
		if rowsLeftOnSeg < s.limiter.objectMinRows {
			return true
		}
		if rowsLeftOnSeg < seg.Stat.Rows/2 {
			return true
		}
		// //
		// if seg.Stat.SameDelsStreak > 24 {
		// 	return true
		// }
		return false
	}

	if iscandidate() {
		s.sortedSegBuilder.push(&mItem[*catalog.SegmentEntry]{
			row:   rowsLeftOnSeg,
			entry: seg,
		})
	}
	return nil
}

// for sorted segments, we just collect the rows and dels on this segment
// for non-sorted segments, flushTableTail will take care of them, here we just check if it is deletable(having no active blocks)
func (s *MergeTaskBuilder) onBlock(entry *catalog.BlockEntry) (err error) {
	if !entry.IsActive() {
		return
	}
	// it has active blk, this seg can't be deleted
	s.delSegBuilder.hintNonDropBlock()

	// this blk is not in a s3 object
	if !s.delSegBuilder.segIsSorted {
		return
	}

	entry.RLock()
	defer entry.RUnlock()

	// Skip uncommitted entries and appendable block
	if !entry.IsCommitted() ||
		!catalog.ActiveWithNoTxnFilter(entry.BaseEntryImpl) {
		// txn appending metalocs
		s.delSegBuilder.isCreating = true
		return
	}

	if !catalog.NonAppendableBlkFilter(entry) {
		panic("append block in sorted segment")
	}

	// nblks in appenable segs or non-sorted non-appendable segs
	// these blks are formed by continuous append
	entry.RUnlock()
	rows := entry.GetBlockData().Rows()
	dels := entry.GetBlockData().GetTotalChanges()
	entry.RLock()
	s.delSegBuilder.segRowCnt += rows
	s.delSegBuilder.segRowDel += dels
	return nil
}
