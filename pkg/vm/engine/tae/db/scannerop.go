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
	"bytes"
	"container/heap"
	"fmt"
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type ScannerOp interface {
	catalog.Processor
	PreExecute() error
	PostExecute() error
}

const (
	constMergeRightNow     = int(options.DefaultBlockMaxRows) * int(options.DefaultBlocksPerSegment)
	constMergeWaitDuration = 3 * time.Minute
	constMergeMinBlks      = 3
	constMergeMinRows      = 3000
	constHeapCapacity      = 300
)

type mItem struct {
	row   int
	entry *catalog.BlockEntry
}

type itemSet []*mItem

func (is itemSet) Len() int { return len(is) }

func (is itemSet) Less(i, j int) bool {
	return is[i].row < is[j].row
}

func (is itemSet) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

func (is *itemSet) Push(x any) {
	item := x.(*mItem)
	*is = append(*is, item)
}

func (is *itemSet) Pop() any {
	old := *is
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*is = old[0 : n-1]
	return item
}

func (is *itemSet) Clear() {
	old := *is
	*is = old[:0]
}

// found out blocks to be merged via maintaining a min heap
type mergedBlkBuilder struct {
	blocks itemSet
	cap    int
}

func (h *mergedBlkBuilder) reset() {
	h.blocks.Clear()
}

func (h *mergedBlkBuilder) push(item *mItem) {
	heap.Push(&h.blocks, item)
	if h.blocks.Len() > h.cap {
		heap.Pop(&h.blocks)
	}
}

func (h *mergedBlkBuilder) finish() []*catalog.BlockEntry {
	ret := make([]*catalog.BlockEntry, h.blocks.Len())
	for i, item := range h.blocks {
		ret[i] = item.entry
	}
	return ret
}

// if a segment has no any non-dropped block, it can be deleted. except the
// segment has the max segment id, appender may creates block in it.
type deletableSegBuilder struct {
	segHasNonDropBlk bool
	maxSegId         uint64
	candidates       []*catalog.SegmentEntry
}

func (d *deletableSegBuilder) reset() {
	d.segHasNonDropBlk = false
	d.maxSegId = 0
	d.candidates = d.candidates[:0]
}

func (d *deletableSegBuilder) resetForNewSeg() {
	d.segHasNonDropBlk = false
}

func (d *deletableSegBuilder) hintNonDropBlock() {
	d.segHasNonDropBlk = true
}

func (d *deletableSegBuilder) push(entry *catalog.SegmentEntry) {
	if d.maxSegId < entry.ID {
		d.maxSegId = entry.ID
	}
	// all blocks has been dropped
	if !d.segHasNonDropBlk {
		d.candidates = append(d.candidates, entry)
	}
}

func (d *deletableSegBuilder) finish() []*catalog.SegmentEntry {
	sort.Slice(d.candidates, func(i, j int) bool { return d.candidates[i].ID < d.candidates[j].ID })
	if last := len(d.candidates) - 1; last >= 0 && d.candidates[last].ID == d.maxSegId {
		d.candidates = d.candidates[:last]
	}
	if len(d.candidates) == 0 {
		return nil
	}
	ret := make([]*catalog.SegmentEntry, len(d.candidates))
	copy(ret, d.candidates)
	return ret
}

type stat struct {
	ttl          time.Time
	lastTotalRow int
}

// mergeLimiter consider update rate and time to decide to merge or not.
type mergeLimiter struct {
	stats map[uint64]*stat
}

func (ml *mergeLimiter) canMerge(tid uint64, totalRow int, blks int) bool {
	if totalRow > constMergeRightNow {
		logutil.Infof("Mergeblocks %d merge right now: %d rows %d blks", tid, totalRow, blks)
		delete(ml.stats, tid)
		return true
	}
	if blks < constMergeMinBlks || totalRow < constMergeMinRows {
		return false
	}

	if st, ok := ml.stats[tid]; !ok {
		ml.stats[tid] = &stat{
			ttl:          ml.ttl(totalRow),
			lastTotalRow: totalRow,
		}
		return false
	} else if d := totalRow - st.lastTotalRow; d > 5 || d < -5 {
		// a lot of things happened in the past scan interval...
		st.ttl = ml.ttl(totalRow)
		st.lastTotalRow = totalRow
		logutil.Infof("Mergeblocks delta %d on table %d, resched to %v", d, tid, st.ttl)
		return false
	} else {
		// this table is quiet finally, check ttl
		return st.ttl.Before(time.Now())
	}
}

func (ml *mergeLimiter) ttl(totalRow int) time.Time {
	return time.Now().Add(time.Duration(
		(float32(constMergeWaitDuration) / float32(constMergeRightNow)) *
			(float32(constMergeRightNow) - float32(totalRow))))
}

func (ml *mergeLimiter) pruneStale() {
	staleIds := make([]uint64, 0)
	t := time.Now().Add(-3 * time.Minute)
	for id, st := range ml.stats {
		if st.ttl.Before(t) {
			staleIds = append(staleIds, id)
		}
	}
	for _, id := range staleIds {
		delete(ml.stats, id)
	}
}

func (ml *mergeLimiter) String() string {
	s := &bytes.Buffer{}
	s.WriteString("{")
	for id, st := range ml.stats {
		s.WriteString(fmt.Sprintf("%d: %d @ %s", id, st.lastTotalRow, st.ttl))
		s.WriteString("\n")
	}
	s.WriteString("}\n")
	return s.String()
}

type MergeTaskBuilder struct {
	db *DB
	*catalog.LoopProcessor
	runCnt      int
	tableRowCnt int
	tid         uint64
	limiter     *mergeLimiter
	segBuilder  *deletableSegBuilder
	blkBuilder  *mergedBlkBuilder
}

func newMergeTaskBuiler(db *DB) *MergeTaskBuilder {
	op := &MergeTaskBuilder{
		db:            db,
		LoopProcessor: new(catalog.LoopProcessor),
		limiter: &mergeLimiter{
			stats: make(map[uint64]*stat),
		},
		segBuilder: &deletableSegBuilder{
			candidates: make([]*catalog.SegmentEntry, 0),
		},
		blkBuilder: &mergedBlkBuilder{
			blocks: make(itemSet, 0, constHeapCapacity),
			cap:    constHeapCapacity,
		},
	}

	op.TableFn = op.onTable
	op.BlockFn = op.onBlock
	op.SegmentFn = op.onSegment
	op.PostSegmentFn = op.onPostSegment
	return op
}

func (s *MergeTaskBuilder) trySchedMergeTask() {
	if s.tid == 0 {
		return
	}
	mergedBlks := s.blkBuilder.finish()
	if !s.limiter.canMerge(s.tid, s.tableRowCnt, len(mergedBlks)) {
		return
	}

	scopes := make([]common.ID, len(mergedBlks))
	for i, blk := range mergedBlks {
		scopes[i] = *blk.AsCommonID()
	}
	// deletable segs
	mergedSegs := s.segBuilder.finish()

	segIds := make([]uint64, len(mergedSegs))
	for i, s := range mergedSegs {
		segIds[i] = s.ID
	}

	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return jobs.NewMergeBlocksTask(ctx, txn, mergedBlks, mergedSegs, nil, s.db.Scheduler)
	}

	_, err := s.db.Scheduler.ScheduleMultiScopedTxnTask(nil, tasks.DataCompactionTask, scopes, factory)
	logutil.Infof("[Mergeblocks] Scheduled | State=%v | Scopes=%v,%s", err, segIds, common.BlockIDArraryString(scopes))
}

func (s *MergeTaskBuilder) resetForTable(tid uint64) {
	s.tableRowCnt = 0
	s.tid = tid
	s.segBuilder.reset()
	s.blkBuilder.reset()
}

func (s *MergeTaskBuilder) PreExecute() error {
	if s.runCnt++; s.runCnt >= 128 {
		s.runCnt = 0
		s.limiter.pruneStale()
	}

	if s.runCnt%10 == 0 {
		logutil.Infof("Mergeblocks stats: %s", s.limiter.String())
	}
	return nil
}
func (s *MergeTaskBuilder) PostExecute() error {
	s.trySchedMergeTask()
	s.resetForTable(0)
	return nil
}

func (s *MergeTaskBuilder) onTable(tableEntry *catalog.TableEntry) (err error) {
	if !tableEntry.IsActive() {
		err = moerr.GetOkStopCurrRecur()
	}
	s.trySchedMergeTask()
	s.resetForTable(tableEntry.ID)
	return
}

func (s *MergeTaskBuilder) onSegment(segmentEntry *catalog.SegmentEntry) (err error) {
	if !segmentEntry.IsActive() || !segmentEntry.IsAppendable() {
		err = moerr.GetOkStopCurrRecur()
	}
	s.segBuilder.resetForNewSeg()
	return
}

func (s *MergeTaskBuilder) onPostSegment(segmentEntry *catalog.SegmentEntry) (err error) {
	s.segBuilder.push(segmentEntry)
	return nil
}

func (s *MergeTaskBuilder) onBlock(entry *catalog.BlockEntry) (err error) {
	if !entry.IsActive() {
		return
	}
	s.segBuilder.hintNonDropBlock()

	entry.RLock()
	defer entry.RUnlock()

	// Skip uncommitted entries and appendable block
	if !entry.IsCommitted() ||
		!catalog.ActiveWithNoTxnFilter(entry.MetaBaseEntry) ||
		!catalog.NonAppendableBlkFilter(entry) {
		return
	}

	rows := entry.GetBlockData().Rows()
	s.tableRowCnt += rows
	s.blkBuilder.push(&mItem{row: rows, entry: entry})
	return nil
}
