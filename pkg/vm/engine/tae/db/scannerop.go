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
	"sort"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
)

type ScannerOp interface {
	catalog.Processor
	PreExecute() error
	PostExecute() error
}

// segHelper holds some temp statistics and founds deletable segemnts of a table.
// If a segment has no any non-dropped blocks, it can be deleted. Except the
// segment has the max segment id, appender may creates block in it.
type segHelper struct {
	// Statistics
	segHasNonDropBlk     bool
	segRowCnt, segRowDel int
	segIsSorted          bool
	isCreating           bool

	// Found deletable segments
	maxSegId    uint64
	segCandids  []*catalog.SegmentEntry // appendable
	nsegCandids []*catalog.SegmentEntry // non-appendable
}

func newSegHelper() *segHelper {
	return &segHelper{
		segCandids:  make([]*catalog.SegmentEntry, 0),
		nsegCandids: make([]*catalog.SegmentEntry, 0),
	}
}

func (d *segHelper) reset() {
	d.resetForNewSeg()
	d.maxSegId = 0
	d.segCandids = d.segCandids[:0]
	d.nsegCandids = d.nsegCandids[:0]
}

func (d *segHelper) resetForNewSeg() {
	d.segHasNonDropBlk = false
	d.segIsSorted = false
	d.isCreating = false
	d.segRowCnt = 0
	d.segRowDel = 0
}

// call this when a non dropped block was found when iterating blocks of a segment,
// which make the builder skip this segment
func (d *segHelper) hintNonDropBlock() {
	d.segHasNonDropBlk = true
}

func (d *segHelper) push(entry *catalog.SegmentEntry) {
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
func (d *segHelper) finish() []*catalog.SegmentEntry {
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
	return ret
}

type MergeTaskBuilder struct {
	db *DB
	*catalog.LoopProcessor
	tid  uint64
	name string
	tbl  *catalog.TableEntry

	segmentHelper *segHelper
	objPolicy     merge.Policy
	executor      *merge.MergeExecutor

	// concurrecy control
	suspend    atomic.Bool
	suspendCnt atomic.Int32
}

func newMergeTaskBuiler(db *DB) *MergeTaskBuilder {
	op := &MergeTaskBuilder{
		db:            db,
		LoopProcessor: new(catalog.LoopProcessor),
		segmentHelper: newSegHelper(),
		objPolicy:     merge.NewBasicPolicy(),
		executor:      merge.NewMergeExecutor(db.Runtime),
	}

	op.TableFn = op.onTable
	op.BlockFn = op.onBlock
	op.SegmentFn = op.onSegment
	op.PostSegmentFn = op.onPostSegment
	op.PostTableFn = op.onPostTable
	return op
}

func (s *MergeTaskBuilder) ManuallyMerge(entry *catalog.TableEntry, segs []*catalog.SegmentEntry) error {
	// stop new merge task
	s.suspend.Store(true)
	defer s.suspend.Store(false)
	// waiting the runing merge sched task to finish
	for s.suspendCnt.Load() < 3 {
		time.Sleep(50 * time.Millisecond)
	}

	// all status are safe in the TaskBuilder
	for _, seg := range segs {
		seg.LoadObjectInfo()
	}
	return s.executor.ManuallyExecute(entry, segs)
}

func (s *MergeTaskBuilder) ConfigPolicy(id uint64, c any) {
	s.objPolicy.Config(id, c)
}

func (s *MergeTaskBuilder) GetPolicy(id uint64) any {
	return s.objPolicy.GetConfig(id)
}

func (s *MergeTaskBuilder) trySchedMergeTask() {
	if s.tid == 0 {
		return
	}
	delSegs := s.segmentHelper.finish()
	s.executor.ExecuteFor(s.tbl, delSegs, s.objPolicy)
}

func (s *MergeTaskBuilder) resetForTable(entry *catalog.TableEntry) {
	s.tid = 0
	if entry != nil {
		s.tid = entry.ID
		s.tbl = entry
		s.name = entry.GetLastestSchema().Name
	}
	s.segmentHelper.reset()
	s.objPolicy.ResetForTable(entry.ID, entry)
}

func (s *MergeTaskBuilder) PreExecute() error {
	s.executor.RefreshMemInfo()
	return nil
}

func (s *MergeTaskBuilder) PostExecute() error {
	s.executor.PrintStats()
	logutil.Infof("mergeblocks ------------------------------------")
	return nil
}

func (s *MergeTaskBuilder) onTable(tableEntry *catalog.TableEntry) (err error) {
	if s.suspend.Load() {
		s.suspendCnt.Add(1)
		return moerr.GetOkStopCurrRecur()
	}
	s.suspendCnt.Store(0)
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
	if !segmentEntry.IsCommitted() || !catalog.ActiveWithNoTxnFilter(segmentEntry.BaseEntryImpl) {
		return moerr.GetOkStopCurrRecur()
	}

	s.segmentHelper.resetForNewSeg()
	s.segmentHelper.segIsSorted = segmentEntry.IsSortedLocked()
	return
}

func (s *MergeTaskBuilder) onPostSegment(seg *catalog.SegmentEntry) (err error) {
	s.segmentHelper.push(seg)

	if !seg.IsSorted() || s.segmentHelper.isCreating {
		return nil
	}
	// for sorted segments, we have to feed it to policy to see if it is qualified to be merged
	seg.Stat.Rows = s.segmentHelper.segRowCnt
	seg.Stat.RemainingRows = s.segmentHelper.segRowCnt - s.segmentHelper.segRowDel
	seg.LoadObjectInfo()
	if s.name == motrace.RawLogTbl && seg.Stat.OriginSize == 0 {
		// after loading object info, original size is still 0, we have to estimate it by experience
		factor := 1 + seg.Stat.Rows/1600
		seg.Stat.OriginSize = (1 << 20) * factor

	}
	s.objPolicy.OnObject(seg)
	return nil
}

// for sorted segments, we just collect the rows and dels on this segment
// for non-sorted segments, flushTableTail will take care of them, here we just check if it is deletable(having no active blocks)
func (s *MergeTaskBuilder) onBlock(entry *catalog.BlockEntry) (err error) {
	if !entry.IsActive() {
		return
	}
	// it has active blk, this seg can't be deleted
	s.segmentHelper.hintNonDropBlock()

	// this blk is not in a s3 object
	if !s.segmentHelper.segIsSorted {
		return
	}

	entry.RLock()
	defer entry.RUnlock()

	// Skip uncommitted entries and appendable block
	if !entry.IsCommitted() || !catalog.ActiveWithNoTxnFilter(entry.BaseEntryImpl) {
		// txn appending metalocs
		s.segmentHelper.isCreating = true
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
	s.segmentHelper.segRowCnt += rows
	s.segmentHelper.segRowDel += dels
	return nil
}
