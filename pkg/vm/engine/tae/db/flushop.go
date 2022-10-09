// Copyright 2021 Matrix Origin
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

package db

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker/base"
	"github.com/tidwall/btree"
)

/*

an application on logtail mgr: flush table data

*/

type rangeGetter struct {
	sync.Mutex
	prevStart types.TS
	alloc     *types.TsAlloctor
	delay     time.Duration
}

func NewRangeGetter(start types.TS, clock clock.Clock, delay time.Duration) *rangeGetter {
	return &rangeGetter{
		prevStart: start,
		alloc:     types.NewTsAlloctor(clock),
		delay:     delay,
	}
}

func (r *rangeGetter) get() (start, end types.TS) {
	r.Lock()
	defer r.Unlock()
	now := r.alloc.Alloc()
	// delay to avoid waiting when get logtail dirty, Physical is nanosecond of wall time
	now = types.BuildTS(now.Physical()-int64(r.delay), now.Logical())
	if now.LessEq(r.prevStart) {
		return
	}
	start = r.prevStart
	end = now
	r.prevStart = end
	return
}

type TSRangedTree struct {
	sync.RWMutex
	start, end types.TS
	tree       *common.Tree
}

type FlushOp struct {
	// collect dirty
	rangeGetter *rangeGetter
	logtail     *logtail.LogtailMgr

	// consume dirty
	catalog *catalog.Catalog
	visitor catalog.Processor

	// dirty
	dirtyTrees *btree.Generic[*TSRangedTree]
}

func NewFlushOperator(logtail *logtail.LogtailMgr, getter *rangeGetter, catalog *catalog.Catalog, visitor catalog.Processor) *FlushOp {
	return &FlushOp{
		rangeGetter: getter,
		logtail:     logtail,
		catalog:     catalog,
		visitor:     visitor,
		dirtyTrees:  btree.NewGeneric(func(a, b *TSRangedTree) bool { return a.start.Less(b.start) && a.end.Less(b.end) }),
	}
}

var _ base.IHBHandle = (*FlushOp)(nil)

func (f *FlushOp) OnExec()    { f.Run() }
func (f *FlushOp) OnStopped() {}

func (f *FlushOp) Run() {
	start, end := f.rangeGetter.get()
	// end is empty means range is invalid if considering delay
	if end.IsEmpty() {
		return
	}
	f.AppendDirty(start, end)
	f.TrySchedFlush()
}

// DirtyCount returns unflushed table, segment, block count
func (f *FlushOp) DirtyCount() (tblCnt, segCnt, blkCnt int) {
	merged := common.NewTree()
	f.dirtyTrees.Scan(func(item *TSRangedTree) bool {
		item.RLock()
		defer item.RUnlock()
		merged.Merge(item.tree)
		return true
	})

	tblCnt = merged.TableCount()
	for _, tblTree := range merged.Tables {
		segCnt += len(tblTree.Segs)
		for _, segTree := range tblTree.Segs {
			blkCnt += len(segTree.Blks)
		}
	}
	return
}

func (f *FlushOp) AppendDirty(start, end types.TS) {
	reader := f.logtail.GetReader(start, end)
	tree := reader.GetDirty()
	dirty := &TSRangedTree{
		start: start,
		end:   end,
		tree:  tree,
	}
	f.dirtyTrees.Set(dirty)
}

func (f *FlushOp) TrySchedFlush() {
	dels := make([]*TSRangedTree, 0)
	f.dirtyTrees.Scan(func(item *TSRangedTree) bool {
		item.Lock()
		defer item.Unlock()
		if item.tree.TableCount() == 0 {
			dels = append(dels, item)
			return true
		}
		if err := f.driveVisitorOnTree(f.visitor, item.tree); err != nil {
			logutil.Warnf("error: visitor on dirty tree: %v", err)
		}
		return true
	})

	for _, del := range dels {
		f.dirtyTrees.Delete(del)
	}
}

// iter the tree and call visitor to process block. flushed block, empty seg and table will be removed from the tree
func (f *FlushOp) driveVisitorOnTree(visitor catalog.Processor, tree *common.Tree) (err error) {
	var (
		db  *catalog.DBEntry
		tbl *catalog.TableEntry
		seg *catalog.SegmentEntry
		blk *catalog.BlockEntry
	)
	for id, tblDirty := range tree.Tables {
		// remove empty tables
		if len(tblDirty.Segs) == 0 {
			delete(tree.Tables, id)
			return
		}
		if db, err = f.catalog.GetDatabaseByID(tblDirty.DbID); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrNotFound) {
				err = nil
				delete(tree.Tables, id)
				continue
			}
			return
		}
		if tbl, err = db.GetTableEntryByID(tblDirty.ID); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrNotFound) {
				err = nil
				delete(tree.Tables, id)
				continue
			}
			return
		}

		for id, dirtySeg := range tblDirty.Segs {
			// remove empty segs
			if len(dirtySeg.Blks) == 0 {
				delete(tblDirty.Segs, id)
				continue
			}

			if seg, err = tbl.GetSegmentByID(dirtySeg.ID); err != nil {
				if moerr.IsMoErrCode(err, moerr.ErrNotFound) {
					err = nil
					delete(tblDirty.Segs, id)
					continue
				}
				return
			}
			for id := range dirtySeg.Blks {
				if blk, err = seg.GetBlockEntryByID(id); err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrNotFound) {
						err = nil
						delete(dirtySeg.Blks, id)
						continue
					}
					return
				}
				// if blk has been flushed, remove it
				if blk.GetMetaLoc() != "" {
					delete(dirtySeg.Blks, id)
					continue
				}
				if err = visitor.OnBlock(blk); err != nil {
					return
				}
			}
		}
	}
	return
}

type FlushDirtyVisitor struct {
	*catalog.LoopProcessor
	ckpDriver checkpoint.Driver
}

func NewFlushDirtyVisitor(ckpDriver checkpoint.Driver) *FlushDirtyVisitor {
	v := &FlushDirtyVisitor{ckpDriver: ckpDriver}
	v.BlockFn = v.visitBlock
	return v
}

func (v *FlushDirtyVisitor) visitBlock(entry *catalog.BlockEntry) (err error) {
	data := entry.GetBlockData()

	// Run calibration and estimate score for checkpoint
	if data.RunCalibration() > 0 {
		v.ckpDriver.EnqueueCheckpointUnit(data)
	}
	return
}

// for test
type printUnflush struct {
	op *FlushOp
}

var _ base.IHBHandle = (*printUnflush)(nil)

func (f *printUnflush) OnExec() {
	f.op.Run()
	t, s, b := f.op.DirtyCount()
	logutil.Infof("unflushed: %d table, %d seg, %d block", t, s, b)
}
func (f *printUnflush) OnStopped() {}

func NewTestUnflushedDirtyObserver(interval time.Duration, clock clock.Clock, logtail *logtail.LogtailMgr, cat *catalog.Catalog) base.IHeartbeater {
	visitor := &catalog.LoopProcessor{}
	// test uses mock clock, where alloc count used as timestamp, so delay is set as 10 count here
	var start types.TS
	rangeGetter := NewRangeGetter(start, clock, 10*time.Nanosecond)
	flushOp := NewFlushOperator(logtail, rangeGetter, cat, visitor)

	return w.NewHeartBeater(interval, &printUnflush{op: flushOp})
}
