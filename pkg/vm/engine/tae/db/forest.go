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
	"github.com/tidwall/btree"
)

/*

an application on logtail mgr: monitor dirty blocks

*/

type TimeRangedTree struct {
	sync.RWMutex
	start, end types.TS
	tree       *common.Tree
}

type forestMaintainer struct {
	// collect dirty
	interval struct {
		sync.Mutex
		prevStart types.TS
		alloc     *types.TsAlloctor
		delay     time.Duration
	}
	logtail *logtail.LogtailMgr

	// consume dirty
	catalog *catalog.Catalog
	visitor catalog.Processor

	// dirty
	forest *btree.Generic[*TimeRangedTree]
}

func newForestMaintainer(logtail *logtail.LogtailMgr, clock clock.Clock, catalog *catalog.Catalog, visitor catalog.Processor) *forestMaintainer {
	watch := &forestMaintainer{
		logtail: logtail,
		catalog: catalog,
		visitor: visitor,
		forest:  btree.NewGeneric(func(a, b *TimeRangedTree) bool { return a.start.Less(b.start) && a.end.Less(b.end) }),
	}
	watch.interval.alloc = types.NewTsAlloctor(clock)
	return watch
}

func (d *forestMaintainer) Run() {
	start, end := d.getTimeRange()
	// end is empty means range is invalid if considering delay
	if end.IsEmpty() {
		return
	}
	d.tryExpand(start, end)
	d.tryShrink()
}

func (d *forestMaintainer) WithStartTS(start types.TS) {
	d.interval.Lock()
	defer d.interval.Unlock()
	d.interval.prevStart = start
}

// WithDelay to avoid waiting when get logtail dirty, Physical is nanosecond of wall time
func (d *forestMaintainer) WithDelay(delay time.Duration) {
	d.interval.Lock()
	defer d.interval.Unlock()
	d.interval.delay = delay
}

func (d *forestMaintainer) getTimeRange() (start, end types.TS) {
	r := &d.interval
	r.Lock()
	defer r.Unlock()
	now := r.alloc.Alloc()
	now = types.BuildTS(now.Physical()-int64(r.delay), now.Logical())
	if now.LessEq(r.prevStart) {
		return
	}
	start = r.prevStart
	end = now
	r.prevStart = end
	return
}

// DirtyCount returns unflushed table, segment, block count
func (d *forestMaintainer) DirtyCount() (tblCnt, segCnt, blkCnt int) {
	merged := d.MergeForest()
	tblCnt = merged.TableCount()
	for _, tblTree := range merged.Tables {
		segCnt += len(tblTree.Segs)
		for _, segTree := range tblTree.Segs {
			blkCnt += len(segTree.Blks)
		}
	}
	return
}

func (d *forestMaintainer) String() string {
	tree := d.MergeForest()
	return tree.String()
}

func (d *forestMaintainer) MergeForest() *common.Tree {
	merged := common.NewTree()
	forest := d.forest.Copy()
	forest.Scan(func(item *TimeRangedTree) bool {
		item.RLock()
		defer item.RUnlock()
		merged.Merge(item.tree)
		return true
	})
	return merged
}

func (d *forestMaintainer) tryExpand(start, end types.TS) {
	reader := d.logtail.GetReader(start, end)
	tree := reader.GetDirty()
	dirty := &TimeRangedTree{
		start: start,
		end:   end,
		tree:  tree,
	}
	d.forest.Set(dirty)
}

// Scan current dirty trees, remove all flushed or not found ones, and drive visitor on remaining block entries.
func (d *forestMaintainer) tryShrink() {
	forestToDelete := make([]*TimeRangedTree, 0)

	forest := d.forest.Copy()
	forest.Scan(func(item *TimeRangedTree) bool {
		item.Lock()
		defer item.Unlock()
		// dirty blocks within the time range has been flushed
		// exclude the related dirty tree from the foreset
		if item.tree.IsEmpty() {
			forestToDelete = append(forestToDelete, item)
			return true
		}
		if err := d.tryShrinkATree(d.visitor, item.tree); err != nil {
			logutil.Warnf("error: visitor on dirty tree: %v", err)
		}
		return true
	})

	for _, tree := range forestToDelete {
		d.forest.Delete(tree)
	}
}

// iter the tree and call visitor to process block. flushed block, empty seg and table will be removed from the tree
func (d *forestMaintainer) tryShrinkATree(visitor catalog.Processor, tree *common.Tree) (err error) {
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
		if db, err = d.catalog.GetDatabaseByID(tblDirty.DbID); err != nil {
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

type DirtyBlockCalibrator struct {
	*catalog.LoopProcessor
	ckpDriver checkpoint.Driver
}

func NewDirtyBlockCalibrator(ckpDriver checkpoint.Driver) *DirtyBlockCalibrator {
	v := &DirtyBlockCalibrator{ckpDriver: ckpDriver}
	v.BlockFn = v.visitBlock
	return v
}

func (v *DirtyBlockCalibrator) visitBlock(entry *catalog.BlockEntry) (err error) {
	data := entry.GetBlockData()

	// Run calibration and estimate score for checkpoint
	if data.RunCalibration() > 0 {
		v.ckpDriver.EnqueueCheckpointUnit(data)
	}
	return
}
