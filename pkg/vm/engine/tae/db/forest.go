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

type DirtyTreeEntry struct {
	sync.RWMutex
	start, end types.TS
	tree       *common.Tree
}

type dirtyCollector struct {
	// sourcer
	sourcer *logtail.LogtailMgr

	// context
	catalog     *catalog.Catalog
	clock       *types.TsAlloctor
	interceptor catalog.Processor

	// storage
	storage struct {
		sync.RWMutex
		entries *btree.BTreeG[*DirtyTreeEntry]
		maxTs   types.TS
	}
}

func newDirtyCollector(
	sourcer *logtail.LogtailMgr,
	clock clock.Clock,
	catalog *catalog.Catalog,
	interceptor catalog.Processor) *dirtyCollector {
	watch := &dirtyCollector{
		sourcer:     sourcer,
		catalog:     catalog,
		interceptor: interceptor,
		clock:       types.NewTsAlloctor(clock),
	}
	watch.storage.entries = btree.NewBTreeGOptions[*DirtyTreeEntry](
		func(a, b *DirtyTreeEntry) bool {
			return a.start.Less(b.start) && a.end.Less(b.end)
		}, btree.Options{
			NoLocks: true,
		})
	return watch
}

func (d *dirtyCollector) Run() {
	from, to := d.findRange()

	// stale range found, skip this run
	if to.IsEmpty() {
		return
	}

	d.collectAndStoreNew(from, to)
	d.scanAndCleanStale()
}

// DirtyCount returns unflushed table, segment, block count
func (d *dirtyCollector) DirtyCount() (tblCnt, segCnt, blkCnt int) {
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

func (d *dirtyCollector) String() string {
	tree := d.MergeForest()
	return tree.String()
}

func (d *dirtyCollector) MergeForest() *common.Tree {
	merged := common.NewTree()

	// get storage snapshot and work on it
	snapshot := d.getStorageSnapshot()

	// scan base on the snapshot
	// merge all trees of the entry
	snapshot.Scan(func(entry *DirtyTreeEntry) bool {
		entry.RLock()
		defer entry.RUnlock()
		merged.Merge(entry.tree)
		return true
	})

	return merged
}

func (d *dirtyCollector) findRange() (from, to types.TS) {
	now := d.clock.Alloc()
	d.storage.RLock()
	defer d.storage.RUnlock()
	if now.LessEq(d.storage.maxTs) {
		return
	}
	from, to = d.storage.maxTs.Next(), now
	return
}

func (d *dirtyCollector) collectAndStoreNew(from, to types.TS) (updated bool) {
	// collect dirty from sourcer
	reader := d.sourcer.GetReader(from, to)
	tree := reader.GetDirty()

	// make a entry
	entry := &DirtyTreeEntry{
		start: from,
		end:   to,
		tree:  tree,
	}

	// try to store the entry
	updated = d.tryStoreEntry(entry)
	return
}

func (d *dirtyCollector) tryStoreEntry(entry *DirtyTreeEntry) (ok bool) {
	ok = true
	d.storage.Lock()
	defer d.storage.Unlock()

	// storage was updated before
	if !entry.start.Equal(d.storage.maxTs.Next()) {
		ok = false
		return
	}

	// update storage maxTs
	d.storage.maxTs = entry.end

	// don't store empty entry
	if entry.tree.IsEmpty() {
		return
	}

	d.storage.entries.Set(entry)
	return
}

func (d *dirtyCollector) getStorageSnapshot() *btree.BTreeG[*DirtyTreeEntry] {
	d.storage.RLock()
	defer d.storage.RUnlock()
	return d.storage.entries.Copy()
}

// Scan current dirty entries, remove all flushed or not found ones, and drive interceptor on remaining block entries.
func (d *dirtyCollector) scanAndCleanStale() {
	toDeletes := make([]*DirtyTreeEntry, 0)

	// get a snapshot of entries
	entries := d.getStorageSnapshot()

	// scan all entries in the storage
	// try compact the dirty tree for each entry
	// if the dirty tree is empty, delete the specified entry from the storage
	entries.Scan(func(entry *DirtyTreeEntry) bool {
		entry.Lock()
		defer entry.Unlock()
		// dirty blocks within the time range has been flushed
		// exclude the related dirty tree from the foreset
		if entry.tree.IsEmpty() {
			toDeletes = append(toDeletes, entry)
			return true
		}
		if err := d.tryCompactTree(d.interceptor, entry.tree); err != nil {
			logutil.Warnf("error: interceptor on dirty tree: %v", err)
		}
		if entry.tree.IsEmpty() {
			toDeletes = append(toDeletes, entry)
		}
		return true
	})

	if len(toDeletes) == 0 {
		return
	}

	// remove entries with empty dirty tree from the storage
	d.storage.Lock()
	defer d.storage.Unlock()
	for _, tree := range toDeletes {
		d.storage.entries.Delete(tree)
	}
}

// iter the tree and call interceptor to process block. flushed block, empty seg and table will be removed from the tree
func (d *dirtyCollector) tryCompactTree(
	interceptor catalog.Processor,
	tree *common.Tree) (err error) {
	var (
		db  *catalog.DBEntry
		tbl *catalog.TableEntry
		seg *catalog.SegmentEntry
		blk *catalog.BlockEntry
	)
	for id, dirtyTable := range tree.Tables {
		// remove empty tables
		if dirtyTable.Compact() {
			tree.Shrink(id)
			continue
		}

		if db, err = d.catalog.GetDatabaseByID(dirtyTable.DbID); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrNotFound) {
				tree.Shrink(id)
				err = nil
				continue
			}
			break
		}
		if tbl, err = db.GetTableEntryByID(dirtyTable.ID); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrNotFound) {
				tree.Shrink(id)
				err = nil
				continue
			}
			break
		}

		for id, dirtySeg := range dirtyTable.Segs {
			// remove empty segs
			if dirtySeg.IsEmpty() {
				dirtyTable.Shrink(id)
				continue
			}
			if seg, err = tbl.GetSegmentByID(dirtySeg.ID); err != nil {
				if moerr.IsMoErrCode(err, moerr.ErrNotFound) {
					dirtyTable.Shrink(id)
					err = nil
					continue
				}
				return
			}
			for id := range dirtySeg.Blks {
				if blk, err = seg.GetBlockEntryByID(id); err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrNotFound) {
						dirtySeg.Shrink(id)
						err = nil
						continue
					}
					return
				}
				if blk.GetBlockData().RunCalibration() == 0 {
					dirtySeg.Shrink(id)
					continue
				}
				if err = interceptor.OnBlock(blk); err != nil {
					return
				}
			}
		}
	}
	tree.Compact()
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
