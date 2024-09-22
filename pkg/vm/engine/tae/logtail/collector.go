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

package logtail

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/tidwall/btree"
)

type TempFilter struct {
	sync.RWMutex
	m map[uint64]bool
}

type TempFKey struct{}

func (f *TempFilter) Add(id uint64) {
	f.Lock()
	defer f.Unlock()
	f.m[id] = true
}

func (f *TempFilter) Check(id uint64) (skip bool) {
	f.Lock()
	defer f.Unlock()
	if _, ok := f.m[id]; ok {
		delete(f.m, id)
		return true
	}
	return false
}

var TempF *TempFilter

func init() {
	TempF = &TempFilter{
		m: make(map[uint64]bool),
	}
}

type Collector interface {
	String() string
	Run(lag time.Duration)
	ScanInRange(from, to types.TS) (*DirtyTreeEntry, int)
	ScanInRangePruned(from, to types.TS) *DirtyTreeEntry
	IsCommitted(from, to types.TS) bool
	GetAndRefreshMerged() *DirtyTreeEntry
	Merge() *DirtyTreeEntry
	GetMaxLSN(from, to types.TS) uint64
	Init(maxts types.TS)
}

type DirtyEntryInterceptor = catalog.Processor

type DirtyTreeEntry struct {
	sync.RWMutex
	start, end types.TS
	tree       *model.Tree
}

func NewEmptyDirtyTreeEntry() *DirtyTreeEntry {
	return &DirtyTreeEntry{
		tree: model.NewTree(),
	}
}

func NewDirtyTreeEntry(start, end types.TS, tree *model.Tree) *DirtyTreeEntry {
	entry := NewEmptyDirtyTreeEntry()
	entry.start = start
	entry.end = end
	entry.tree = tree
	return entry
}

func (entry *DirtyTreeEntry) Merge(o *DirtyTreeEntry) {
	if entry.start.Greater(&o.start) {
		entry.start = o.start
	}
	if entry.end.LT(&o.end) {
		entry.end = o.end
	}
	entry.tree.Merge(o.tree)
}

func (entry *DirtyTreeEntry) IsEmpty() bool {
	return entry.tree.IsEmpty()
}

func (entry *DirtyTreeEntry) GetTimeRange() (from, to types.TS) {
	return entry.start, entry.end
}

func (entry *DirtyTreeEntry) GetTree() (tree *model.Tree) {
	return entry.tree
}

func (entry *DirtyTreeEntry) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString(
		fmt.Sprintf("DirtyTreeEntry[%s=>%s]\n",
			entry.start.ToString(),
			entry.end.ToString()))
	_, _ = buf.WriteString(entry.tree.String())
	return buf.String()
}

type dirtyCollector struct {
	// sourcer
	sourcer *Manager

	// context
	catalog     *catalog.Catalog
	clock       *types.TsAlloctor
	interceptor DirtyEntryInterceptor

	// storage
	storage struct {
		sync.RWMutex
		entries *btree.BTreeG[*DirtyTreeEntry]
		maxTs   types.TS
	}
	merged atomic.Pointer[DirtyTreeEntry]
}

func NewDirtyCollector(
	sourcer *Manager,
	clock clock.Clock,
	catalog *catalog.Catalog,
	interceptor DirtyEntryInterceptor) *dirtyCollector {
	collector := &dirtyCollector{
		sourcer:     sourcer,
		catalog:     catalog,
		interceptor: interceptor,
		clock:       types.NewTsAlloctor(clock),
	}
	collector.storage.entries = btree.NewBTreeGOptions(
		func(a, b *DirtyTreeEntry) bool {
			return a.start.LT(&b.start) && a.end.LT(&b.end)
		}, btree.Options{
			NoLocks: true,
		})

	collector.merged.Store(NewEmptyDirtyTreeEntry())
	return collector
}
func (d *dirtyCollector) Init(maxts types.TS) {
	d.storage.maxTs = maxts
}
func (d *dirtyCollector) Run(lag time.Duration) {
	from, to := d.findRange(lag)

	// stale range found, skip this run
	if to.IsEmpty() {
		return
	}

	d.rangeScanAndUpdate(from, to)
	d.cleanupStorage()
	d.GetAndRefreshMerged()
}

func (d *dirtyCollector) ScanInRangePruned(from, to types.TS) (
	tree *DirtyTreeEntry) {
	tree, _ = d.ScanInRange(from, to)
	if err := d.tryCompactTree(context.Background(), d.interceptor, tree.tree); err != nil {
		panic(err)
	}
	return
}

func (d *dirtyCollector) GetMaxLSN(from, to types.TS) uint64 {
	reader := d.sourcer.GetReader(from, to)
	return reader.GetMaxLSN()
}
func (d *dirtyCollector) ScanInRange(from, to types.TS) (
	entry *DirtyTreeEntry, count int) {
	reader := d.sourcer.GetReader(from, to)
	tree, count := reader.GetDirty()

	// make a entry
	entry = &DirtyTreeEntry{
		start: from,
		end:   to,
		tree:  tree,
	}
	return
}

func (d *dirtyCollector) IsCommitted(from, to types.TS) bool {
	reader := d.sourcer.GetReader(from, to)
	return reader.IsCommitted()
}

// DirtyCount returns unflushed table, Object, block count
func (d *dirtyCollector) DirtyCount() (tblCnt, objCnt int) {
	merged := d.GetAndRefreshMerged()
	tblCnt = merged.tree.TableCount()
	for _, tblTree := range merged.tree.Tables {
		objCnt += len(tblTree.Objs)
	}
	return
}

func (d *dirtyCollector) String() string {
	merged := d.GetAndRefreshMerged()
	return merged.tree.String()
}

func (d *dirtyCollector) GetAndRefreshMerged() (merged *DirtyTreeEntry) {
	merged = d.merged.Load()
	d.storage.RLock()
	maxTs := d.storage.maxTs
	d.storage.RUnlock()
	if maxTs.LessEq(&merged.end) {
		return
	}
	merged = d.Merge()
	d.tryUpdateMerged(merged)
	return
}

func (d *dirtyCollector) Merge() *DirtyTreeEntry {
	// get storage snapshot and work on it
	snapshot, maxTs := d.getStorageSnapshot()

	merged := NewEmptyDirtyTreeEntry()
	merged.end = maxTs

	// scan base on the snapshot
	// merge all trees of the entry
	snapshot.Scan(func(entry *DirtyTreeEntry) bool {
		entry.RLock()
		defer entry.RUnlock()
		merged.tree.Merge(entry.tree)
		return true
	})

	return merged
}

func (d *dirtyCollector) tryUpdateMerged(merged *DirtyTreeEntry) (updated bool) {
	var old *DirtyTreeEntry
	for {
		old = d.merged.Load()
		if old.end.GreaterEq(&merged.end) {
			break
		}
		if d.merged.CompareAndSwap(old, merged) {
			updated = true
			break
		}
	}
	return
}

func (d *dirtyCollector) findRange(lagDuration time.Duration) (from, to types.TS) {
	now := d.clock.Alloc()
	// a deliberate lag is made here for flushing and checkpoint to
	// avoid fierce competition on the very new ablock, whose PrepareCompact probably
	// returns false
	lag := types.BuildTS(now.Physical()-int64(lagDuration), now.Logical())
	d.storage.RLock()
	defer d.storage.RUnlock()
	if lag.LessEq(&d.storage.maxTs) {
		return
	}
	from, to = d.storage.maxTs.Next(), lag
	return
}

func (d *dirtyCollector) rangeScanAndUpdate(from, to types.TS) (updated bool) {
	entry, _ := d.ScanInRange(from, to)

	// try to store the entry
	updated = d.tryStoreEntry(entry)
	return
}

func (d *dirtyCollector) tryStoreEntry(entry *DirtyTreeEntry) (ok bool) {
	ok = true
	d.storage.Lock()
	defer d.storage.Unlock()

	// storage was updated before
	maxTS := d.storage.maxTs.Next()
	if !entry.start.Equal(&maxTS) {
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

func (d *dirtyCollector) getStorageSnapshot() (ss *btree.BTreeG[*DirtyTreeEntry], ts types.TS) {
	d.storage.Lock()
	defer d.storage.Unlock()
	ss = d.storage.entries.Copy()
	ts = d.storage.maxTs
	return
}

// Scan current dirty entries, remove all flushed or not found ones, and drive interceptor on remaining block entries.
func (d *dirtyCollector) cleanupStorage() {
	toDeletes := make([]*DirtyTreeEntry, 0)

	// get a snapshot of entries
	entries, _ := d.getStorageSnapshot()

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
		if err := d.tryCompactTree(context.Background(), d.interceptor, entry.tree); err != nil {
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

// iter the tree and call interceptor to process block.
// Those entries that will be removed from the tree:
// 1. not found db
// 2. not found table
// 3. empty table
// 4. dropped aobject
// 5. nobject
// Or, put it in a more concise way, **not dropped aobjects** will be kept in the tree.
func (d *dirtyCollector) tryCompactTree(
	ctx context.Context,
	interceptor DirtyEntryInterceptor,
	tree *model.Tree) (err error) {
	var (
		db  *catalog.DBEntry
		tbl *catalog.TableEntry
	)
	for id, dirtyTable := range tree.Tables {
		// remove empty tables
		if dirtyTable.Compact() {
			tree.Shrink(id)
			continue
		}

		if db, err = d.catalog.GetDatabaseByID(dirtyTable.DbID); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				tree.Shrink(id)
				err = nil
				continue
			}
			break
		}
		if tbl, err = db.GetTableEntryByID(dirtyTable.ID); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				tree.Shrink(id)
				err = nil
				continue
			}
			break
		}

		if x := ctx.Value(TempFKey{}); x != nil && TempF.Check(tbl.ID) {
			logutil.Infof("temp filter skip table %v-%v", tbl.ID, tbl.GetLastestSchemaLocked(false).Name)
			tree.Shrink(id)
			continue
		}

		checkAndTrimObject := func(id types.Objectid, isTombstone bool) error {
			obj, err := tbl.GetObjectByID(&id, isTombstone)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
					dirtyTable.Shrink(id, isTombstone)
					return nil
				}
				return err
			}
			// keep only non-dropped aobjects
			if !(obj.IsAppendable() && !obj.HasDropCommitted()) {
				dirtyTable.Shrink(id, isTombstone)
				return nil
			}
			if err := interceptor.OnObject(obj); err != nil {
				return err
			}
			return nil
		}

		for id := range dirtyTable.Objs {
			if err = checkAndTrimObject(id, false); err != nil {
				return
			}
		}
		for id := range dirtyTable.Tombstones {
			if err = checkAndTrimObject(id, true); err != nil {
				return
			}
		}
	}
	tree.Compact()
	return
}
