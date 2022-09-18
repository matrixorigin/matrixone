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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"

	"github.com/tidwall/btree"
)

type txnPage struct {
	minTs types.TS
	txns  []txnif.AsyncTxn
}

func cmpTxnPage(a, b *txnPage) bool { return a.minTs.Less(b.minTs) }

type LogtailMgr struct {
	txnbase.NoopCommitListener
	pageSize int32             // for test
	minTs    types.TS          // the lower bound of active page
	tsAlloc  *types.TsAlloctor // share same clock with txnMgr

	// TODO: move the active page to btree, simplify the iteration of pages
	activeSize *int32
	// activePage is a fixed size array, has fixed memory address during its whole lifetime
	activePage []txnif.AsyncTxn
	// Lock is used to protect pages. there are three cases to hold lock
	// 1. activePage is full and moves to pages
	// 2. prune txn because of checkpoint TODO
	// 3. copy btree
	// Not RwLock because a copied btree can be read without holding read lock
	sync.Mutex
	pages *btree.Generic[*txnPage]
}

func NewLogtailMgr(pageSize int32, clock clock.Clock) *LogtailMgr {
	tsAlloc := types.NewTsAlloctor(clock)
	minTs := tsAlloc.Alloc()

	return &LogtailMgr{
		pageSize:   pageSize,
		minTs:      minTs,
		tsAlloc:    tsAlloc,
		activeSize: new(int32),
		activePage: make([]txnif.AsyncTxn, pageSize),
		pages:      btree.NewGenericOptions(cmpTxnPage, btree.Options{NoLocks: true}),
	}
}

// LogtailMgr as a commit listener
func (l *LogtailMgr) OnEndPrePrepare(op *txnbase.OpTxn) { l.AddTxn(op.Txn) }

// Notes:
// 1. AddTxn happens in a queue, it is safe to assume there is no concurrent AddTxn now.
// 2. the added txn has no prepareTS because it happens in OnEndPrePrepare, so it is safe to alloc ts to be minTs
func (l *LogtailMgr) AddTxn(txn txnif.AsyncTxn) {
	size := atomic.LoadInt32(l.activeSize)
	l.activePage[size] = txn
	newsize := atomic.AddInt32(l.activeSize, 1)

	if newsize == l.pageSize {
		// alloc ts without lock
		prevMinTs := l.minTs
		l.minTs = l.tsAlloc.Alloc()
		l.Lock()
		defer l.Unlock()
		newPage := &txnPage{
			minTs: prevMinTs,
			txns:  l.activePage,
		}
		l.pages.Set(newPage)

		l.activePage = make([]txnif.AsyncTxn, l.pageSize)
		atomic.StoreInt32(l.activeSize, 0)
	}
}

// GetLogtailView returns a read only view of txns at call time.
// this is cheap operation, the returned view can be accessed without any locks
func (l *LogtailMgr) GetLogtailView(start, end types.TS, tid uint64) *LogtailView {
	l.Lock()
	size := atomic.LoadInt32(l.activeSize)
	activeView := l.activePage[:size]
	btreeView := l.pages.Copy()
	l.Unlock()
	return &LogtailView{
		start:      start,
		end:        end,
		tid:        tid,
		btreeView:  btreeView,
		activeView: activeView,
	}
}

// GetLogtailCollector try to fix a read view, use LogtailCollector.BindCollectEnv to set other collect args
func (l *LogtailMgr) GetLogtailCollector(start, end types.TS, did, tid uint64) *LogtailCollector {
	view := l.GetLogtailView(start, end, tid)
	return &LogtailCollector{
		did:  did,
		tid:  tid,
		view: view,
	}
}

// a read only view of txns
type LogtailView struct {
	start, end types.TS                 // included
	tid        uint64                   // table id
	btreeView  *btree.Generic[*txnPage] // read only btree
	activeView []txnif.AsyncTxn         // read only active page
}

// DirtySegs has determined iter order
type DirtySegs struct {
	Segs []dirtySeg
}

type dirtySeg struct {
	Sig  uint64
	Blks []uint64
}

func (v *LogtailView) GetDirty() DirtySegs {
	// if tree[segID] is nil, it means that is just a seg operation
	tree := make(map[uint64]map[uint64]struct{}, 0)
	var blkSet map[uint64]struct{}
	var exist bool
	f := func(txn txnif.AsyncTxn) (moveOn bool) {
		store := txn.GetStore()
		if store.HasTableDataChanges(v.tid) {
			pointSet := store.GetTableDirtyPoints(v.tid)
			for dirty := range pointSet {
				if dirty.BlkID == 0 {
					// a segment operation
					if _, exist = tree[dirty.SegID]; !exist {
						tree[dirty.SegID] = nil
					}
				} else {
					// merge the dirty block
					blkSet, exist = tree[dirty.SegID]
					if !exist || blkSet == nil {
						blkSet = make(map[uint64]struct{})
						tree[dirty.SegID] = blkSet
					}
					blkSet[dirty.BlkID] = struct{}{}
				}
			}
		}
		return true
	}
	v.scanTxnBetween(v.start, v.end, f)

	segs := make([]dirtySeg, 0, len(tree))
	for sig, blkSet := range tree {
		blk := make([]uint64, 0, len(blkSet))
		for b := range blkSet {
			blk = append(blk, b)
		}
		segs = append(segs, dirtySeg{Sig: sig, Blks: blk})
	}
	return DirtySegs{Segs: segs}
}

func (v *LogtailView) HasCatalogChanges() bool {
	changed := false
	f := func(txn txnif.AsyncTxn) (moveOn bool) {
		if txn.GetStore().HasCatalogChanges() {
			changed = true
			return false
		}
		return true
	}
	v.scanTxnBetween(v.start, v.end, f)
	return changed
}

func (v *LogtailView) scanTxnBetween(start, end types.TS, f func(txn txnif.AsyncTxn) (moveOn bool)) {
	var pivot types.TS
	v.btreeView.Descend(&txnPage{minTs: start}, func(item *txnPage) bool { pivot = item.minTs; return false })
	stopInOldPages := false
	v.btreeView.Ascend(&txnPage{minTs: pivot}, func(page *txnPage) bool {
		var ts types.TS
		for _, txn := range page.txns {
			ts = txn.GetPrepareTS()
			// stop if not prepared or exceed request range
			if ts.IsEmpty() || ts.Greater(end) {
				stopInOldPages = true
				return false
			}
			if ts.Less(start) {
				continue
			}
			if !f(txn) {
				stopInOldPages = true
				return false
			}
		}
		return true
	})

	if stopInOldPages {
		return
	}

	var ts types.TS
	for _, txn := range v.activeView {
		ts = txn.GetPrepareTS()
		if ts.IsEmpty() || ts.Greater(end) {
			return
		}
		if ts.Less(start) {
			continue
		}
		if !f(txn) {
			return
		}
	}
}

type CollectMode = int

const (
	// changes for mo_databases
	CollectModeDB CollectMode = iota + 1
	// changes for mo_tables
	CollectModeTbl
	// changes for user tables
	CollectModeSegAndBlk
)

// LogtailCollector holds a read only view, knows how to iter entry.
// Drive a entry visitor, which acts as an api resp builder
type LogtailCollector struct {
	mode    CollectMode
	did     uint64
	tid     uint64
	catalog *catalog.Catalog
	view    *LogtailView
	visitor EntryVisitor
}

// BindInfo set collect env args and these args don't affect dirty seg/blk ids
func (c *LogtailCollector) BindCollectEnv(mode CollectMode, catalog *catalog.Catalog, visitor EntryVisitor) {
	c.mode, c.catalog, c.visitor = mode, catalog, visitor
}

func (c *LogtailCollector) Collect() error {
	switch c.mode {
	case CollectModeDB:
		return c.collectCatalogDB()
	case CollectModeTbl:
		return c.collectCatalogTbl()
	case CollectModeSegAndBlk:
		return c.collectUserTbl()
	default:
		panic("unknown logtail collect mode")
	}
}

func (c *LogtailCollector) collectUserTbl() (err error) {
	var (
		db  *catalog.DBEntry
		tbl *catalog.TableEntry
		seg *catalog.SegmentEntry
		blk *catalog.BlockEntry
	)
	if db, err = c.catalog.GetDatabaseByID(c.did); err != nil {
		return
	}
	if tbl, err = db.GetTableEntryByID(c.tid); err != nil {
		return
	}
	dirty := c.view.GetDirty()
	for _, dirtySeg := range dirty.Segs {
		if seg, err = tbl.GetSegmentByID(dirtySeg.Sig); err != nil {
			return err
		}
		if err = c.visitor.visitSeg(seg); err != nil {
			return err
		}
		for _, blkid := range dirtySeg.Blks {
			if blk, err = seg.GetBlockEntryByID(blkid); err != nil {
				return err
			}
			if err = c.visitor.visitBlk(blk); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *LogtailCollector) collectCatalogDB() error {
	if !c.view.HasCatalogChanges() {
		return nil
	}
	dbIt := c.catalog.MakeDBIt(true)
	for dbIt.Valid() {
		if err := c.visitor.visitDB(dbIt.Get().GetPayload()); err != nil {
			return err
		}
		dbIt.Next()
	}
	return nil
}

func (c *LogtailCollector) collectCatalogTbl() error {
	if !c.view.HasCatalogChanges() {
		return nil
	}
	dbIt := c.catalog.MakeDBIt(true)
	for dbIt.Valid() {
		db := dbIt.Get().GetPayload()
		tblIt := db.MakeTableIt(true)
		for tblIt.Valid() {
			if err := c.visitor.visitTbl(tblIt.Get().GetPayload()); err != nil {
				return err
			}
			tblIt.Next()
		}
		dbIt.Next()
	}
	return nil
}

type EntryVisitor interface {
	visitDB(entry *catalog.DBEntry) error
	visitTbl(entry *catalog.TableEntry) error
	visitSeg(entry *catalog.SegmentEntry) error
	visitBlk(entry *catalog.BlockEntry) error
}

// LogtailRespBuilder knows how to make api-entry from catalog entry
// impl EntryVisitor interface, driven by LogtailCollector
type LogtailRespBuilder struct {
	checkpoint string
	entries    []*api.Entry
}

func NewLogtailRespBuilder(checkpoint string) *LogtailRespBuilder {
	return &LogtailRespBuilder{
		checkpoint: checkpoint,
		entries:    make([]*api.Entry, 0),
	}
}

func (c *LogtailRespBuilder) visitDB(entry *catalog.DBEntry) error       { return nil }
func (c *LogtailRespBuilder) visitTbl(entry *catalog.TableEntry) error   { return nil }
func (c *LogtailRespBuilder) visitSeg(entry *catalog.SegmentEntry) error { return nil }
func (c *LogtailRespBuilder) visitBlk(entry *catalog.BlockEntry) error   { return nil }

func (c *LogtailRespBuilder) BuildResp() api.SyncLogTailResp {
	return api.SyncLogTailResp{
		CkpLocation: c.checkpoint,
		Commands:    c.entries,
	}
}

func sampleHandler(db *DB, req api.SyncLogTailReq) (api.SyncLogTailResp, error) {
	start := types.BuildTS(req.CnHave.PhysicalTime, req.CnHave.LogicalTime)
	end := types.BuildTS(req.CnWant.PhysicalTime, req.CnWant.LogicalTime)
	did, tid := req.Table.DbId, req.Table.TbId
	verifiedCheckpoint := ""
	// TODO
	// verifiedCheckpoint, start, end = db.CheckpointMgr.check(start, end)

	// get a collector with a read only view for txns
	collector := db.LogtailMgr.GetLogtailCollector(start, end, did, tid)
	respBuilder := NewLogtailRespBuilder(verifiedCheckpoint)
	mode := CollectModeSegAndBlk
	if tid == catalog.SystemTable_DB_ID {
		mode = CollectModeDB
	} else if tid == catalog.SystemTable_Table_ID {
		mode = CollectModeTbl
	}
	collector.BindCollectEnv(mode, db.Catalog, respBuilder)
	if err := collector.Collect(); err != nil {
		return api.SyncLogTailResp{}, err
	}
	return respBuilder.BuildResp(), nil

}
