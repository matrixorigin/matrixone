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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"

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
		if err = c.visitor.VisitSeg(seg); err != nil {
			return err
		}
		for _, blkid := range dirtySeg.Blks {
			if blk, err = seg.GetBlockEntryByID(blkid); err != nil {
				return err
			}
			if err = c.visitor.VisitBlk(blk); err != nil {
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
		dbentry := dbIt.Get().GetPayload()
		if !dbentry.IsSystemDB() {
			if err := c.visitor.VisitDB(dbentry); err != nil {
				return err
			}
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
		if db.IsSystemDB() {
			dbIt.Next()
			continue
		}
		tblIt := db.MakeTableIt(true)
		for tblIt.Valid() {
			if err := c.visitor.VisitTbl(tblIt.Get().GetPayload()); err != nil {
				return err
			}
			tblIt.Next()
		}
		dbIt.Next()
	}
	return nil
}

const (
	blkMetaAttrBlockID    = "block_id"
	blkMetaAttrEntryState = "entry_state"
	blkMetaAttrCreateAt   = "create_at"
	blkMetaAttrDeleteAt   = "delete_at"
	blkMetaAttrMetaLoc    = "meta_loc"
	blkMetaAttrDeltaLoc   = "delta_loc"
)

var (
	// for blk meta response
	blkMetaSchema *catalog.Schema
)

func init() {
	blkMetaSchema = catalog.NewEmptySchema("blkMeta")
	blkMetaSchema.AppendCol(blkMetaAttrBlockID, types.T_uint64.ToType())
	blkMetaSchema.AppendCol(blkMetaAttrEntryState, types.T_bool.ToType()) // 0: Nonappendable 1: appendable
	blkMetaSchema.AppendCol(blkMetaAttrCreateAt, types.T_TS.ToType())
	blkMetaSchema.AppendCol(blkMetaAttrDeleteAt, types.T_TS.ToType())
	blkMetaSchema.AppendCol(blkMetaAttrMetaLoc, types.T_varchar.ToType())
	blkMetaSchema.AppendCol(blkMetaAttrDeltaLoc, types.T_varchar.ToType())
	blkMetaSchema.Finalize(true) // no phyaddr column
}

type RespBuilder interface {
	EntryVisitor
	BuildResp() (api.SyncLogTailResp, error)
}

type EntryVisitor interface {
	VisitDB(entry *catalog.DBEntry) error
	VisitTbl(entry *catalog.TableEntry) error
	VisitSeg(entry *catalog.SegmentEntry) error
	VisitBlk(entry *catalog.BlockEntry) error
}

type noopEntryVisitor struct{}

func (v *noopEntryVisitor) VisitDB(entry *catalog.DBEntry) error       { return nil }
func (v *noopEntryVisitor) VisitTbl(entry *catalog.TableEntry) error   { return nil }
func (v *noopEntryVisitor) VisitSeg(entry *catalog.SegmentEntry) error { return nil }
func (v *noopEntryVisitor) VisitBlk(entry *catalog.BlockEntry) error   { return nil }

// CatalogLogtailRespBuilder knows how to make api-entry from catalog entry
// impl EntryVisitor interface, driven by LogtailCollector
type CatalogLogtailRespBuilder struct {
	noopEntryVisitor
	mode       CollectMode
	start, end types.TS
	checkpoint string
	schema     *catalog.Schema
	insBatch   *containers.Batch
	delBatch   *containers.Batch
}

func NewCatalogLogtailRespBuilder(mode CollectMode, ckp string, start, end types.TS) *CatalogLogtailRespBuilder {
	b := &CatalogLogtailRespBuilder{
		mode:       mode,
		start:      start,
		end:        end,
		checkpoint: ckp,
	}
	if mode == CollectModeDB {
		b.schema = catalog.SystemDBSchema
	} else {
		b.schema = catalog.SystemTableSchema
	}
	b.insBatch = makeRespBatchFromSchema(b.schema)
	b.delBatch = makeRespBatchFromSchema(b.schema)
	return b
}

func (b *CatalogLogtailRespBuilder) VisitDB(entry *catalog.DBEntry) error {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	entry.RUnlock()
	var dstBatch *containers.Batch
	for _, node := range mvccNodes {
		dbNode := node.(*catalog.DBMVCCNode)
		if dbNode.HasDropped() {
			dstBatch = b.delBatch
		} else {
			dstBatch = b.insBatch
		}
		catalogEntry2Batch(dstBatch, entry, b.schema, txnimpl.FillDBRow, dbNode.GetEnd(), false)
	}
	return nil
}

func (b *CatalogLogtailRespBuilder) VisitTbl(entry *catalog.TableEntry) error {
	entry.RLock()
	defer entry.RUnlock()
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	var dstBatch *containers.Batch
	for _, node := range mvccNodes {
		tblNode := node.(*catalog.TableMVCCNode)
		if tblNode.HasDropped() {
			dstBatch = b.delBatch
		} else {
			dstBatch = b.insBatch
		}
		catalogEntry2Batch(dstBatch, entry, b.schema, txnimpl.FillTableRow, tblNode.GetEnd(), false)
	}
	return nil
}

func (b *CatalogLogtailRespBuilder) BuildResp() (api.SyncLogTailResp, error) {
	entries := make([]*api.Entry, 0)
	var tblID uint64
	var tblName string
	if b.mode == CollectModeDB {
		tblID = catalog.SystemTable_DB_ID
		tblName = catalog.SystemTable_DB_Name
	} else {
		tblID = catalog.SystemTable_Table_ID
		tblName = catalog.SystemTable_Table_Name
	}

	if b.insBatch.Length() > 0 {
		bat, err := containersBatchToProtoBatch(b.insBatch)
		if err != nil {
			return api.SyncLogTailResp{}, err
		}
		insEntry := &api.Entry{
			EntryType:    api.Entry_Insert,
			TableId:      tblID,
			TableName:    tblName,
			DatabaseId:   catalog.SystemDBID,
			DatabaseName: catalog.SystemDBName,
			Bat:          bat,
		}
		entries = append(entries, insEntry)
	}
	if b.delBatch.Length() > 0 {
		bat, err := containersBatchToProtoBatch(b.delBatch)
		if err != nil {
			return api.SyncLogTailResp{}, err
		}
		delEntry := &api.Entry{
			EntryType:    api.Entry_Delete,
			TableId:      tblID,
			TableName:    tblName,
			DatabaseId:   catalog.SystemDBID,
			DatabaseName: catalog.SystemDBName,
			Bat:          bat,
		}
		entries = append(entries, delEntry)
	}
	return api.SyncLogTailResp{
		CkpLocation: b.checkpoint,
		Commands:    entries,
	}, nil
}

func catalogEntry2Batch[T *catalog.DBEntry | *catalog.TableEntry](
	dstBatch *containers.Batch,
	e T,
	schema *catalog.Schema,
	fillDataRow func(e T, attr string, col containers.Vector),
	commitTs types.TS,
	aborted bool,
) {
	for _, col := range schema.ColDefs {
		fillDataRow(e, col.Name, dstBatch.GetVectorByName(col.Name))
	}
	dstBatch.GetVectorByName(catalog.AttrCommitTs).Append(commitTs)
	dstBatch.GetVectorByName(catalog.AttrAborted).Append(aborted)
}

// make batch, append necessary field like commit ts
func makeRespBatchFromSchema(schema *catalog.Schema) *containers.Batch {
	batch := containers.NewBatch()

	typs := schema.Types()
	attrs := schema.Attrs()
	for i, attr := range attrs {
		batch.AddVector(attr, containers.MakeVector(typs[i], true))
	}
	batch.AddVector(catalog.AttrCommitTs, containers.MakeVector(types.T_TS.ToType(), true))
	batch.AddVector(catalog.AttrAborted, containers.MakeVector(types.T_bool.ToType(), true))
	return batch
}

// consume containers.Batch to construct api batch
func containersBatchToProtoBatch(batch *containers.Batch) (*api.Batch, error) {
	protoBatch := &api.Batch{
		Attrs: batch.Attrs,
		Vecs:  make([]*api.Vector, len(batch.Attrs)),
	}
	vecs := containers.CopyToMoVectors(batch.Vecs)
	for i, vec := range vecs {
		apivec, err := vector.VectorToProtoVector(vec)
		if err != nil {
			return nil, err
		}
		protoBatch.Vecs[i] = apivec
	}
	batch.Close()
	return protoBatch, nil
}

type TableLogtailRespBuilder struct {
	noopEntryVisitor
	start, end    types.TS
	did, tid      uint64
	dname, tname  string
	checkpoint    string
	blkMetaBatch  *containers.Batch
	dataInsSchema *catalog.Schema
	// dataDelSchema *catalog.Schema
	dataInsBatch *containers.Batch
	// dataDelBatch  *containers.Batch
}

func NewTableLogtailRespBuilder(ckp string, start, end types.TS, tbl *catalog.TableEntry) *TableLogtailRespBuilder {
	b := &TableLogtailRespBuilder{
		start:      start,
		end:        end,
		checkpoint: ckp,
	}

	b.dataInsSchema = tbl.GetSchema()
	b.dataInsBatch = makeRespBatchFromSchema(b.dataInsSchema)

	b.did = tbl.GetDB().GetID()
	b.tid = tbl.ID
	b.dname = tbl.GetDB().GetName()
	b.tname = b.dataInsSchema.Name

	// TODO: dataDelBatch

	b.blkMetaBatch = makeRespBatchFromSchema(blkMetaSchema)

	return b
}

func (b *TableLogtailRespBuilder) visitBlkMeta(e *catalog.BlockEntry) {
	e.RLock()
	mvccNodes := e.ClonePreparedInRange(b.start, b.end)
	e.RUnlock()
	for _, node := range mvccNodes {
		metaNode := node.(*catalog.MetadataMVCCNode)
		b.blkMetaBatch.GetVectorByName(blkMetaAttrBlockID).Append(e.ID)
		b.blkMetaBatch.GetVectorByName(blkMetaAttrEntryState).Append(e.IsAppendable())
		b.blkMetaBatch.GetVectorByName(blkMetaAttrCreateAt).Append(metaNode.CreatedAt)
		b.blkMetaBatch.GetVectorByName(blkMetaAttrDeleteAt).Append(metaNode.DeletedAt)
		b.blkMetaBatch.GetVectorByName(blkMetaAttrMetaLoc).Append([]byte(metaNode.MetaLoc))
		b.blkMetaBatch.GetVectorByName(blkMetaAttrDeltaLoc).Append([]byte(metaNode.DeltaLoc))
		b.blkMetaBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd())
		b.blkMetaBatch.GetVectorByName(catalog.AttrAborted).Append(false)
	}
}

func (b *TableLogtailRespBuilder) visitBlkData(e *catalog.BlockEntry) {
	_ = e.GetBlockData()
}

func (b *TableLogtailRespBuilder) VisitBlk(entry *catalog.BlockEntry) error {
	b.visitBlkMeta(entry)
	b.visitBlkData(entry)
	return nil
}

func (b *TableLogtailRespBuilder) BuildResp() (api.SyncLogTailResp, error) {
	entries := make([]*api.Entry, 0)
	makeBasicEntry := func() *api.Entry {
		return &api.Entry{
			TableId:      b.tid,
			TableName:    b.tname,
			DatabaseId:   b.did,
			DatabaseName: b.dname,
		}
	}

	if b.blkMetaBatch.Length() > 0 {
		entry := makeBasicEntry()
		entry.EntryType = api.Entry_Insert
		bat, err := containersBatchToProtoBatch(b.blkMetaBatch)
		if err != nil {
			return api.SyncLogTailResp{}, err
		}
		entry.Bat = bat
		entries = append(entries, entry)
	}

	return api.SyncLogTailResp{
		CkpLocation: b.checkpoint,
		Commands:    entries,
	}, nil
}

func logtailSampleHandler(db *DB, req api.SyncLogTailReq) (api.SyncLogTailResp, error) {
	start := types.BuildTS(req.CnHave.PhysicalTime, req.CnHave.LogicalTime)
	end := types.BuildTS(req.CnWant.PhysicalTime, req.CnWant.LogicalTime)
	did, tid := req.Table.DbId, req.Table.TbId
	verifiedCheckpoint := ""
	// TODO
	// verifiedCheckpoint, start, end = db.CheckpointMgr.check(start, end)

	// get a collector with a read only view for txns
	collector := db.LogtailMgr.GetLogtailCollector(start, end, did, tid)

	mode := CollectModeSegAndBlk
	if tid == catalog.SystemTable_DB_ID {
		mode = CollectModeDB
	} else if tid == catalog.SystemTable_Table_ID {
		mode = CollectModeTbl
	}

	var respBuilder RespBuilder

	if mode == CollectModeDB || mode == CollectModeTbl {
		respBuilder = NewCatalogLogtailRespBuilder(mode, verifiedCheckpoint, start, end)
	} else {
		var tableEntry *catalog.TableEntry
		// table logtail needs information about this table, so give it the table entry.
		if db, err := db.Catalog.GetDatabaseByID(did); err != nil {
			return api.SyncLogTailResp{}, err
		} else if tableEntry, err = db.GetTableEntryByID(tid); err != nil {
			return api.SyncLogTailResp{}, err
		}
		respBuilder = NewTableLogtailRespBuilder(verifiedCheckpoint, start, end, tableEntry)
	}

	collector.BindCollectEnv(mode, db.Catalog, respBuilder)
	if err := collector.Collect(); err != nil {
		return api.SyncLogTailResp{}, err
	}
	return respBuilder.BuildResp()
}
