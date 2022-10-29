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

/*

an application on logtail mgr: build reponse to SyncLogTailRequest

*/

import (
	"fmt"
	"hash/fnv"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

func HandleSyncLogTailReq(mgr *LogtailMgr, c *catalog.Catalog, req api.SyncLogTailReq) (resp api.SyncLogTailResp, err error) {
	logutil.Debugf("[Logtail] begin handle %v", req)
	defer func() {
		logutil.Debugf("[Logtail] end handle err %v", err)
	}()
	start := types.BuildTS(req.CnHave.PhysicalTime, req.CnHave.LogicalTime)
	end := types.BuildTS(req.CnWant.PhysicalTime, req.CnWant.LogicalTime)
	did, tid := req.Table.DbId, req.Table.TbId
	verifiedCheckpoint := ""
	// TODO
	// verifiedCheckpoint, start, end = db.CheckpointMgr.check(start, end)

	scope := mgr.DecideScope(tid)

	var visitor RespBuilder

	if scope == ScopeUserTables {
		var tableEntry *catalog.TableEntry
		// table logtail needs information about this table, so give it the table entry.
		if db, err := c.GetDatabaseByID(did); err != nil {
			return api.SyncLogTailResp{}, err
		} else if tableEntry, err = db.GetTableEntryByID(tid); err != nil {
			return api.SyncLogTailResp{}, err
		}
		visitor = NewTableLogtailRespBuilder(verifiedCheckpoint, start, end, tableEntry)
	} else {
		visitor = NewCatalogLogtailRespBuilder(scope, verifiedCheckpoint, start, end)
	}
	defer visitor.Close()

	operator := mgr.GetTableOperator(start, end, c, did, tid, scope, visitor)
	if err := operator.Run(); err != nil {
		return api.SyncLogTailResp{}, err
	}
	return visitor.BuildResp()
}

func CollectSnapshot(c *catalog.Catalog, start, end types.TS) (*CheckpointLogtailRespBuilder, error) {
	visitor := NewCheckpointLogtailRespBuilder(start, end)
	c.RecurLoop(visitor)
	return visitor, nil
}

type RespBuilder interface {
	catalog.Processor
	BuildResp() (api.SyncLogTailResp, error)
	Close()
}

// CatalogLogtailRespBuilder knows how to make api-entry from catalog entry
// impl catalog.Processor interface, driven by LogtailCollector
type CatalogLogtailRespBuilder struct {
	*catalog.LoopProcessor
	scope      Scope
	start, end types.TS
	checkpoint string
	insBatch   *containers.Batch
	delBatch   *containers.Batch
}

func NewCatalogLogtailRespBuilder(scope Scope, ckp string, start, end types.TS) *CatalogLogtailRespBuilder {
	b := &CatalogLogtailRespBuilder{
		LoopProcessor: new(catalog.LoopProcessor),
		scope:         scope,
		start:         start,
		end:           end,
		checkpoint:    ckp,
	}
	switch scope {
	case ScopeDatabases:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemDBSchema)
	case ScopeTables:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemTableSchema)
	case ScopeColumns:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemColumnSchema)
	}
	b.delBatch = makeRespBatchFromSchema(DelSchema)
	b.DatabaseFn = b.VisitDB
	b.TableFn = b.VisitTbl

	return b
}

func (b *CatalogLogtailRespBuilder) Close() {
	if b.insBatch != nil {
		b.insBatch.Close()
		b.insBatch = nil
	}
	if b.delBatch != nil {
		b.delBatch.Close()
		b.delBatch = nil
	}
}

func (b *CatalogLogtailRespBuilder) VisitDB(entry *catalog.DBEntry) error {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		dbNode := node.(*catalog.DBMVCCNode)
		if dbNode.HasDropCommitted() {
			// delScehma is empty, it will just fill rowid / commit ts
			catalogEntry2Batch(b.delBatch, entry, DelSchema, txnimpl.FillDBRow, u64ToRowID(entry.GetID()), dbNode.GetEnd())
		} else {
			catalogEntry2Batch(b.insBatch, entry, catalog.SystemDBSchema, txnimpl.FillDBRow, u64ToRowID(entry.GetID()), dbNode.GetEnd())
		}
	}
	return nil
}

func (b *CatalogLogtailRespBuilder) VisitTbl(entry *catalog.TableEntry) error {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		tblNode := node.(*catalog.TableMVCCNode)
		if b.scope == ScopeColumns {
			var dstBatch *containers.Batch
			if !tblNode.HasDropCommitted() {
				dstBatch = b.insBatch
				// fill unique syscol fields if inserting
				for _, syscol := range catalog.SystemColumnSchema.ColDefs {
					txnimpl.FillColumnRow(entry, syscol.Name, b.insBatch.GetVectorByName(syscol.Name))
				}
			} else {
				dstBatch = b.delBatch
			}

			// fill common syscol fields for every user column
			rowidVec := dstBatch.GetVectorByName(catalog.AttrRowID)
			commitVec := dstBatch.GetVectorByName(catalog.AttrCommitTs)
			tableID := entry.GetID()
			commitTs := tblNode.GetEnd()
			for _, usercol := range entry.GetSchema().ColDefs {
				rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", tableID, usercol.Name))))
				commitVec.Append(commitTs)
			}
		} else {
			if tblNode.HasDropCommitted() {
				catalogEntry2Batch(b.delBatch, entry, DelSchema, txnimpl.FillTableRow, u64ToRowID(entry.GetID()), tblNode.GetEnd())
			} else {
				catalogEntry2Batch(b.insBatch, entry, catalog.SystemTableSchema, txnimpl.FillTableRow, u64ToRowID(entry.GetID()), tblNode.GetEnd())
			}
		}
	}
	return nil
}

func (b *CatalogLogtailRespBuilder) BuildResp() (api.SyncLogTailResp, error) {
	entries := make([]*api.Entry, 0)
	var tblID uint64
	var tblName string
	switch b.scope {
	case ScopeDatabases:
		tblID = pkgcatalog.MO_DATABASE_ID
		tblName = pkgcatalog.MO_DATABASE
	case ScopeTables:
		tblID = pkgcatalog.MO_TABLES_ID
		tblName = pkgcatalog.MO_TABLES
	case ScopeColumns:
		tblID = pkgcatalog.MO_COLUMNS_ID
		tblName = pkgcatalog.MO_COLUMNS
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
			DatabaseId:   pkgcatalog.MO_CATALOG_ID,
			DatabaseName: pkgcatalog.MO_CATALOG,
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
			DatabaseId:   pkgcatalog.MO_CATALOG_ID,
			DatabaseName: pkgcatalog.MO_CATALOG,
			Bat:          bat,
		}
		entries = append(entries, delEntry)
	}
	return api.SyncLogTailResp{
		CkpLocation: b.checkpoint,
		Commands:    entries,
	}, nil
}

// this is used to collect ONE ROW of db or table change
func catalogEntry2Batch[T *catalog.DBEntry | *catalog.TableEntry](
	dstBatch *containers.Batch,
	e T,
	schema *catalog.Schema,
	fillDataRow func(e T, attr string, col containers.Vector),
	rowid types.Rowid,
	commitTs types.TS,
) {
	for _, col := range schema.ColDefs {
		fillDataRow(e, col.Name, dstBatch.GetVectorByName(col.Name))
	}
	dstBatch.GetVectorByName(catalog.AttrRowID).Append(rowid)
	dstBatch.GetVectorByName(catalog.AttrCommitTs).Append(commitTs)
}

func u64ToRowID(v uint64) types.Rowid {
	var rowid types.Rowid
	bs := types.EncodeUint64(&v)
	copy(rowid[0:], bs)
	return rowid
}

func bytesToRowID(bs []byte) types.Rowid {
	var rowid types.Rowid
	if size := len(bs); size <= types.RowidSize {
		copy(rowid[:size], bs[:size])
	} else {
		hasher := fnv.New128()
		hasher.Write(bs)
		hasher.Sum(rowid[:0])
	}
	return rowid
}

// make batch, append necessary field like commit ts
func makeRespBatchFromSchema(schema *catalog.Schema) *containers.Batch {
	bat := containers.NewBatch()

	bat.AddVector(catalog.AttrRowID, containers.MakeVector(types.T_Rowid.ToType(), false))
	bat.AddVector(catalog.AttrCommitTs, containers.MakeVector(types.T_TS.ToType(), false))
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	nullables := schema.AllNullables()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], nullables[i]))
	}
	return bat
}

// consume containers.Batch to construct api batch
func containersBatchToProtoBatch(bat *containers.Batch) (*api.Batch, error) {
	mobat := containers.CopyToMoBatch(bat)
	return batch.BatchToProtoBatch(mobat)
}

type TableLogtailRespBuilder struct {
	*catalog.LoopProcessor
	start, end      types.TS
	did, tid        uint64
	dname, tname    string
	checkpoint      string
	blkMetaInsBatch *containers.Batch
	blkMetaDelBatch *containers.Batch
	dataInsBatch    *containers.Batch
	dataDelBatch    *containers.Batch
}

func NewTableLogtailRespBuilder(ckp string, start, end types.TS, tbl *catalog.TableEntry) *TableLogtailRespBuilder {
	b := &TableLogtailRespBuilder{
		LoopProcessor: new(catalog.LoopProcessor),
		start:         start,
		end:           end,
		checkpoint:    ckp,
	}
	b.BlockFn = b.VisitBlk

	schema := tbl.GetSchema()

	b.did = tbl.GetDB().GetID()
	b.tid = tbl.ID
	b.dname = tbl.GetDB().GetName()
	b.tname = schema.Name

	b.dataInsBatch = makeRespBatchFromSchema(schema)
	b.dataDelBatch = makeRespBatchFromSchema(DelSchema)
	b.blkMetaInsBatch = makeRespBatchFromSchema(BlkMetaSchema)
	b.blkMetaDelBatch = makeRespBatchFromSchema(DelSchema)
	return b
}

func (b *TableLogtailRespBuilder) Close() {
	if b.dataInsBatch != nil {
		b.dataInsBatch.Close()
		b.dataInsBatch = nil
	}
	if b.dataDelBatch != nil {
		b.dataDelBatch.Close()
		b.dataDelBatch = nil
	}
	if b.blkMetaInsBatch != nil {
		b.blkMetaInsBatch.Close()
		b.blkMetaInsBatch = nil
	}
	if b.blkMetaDelBatch != nil {
		b.blkMetaDelBatch.Close()
		b.blkMetaDelBatch = nil
	}
}

func (b *TableLogtailRespBuilder) visitBlkMeta(e *catalog.BlockEntry) (skipData bool) {
	var latestCommittedNode *catalog.MetadataMVCCNode
	newEnd := b.end
	e.RLock()
	// try to find new end
	if newest := e.GetLatestCommittedNode(); newest != nil {
		latestCommittedNode = newest.CloneAll().(*catalog.MetadataMVCCNode)
		latestPrepareTs := latestCommittedNode.GetPrepare()
		if latestPrepareTs.Greater(b.end) {
			newEnd = latestPrepareTs
		}
	}
	mvccNodes := e.ClonePreparedInRange(b.start, newEnd)
	e.RUnlock()

	for _, node := range mvccNodes {
		metaNode := node.(*catalog.MetadataMVCCNode)
		if metaNode.MetaLoc != "" && !metaNode.IsAborted() {
			b.appendBlkMeta(e, metaNode)
		}
	}

	if latestCommittedNode != nil {
		if e.IsAppendable() {
			if latestCommittedNode.MetaLoc != "" {
				// appendable block has been flushed, no need to collect data
				return true
			}
		} else {
			if latestCommittedNode.DeltaLoc != "" && latestCommittedNode.GetEnd().GreaterEq(b.end) {
				// non-appendable block has newer delta data on s3, no need to collect data
				return true
			}
		}
	}
	return false
}

func (b *TableLogtailRespBuilder) appendBlkMeta(e *catalog.BlockEntry, metaNode *catalog.MetadataMVCCNode) {
	logutil.Infof("[Logtail] record block meta row %s, %v, %s, %s, %s, %s",
		e.AsCommonID().String(), e.IsAppendable(),
		metaNode.CreatedAt.ToString(), metaNode.DeletedAt.ToString(), metaNode.MetaLoc, metaNode.DeltaLoc)
	is_sorted := false
	if !e.IsAppendable() && e.GetSchema().HasPK() {
		is_sorted = true
	}
	insBatch := b.blkMetaInsBatch
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(e.ID)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(e.IsAppendable())
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.MetaLoc))
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.DeltaLoc))
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd())
	insBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt)
	insBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(e.ID))

	if metaNode.HasDropCommitted() {
		if metaNode.DeletedAt.IsEmpty() {
			panic(moerr.NewInternalError("no delete at time in a dropped entry"))
		}
		delBatch := b.blkMetaDelBatch
		delBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.DeletedAt)
		delBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(e.ID))
	}
}

func (b *TableLogtailRespBuilder) visitBlkData(e *catalog.BlockEntry) (err error) {
	block := e.GetBlockData()
	insBatch, err := block.CollectAppendInRange(b.start, b.end, false)
	if err != nil {
		return
	}
	if insBatch != nil && insBatch.Length() > 0 {
		b.dataInsBatch.Extend(insBatch)
		// insBatch is freed, don't use anymore
	}
	delBatch, err := block.CollectDeleteInRange(b.start, b.end, false)
	if err != nil {
		return
	}
	if delBatch != nil && delBatch.Length() > 0 {
		b.dataDelBatch.Extend(delBatch)
		// delBatch is freed, don't use anymore
	}
	return nil
}

func (b *TableLogtailRespBuilder) VisitBlk(entry *catalog.BlockEntry) error {
	if b.visitBlkMeta(entry) {
		// data has been flushed, no need to collect data
		return nil
	}
	return b.visitBlkData(entry)
}

func (b *TableLogtailRespBuilder) BuildResp() (api.SyncLogTailResp, error) {
	entries := make([]*api.Entry, 0)
	tryAppendEntry := func(typ api.Entry_EntryType, metaChange bool, batch *containers.Batch) error {
		if batch.Length() == 0 {
			return nil
		}
		bat, err := containersBatchToProtoBatch(batch)
		if err != nil {
			return err
		}

		blockID := uint64(0)
		tableName := b.tname
		if metaChange {
			tableName = fmt.Sprintf("_%d_meta", b.tid)
			logutil.Infof("[Logtail] send block meta for %q", b.tname)
		}
		entry := &api.Entry{
			EntryType:    typ,
			TableId:      b.tid,
			TableName:    tableName,
			DatabaseId:   b.did,
			DatabaseName: b.dname,
			BlockId:      blockID,
			Bat:          bat,
		}
		entries = append(entries, entry)
		return nil
	}

	empty := api.SyncLogTailResp{}
	if err := tryAppendEntry(api.Entry_Insert, true, b.blkMetaInsBatch); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_Delete, true, b.blkMetaDelBatch); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_Insert, false, b.dataInsBatch); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_Delete, false, b.dataDelBatch); err != nil {
		return empty, err
	}

	return api.SyncLogTailResp{
		CkpLocation: b.checkpoint,
		Commands:    entries,
	}, nil
}

type CheckpointLogtailRespBuilder struct {
	*catalog.LoopProcessor
	start, end types.TS
	// checkpoint      string
	dbInsBatch         *containers.Batch
	dbInsTxnBatch      *containers.Batch
	dbDelBatch         *containers.Batch
	dbDelTxnBatch      *containers.Batch
	tblInsBatch        *containers.Batch
	tblInsTxnBatch     *containers.Batch
	tblDelBatch        *containers.Batch
	tblDelTxnBatch     *containers.Batch
	tblColInsBatch     *containers.Batch
	tblColDelBatch     *containers.Batch
	segInsBatch        *containers.Batch
	segInsTxnBatch     *containers.Batch
	segDelBatch        *containers.Batch
	segDelTxnBatch     *containers.Batch
	blkMetaInsBatch    *containers.Batch
	blkMetaInsTxnBatch *containers.Batch
	blkMetaDelBatch    *containers.Batch
	blkMetaDelTxnBatch *containers.Batch
}

func NewCheckpointLogtailRespBuilder(start, end types.TS) *CheckpointLogtailRespBuilder {
	b := &CheckpointLogtailRespBuilder{
		LoopProcessor:      new(catalog.LoopProcessor),
		start:              start,
		end:                end,
		dbInsBatch:         makeRespBatchFromSchema(catalog.SystemDBSchema),
		dbInsTxnBatch:      makeRespBatchFromSchema(TxnNodeSchema),
		dbDelBatch:         makeRespBatchFromSchema(DelSchema),
		dbDelTxnBatch:      makeRespBatchFromSchema(DBDNSchema),
		tblInsBatch:        makeRespBatchFromSchema(catalog.SystemTableSchema),
		tblInsTxnBatch:     makeRespBatchFromSchema(TxnNodeSchema),
		tblDelBatch:        makeRespBatchFromSchema(DelSchema),
		tblDelTxnBatch:     makeRespBatchFromSchema(TblDNSchema),
		tblColInsBatch:     makeRespBatchFromSchema(catalog.SystemColumnSchema),
		tblColDelBatch:     makeRespBatchFromSchema(DelSchema),
		segInsBatch:        makeRespBatchFromSchema(SegSchema),
		segInsTxnBatch:     makeRespBatchFromSchema(SegDNSchema),
		segDelBatch:        makeRespBatchFromSchema(DelSchema),
		segDelTxnBatch:     makeRespBatchFromSchema(SegDNSchema),
		blkMetaInsBatch:    makeRespBatchFromSchema(BlkMetaSchema),
		blkMetaInsTxnBatch: makeRespBatchFromSchema(BlkDNSchema),
		blkMetaDelBatch:    makeRespBatchFromSchema(DelSchema),
		blkMetaDelTxnBatch: makeRespBatchFromSchema(BlkDNSchema),
	}
	b.DatabaseFn = b.VisitDB
	b.TableFn = b.VisitTable
	b.SegmentFn = b.VisitSeg
	b.BlockFn = b.VisitBlk
	return b
}

func (b *CheckpointLogtailRespBuilder) ReplayCatalog(c *catalog.Catalog) {
	c.OnReplayDatabaseBatch(b.GetDBBatchs())
	c.OnReplayTableBatch(b.GetTblBatchs())
	c.OnReplaySegmentBatch(b.GetSegBatchs())
	c.OnReplayBlockBatch(b.GetBlkBatchs())
}

func (b *CheckpointLogtailRespBuilder) WriteToFS(writer *blockio.Writer) (blks []objectio.BlockObject) {
	if _, err := writer.WriteBlock(b.dbInsBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.dbInsTxnBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.dbDelBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.dbDelTxnBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.tblInsBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.tblInsTxnBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.tblDelBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.tblDelTxnBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.tblColInsBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.tblColDelBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.segInsBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.segInsTxnBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.segDelBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.segDelTxnBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.blkMetaInsBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.blkMetaInsTxnBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.blkMetaDelBatch); err != nil {
		panic(err)
	}
	if _, err := writer.WriteBlock(b.blkMetaDelTxnBatch); err != nil {
		panic(err)
	}
	blks, err := writer.Sync()
	if err != nil {
		panic(err)
	}
	return blks
}

func (b *CheckpointLogtailRespBuilder) ReadFromFS(reader *blockio.Reader, m *mpool.MPool) {
	metas, err := reader.ReadMetas(m)
	if err != nil {
		panic(err)
	}
	if b.dbInsBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, catalog.SystemDBSchema.Types()...),
		append(BaseAttr, catalog.SystemDBSchema.AllNames()...),
		append([]bool{false, false}, catalog.SystemDBSchema.AllNullables()...),
		metas[0]); err != nil {
		panic(err)
	}
	if b.dbInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, TxnNodeSchema.Types()...),
		append(BaseAttr, TxnNodeSchema.AllNames()...),
		append([]bool{false, false}, TxnNodeSchema.AllNullables()...),
		metas[1]); err != nil {
		panic(err)
	}
	if b.dbDelBatch, err = reader.LoadBlkColumnsByMeta(
		BaseTypes,
		BaseAttr,
		[]bool{false, false},
		metas[2]); err != nil {
		panic(err)
	}
	if b.dbDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, DBDNSchema.Types()...),
		append(BaseAttr, DBDNSchema.AllNames()...),
		append([]bool{false, false}, DBDNSchema.AllNullables()...),
		metas[3]); err != nil {
		panic(err)
	}
	if b.tblInsBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, catalog.SystemTableSchema.Types()...),
		append(BaseAttr, catalog.SystemTableSchema.AllNames()...),
		append([]bool{false, false}, catalog.SystemTableSchema.AllNullables()...),
		metas[4]); err != nil {
		panic(err)
	}
	if b.tblInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, TxnNodeSchema.Types()...),
		append(BaseAttr, TxnNodeSchema.AllNames()...),
		append([]bool{false, false}, TxnNodeSchema.AllNullables()...),
		metas[5]); err != nil {
		panic(err)
	}
	if b.tblDelBatch, err = reader.LoadBlkColumnsByMeta(
		BaseTypes,
		BaseAttr,
		[]bool{false, false},
		metas[6]); err != nil {
		panic(err)
	}
	if b.tblDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, TblDNSchema.Types()...),
		append(BaseAttr, TblDNSchema.AllNames()...),
		append([]bool{false, false}, TblDNSchema.AllNullables()...),
		metas[7]); err != nil {
		panic(err)
	}
	if b.tblColInsBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, catalog.SystemColumnSchema.Types()...),
		append(BaseAttr, catalog.SystemColumnSchema.AllNames()...),
		append([]bool{false, false}, catalog.SystemColumnSchema.AllNullables()...),
		metas[8]); err != nil {
		panic(err)
	}
	if b.tblColDelBatch, err = reader.LoadBlkColumnsByMeta(
		BaseTypes,
		BaseAttr,
		[]bool{false, false},
		metas[9]); err != nil {
		panic(err)
	}
	if b.segInsBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, SegSchema.Types()...),
		append(BaseAttr, SegSchema.AllNames()...),
		append([]bool{false, false}, SegSchema.AllNullables()...),
		metas[10]); err != nil {
		panic(err)
	}
	if b.segInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, SegDNSchema.Types()...),
		append(BaseAttr, SegDNSchema.AllNames()...),
		append([]bool{false, false}, SegDNSchema.AllNullables()...),
		metas[11]); err != nil {
		panic(err)
	}
	if b.segDelBatch, err = reader.LoadBlkColumnsByMeta(
		BaseTypes,
		BaseAttr,
		[]bool{false, false},
		metas[12]); err != nil {
		panic(err)
	}
	if b.segDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, SegDNSchema.Types()...),
		append(BaseAttr, SegDNSchema.AllNames()...),
		append([]bool{false, false}, SegDNSchema.AllNullables()...),
		metas[13]); err != nil {
		panic(err)
	}
	if b.blkMetaInsBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, BlkMetaSchema.Types()...),
		append(BaseAttr, BlkMetaSchema.AllNames()...),
		append([]bool{false, false}, BlkMetaSchema.AllNullables()...),
		metas[14]); err != nil {
		panic(err)
	}
	if b.blkMetaInsTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, BlkDNSchema.Types()...),
		append(BaseAttr, BlkDNSchema.AllNames()...),
		append([]bool{false, false}, BlkDNSchema.AllNullables()...),
		metas[15]); err != nil {
		panic(err)
	}
	if b.blkMetaDelBatch, err = reader.LoadBlkColumnsByMeta(
		BaseTypes,
		BaseAttr,
		[]bool{false, false},
		metas[16]); err != nil {
		panic(err)
	}
	if b.blkMetaDelTxnBatch, err = reader.LoadBlkColumnsByMeta(
		append(BaseTypes, BlkDNSchema.Types()...),
		append(BaseAttr, BlkDNSchema.AllNames()...),
		append([]bool{false, false}, BlkDNSchema.AllNullables()...),
		metas[17]); err != nil {
		panic(err)
	}
}

func (b *CheckpointLogtailRespBuilder) GetDBBatchs() (*containers.Batch, *containers.Batch, *containers.Batch, *containers.Batch) {
	return b.dbInsBatch, b.dbInsTxnBatch, b.dbDelBatch, b.dbDelTxnBatch
}
func (b *CheckpointLogtailRespBuilder) GetTblBatchs() (*containers.Batch, *containers.Batch, *containers.Batch, *containers.Batch, *containers.Batch) {
	return b.tblInsBatch, b.tblInsTxnBatch, b.tblColInsBatch, b.tblDelBatch, b.tblDelTxnBatch
}
func (b *CheckpointLogtailRespBuilder) GetSegBatchs() (*containers.Batch, *containers.Batch, *containers.Batch, *containers.Batch) {
	return b.segInsBatch, b.segInsTxnBatch, b.segDelBatch, b.segDelTxnBatch
}
func (b *CheckpointLogtailRespBuilder) GetBlkBatchs() (*containers.Batch, *containers.Batch, *containers.Batch, *containers.Batch) {
	return b.blkMetaInsBatch, b.blkMetaInsTxnBatch, b.blkMetaDelBatch, b.blkMetaDelTxnBatch
}
func (b *CheckpointLogtailRespBuilder) VisitDB(entry *catalog.DBEntry) error {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		dbNode := node.(*catalog.DBMVCCNode)
		if dbNode.HasDropCommitted() {
			// delScehma is empty, it will just fill rowid / commit ts
			catalogEntry2Batch(b.dbDelBatch, entry, DelSchema, txnimpl.FillDBRow, u64ToRowID(entry.GetID()), dbNode.GetEnd())
			dbNode.TxnMVCCNode.FillTxnRows(b.dbDelTxnBatch)
			b.dbDelTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetID())
		} else {
			catalogEntry2Batch(b.dbInsBatch, entry, catalog.SystemDBSchema, txnimpl.FillDBRow, u64ToRowID(entry.GetID()), dbNode.GetEnd())
			dbNode.TxnMVCCNode.FillTxnRows(b.dbInsTxnBatch)
		}
	}
	return nil
}

func (b *CheckpointLogtailRespBuilder) VisitTable(entry *catalog.TableEntry) (err error) {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		tblNode := node.(*catalog.TableMVCCNode)
		if !tblNode.HasDropCommitted() {
			for _, syscol := range catalog.SystemColumnSchema.ColDefs {
				txnimpl.FillColumnRow(entry, syscol.Name, b.tblColInsBatch.GetVectorByName(syscol.Name))
				rowidVec := b.tblColInsBatch.GetVectorByName(catalog.AttrRowID)
				commitVec := b.tblColInsBatch.GetVectorByName(catalog.AttrCommitTs)
				for _, usercol := range entry.GetSchema().ColDefs {
					rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))))
					commitVec.Append(tblNode.GetEnd())
				}
			}
			catalogEntry2Batch(b.tblInsBatch, entry, catalog.SystemTableSchema, txnimpl.FillTableRow, u64ToRowID(entry.GetID()), tblNode.GetEnd())
			tblNode.TxnMVCCNode.FillTxnRows(b.tblInsTxnBatch)
		} else {
			b.tblDelTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetDB().GetID())
			b.tblDelTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetID())
			rowidVec := b.tblColDelBatch.GetVectorByName(catalog.AttrRowID)
			commitVec := b.tblColDelBatch.GetVectorByName(catalog.AttrCommitTs)
			for _, usercol := range entry.GetSchema().ColDefs {
				rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))))
				commitVec.Append(tblNode.GetEnd())
			}
			catalogEntry2Batch(b.tblDelBatch, entry, DelSchema, txnimpl.FillTableRow, u64ToRowID(entry.GetID()), tblNode.GetEnd())
			tblNode.TxnMVCCNode.FillTxnRows(b.tblDelTxnBatch)
		}
	}
	return nil
}
func (b *CheckpointLogtailRespBuilder) VisitSeg(entry *catalog.SegmentEntry) (err error) {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		segNode := node.(*catalog.MetadataMVCCNode)
		if segNode.HasDropCommitted() {
			b.segDelBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(entry.ID))
			b.segDelBatch.GetVectorByName(catalog.AttrCommitTs).Append(segNode.GetEnd())
			b.segDelTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().GetID())
			b.segDelTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().GetID())
			segNode.TxnMVCCNode.FillTxnRows(b.segDelTxnBatch)
		} else {
			b.segInsBatch.GetVectorByName(SegmentAttr_ID).Append(entry.GetID())
			b.segInsBatch.GetVectorByName(SegmentAttr_CreateAt).Append(segNode.GetEnd())
			b.segInsBatch.GetVectorByName(SegmentAttr_State).Append(entry.IsAppendable())
			b.segInsTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().GetID())
			b.segInsTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().GetID())
			segNode.TxnMVCCNode.FillTxnRows(b.segInsTxnBatch)
		}
	}
	return nil
}
func (b *CheckpointLogtailRespBuilder) VisitBlk(entry *catalog.BlockEntry) (err error) {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		metaNode := node.(*catalog.MetadataMVCCNode)
		if metaNode.HasDropCommitted() {
			b.blkMetaDelBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(entry.ID))
			b.blkMetaDelBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd())
			b.blkMetaDelTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID())
			b.blkMetaDelTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID())
			b.blkMetaDelTxnBatch.GetVectorByName(SnapshotAttr_SegID).Append(entry.GetSegment().GetID())
			metaNode.TxnMVCCNode.FillTxnRows(b.blkMetaDelTxnBatch)
		} else {
			b.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(entry.ID)
			b.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(entry.IsAppendable())
			b.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(metaNode.MetaLoc))
			b.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(metaNode.DeltaLoc))
			b.blkMetaInsBatch.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(metaNode.GetEnd())
			b.blkMetaInsBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.CreatedAt)
			b.blkMetaInsBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(entry.ID))
			b.blkMetaInsTxnBatch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetSegment().GetTable().GetDB().GetID())
			b.blkMetaInsTxnBatch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetSegment().GetTable().GetID())
			b.blkMetaInsTxnBatch.GetVectorByName(SnapshotAttr_SegID).Append(entry.GetSegment().GetID())
			metaNode.TxnMVCCNode.FillTxnRows(b.blkMetaInsTxnBatch)
		}
	}
	return nil
}
func (b *CheckpointLogtailRespBuilder) Close() {
	if b.dbInsBatch != nil {
		b.dbInsBatch.Close()
		b.dbInsBatch = nil
	}
	if b.dbInsTxnBatch != nil {
		b.dbInsTxnBatch.Close()
		b.dbInsTxnBatch = nil
	}
	if b.dbDelBatch != nil {
		b.dbDelBatch.Close()
		b.dbDelBatch = nil
	}
	if b.dbDelTxnBatch != nil {
		b.dbDelTxnBatch.Close()
		b.dbDelTxnBatch = nil
	}
	if b.tblInsBatch != nil {
		b.tblInsBatch.Close()
		b.tblInsBatch = nil
	}
	if b.tblInsTxnBatch != nil {
		b.tblInsTxnBatch.Close()
		b.tblInsTxnBatch = nil
	}
	if b.tblDelBatch != nil {
		b.tblDelBatch.Close()
		b.tblDelBatch = nil
	}
	if b.tblDelTxnBatch != nil {
		b.tblDelTxnBatch.Close()
		b.tblDelTxnBatch = nil
	}
	if b.tblColInsBatch != nil {
		b.tblColInsBatch.Close()
		b.tblColInsBatch = nil
	}
	if b.tblColDelBatch != nil {
		b.tblColDelBatch.Close()
		b.tblColDelBatch = nil
	}
	if b.segInsBatch != nil {
		b.segInsBatch.Close()
		b.segInsBatch = nil
	}
	if b.segInsTxnBatch != nil {
		b.segInsTxnBatch.Close()
		b.segInsTxnBatch = nil
	}
	if b.segDelBatch != nil {
		b.segDelBatch.Close()
		b.segDelBatch = nil
	}
	if b.segDelTxnBatch != nil {
		b.segDelTxnBatch.Close()
		b.segDelTxnBatch = nil
	}
	if b.blkMetaInsBatch != nil {
		b.blkMetaInsBatch.Close()
		b.blkMetaInsBatch = nil
	}
	if b.blkMetaInsTxnBatch != nil {
		b.blkMetaInsTxnBatch.Close()
		b.blkMetaInsTxnBatch = nil
	}
	if b.blkMetaDelBatch != nil {
		b.blkMetaDelBatch.Close()
		b.blkMetaDelBatch = nil
	}
	if b.blkMetaDelTxnBatch != nil {
		b.blkMetaDelTxnBatch.Close()
		b.blkMetaDelTxnBatch = nil
	}
}
