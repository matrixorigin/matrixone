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
	"strings"
	"sync"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

const Size90M = 80 * 1024 * 1024

type CheckpointClient interface {
	CollectCheckpointsInRange(start, end types.TS) (location string, checkpointed types.TS)
	FlushTable(dbID, tableID uint64, ts types.TS) error
}

func HandleSyncLogTailReq(
	ckpClient CheckpointClient,
	mgr *LogtailMgr,
	c *catalog.Catalog,
	req api.SyncLogTailReq,
	canRetry bool) (resp api.SyncLogTailResp, err error) {
	logutil.Debugf("[Logtail] begin handle %+v", req)
	defer func() {
		logutil.Debugf("[Logtail] end handle %d entries[%q], err %v", len(resp.Commands), resp.CkpLocation, err)
	}()
	start := types.BuildTS(req.CnHave.PhysicalTime, req.CnHave.LogicalTime)
	end := types.BuildTS(req.CnWant.PhysicalTime, req.CnWant.LogicalTime)
	did, tid := req.Table.DbId, req.Table.TbId
	dbEntry, err := c.GetDatabaseByID(did)
	if err != nil {
		return
	}
	tableEntry, err := dbEntry.GetTableEntryByID(tid)
	if err != nil {
		return
	}
	if start.Less(tableEntry.GetCreatedAt()) {
		start = tableEntry.GetCreatedAt()
	}

	ckpLoc, checkpointed := ckpClient.CollectCheckpointsInRange(start, end)

	if checkpointed.GreaterEq(end) {
		return api.SyncLogTailResp{
			CkpLocation: ckpLoc,
		}, err
	} else if ckpLoc != "" {
		start = checkpointed.Next()
	}

	scope := mgr.DecideScope(tid)

	var visitor RespBuilder

	if scope == ScopeUserTables {
		visitor = NewTableLogtailRespBuilder(ckpLoc, start, end, tableEntry)
	} else {
		visitor = NewCatalogLogtailRespBuilder(scope, ckpLoc, start, end)
	}
	defer visitor.Close()

	operator := mgr.GetTableOperator(start, end, c, did, tid, scope, visitor)
	if err := operator.Run(); err != nil {
		return api.SyncLogTailResp{}, err
	}
	resp, err = visitor.BuildResp()

	if canRetry && scope == ScopeUserTables { // check simple conditions first
		if _, forceFlush := fault.TriggerFault("logtail_max_size"); forceFlush || resp.ProtoSize() > Size90M {
			if err = ckpClient.FlushTable(did, tid, end); err != nil {
				logutil.Errorf("[logtail] flush err: %v", err)
				return api.SyncLogTailResp{}, err
			}
			// try again after flushing
			newResp, err := HandleSyncLogTailReq(ckpClient, mgr, c, req, false)
			logutil.Infof("[logtail] flush result: %d -> %d err: %v", resp.ProtoSize(), newResp.ProtoSize(), err)
			return newResp, err
		}
	}
	return visitor.BuildResp()
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
	if shouldIgnoreDBInLogtail(entry.ID) {
		entry.RUnlock()
		return nil
	}
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
	if shouldIgnoreTblInLogtail(entry.ID) {
		entry.RUnlock()
		return nil
	}
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
	var tableName string
	switch b.scope {
	case ScopeDatabases:
		tblID = pkgcatalog.MO_DATABASE_ID
		tableName = pkgcatalog.MO_DATABASE
	case ScopeTables:
		tblID = pkgcatalog.MO_TABLES_ID
		tableName = pkgcatalog.MO_TABLES
	case ScopeColumns:
		tblID = pkgcatalog.MO_COLUMNS_ID
		tableName = pkgcatalog.MO_COLUMNS
	}

	if b.insBatch.Length() > 0 {
		bat, err := containersBatchToProtoBatch(b.insBatch)
		logutil.Debugf("[logtail] catalog insert to %d-%s, %s", tblID, tableName, DebugBatchToString("catalog", b.insBatch, true))
		if err != nil {
			return api.SyncLogTailResp{}, err
		}
		insEntry := &api.Entry{
			EntryType:    api.Entry_Insert,
			TableId:      tblID,
			TableName:    tableName,
			DatabaseId:   pkgcatalog.MO_CATALOG_ID,
			DatabaseName: pkgcatalog.MO_CATALOG,
			Bat:          bat,
		}
		entries = append(entries, insEntry)
	}
	if b.delBatch.Length() > 0 {
		bat, err := containersBatchToProtoBatch(b.delBatch)
		logutil.Debugf("[logtail] catalog delete from %d-%s, %s", tblID, tableName, DebugBatchToString("catalog", b.delBatch, false))
		if err != nil {
			return api.SyncLogTailResp{}, err
		}
		delEntry := &api.Entry{
			EntryType:    api.Entry_Delete,
			TableId:      tblID,
			TableName:    tableName,
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
	newEnd := b.end
	e.RLock()
	// try to find new end
	if newest := e.GetLatestCommittedNode(); newest != nil {
		latestPrepareTs := newest.CloneAll().(*catalog.MetadataMVCCNode).GetPrepare()
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

	if n := len(mvccNodes); n > 0 {
		newest := mvccNodes[n-1].(*catalog.MetadataMVCCNode)
		if e.IsAppendable() {
			if newest.MetaLoc != "" {
				// appendable block has been flushed, no need to collect data
				return true
			}
		} else {
			if newest.DeltaLoc != "" && newest.GetEnd().GreaterEq(b.end) {
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
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(e.GetSegment().ID)
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
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			logutil.Infof("[logtail] found race and collect meta again")
			// do it again,  it is ok to insert duplicate entries
			b.visitBlkMeta(e)
		} else {
			return
		}
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
		if metaChange {
			logutil.Debugf("[logtail] table meta [%v] %d-%s: %s", typ, b.tid, b.tname, DebugBatchToString("meta", batch, true))
		} else {
			logutil.Debugf("[logtail] table data [%v] %d-%s: %s", typ, b.tid, b.tname, DebugBatchToString("data", batch, false))
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

func LoadCheckpointEntries(
	metLoc string,
	tableID uint64,
	tableName string,
	dbID uint64,
	dbName string,
	fs fileservice.FileService) (entries []*api.Entry, err error) {
	if metLoc == "" {
		return
	}

	locations := strings.Split(metLoc, ";")
	datas := make([]*CheckpointData, len(locations))
	defer func() {
		for _, data := range datas {
			if data != nil {
				data.Close()
			}
		}
	}()
	var errMu sync.Mutex
	var wg sync.WaitGroup
	readfn := func(i int) {
		defer wg.Done()
		location := locations[i]
		reader, err2 := blockio.NewCheckpointReader(fs, location)
		if err2 != nil {
			errMu.Lock()
			err = err2
			errMu.Unlock()
			return
		}
		data := NewCheckpointData()
		if err2 = data.ReadFrom(reader, common.DefaultAllocator); err2 != nil {
			errMu.Lock()
			err = err2
			errMu.Unlock()
			return
		}
		datas[i] = data
	}
	wg.Add(len(locations))
	for i := range locations {
		go readfn(i)
	}
	wg.Wait()
	if err != nil {
		return
	}
	entries = make([]*api.Entry, 0)
	for i := range locations {
		data := datas[i]
		ins, del, cnIns, err := data.GetTableData(tableID)
		if err != nil {
			return nil, err
		}
		if tableName != pkgcatalog.MO_DATABASE &&
			tableName != pkgcatalog.MO_COLUMNS &&
			tableName != pkgcatalog.MO_TABLES {
			tableName = fmt.Sprintf("_%d_meta", tableID)
		}
		if ins != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Insert,
				TableId:      tableID,
				TableName:    tableName,
				DatabaseId:   dbID,
				DatabaseName: dbName,
				Bat:          ins,
			}
			entries = append(entries, entry)
		}
		if cnIns != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Insert,
				TableId:      tableID,
				TableName:    tableName,
				DatabaseId:   dbID,
				DatabaseName: dbName,
				Bat:          cnIns,
			}
			entries = append(entries, entry)
		}
		if del != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Delete,
				TableId:      tableID,
				TableName:    tableName,
				DatabaseId:   dbID,
				DatabaseName: dbName,
				Bat:          del,
			}
			entries = append(entries, entry)
		}
	}
	return
}
