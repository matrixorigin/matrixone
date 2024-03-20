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

More docs:
https://github.com/matrixorigin/docs/blob/main/tech-notes/dnservice/ref_logtail_impl.md


Main workflow:

          +------------------+
          | CheckpointRunner |
          +------------------+
            ^         |
            | range   | ckp & newRange
            |         v
          +------------------+  newRange  +----------------+  snapshot   +--------------+
 user ->  | HandleGetLogTail | ---------> | LogtailManager | ----------> | LogtailTable |
   ^      +------------------+            +----------------+             +--------------+
   |                                                                        |
   |           +------------------+                                         |
   +---------- |   RespBuilder    |  ------------------>+-------------------+
      return   +------------------+                     |
      entries                                           |  visit
                                                        |
                                                        v
                                  +-----------------------------------+
                                  |     txnblock2                     |
                     ...          +-----------------------------------+   ...
                                  | bornTs  | ... txn100 | txn101 |.. |
                                  +-----------------+---------+-------+
                                                    |         |
                                                    |         |
                                                    |         |
                                  +-----------------+    +----+-------+     dirty blocks
                                  |                 |    |            |
                                  v                 v    v            v
                              +-------+           +-------+       +-------+
                              | BLK-1 |           | BLK-2 |       | BLK-3 |
                              +---+---+           +---+---+       +---+---+
                                  |                   |               |
                                  v                   v               v
                            [V1@t25,disk]       [V1@t17,mem]     [V1@t17,disk]
                                  |                   |               |
                                  v                   v               v
                            [V0@t12,mem]        [V0@t10,mem]     [V0@t10,disk]
                                  |                                   |
                                  v                                   v
                            [V0@t7,mem]                           [V0@t7,mem]


*/

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"go.uber.org/zap"
)

const Size90M = 90 * 1024 * 1024

type CheckpointClient interface {
	CollectCheckpointsInRange(ctx context.Context, start, end types.TS) (ckpLoc string, lastEnd types.TS, err error)
	FlushTable(ctx context.Context, dbID, tableID uint64, ts types.TS) error
}

func DecideTableScope(tableID uint64) Scope {
	var scope Scope
	switch tableID {
	case pkgcatalog.MO_DATABASE_ID:
		scope = ScopeDatabases
	case pkgcatalog.MO_TABLES_ID:
		scope = ScopeTables
	case pkgcatalog.MO_COLUMNS_ID:
		scope = ScopeColumns
	default:
		scope = ScopeUserTables
	}
	return scope
}

func HandleSyncLogTailReq(
	ctx context.Context,
	ckpClient CheckpointClient,
	mgr *Manager,
	c *catalog.Catalog,
	req api.SyncLogTailReq,
	canRetry bool) (resp api.SyncLogTailResp, closeCB func(), err error) {
	now := time.Now()
	logutil.Debugf("[Logtail] begin handle %+v", req)
	defer func() {
		if elapsed := time.Since(now); elapsed > 5*time.Second {
			logutil.Infof("[Logtail] long pull cost %v, %v: %+v, %v ", elapsed, canRetry, req, err)
		}
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
	tableEntry.RLock()
	createTS := tableEntry.GetCreatedAtLocked()
	tableEntry.RUnlock()
	if start.Less(&createTS) {
		start = createTS
	}

	ckpLoc, checkpointed, err := ckpClient.CollectCheckpointsInRange(ctx, start, end)
	if err != nil {
		return
	}

	if checkpointed.GreaterEq(&end) {
		return api.SyncLogTailResp{
			CkpLocation: ckpLoc,
		}, nil, err
	} else if ckpLoc != "" {
		start = checkpointed.Next()
	}

	scope := DecideTableScope(tid)

	var visitor RespBuilder

	if scope == ScopeUserTables {
		visitor = NewTableLogtailRespBuilder(ctx, ckpLoc, start, end, tableEntry)
	} else {
		visitor = NewCatalogLogtailRespBuilder(ctx, scope, ckpLoc, start, end)
	}
	closeCB = visitor.Close

	operator := mgr.GetTableOperator(start, end, c, did, tid, scope, visitor)
	if err := operator.Run(); err != nil {
		return api.SyncLogTailResp{}, visitor.Close, err
	}
	resp, err = visitor.BuildResp()

	if canRetry && scope == ScopeUserTables { // check simple conditions first
		_, name, forceFlush := fault.TriggerFault("logtail_max_size")
		if (forceFlush && name == tableEntry.GetLastestSchemaLocked().Name) || resp.ProtoSize() > Size90M {
			_ = ckpClient.FlushTable(ctx, did, tid, end)
			// try again after flushing
			newResp, closeCB, err := HandleSyncLogTailReq(ctx, ckpClient, mgr, c, req, false)
			logutil.Infof("[logtail] flush result: %d -> %d err: %v", resp.ProtoSize(), newResp.ProtoSize(), err)
			return newResp, closeCB, err
		}
	}
	return
}

type RespBuilder interface {
	catalog.Processor
	BuildResp() (api.SyncLogTailResp, error)
	Close()
}

// CatalogLogtailRespBuilder knows how to make api-entry from db and table entry.
// impl catalog.Processor interface, driven by BoundTableOperator
type CatalogLogtailRespBuilder struct {
	ctx context.Context
	*catalog.LoopProcessor
	scope              Scope
	start, end         types.TS
	checkpoint         string
	insBatch           *containers.Batch
	delBatch           *containers.Batch
	specailDeleteBatch *containers.Batch
}

func NewCatalogLogtailRespBuilder(ctx context.Context, scope Scope, ckp string, start, end types.TS) *CatalogLogtailRespBuilder {
	b := &CatalogLogtailRespBuilder{
		ctx:           ctx,
		LoopProcessor: new(catalog.LoopProcessor),
		scope:         scope,
		start:         start,
		end:           end,
		checkpoint:    ckp,
	}
	switch scope {
	case ScopeDatabases:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemDBSchema, common.LogtailAllocator)
		b.delBatch = makeRespBatchFromSchema(DBDelSchema, common.LogtailAllocator)
		b.specailDeleteBatch = makeRespBatchFromSchema(DBSpecialDeleteSchema, common.LogtailAllocator)
	case ScopeTables:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemTableSchema, common.LogtailAllocator)
		b.delBatch = makeRespBatchFromSchema(TblDelSchema, common.LogtailAllocator)
		b.specailDeleteBatch = makeRespBatchFromSchema(TBLSpecialDeleteSchema, common.LogtailAllocator)
	case ScopeColumns:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemColumnSchema, common.LogtailAllocator)
		b.delBatch = makeRespBatchFromSchema(ColumnDelSchema, common.LogtailAllocator)
	}
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

// VisitDB = catalog.Processor.OnDatabase
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
		dbNode := node
		if dbNode.HasDropCommitted() {
			// delScehma is empty, it will just fill rowid / commit ts
			catalogEntry2Batch(b.delBatch, entry, dbNode, DBDelSchema, txnimpl.FillDBRow, objectio.HackU64ToRowid(entry.GetID()), dbNode.GetEnd())
			catalogEntry2Batch(b.specailDeleteBatch, entry, node, DBSpecialDeleteSchema, txnimpl.FillDBRow, objectio.HackU64ToRowid(entry.GetID()), node.GetEnd())
		} else {
			catalogEntry2Batch(b.insBatch, entry, dbNode, catalog.SystemDBSchema, txnimpl.FillDBRow, objectio.HackU64ToRowid(entry.GetID()), dbNode.GetEnd())
		}
	}
	return nil
}

// VisitTbl = catalog.Processor.OnTable
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
		if b.scope == ScopeColumns {
			var dstBatch *containers.Batch
			if !node.HasDropCommitted() {
				dstBatch = b.insBatch
				// fill unique syscol fields if inserting
				for _, syscol := range catalog.SystemColumnSchema.ColDefs {
					txnimpl.FillColumnRow(entry, node, syscol.Name, b.insBatch.GetVectorByName(syscol.Name))
				}
				// send dropped column del
				for _, name := range node.BaseNode.Schema.Extra.DroppedAttrs {
					b.delBatch.GetVectorByName(catalog.AttrRowID).Append(objectio.HackBytes2Rowid([]byte(fmt.Sprintf("%d-%s", entry.GetID(), name))), false)
					b.delBatch.GetVectorByName(catalog.AttrCommitTs).Append(node.GetEnd(), false)
					b.delBatch.GetVectorByName(pkgcatalog.SystemColAttr_UniqName).Append([]byte(fmt.Sprintf("%d-%s", entry.GetID(), name)), false)
				}
			} else {
				dstBatch = b.delBatch
			}

			// fill common syscol fields for every user column
			rowidVec := dstBatch.GetVectorByName(catalog.AttrRowID)
			commitVec := dstBatch.GetVectorByName(catalog.AttrCommitTs)
			tableID := entry.GetID()
			commitTs := node.GetEnd()
			for _, usercol := range node.BaseNode.Schema.ColDefs {
				rowidVec.Append(objectio.HackBytes2Rowid([]byte(fmt.Sprintf("%d-%s", tableID, usercol.Name))), false)
				commitVec.Append(commitTs, false)
			}
		} else {
			if node.HasDropCommitted() {
				catalogEntry2Batch(b.delBatch, entry, node, TblDelSchema, txnimpl.FillTableRow, objectio.HackU64ToRowid(entry.GetID()), node.GetEnd())
				catalogEntry2Batch(b.specailDeleteBatch, entry, node, TBLSpecialDeleteSchema, txnimpl.FillTableRow, objectio.HackU64ToRowid(entry.GetID()), node.GetEnd())
			} else {
				catalogEntry2Batch(b.insBatch, entry, node, catalog.SystemTableSchema, txnimpl.FillTableRow, objectio.HackU64ToRowid(entry.GetID()), node.GetEnd())
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
		logutil.Debugf("[logtail] catalog insert to %d-%s, %s", tblID, tableName,
			DebugBatchToString("catalog", b.insBatch, true, zap.DebugLevel))
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
		perfcounter.Update(b.ctx, func(counter *perfcounter.CounterSet) {
			counter.TAE.LogTail.Entries.Add(int64(b.insBatch.Length()))
			counter.TAE.LogTail.InsertEntries.Add(int64(b.insBatch.Length()))
		})
	}
	if b.delBatch.Length() > 0 {
		bat, err := containersBatchToProtoBatch(b.delBatch)
		logutil.Debugf("[logtail] catalog delete from %d-%s, %s", tblID, tableName,
			DebugBatchToString("catalog", b.delBatch, false, zap.DebugLevel))
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
		perfcounter.Update(b.ctx, func(counter *perfcounter.CounterSet) {
			counter.TAE.LogTail.Entries.Add(int64(b.delBatch.Length()))
			counter.TAE.LogTail.DeleteEntries.Add(int64(b.delBatch.Length()))
		})
	}
	if b.specailDeleteBatch != nil && b.specailDeleteBatch.Length() > 0 {
		bat, err := containersBatchToProtoBatch(b.specailDeleteBatch)
		if err != nil {
			return api.SyncLogTailResp{}, err
		}
		delEntry := &api.Entry{
			EntryType:    api.Entry_SpecialDelete,
			TableId:      tblID,
			TableName:    tableName,
			DatabaseId:   pkgcatalog.MO_CATALOG_ID,
			DatabaseName: pkgcatalog.MO_CATALOG,
			Bat:          bat,
		}
		entries = append(entries, delEntry)
		perfcounter.Update(b.ctx, func(counter *perfcounter.CounterSet) {
			counter.TAE.LogTail.Entries.Add(int64(b.delBatch.Length()))
			counter.TAE.LogTail.DeleteEntries.Add(int64(b.delBatch.Length()))
		})
	}
	return api.SyncLogTailResp{
		CkpLocation: b.checkpoint,
		Commands:    entries,
	}, nil
}

// this is used to collect ONE ROW of db or table change
func catalogEntry2Batch[
	T *catalog.DBEntry | *catalog.TableEntry,
	N *catalog.MVCCNode[*catalog.EmptyMVCCNode] | *catalog.MVCCNode[*catalog.TableMVCCNode]](
	dstBatch *containers.Batch,
	e T,
	node N,
	schema *catalog.Schema,
	fillDataRow func(e T, node N, attr string, col containers.Vector),
	rowid types.Rowid,
	commitTs types.TS,
) {
	for _, col := range schema.ColDefs {
		fillDataRow(e, node, col.Name, dstBatch.GetVectorByName(col.Name))
	}
	dstBatch.GetVectorByName(catalog.AttrRowID).Append(rowid, false)
	dstBatch.GetVectorByName(catalog.AttrCommitTs).Append(commitTs, false)
}

// CatalogLogtailRespBuilder knows how to make api-entry from block entry.
// impl catalog.Processor interface, driven by BoundTableOperator
type TableLogtailRespBuilder struct {
	ctx context.Context
	*catalog.LoopProcessor
	start, end      types.TS
	did, tid        uint64
	dname, tname    string
	checkpoint      string
	blkMetaInsBatch *containers.Batch
	blkMetaDelBatch *containers.Batch
	segMetaDelBatch *containers.Batch
	objectMetaBatch *containers.Batch
	dataInsBatches  map[uint32]*containers.Batch // schema version -> data batch
	dataDelBatch    *containers.Batch
}

func NewTableLogtailRespBuilder(ctx context.Context, ckp string, start, end types.TS, tbl *catalog.TableEntry) *TableLogtailRespBuilder {
	b := &TableLogtailRespBuilder{
		ctx:           ctx,
		LoopProcessor: new(catalog.LoopProcessor),
		start:         start,
		end:           end,
		checkpoint:    ckp,
	}
	b.BlockFn = b.VisitBlk
	b.ObjectFn = b.VisitObj

	b.did = tbl.GetDB().GetID()
	b.tid = tbl.ID
	b.dname = tbl.GetDB().GetName()
	b.tname = tbl.GetLastestSchemaLocked().Name

	b.dataInsBatches = make(map[uint32]*containers.Batch)
	b.dataDelBatch = makeRespBatchFromSchema(DelSchema, common.LogtailAllocator)
	b.blkMetaInsBatch = makeRespBatchFromSchema(BlkMetaSchema, common.LogtailAllocator)
	b.blkMetaDelBatch = makeRespBatchFromSchema(DelSchema, common.LogtailAllocator)
	b.segMetaDelBatch = makeRespBatchFromSchema(DelSchema, common.LogtailAllocator)
	b.objectMetaBatch = makeRespBatchFromSchema(ObjectInfoSchema, common.LogtailAllocator)
	return b
}

func (b *TableLogtailRespBuilder) Close() {
	for _, vec := range b.dataInsBatches {
		if vec != nil {
			vec.Close()
		}
	}
	b.dataInsBatches = nil
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

func (b *TableLogtailRespBuilder) VisitObj(e *catalog.ObjectEntry) error {
	e.RLock()
	mvccNodes := e.ClonePreparedInRange(b.start, b.end)
	e.RUnlock()

	for _, node := range mvccNodes {
		if node.HasDropCommitted() {
			// send Object deletation event
			b.segMetaDelBatch.GetVectorByName(catalog.AttrCommitTs).Append(node.DeletedAt, false)
			b.segMetaDelBatch.GetVectorByName(catalog.AttrRowID).Append(objectio.HackObjid2Rowid(&e.ID), false)
		}
		if e.IsAppendable() && node.BaseNode.IsEmpty() {
			continue
		}
		visitObject(b.objectMetaBatch, e, node, false, types.TS{})
	}
	return nil
}

func visitObject(batch *containers.Batch, entry *catalog.ObjectEntry, node *catalog.MVCCNode[*catalog.ObjectMVCCNode], push bool, committs types.TS) {
	batch.GetVectorByName(catalog.AttrRowID).Append(objectio.HackObjid2Rowid(&entry.ID), false)
	if push {
		batch.GetVectorByName(catalog.AttrCommitTs).Append(committs, false)
	} else {
		batch.GetVectorByName(catalog.AttrCommitTs).Append(node.TxnMVCCNode.End, false)
	}
	node.BaseNode.AppendTuple(&entry.ID, batch)
	if push {
		node.TxnMVCCNode.AppendTupleWithCommitTS(batch, committs)
	} else {
		node.TxnMVCCNode.AppendTuple(batch)
	}
	if push {
		node.EntryMVCCNode.AppendTupleWithCommitTS(batch, committs)
	} else {
		node.EntryMVCCNode.AppendTuple(batch)
	}
	batch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().ID, false)
	batch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().ID, false)
	batch.GetVectorByName(ObjectAttr_State).Append(entry.IsAppendable(), false)
	sorted := false
	if entry.GetTable().GetLastestSchemaLocked().HasSortKey() && !entry.IsAppendable() {
		sorted = true
	}
	batch.GetVectorByName(ObjectAttr_Sorted).Append(sorted, false)
}

// visitBlkMeta try to collect block metadata. It might prefetch and generate duplicated entry.
// see also https://github.com/matrixorigin/docs/blob/main/tech-notes/dnservice/ref_logtail_protocol.md#table-metadata-prefetch
func (b *TableLogtailRespBuilder) visitBlkMeta(e *catalog.BlockEntry) (types.TS, bool) {
	newEnd := b.end
	e.RLock()
	// try to find new end
	if newest := e.GetLatestCommittedNode(); newest != nil {
		latestPrepareTs := newest.GetPrepare()
		if latestPrepareTs.Greater(&b.end) {
			newEnd = latestPrepareTs
		}
	}
	mvccNodes := e.ClonePreparedInRange(b.start, newEnd)
	e.RUnlock()

	for _, node := range mvccNodes {
		metaNode := node
		if !metaNode.BaseNode.MetaLoc.IsEmpty() && !metaNode.BaseNode.DeltaLoc.IsEmpty() && !metaNode.IsAborted() {
			b.appendBlkMeta(e, metaNode)
		}
	}

	if n := len(mvccNodes); n > 0 {
		newest := mvccNodes[n-1]
		if e.IsAppendable() {
			if !newest.BaseNode.MetaLoc.IsEmpty() {
				// appendable block has been flushed, no need to collect data
				return types.MaxTs(), true
			}
		} else {
			if !newest.BaseNode.DeltaLoc.IsEmpty() {
				startTS := newest.GetStart()
				if startTS.GreaterEq(&b.end) {
					// non-appendable block has newer delta data on s3, no need to collect data
					return types.MaxTs(), true
				} else {
					return newest.GetStart(), false
				}
			}
		}
	}
	return types.MaxTs(), false
}

// appendBlkMeta add block metadata into api entry according to logtail protocol
// see also https://github.com/matrixorigin/docs/blob/main/tech-notes/dnservice/ref_logtail_protocol.md#table-metadata
func (b *TableLogtailRespBuilder) appendBlkMeta(e *catalog.BlockEntry, metaNode *catalog.MVCCNode[*catalog.MetadataMVCCNode]) {
	visitBlkMeta(e, metaNode, b.blkMetaInsBatch, b.blkMetaDelBatch, metaNode.HasDropCommitted(), metaNode.End, metaNode.CreatedAt, metaNode.DeletedAt)
}

func visitBlkMeta(e *catalog.BlockEntry, node *catalog.MVCCNode[*catalog.MetadataMVCCNode], insBatch, delBatch *containers.Batch, delete bool, committs, createts, deletets types.TS) {
	common.DoIfDebugEnabled(func() {
		logutil.Debugf("[Logtail] record block meta row %s, %v, %s, %s, %s, %s",
			e.AsCommonID().String(), e.IsAppendable(),
			createts.ToString(), node.DeletedAt.ToString(), node.BaseNode.MetaLoc, node.BaseNode.DeltaLoc)
	})
	is_sorted := false
	if !e.IsAppendable() && e.GetSchema().HasSortKey() {
		is_sorted = true
	}
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(e.ID, false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(e.IsAppendable(), false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted, false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(node.BaseNode.MetaLoc), false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(node.BaseNode.DeltaLoc), false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(committs, false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(*e.GetObject().ID.Segment(), false)
	// for appendable block(deleted, because we skip empty metaloc), non-dropped non-appendabled blocks, those new nodes are
	// produced by flush table tail, it's safe to truncate mem data in CN
	memTruncTs := node.Start
	if !e.IsAppendable() && committs.Equal(&deletets) {
		// for deleted non-appendable block, it must be produced by merging blocks. In this case,
		// do not truncate any data in CN, because merging blocks didn't flush deletes to disk.
		memTruncTs = types.TS{}
	}

	insBatch.GetVectorByName(pkgcatalog.BlockMeta_MemTruncPoint).Append(memTruncTs, false)
	insBatch.GetVectorByName(catalog.AttrCommitTs).Append(createts, false)
	insBatch.GetVectorByName(catalog.AttrRowID).Append(objectio.HackBlockid2Rowid(&e.ID), false)

	// if block is deleted, send both Insert and Delete api entry
	// see also https://github.com/matrixorigin/docs/blob/main/tech-notes/dnservice/ref_logtail_protocol.md#table-metadata-deletion-invalidate-table-data
	if delete {
		if node.DeletedAt.IsEmpty() {
			panic(moerr.NewInternalErrorNoCtx("no delete at time in a dropped entry"))
		}
		delBatch.GetVectorByName(catalog.AttrCommitTs).Append(deletets, false)
		delBatch.GetVectorByName(catalog.AttrRowID).Append(objectio.HackBlockid2Rowid(&e.ID), false)
	}

}

// visitBlkData collects logtail in memory
func (b *TableLogtailRespBuilder) visitBlkData(ctx context.Context, ts types.TS, e *catalog.BlockEntry) (err error) {
	block := e.GetBlockData()
	insBatch, err := block.CollectAppendInRange(b.start, b.end, false, common.LogtailAllocator)
	if err != nil {
		return
	}
	if insBatch != nil && insBatch.Length() > 0 {
		dest, ok := b.dataInsBatches[insBatch.Version]
		if !ok {
			// create new dest batch
			dest = DataChangeToLogtailBatch(insBatch)
			b.dataInsBatches[insBatch.Version] = dest
		} else {
			dest.Extend(insBatch.Batch)
			// insBatch is freed, don't use anymore
		}
	}
	var delBatch *containers.Batch
	maxTS := types.MaxTs()
	if !ts.Equal(&maxTS) {
		delBatch, err = block.CollectDeleteInRangeAfterDeltalocation(ctx, ts, b.end, false, common.LogtailAllocator)
	} else {
		delBatch, err = block.CollectDeleteInRange(ctx, b.start, b.end, false, common.LogtailAllocator)
	}
	if err != nil {
		return
	}
	if delBatch != nil && delBatch.Length() > 0 {
		if len(b.dataDelBatch.Vecs) == 2 {
			b.dataDelBatch.AddVector(catalog.AttrPKVal,
				containers.MakeVector(*delBatch.GetVectorByName(catalog.AttrPKVal).GetType(), common.LogtailAllocator))
		}
		b.dataDelBatch.Extend(delBatch)
		// delBatch is freed, don't use anymore
	}
	return nil
}

// VisitBlk = catalog.Processor.OnBlock
func (b *TableLogtailRespBuilder) VisitBlk(entry *catalog.BlockEntry) error {
	ts, skip := b.visitBlkMeta(entry)
	if skip {
		// data has been flushed, no need to collect data
		return nil
	}
	return b.visitBlkData(b.ctx, ts, entry)
}

type TableRespKind int

const (
	TableRespKind_Data TableRespKind = iota
	TableRespKind_Blk
	TableRespKind_Obj
)

func (b *TableLogtailRespBuilder) BuildResp() (api.SyncLogTailResp, error) {
	entries := make([]*api.Entry, 0)
	tryAppendEntry := func(typ api.Entry_EntryType, kind TableRespKind, batch *containers.Batch, version uint32) error {
		if batch.Length() == 0 {
			return nil
		}
		bat, err := containersBatchToProtoBatch(batch)
		if err != nil {
			return err
		}

		tableName := ""
		switch kind {
		case TableRespKind_Data:
			tableName = b.tname
			logutil.Debugf("[logtail] table data [%v] %d-%s-%d: %s", typ, b.tid, b.tname, version,
				DebugBatchToString("data", batch, false, zap.InfoLevel))
		case TableRespKind_Blk:
			tableName = fmt.Sprintf("_%d_meta", b.tid)
			logutil.Debugf("[logtail] table meta [%v] %d-%s: %s", typ, b.tid, b.tname,
				DebugBatchToString("blkmeta", batch, false, zap.InfoLevel))
		case TableRespKind_Obj:
			tableName = fmt.Sprintf("_%d_obj", b.tid)
			logutil.Debugf("[logtail] table meta [%v] %d-%s: %s", typ, b.tid, b.tname,
				DebugBatchToString("object", batch, false, zap.InfoLevel))
		}

		entry := &api.Entry{
			EntryType:    typ,
			TableId:      b.tid,
			TableName:    tableName,
			DatabaseId:   b.did,
			DatabaseName: b.dname,
			Bat:          bat,
		}
		entries = append(entries, entry)
		return nil
	}

	empty := api.SyncLogTailResp{}
	if err := tryAppendEntry(api.Entry_Insert, TableRespKind_Blk, b.blkMetaInsBatch, 0); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_Delete, TableRespKind_Blk, b.blkMetaDelBatch, 0); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_Insert, TableRespKind_Obj, b.objectMetaBatch, 0); err != nil {
		return empty, err
	}
	keys := make([]uint32, 0, len(b.dataInsBatches))
	for k := range b.dataInsBatches {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		if err := tryAppendEntry(api.Entry_Insert, TableRespKind_Data, b.dataInsBatches[k], k); err != nil {
			return empty, err
		}
	}
	if err := tryAppendEntry(api.Entry_Delete, TableRespKind_Data, b.dataDelBatch, 0); err != nil {
		return empty, err
	}

	return api.SyncLogTailResp{
		CkpLocation: b.checkpoint,
		Commands:    entries,
	}, nil
}
func GetMetaIdxesByVersion(ver uint32) []uint16 {
	meteIdxSchema := checkpointDataReferVersions[ver][MetaIDX]
	idxes := make([]uint16, len(meteIdxSchema.attrs))
	for attr := range meteIdxSchema.attrs {
		idxes[attr] = uint16(attr)
	}
	return idxes
}
func LoadCheckpointEntries(
	ctx context.Context,
	metLoc string,
	tableID uint64,
	tableName string,
	dbID uint64,
	dbName string,
	mp *mpool.MPool,
	fs fileservice.FileService) ([]*api.Entry, []func(), error) {
	if metLoc == "" {
		return nil, nil, nil
	}
	v2.LogtailLoadCheckpointCounter.Inc()
	now := time.Now()
	defer func() {
		v2.LogTailLoadCheckpointDurationHistogram.Observe(time.Since(now).Seconds())
	}()
	locationsAndVersions := strings.Split(metLoc, ";")
	datas := make([]*CNCheckpointData, len(locationsAndVersions)/2)

	readers := make([]*blockio.BlockReader, len(locationsAndVersions)/2)
	objectLocations := make([]objectio.Location, len(locationsAndVersions)/2)
	versions := make([]uint32, len(locationsAndVersions)/2)
	locations := make([]objectio.Location, len(locationsAndVersions)/2)
	for i := 0; i < len(locationsAndVersions); i += 2 {
		key := locationsAndVersions[i]
		version, err := strconv.ParseUint(locationsAndVersions[i+1], 10, 32)
		if err != nil {
			return nil, nil, err
		}
		location, err := blockio.EncodeLocationFromString(key)
		if err != nil {
			return nil, nil, err
		}
		locations[i/2] = location
		reader, err := blockio.NewObjectReader(fs, location)
		if err != nil {
			return nil, nil, err
		}
		readers[i/2] = reader
		err = blockio.PrefetchMeta(fs, location)
		if err != nil {
			return nil, nil, err
		}
		objectLocations[i/2] = location
		versions[i/2] = uint32(version)
	}

	for i := range objectLocations {
		data := NewCNCheckpointData()
		meteIdxSchema := checkpointDataReferVersions[versions[i]][MetaIDX]
		idxes := make([]uint16, len(meteIdxSchema.attrs))
		for attr := range meteIdxSchema.attrs {
			idxes[attr] = uint16(attr)
		}
		err := data.PrefetchMetaIdx(ctx, versions[i], idxes, objectLocations[i], fs)
		if err != nil {
			return nil, nil, err
		}
		datas[i] = data
	}

	for i := range datas {
		err := datas[i].InitMetaIdx(ctx, versions[i], readers[i], locations[i], mp)
		if err != nil {
			return nil, nil, err
		}
	}

	for i := range datas {
		err := datas[i].PrefetchMetaFrom(ctx, versions[i], locations[i], fs, tableID)
		if err != nil {
			return nil, nil, err
		}
	}

	for i := range datas {
		err := datas[i].PrefetchFrom(ctx, versions[i], fs, locations[i], tableID)
		if err != nil {
			return nil, nil, err
		}
	}

	closeCBs := make([]func(), 0)
	bats := make([][]*batch.Batch, len(locationsAndVersions)/2)
	var err error
	for i, data := range datas {
		var bat []*batch.Batch
		bat, err = data.ReadFromData(ctx, tableID, locations[i], readers[i], versions[i], mp)
		closeCBs = append(closeCBs, data.GetCloseCB(versions[i], mp))
		if err != nil {
			for j := range closeCBs {
				if closeCBs[j] != nil {
					closeCBs[j]()
				}
			}
			return nil, nil, err
		}
		bats[i] = bat
	}

	entries := make([]*api.Entry, 0)
	for i := range objectLocations {
		data := datas[i]
		ins, del, cnIns, objInfo, err := data.GetTableDataFromBats(tableID, bats[i])
		if err != nil {
			for j := range closeCBs {
				if closeCBs[j] != nil {
					closeCBs[j]()
				}
			}
			return nil, nil, err
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
		if objInfo != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Insert,
				TableId:      tableID,
				TableName:    fmt.Sprintf("_%d_obj", tableID),
				DatabaseId:   dbID,
				DatabaseName: dbName,
				Bat:          objInfo,
			}
			entries = append(entries, entry)
		}
	}
	return entries, closeCBs, nil
}
