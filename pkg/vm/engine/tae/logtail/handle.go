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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"go.uber.org/zap"
)

const Size90M = 90 * 1024 * 1024

type CheckpointClient interface {
	CollectCheckpointsInRange(ctx context.Context, start, end types.TS) (ckpLoc string, lastEnd types.TS, err error)
	FlushTable(ctx context.Context, accoutID uint32, dbID, tableID uint64, ts types.TS) error
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

	var visitor *TableLogtailRespBuilder
	var operator *BoundTableOperator
	defer func() {
		if elapsed := time.Since(now); elapsed > time.Second {
			logutil.Warn(
				"LOGTAIL-SLOW-PULL",
				zap.Duration("duration", elapsed),
				zap.Any("request", req),
				zap.Bool("can-retry", canRetry),
				zap.Error(err),
				zap.String("scanReport", operator.Report()),
				zap.String("respSize", common.HumanReadableBytes(resp.ProtoSize())),
				zap.Int("entries", len(resp.Commands)),
				zap.String("ckp", resp.CkpLocation),
			)
		}
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
	// fill table info, as req.Table will be used as the Table field in TableLogtail response.
	schema := tableEntry.GetLastestSchemaLocked(false)
	req.Table.AccId = schema.AcInfo.TenantID
	req.Table.DbName = dbEntry.GetName()
	req.Table.TbName = schema.Name
	req.Table.PrimarySeqnum = uint32(schema.GetPrimaryKey().SeqNum)

	ckpLoc, checkpointed, err := ckpClient.CollectCheckpointsInRange(ctx, start, end)
	if err != nil {
		return
	}

	if checkpointed.GE(&end) {
		return api.SyncLogTailResp{
			CkpLocation: ckpLoc,
		}, nil, err
	} else if ckpLoc != "" {
		start = checkpointed.Next()
	}

	visitor = NewTableLogtailRespBuilder(ctx, ckpLoc, start, end, tableEntry)
	closeCB = visitor.Close

	operator = mgr.GetTableOperator(start, end, tableEntry, visitor)
	if err := operator.Run(); err != nil {
		return api.SyncLogTailResp{}, visitor.Close, err
	}
	resp, err = visitor.BuildResp()

	if canRetry { // check simple conditions first
		_, name, forceFlush := fault.TriggerFault("logtail_max_size")
		if (forceFlush && name == tableEntry.GetLastestSchemaLocked(false).Name) || resp.ProtoSize() > Size90M {
			flushErr := ckpClient.FlushTable(ctx, 0, did, tid, end)
			// try again after flushing
			closeCB()
			newResp, closeCB, err := HandleSyncLogTailReq(ctx, ckpClient, mgr, c, req, false)
			logutil.Info(
				"LOGTAIL-WITH-FLUSH",
				zap.Any("flush-err", flushErr),
				zap.Error(err),
				zap.Int("from-size", resp.ProtoSize()),
				zap.Int("to-size", newResp.ProtoSize()),
			)
			return newResp, closeCB, err
		}
	}
	return
}

type TableLogtailRespBuilder struct {
	ctx context.Context
	*catalog.LoopProcessor
	start, end         types.TS
	did, tid           uint64
	dname, tname       string
	checkpoint         string
	dataMetaBatch      *containers.Batch
	tombstoneMetaBatch *containers.Batch
	dataInsBatches     map[uint32]*containers.BatchWithVersion // schema version -> data batch
	dataDelBatches     map[uint32]*containers.BatchWithVersion
}

func NewTableLogtailRespBuilder(ctx context.Context, ckp string, start, end types.TS, tbl *catalog.TableEntry) *TableLogtailRespBuilder {
	b := &TableLogtailRespBuilder{
		ctx:           ctx,
		LoopProcessor: new(catalog.LoopProcessor),
		start:         start,
		end:           end,
		checkpoint:    ckp,
	}
	b.ObjectFn = b.VisitObj
	b.TombstoneFn = b.VisitObj

	b.did = tbl.GetDB().GetID()
	b.tid = tbl.ID
	b.dname = tbl.GetDB().GetName()
	b.tname = tbl.GetLastestSchemaLocked(false).Name

	b.dataInsBatches = make(map[uint32]*containers.BatchWithVersion)
	b.dataDelBatches = make(map[uint32]*containers.BatchWithVersion)
	b.dataMetaBatch = makeRespBatchFromSchema(ObjectInfoSchema, common.LogtailAllocator)
	b.tombstoneMetaBatch = makeRespBatchFromSchema(ObjectInfoSchema, common.LogtailAllocator)
	return b
}

func (b *TableLogtailRespBuilder) Close() {
	for _, vec := range b.dataInsBatches {
		if vec != nil {
			vec.Close()
		}
	}
	b.dataInsBatches = nil
	for _, vec := range b.dataDelBatches {
		if vec != nil {
			vec.Close()
		}
	}
	b.dataDelBatches = nil
	if b.dataMetaBatch != nil {
		b.dataMetaBatch.Close()
		b.dataMetaBatch = nil
	}
	if b.tombstoneMetaBatch != nil {
		b.tombstoneMetaBatch.Close()
		b.tombstoneMetaBatch = nil
	}
}

func (b *TableLogtailRespBuilder) VisitObj(e *catalog.ObjectEntry) error {
	if skip, err := b.visitObjMeta(e); err != nil {
		return err
	} else if skip {
		return nil
	}
	return b.visitObjData(e)
}

func (b *TableLogtailRespBuilder) visitObjMeta(e *catalog.ObjectEntry) (bool, error) {
	var destBatch *containers.Batch
	if e.IsTombstone {
		destBatch = b.tombstoneMetaBatch
	} else {
		destBatch = b.dataMetaBatch
	}
	e.ForeachMVCCNodeInRange(b.start, b.end, func(node *txnbase.TxnMVCCNode) error {
		visitObject(destBatch, e, node, node.End.Equal(&e.CreatedAt), false, types.TS{})
		return nil
	})

	if e.IsAppendable() && !e.HasDropCommitted() {
		return false, nil
	}
	return true, nil
}

func (b *TableLogtailRespBuilder) visitObjData(e *catalog.ObjectEntry) error {
	var destBatches map[uint32]*containers.BatchWithVersion
	if e.IsTombstone {
		destBatches = b.dataDelBatches
	} else {
		destBatches = b.dataInsBatches
	}
	return tables.RangeScanInMemoryByObject(b.ctx, e, destBatches, b.start, b.end, common.LogtailAllocator)
}

func visitObject(batch *containers.Batch, entry *catalog.ObjectEntry, txnMVCCNode *txnbase.TxnMVCCNode, create bool, push bool, committs types.TS) {
	var rowid types.Rowid
	// refer to ObjectInfoAttr for batch schema
	if !push {
		committs = txnMVCCNode.End
		entry.EntryMVCCNode.AppendObjectTuple(batch, create) // createAt and deleteAt for pull
	} else {
		entry.EntryMVCCNode.AppendTupleWithCommitTS(batch, committs) // createAt and deleteAt for push
	}
	// two padding columns
	batch.GetVectorByName(catalog.PhyAddrColumnName).Append(rowid, false)
	batch.GetVectorByName(objectio.DefaultCommitTS_Attr).Append(committs, false)

	batch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().ID, false)
	batch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().ID, false)
	batch.GetVectorByName(ObjectAttr_ObjectStats).Append(entry.ObjectMVCCNode.ObjectStats[:], false)
	txnMVCCNode.AppendTupleWithCommitTS(batch, committs) // start prepare and commit ts
}

type TableRespKind int

const (
	TableRespKind_Data TableRespKind = iota
	TableRespKind_DataMeta
	TableRespKind_TombstoneMeta
)

func (b *TableLogtailRespBuilder) BuildResp() (api.SyncLogTailResp, error) {
	entries := make([]*api.Entry, 0)
	tryAppendEntry := func(typ api.Entry_EntryType, kind TableRespKind, batch *containers.Batch, version uint32) error {
		if batch == nil || batch.Length() == 0 {
			return nil
		}
		bat, err := containersBatchToProtoBatch(batch)
		if err != nil {
			return err
		}

		tableName := b.tname
		// switch kind {
		// case TableRespKind_Data:
		// 	logutil.Infof("[logtail] table data [%v] %d-%s-%d: %s", typ, b.tid, b.tname, version,
		// 		DebugBatchToString("data", batch, false, zap.InfoLevel))
		// case TableRespKind_DataMeta:
		// 	logutil.Infof("[logtail] table data meta [%v] %d-%s: %s", typ, b.tid, b.tname,
		// 		DebugBatchToString("object", batch, false, zap.InfoLevel))
		// case TableRespKind_TombstoneMeta:
		// 	logutil.Infof("[logtail] table tombstone meta [%v] %d-%s: %s", typ, b.tid, b.tname,
		// 		DebugBatchToString("object", batch, false, zap.InfoLevel))
		// }

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
	if err := tryAppendEntry(api.Entry_DataObject, TableRespKind_DataMeta, b.dataMetaBatch, 0); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_TombstoneObject, TableRespKind_TombstoneMeta, b.tombstoneMetaBatch, 0); err != nil {
		return empty, err
	}
	keys := make([]uint32, 0, len(b.dataInsBatches))
	for k := range b.dataInsBatches {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		if err := tryAppendEntry(api.Entry_Insert, TableRespKind_Data, DataChangeToLogtailBatch(b.dataInsBatches[k]), k); err != nil {
			return empty, err
		}
	}
	if len(b.dataDelBatches) > 1 {
		panic(fmt.Sprintf("logic err, batch %v", b.dataDelBatches))
	}
	for _, bat := range b.dataDelBatches {
		if err := tryAppendEntry(api.Entry_Delete, TableRespKind_Data, TombstoneChangeToLogtailBatch(bat), 0); err != nil {
			return empty, err
		}
	}

	return api.SyncLogTailResp{
		CkpLocation: b.checkpoint,
		Commands:    entries,
	}, nil
}
