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

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
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
	FlushTable(ctx context.Context, dbID, tableID uint64, ts types.TS) error
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
			logutil.Warn(
				"LOGTAIL-SLOW-PULL",
				zap.Duration("duration", elapsed),
				zap.Any("request", req),
				zap.Bool("can-retry", canRetry),
				zap.Error(err),
			)
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

	visitor := NewTableLogtailRespBuilder(ctx, ckpLoc, start, end, tableEntry)
	closeCB = visitor.Close

	operator := mgr.GetTableOperator(start, end, c, did, tid, visitor)
	if err := operator.Run(); err != nil {
		return api.SyncLogTailResp{}, visitor.Close, err
	}
	resp, err = visitor.BuildResp()

	if canRetry { // check simple conditions first
		_, name, forceFlush := fault.TriggerFault("logtail_max_size")
		if (forceFlush && name == tableEntry.GetLastestSchemaLocked(false).Name) || resp.ProtoSize() > Size90M {
			flushErr := ckpClient.FlushTable(ctx, did, tid, end)
			// try again after flushing
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

type RespBuilder interface {
	catalog.Processor
	BuildResp() (api.SyncLogTailResp, error)
	Close()
}

// CatalogLogtailRespBuilder knows how to make api-entry from block entry.
// impl catalog.Processor interface, driven by BoundTableOperator
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
	skip, err := b.visitObjMeta(e)
	if err != nil {
		return err
	}
	if skip {
		return nil
	} else {
		return b.visitObjData(e)
	}
}
func (b *TableLogtailRespBuilder) visitObjMeta(e *catalog.ObjectEntry) (bool, error) {
	mvccNodes := e.GetMVCCNodeInRange(b.start, b.end)
	if len(mvccNodes) == 0 {
		return false, nil
	}

	var objectMVCCNode *catalog.ObjectMVCCNode
	for _, node := range mvccNodes {
		if e.IsTombstone {
			visitObject(b.tombstoneMetaBatch, e, node, node.End.Equal(&e.CreatedAt), false, types.TS{})
		} else {
			visitObject(b.dataMetaBatch, e, node, node.End.Equal(&e.CreatedAt), false, types.TS{})
		}
	}
	return b.skipObjectData(e, objectMVCCNode), nil
}
func (b *TableLogtailRespBuilder) skipObjectData(e *catalog.ObjectEntry, objectMVCCNode *catalog.ObjectMVCCNode) bool {
	if e.IsAppendable() {
		// appendable block has been flushed, no need to collect data
		return objectMVCCNode != nil
	} else {
		return true
	}
}
func (b *TableLogtailRespBuilder) visitObjData(e *catalog.ObjectEntry) error {
	var err error
	if e.IsTombstone {
		err = tables.RangeScanInMemoryByObject(b.ctx, e, b.dataDelBatches, b.start, b.end, common.LogtailAllocator)
	} else {
		err = tables.RangeScanInMemoryByObject(b.ctx, e, b.dataInsBatches, b.start, b.end, common.LogtailAllocator)
	}
	if err != nil {
		return err
	}
	return nil
}
func visitObject(batch *containers.Batch, entry *catalog.ObjectEntry, txnMVCCNode *txnbase.TxnMVCCNode, create bool, push bool, committs types.TS) {
	batch.GetVectorByName(catalog.PhyAddrColumnName).Append(objectio.HackObjid2Rowid(entry.ID()), false)
	if push {
		batch.GetVectorByName(objectio.DefaultCommitTS_Attr).Append(committs, false)
	} else {
		batch.GetVectorByName(objectio.DefaultCommitTS_Attr).Append(txnMVCCNode.End, false)
	}
	entry.ObjectMVCCNode.AppendTuple(entry.ID(), batch)
	if push {
		txnMVCCNode.AppendTupleWithCommitTS(batch, committs)
	} else {
		txnMVCCNode.AppendTuple(batch)
	}
	if push {
		entry.EntryMVCCNode.AppendTupleWithCommitTS(batch, committs)
	} else {
		entry.EntryMVCCNode.AppendObjectTuple(batch, create)
	}
	batch.GetVectorByName(SnapshotAttr_DBID).Append(entry.GetTable().GetDB().ID, false)
	batch.GetVectorByName(SnapshotAttr_TID).Append(entry.GetTable().ID, false)
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

		tableName := ""
		switch kind {
		case TableRespKind_Data:
			tableName = b.tname
			logutil.Debugf("[logtail] table data [%v] %d-%s-%d: %s", typ, b.tid, b.tname, version,
				DebugBatchToString("data", batch, false, zap.InfoLevel))
		case TableRespKind_DataMeta:
			tableName = fmt.Sprintf("_%d_data_meta", b.tid)
			logutil.Debugf("[logtail] table data meta [%v] %d-%s: %s", typ, b.tid, b.tname,
				DebugBatchToString("object", batch, false, zap.InfoLevel))
		case TableRespKind_TombstoneMeta:
			tableName = fmt.Sprintf("_%d_tombstone_meta", b.tid)
			logutil.Debugf("[logtail] table tombstone meta [%v] %d-%s: %s", typ, b.tid, b.tname,
				DebugBatchToString("object", batch, false, zap.InfoLevel))
		}

		// if b.tid == pkgcatalog.MO_DATABASE_ID || b.tid == pkgcatalog.MO_TABLES_ID || b.tid == pkgcatalog.MO_COLUMNS_ID {
		// 	switch kind {
		// 	case TableRespKind_Data:
		// 		logutil.Infof("[yyyy pull] table data [%v] %d-%s-%d: %s", typ, b.tid, b.tname, version,
		// 			DebugBatchToString("data", batch, false, zap.InfoLevel))
		// 	case TableRespKind_Blk:
		// 		logutil.Infof("[yyyy pull] blk meta [%v] %d-%s: %s", typ, b.tid, tableName,
		// 			// batch.PPString(30)) // DebugBatchToString("blkmeta", batch, false, zap.InfoLevel))
		// 			DebugBatchToString("blkmeta", batch, false, zap.InfoLevel))
		// 	case TableRespKind_Obj:
		// 		logutil.Infof("[yyyy pull] obj meta [%v] %d-%s: %s", typ, b.tid, tableName,
		// 			// batch.PPString(30)) // DebugBatchToString("object", batch, false, zap.InfoLevel))
		// 			DebugBatchToString("object", batch, false, zap.InfoLevel))
		// 	}
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
	if err := tryAppendEntry(api.Entry_Insert, TableRespKind_DataMeta, b.dataMetaBatch, 0); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_Insert, TableRespKind_TombstoneMeta, b.tombstoneMetaBatch, 0); err != nil {
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

	// if b.tid == pkgcatalog.MO_DATABASE_ID || b.tid == pkgcatalog.MO_TABLES_ID || b.tid == pkgcatalog.MO_COLUMNS_ID {
	// 	logutil.Infof("[yyyy pull] table %s: %d, ckp:%v", b.tname, len(entries), b.checkpoint)
	// }

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
	sid string,
	metLoc string,
	tableID uint64,
	_ string,
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
		reader, err := blockio.NewObjectReader(sid, fs, location)
		if err != nil {
			return nil, nil, err
		}
		readers[i/2] = reader
		err = blockio.PrefetchMeta(sid, fs, location)
		if err != nil {
			return nil, nil, err
		}
		objectLocations[i/2] = location
		versions[i/2] = uint32(version)
	}

	shouldSkip := func(i int) bool {
		versionTry := CheckpointCurrentVersion >= CheckpointVersion12 && versions[i] < CheckpointVersion12
		sysTable := tableID == pkgcatalog.MO_DATABASE_ID || tableID == pkgcatalog.MO_TABLES_ID || tableID == pkgcatalog.MO_COLUMNS_ID
		return versionTry && sysTable
	}

	for i := range objectLocations {
		data := NewCNCheckpointData(sid)
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
		if shouldSkip(i) {
			continue
		}
		err := datas[i].InitMetaIdx(ctx, versions[i], readers[i], locations[i], mp)
		if err != nil {
			return nil, nil, err
		}
	}

	for i := range datas {
		if shouldSkip(i) {
			continue
		}
		err := datas[i].PrefetchMetaFrom(ctx, versions[i], locations[i], fs, tableID)
		if err != nil {
			return nil, nil, err
		}
	}

	for i := range datas {
		if shouldSkip(i) {
			continue
		}
		err := datas[i].PrefetchFrom(ctx, versions[i], fs, locations[i], tableID)
		if err != nil {
			return nil, nil, err
		}
	}

	closeCBs := make([]func(), 0)
	dataBats := make([][]*batch.Batch, len(locationsAndVersions)/2)
	var err error
	for i, data := range datas {
		if shouldSkip(i) {
			continue
		}
		var bats []*batch.Batch
		bats, err = data.ReadFromData(ctx, tableID, locations[i], readers[i], versions[i], mp)
		cb := data.GetCloseCB(versions[i], mp)
		closeCBs = append(closeCBs, func() {
			for _, bat := range bats {
				if bat != nil {
					bat.Clean(mp)
				}
			}
			cb()
		})
		if err != nil {
			for j := range closeCBs {
				if closeCBs[j] != nil {
					closeCBs[j]()
				}
			}
			return nil, nil, err
		}
		dataBats[i] = bats
	}

	entries := make([]*api.Entry, 0)
	for i := range objectLocations {
		if shouldSkip(i) {
			continue
		}
		data := datas[i]
		ins, del, dataObj, tombstoneObj, err := data.GetTableDataFromBats(tableID, dataBats[i])
		if err != nil {
			for j := range closeCBs {
				if closeCBs[j] != nil {
					closeCBs[j]()
				}
			}
			return nil, nil, err
		}
		tableName := fmt.Sprintf("_%d_meta", tableID)
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
		if dataObj != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Insert,
				TableId:      tableID,
				TableName:    fmt.Sprintf("_%d_data_meta", tableID),
				DatabaseId:   dbID,
				DatabaseName: dbName,
				Bat:          dataObj,
			}
			entries = append(entries, entry)
		}
		if tombstoneObj != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Insert,
				TableId:      tableID,
				TableName:    fmt.Sprintf("_%d_tombstone_meta", tableID),
				DatabaseId:   dbID,
				DatabaseName: dbName,
				Bat:          tombstoneObj,
			}
			entries = append(entries, entry)
		}
	}

	// if tableID <= 3 {
	// 	logutil.Infof("[yyyy ckp] load checkpoint entries %d: %d", tableID, len(entries))
	// }
	return entries, closeCBs, nil
}
