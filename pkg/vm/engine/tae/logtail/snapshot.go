// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"go.uber.org/zap"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	SnapshotTypeIdx types.Enum = iota
	SnapshotTypeCluster
	SnapshotTypeAccount
)

// mo_snapshot's schema
const (
	ColSnapshotId uint16 = iota
	ColSName
	ColTS
	ColLevel
	ColAccountName
	ColDatabaseName
	ColTableName
	ColObjId
)

var (
	objectInfoSchemaAttr = []string{
		catalog.ObjectAttr_ObjectStats,
		catalog.EntryNode_CreateAt,
		catalog.EntryNode_DeleteAt,
		catalog2.BlockMeta_DeltaLoc,
		SnapshotAttr_TID,
	}
	objectInfoSchemaTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, 5000, 0),
		types.New(types.T_uint64, 0, 0),
	}

	objectDeltaSchemaAttr = []string{
		catalog2.BlockMeta_ID,
		catalog2.BlockMeta_DeltaLoc,
		SnapshotAttr_TID,
	}

	objectDeltaSchemaTypes = []types.Type{
		types.New(types.T_Blockid, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_uint64, 0, 0),
	}

	tableInfoSchemaAttr = []string{
		catalog2.SystemColAttr_AccID,
		catalog2.SystemRelAttr_DBID,
		SnapshotAttr_TID,
		catalog2.SystemRelAttr_CreateAt,
		catalog.EntryNode_DeleteAt,
	}

	tableInfoSchemaTypes = []types.Type{
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
	}

	snapshotSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_int64, 0, 0),
		types.New(types.T_enum, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_uint64, 0, 0),
	}
)

type DeltaSource interface {
	GetDeltaLoc(bid objectio.Blockid) (objectio.Location, types.TS)
	SetTS(ts types.TS)
}

type ObjectMapDeltaSource struct {
	objectMeta map[objectio.Segmentid]*objectInfo
}

func NewOMapDeltaSource(objectMeta map[objectio.Segmentid]*objectInfo) DeltaSource {
	return &ObjectMapDeltaSource{
		objectMeta: objectMeta,
	}
}

func (m *ObjectMapDeltaSource) GetDeltaLoc(bid objectio.Blockid) (objectio.Location, types.TS) {
	segment := bid.Segment()
	oInfo, ok := m.objectMeta[*segment]
	if !ok {
		return objectio.Location{}, types.TS{}
	}
	if oInfo.deltaLocation == nil {
		return objectio.Location{}, types.TS{}
	}
	return *oInfo.deltaLocation[uint32(bid.Sequence())], types.TS{}
}

func (m *ObjectMapDeltaSource) SetTS(ts types.TS) {
	panic("implement me")
}

type DeltaLocDataSource struct {
	ctx context.Context
	fs  fileservice.FileService
	ts  types.TS
	ds  DeltaSource
}

func NewDeltaLocDataSource(
	ctx context.Context,
	fs fileservice.FileService,
	ts types.TS,
	ds DeltaSource,
) *DeltaLocDataSource {
	return &DeltaLocDataSource{
		ctx: ctx,
		fs:  fs,
		ts:  ts,
		ds:  ds,
	}
}

func (d *DeltaLocDataSource) Next(
	_ context.Context,
	_ []string,
	_ []types.Type,
	_ []uint16,
	_ any,
	_ *mpool.MPool,
	_ engine.VectorPool,
	_ *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {
	return nil, engine.Persisted, nil
}

func (d *DeltaLocDataSource) Close() {

}

func (d *DeltaLocDataSource) ApplyTombstones(
	ctx context.Context,
	bid objectio.Blockid,
	rowsOffset []int64,
	applyPolicy engine.TombstoneApplyPolicy,
) ([]int64, error) {
	deleteMask, err := d.getAndApplyTombstones(ctx, bid)
	if err != nil {
		return nil, err
	}
	var rows []int64
	if !deleteMask.IsEmpty() {
		for _, row := range rowsOffset {
			if !deleteMask.Contains(uint64(row)) {
				rows = append(rows, row)
			}
		}
	}
	return rows, nil
}

func (d *DeltaLocDataSource) GetTombstones(
	ctx context.Context, bid objectio.Blockid,
) (deletedRows *nulls.Nulls, err error) {
	var rows *nulls.Bitmap
	rows, err = d.getAndApplyTombstones(ctx, bid)
	if err != nil {
		return
	}
	if rows == nil || rows.IsEmpty() {
		return
	}
	return rows, nil
}

func (d *DeltaLocDataSource) getAndApplyTombstones(
	ctx context.Context, bid objectio.Blockid,
) (*nulls.Bitmap, error) {
	deltaLoc, ts := d.ds.GetDeltaLoc(bid)
	if deltaLoc.IsEmpty() {
		return nil, nil
	}
	logutil.Infof("deltaLoc: %v, id is %d", deltaLoc.String(), bid.Sequence())
	deletes, _, release, err := blockio.ReadBlockDelete(ctx, deltaLoc, d.fs)
	if err != nil {
		return nil, err
	}
	defer release()
	if ts.IsEmpty() {
		ts = d.ts
	}
	return blockio.EvalDeleteRowsByTimestamp(deletes, ts, &bid), nil
}

func (d *DeltaLocDataSource) SetOrderBy(orderby []*plan.OrderBySpec) {
	panic("Not Support order by")
}

func (d *DeltaLocDataSource) GetOrderBy() []*plan.OrderBySpec {
	panic("Not Support order by")
}

func (d *DeltaLocDataSource) SetFilterZM(zm objectio.ZoneMap) {
	panic("Not Support order by")
}

type objectInfo struct {
	stats         objectio.ObjectStats
	deltaLocation map[uint32]*objectio.Location
	createAt      types.TS
	deleteAt      types.TS
}

type SnapshotMeta struct {
	sync.RWMutex
	objects     map[uint64]map[objectio.Segmentid]*objectInfo
	tid         uint64
	tables      map[uint32]map[uint64]*TableInfo
	acctIndexes map[uint64]*TableInfo
	tides       map[uint64]struct{}
}

type TableInfo struct {
	accID    uint32
	dbID     uint64
	tid      uint64
	createAt types.TS
	deleteAt types.TS
}

func NewSnapshotMeta() *SnapshotMeta {
	return &SnapshotMeta{
		objects:     make(map[uint64]map[objectio.Segmentid]*objectInfo),
		tables:      make(map[uint32]map[uint64]*TableInfo),
		acctIndexes: make(map[uint64]*TableInfo),
		tides:       make(map[uint64]struct{}),
	}
}

func (sm *SnapshotMeta) CopyObjectsLocked() map[uint64]map[objectio.Segmentid]*objectInfo {
	objects := make(map[uint64]map[objectio.Segmentid]*objectInfo)
	for k, v := range sm.objects {
		objects[k] = make(map[objectio.Segmentid]*objectInfo)
		for kk, vv := range v {
			objects[k][kk] = vv
		}
	}
	return objects
}

func (sm *SnapshotMeta) CopyTablesLocked() map[uint32]map[uint64]*TableInfo {
	tables := make(map[uint32]map[uint64]*TableInfo)
	for k, v := range sm.tables {
		tables[k] = make(map[uint64]*TableInfo)
		for kk, vv := range v {
			tables[k][kk] = vv
		}
	}
	return tables
}

func (sm *SnapshotMeta) updateTableInfo(data *CheckpointData) {
	insTable, insTableTxn, _, _, delTableTxn := data.GetTblBatchs()
	insAccIDs := vector.MustFixedCol[uint32](insTable.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector())
	insTIDs := vector.MustFixedCol[uint64](insTable.GetVectorByName(catalog2.SystemRelAttr_ID).GetDownstreamVector())
	insDBIDs := vector.MustFixedCol[uint64](insTable.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector())
	insCreateAts := vector.MustFixedCol[types.TS](insTableTxn.GetVectorByName(txnbase.SnapshotAttr_CommitTS).GetDownstreamVector())
	insTableNameVec := insTable.GetVectorByName(catalog2.SystemRelAttr_Name).GetDownstreamVector()
	insTableArea := insTableNameVec.GetArea()
	insTableNames := vector.MustFixedCol[types.Varlena](insTableNameVec)
	for i := 0; i < insTable.Length(); i++ {
		tid := insTIDs[i]
		name := string(insTableNames[i].GetByteSlice(insTableArea))
		if name == "mo_snapshots" {
			if sm.tid == 0 {
				//for ut
				sm.SetTid(tid)
			}
			logutil.Info("[UpdateSnapTable]", zap.Uint64("tid", tid))
			sm.tides[tid] = struct{}{}
		}
		accID := insAccIDs[i]
		if sm.tables[accID] == nil {
			sm.tables[accID] = make(map[uint64]*TableInfo)
		}
		dbid := insDBIDs[i]
		createAt := insCreateAts[i]
		if sm.tables[accID][tid] != nil {
			continue
		}
		table := &TableInfo{
			accID:    accID,
			dbID:     dbid,
			tid:      tid,
			createAt: createAt,
		}
		sm.tables[accID][tid] = table

		if sm.acctIndexes[tid] == nil {
			sm.acctIndexes[tid] = table
		}
	}

	delTableIDs := vector.MustFixedCol[uint64](delTableTxn.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	delDropAts := vector.MustFixedCol[types.TS](delTableTxn.GetVectorByName(txnbase.SnapshotAttr_CommitTS).GetDownstreamVector())
	for i := 0; i < delTableTxn.Length(); i++ {
		tid := delTableIDs[i]
		dropAt := delDropAts[i]
		if sm.acctIndexes[tid] == nil {
			//In the upgraded cluster, because the inc checkpoint is consumed halfway,
			// there may be no record of the create table entry, only the delete entry
			continue
		}
		table := sm.acctIndexes[tid]
		table.deleteAt = dropAt
		sm.acctIndexes[tid] = table
		sm.tables[table.accID][tid] = table
	}
}

func (sm *SnapshotMeta) Update(data *CheckpointData) *SnapshotMeta {
	sm.Lock()
	defer sm.Unlock()
	now := time.Now()
	defer func() {
		logutil.Infof("[UpdateSnapshot] cost %v", time.Since(now))
	}()
	sm.updateTableInfo(data)
	if sm.tid == 0 && len(sm.tides) == 0 {
		return sm
	}
	ins := data.GetObjectBatchs()
	insDeleteTSs := vector.MustFixedCol[types.TS](ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
	insCreateTSs := vector.MustFixedCol[types.TS](ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector())
	insTableIDs := vector.MustFixedCol[uint64](ins.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		table := insTableIDs[i]
		if _, ok := sm.tides[table]; !ok {
			continue
		}
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		deleteTS := insDeleteTSs[i]
		createTS := insCreateTSs[i]
		if sm.objects[table] == nil {
			sm.objects[table] = make(map[objectio.Segmentid]*objectInfo)
		}
		if sm.objects[table][objectStats.ObjectName().SegmentId()] == nil {
			if !deleteTS.IsEmpty() {
				continue
			}
			sm.objects[table][objectStats.ObjectName().SegmentId()] = &objectInfo{
				stats:    objectStats,
				createAt: createTS,
			}
			logutil.Info("[UpdateSnapshot] Add object",
				zap.Uint64("table id", table),
				zap.String("object name", objectStats.ObjectName().String()),
				zap.String("create at", createTS.ToString()))

			continue
		}
		if deleteTS.IsEmpty() {
			panic(any("deleteTS is empty"))
		}
		logutil.Info("[UpdateSnapshot] Delete object",
			zap.Uint64("table id", table),
			zap.String("object name", objectStats.ObjectName().String()))
		delete(sm.objects[table], objectStats.ObjectName().SegmentId())
	}
	// TODO
	// del, delTxn, _, _ := data.GetBlkBatchs()
	// delBlockIDs := vector.MustFixedCol[types.Blockid](del.GetVectorByName(catalog2.BlockMeta_ID).GetDownstreamVector())
	// delTableIDs := vector.MustFixedCol[uint64](delTxn.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	// for i := 0; i < del.Length(); i++ {
	// 	blockID := delBlockIDs[i]
	// 	tableID := delTableIDs[i]
	// 	deltaLoc := objectio.Location(del.GetVectorByName(catalog2.BlockMeta_DeltaLoc).Get(i).([]byte))
	// 	if _, ok := sm.tides[tableID]; !ok {
	// 		continue
	// 	}
	// 	if sm.objects[tableID] == nil {
	// 		panic(any(fmt.Sprintf("tableID %d not found", tableID)))
	// 	}
	// 	if sm.objects[tableID][*blockID.Segment()] != nil {
	// 		if sm.objects[tableID][*blockID.Segment()].deltaLocation == nil {
	// 			sm.objects[tableID][*blockID.Segment()].deltaLocation = make(map[uint32]*objectio.Location)
	// 		}
	// 		sm.objects[tableID][*blockID.Segment()].deltaLocation[uint32(blockID.Sequence())] = &deltaLoc
	// 	}
	// }
	return nil
}

func (sm *SnapshotMeta) GetSnapshot(ctx context.Context, sid string, fs fileservice.FileService, mp *mpool.MPool) (map[uint32]containers.Vector, error) {
	now := time.Now()
	defer func() {
		logutil.Infof("[GetSnapshot] cost %v", time.Since(now))
	}()
	sm.RLock()
	objects := sm.CopyObjectsLocked()
	tables := sm.CopyTablesLocked()
	sm.RUnlock()
	snapshotList := make(map[uint32]containers.Vector)
	idxes := []uint16{ColTS, ColLevel, ColObjId}
	colTypes := []types.Type{
		snapshotSchemaTypes[ColTS],
		snapshotSchemaTypes[ColLevel],
		snapshotSchemaTypes[ColObjId],
	}
	for _, objectMap := range objects {
		checkpointTS := types.BuildTS(time.Now().UTC().UnixNano(), 0)
		ds := NewDeltaLocDataSource(ctx, fs, checkpointTS, NewOMapDeltaSource(objectMap))
		for _, object := range objectMap {
			location := object.stats.ObjectLocation()
			name := object.stats.ObjectName()
			for i := uint32(0); i < object.stats.BlkCnt(); i++ {
				loc := objectio.BuildLocation(name, location.Extent(), 0, uint16(i))
				blk := objectio.BlockInfo{
					BlockID: *objectio.BuildObjectBlockid(name, uint16(i)),
					MetaLoc: objectio.ObjectLocation(loc),
				}

				var vp engine.VectorPool
				buildBatch := func() *batch.Batch {
					result := batch.NewWithSize(len(colTypes))
					for i, typ := range colTypes {
						if vp == nil {
							result.Vecs[i] = vector.NewVec(typ)
						} else {
							result.Vecs[i] = vp.GetVector(typ)
						}
					}
					return result
				}

				bat := buildBatch()
				defer bat.Clean(mp)
				err := blockio.BlockDataRead(ctx, sid, &blk, ds, idxes, colTypes, checkpointTS.ToTimestamp(),
					nil, nil, blockio.BlockReadFilter{}, fs, mp, nil, fileservice.Policy(0), "", bat)
				if err != nil {
					return nil, err
				}
				tsList := vector.MustFixedCol[int64](bat.Vecs[0])
				typeList := vector.MustFixedCol[types.Enum](bat.Vecs[1])
				acctList := vector.MustFixedCol[uint64](bat.Vecs[2])
				for r := 0; r < bat.Vecs[0].Length(); r++ {
					ts := tsList[r]
					snapTs := types.BuildTS(ts, 0)
					acct := acctList[r]
					snapshotType := typeList[r]
					if snapshotType == SnapshotTypeCluster {
						for account := range tables {
							if snapshotList[account] == nil {
								snapshotList[account] = containers.MakeVector(types.T_TS.ToType(), mp)
							}
							err = vector.AppendFixed[types.TS](snapshotList[account].GetDownstreamVector(), snapTs, false, mp)
							if err != nil {
								return nil, err
							}
							// TODO: info to debug
							logutil.Info("[GetSnapshot] cluster snapshot",
								common.OperationField(snapTs.ToString()))
						}
						continue
					}
					id := uint32(acct)
					if snapshotList[id] == nil {
						snapshotList[id] = containers.MakeVector(types.T_TS.ToType(), mp)
					}
					// TODO: info to debug
					logutil.Info("[GetSnapshot] snapshot",
						zap.Uint32("account", id),
						zap.String("snap ts", snapTs.ToString()))
					err = vector.AppendFixed[types.TS](snapshotList[id].GetDownstreamVector(), snapTs, false, mp)
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}
	for i := range snapshotList {
		snapshotList[i].GetDownstreamVector().InplaceSort()
	}
	return snapshotList, nil
}

func (sm *SnapshotMeta) SetTid(tid uint64) {
	sm.tid = tid
}

func (sm *SnapshotMeta) SaveMeta(name string, fs fileservice.FileService) (uint32, error) {
	if len(sm.objects) == 0 {
		return 0, nil
	}
	bat := containers.NewBatch()
	for i, attr := range objectInfoSchemaAttr {
		bat.AddVector(attr, containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator))
	}
	deltaBat := containers.NewBatch()
	for i, attr := range objectDeltaSchemaAttr {
		deltaBat.AddVector(attr, containers.MakeVector(objectDeltaSchemaTypes[i], common.DebugAllocator))
	}
	for tid, objectMap := range sm.objects {
		for _, entry := range objectMap {
			vector.AppendBytes(
				bat.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector(),
				entry.stats[:], false, common.DebugAllocator)
			vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector(),
				entry.createAt, false, common.DebugAllocator)
			vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
				entry.deleteAt, false, common.DebugAllocator)
			for id, delta := range entry.deltaLocation {
				blockID := objectio.BuildObjectBlockid(entry.stats.ObjectName(), uint16(id))
				vector.AppendFixed[types.Blockid](deltaBat.GetVectorByName(catalog2.BlockMeta_ID).GetDownstreamVector(),
					*blockID, false, common.DebugAllocator)
				vector.AppendBytes(deltaBat.GetVectorByName(catalog2.BlockMeta_DeltaLoc).GetDownstreamVector(),
					[]byte(*delta), false, common.DebugAllocator)
				vector.AppendFixed[uint64](
					deltaBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
					tid, false, common.DebugAllocator)
			}
			vector.AppendFixed[uint64](
				bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
				tid, false, common.DebugAllocator)
		}
	}
	defer bat.Close()
	defer deltaBat.Close()
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs)
	if err != nil {
		return 0, err
	}
	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(bat)); err != nil {
		return 0, err
	}
	if deltaBat.Length() > 0 {
		logutil.Infof("deltaBat length is %d", deltaBat.Length())
		if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(deltaBat)); err != nil {
			return 0, err
		}
	}

	_, err = writer.WriteEnd(context.Background())
	if err != nil {
		return 0, err
	}
	size := writer.GetObjectStats()[0].OriginSize()
	return size, err
}

func (sm *SnapshotMeta) SaveTableInfo(name string, fs fileservice.FileService) (uint32, error) {
	if len(sm.tables) == 0 {
		return 0, nil
	}
	bat := containers.NewBatch()
	snapTableBat := containers.NewBatch()
	for i, attr := range tableInfoSchemaAttr {
		bat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
		snapTableBat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
	}
	for _, entry := range sm.tables {
		for _, table := range entry {
			vector.AppendFixed[uint32](
				bat.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector(),
				table.accID, false, common.DebugAllocator)
			vector.AppendFixed[uint64](
				bat.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector(),
				table.dbID, false, common.DebugAllocator)
			vector.AppendFixed[uint64](
				bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
				table.tid, false, common.DebugAllocator)
			vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector(),
				table.createAt, false, common.DebugAllocator)
			vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
				table.deleteAt, false, common.DebugAllocator)

			if _, ok := sm.tides[table.tid]; ok {
				vector.AppendFixed[uint32](
					snapTableBat.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector(),
					table.accID, false, common.DebugAllocator)
				vector.AppendFixed[uint64](
					snapTableBat.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector(),
					table.dbID, false, common.DebugAllocator)
				vector.AppendFixed[uint64](
					snapTableBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
					table.tid, false, common.DebugAllocator)
				vector.AppendFixed[types.TS](
					snapTableBat.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector(),
					table.createAt, false, common.DebugAllocator)
				vector.AppendFixed[types.TS](
					snapTableBat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
					table.deleteAt, false, common.DebugAllocator)
			}
		}
	}
	defer bat.Close()
	defer snapTableBat.Close()
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs)
	if err != nil {
		return 0, err
	}
	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(bat)); err != nil {
		return 0, err
	}
	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(snapTableBat)); err != nil {
		return 0, err
	}

	_, err = writer.WriteEnd(context.Background())
	if err != nil {
		return 0, err
	}
	size := writer.GetObjectStats()[0].OriginSize()
	return size, err
}

func (sm *SnapshotMeta) RebuildTableInfo(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	insTIDs := vector.MustFixedCol[uint64](ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	insAccIDs := vector.MustFixedCol[uint32](ins.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector())
	insDBIDs := vector.MustFixedCol[uint64](ins.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector())
	insCreateTSs := vector.MustFixedCol[types.TS](ins.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector())
	insDeleteTSs := vector.MustFixedCol[types.TS](ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		tid := insTIDs[i]
		dbid := insDBIDs[i]
		accid := insAccIDs[i]
		createTS := insCreateTSs[i]
		deleteTS := insDeleteTSs[i]
		if sm.tables[accid] == nil {
			sm.tables[accid] = make(map[uint64]*TableInfo)
		}
		table := &TableInfo{
			tid:      tid,
			dbID:     dbid,
			accID:    accid,
			createAt: createTS,
			deleteAt: deleteTS,
		}
		sm.tables[accid][tid] = table
		sm.acctIndexes[tid] = table
	}
}

func (sm *SnapshotMeta) RebuildTid(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	insTIDs := vector.MustFixedCol[uint64](ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	accIDs := vector.MustFixedCol[uint32](ins.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector())
	if ins.Length() < 1 {
		logutil.Warnf("RebuildTid unexpected length %d", ins.Length())
		return
	}
	logutil.Infof("RebuildTid tid %d", insTIDs[0])
	sm.SetTid(insTIDs[0])
	for i := 0; i < ins.Length(); i++ {
		tid := insTIDs[i]
		accid := accIDs[i]
		if _, ok := sm.tides[tid]; !ok {
			sm.tides[tid] = struct{}{}
			logutil.Info("[RebuildSnapshotTid]", zap.Uint64("tid", tid), zap.Uint32("account id", accid))
		}
	}
}

func (sm *SnapshotMeta) Rebuild(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	insCreateTSs := vector.MustFixedCol[types.TS](ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector())
	insTides := vector.MustFixedCol[uint64](ins.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		createTS := insCreateTSs[i]
		tid := insTides[i]
		if sm.tid == 0 {
			if tid == 0 {
				panic("tid is 0")
			}
			sm.SetTid(tid)
		}
		if _, ok := sm.tides[tid]; !ok {
			sm.tides[tid] = struct{}{}
			logutil.Info("[RebuildSnapTable]", zap.Uint64("tid", tid))
		}
		if sm.objects[tid] == nil {
			sm.objects[tid] = make(map[objectio.Segmentid]*objectInfo)
		}
		if sm.objects[tid][objectStats.ObjectName().SegmentId()] == nil {
			sm.objects[tid][objectStats.ObjectName().SegmentId()] = &objectInfo{
				stats:    objectStats,
				createAt: createTS,
			}
			logutil.Info("[RebuildSnapshot] Add object",
				zap.Uint64("table id", tid),
				zap.String("object name", objectStats.ObjectName().String()),
				zap.String("create at", createTS.ToString()))
			continue
		}
	}
}

func (sm *SnapshotMeta) RebuildDelta(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	insBlockIDs := vector.MustFixedCol[types.Blockid](ins.GetVectorByName(catalog2.BlockMeta_ID).GetDownstreamVector())
	insTides := vector.MustFixedCol[uint64](ins.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		blockID := insBlockIDs[i]
		tid := insTides[i]
		deltaLoc := objectio.Location(ins.GetVectorByName(catalog2.BlockMeta_DeltaLoc).Get(i).([]byte))
		if sm.objects[tid] == nil {
			panic(any(fmt.Sprintf("tableID %d not found", tid)))
		}
		if sm.objects[tid][*blockID.Segment()] != nil {
			if sm.objects[tid][*blockID.Segment()].deltaLocation == nil {
				sm.objects[tid][*blockID.Segment()].deltaLocation = make(map[uint32]*objectio.Location)
			}
			logutil.Infof("RebuildDelta: %v, loc is %v", blockID.String(), deltaLoc.String())
			sm.objects[tid][*blockID.Segment()].deltaLocation[uint32(blockID.Sequence())] = &deltaLoc
		} else {
			panic(any(fmt.Sprintf("blockID %s not found", blockID.String())))
		}
	}
}

func (sm *SnapshotMeta) ReadMeta(ctx context.Context, name string, fs fileservice.FileService) error {
	reader, err := blockio.NewFileReaderNoCache(fs, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DebugAllocator)
	if err != nil {
		return err
	}
	idxes := make([]uint16, len(objectInfoSchemaAttr))
	for i := range objectInfoSchemaAttr {
		idxes[i] = uint16(i)
	}
	mobat, release, err := reader.LoadColumns(ctx, idxes, nil, bs[0].GetID(), common.DebugAllocator)
	if err != nil {
		return err
	}
	defer release()
	bat := containers.NewBatch()
	defer bat.Close()
	for i := range objectInfoSchemaAttr {
		pkgVec := mobat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
		}
		bat.AddVector(objectInfoSchemaAttr[i], vec)
	}
	sm.Rebuild(bat)

	if len(bs) == 1 {
		return nil
	}

	idxes = make([]uint16, len(objectDeltaSchemaAttr))
	for i := range objectDeltaSchemaAttr {
		idxes[i] = uint16(i)
	}
	moDeltaBat, releaseDelta, err := reader.LoadColumns(ctx, idxes, nil, bs[1].GetID(), common.DebugAllocator)
	if err != nil {
		return err
	}
	defer releaseDelta()
	deltaBat := containers.NewBatch()
	defer deltaBat.Close()
	for i := range objectDeltaSchemaAttr {
		pkgVec := moDeltaBat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(objectDeltaSchemaTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
		}
		deltaBat.AddVector(objectDeltaSchemaAttr[i], vec)
	}
	sm.RebuildDelta(deltaBat)
	return nil
}

func (sm *SnapshotMeta) ReadTableInfo(ctx context.Context, name string, fs fileservice.FileService) error {
	reader, err := blockio.NewFileReaderNoCache(fs, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DebugAllocator)
	if err != nil {
		return err
	}
	idxes := make([]uint16, len(tableInfoSchemaAttr))
	for i := range tableInfoSchemaAttr {
		idxes[i] = uint16(i)
	}
	for id, block := range bs {
		mobat, release, err := reader.LoadColumns(ctx, idxes, nil, block.GetID(), common.DebugAllocator)
		if err != nil {
			return err
		}
		defer release()
		bat := containers.NewBatch()
		defer bat.Close()
		for i := range tableInfoSchemaAttr {
			pkgVec := mobat.Vecs[i]
			var vec containers.Vector
			if pkgVec.Length() == 0 {
				vec = containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator)
			} else {
				vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
			}
			bat.AddVector(tableInfoSchemaAttr[i], vec)
		}
		if id == 0 {
			sm.RebuildTableInfo(bat)
		} else {
			sm.RebuildTid(bat)
		}
	}
	return nil
}

func (sm *SnapshotMeta) InitTableInfo(data *CheckpointData) {
	sm.Lock()
	defer sm.Unlock()
	sm.updateTableInfo(data)
}

func (sm *SnapshotMeta) TableInfoString() string {
	sm.RLock()
	defer sm.RUnlock()
	var buf bytes.Buffer
	for accID, tables := range sm.tables {
		buf.WriteString(fmt.Sprintf("accountID: %d\n", accID))
		for tid, table := range tables {
			buf.WriteString(fmt.Sprintf("tableID: %d, create: %s, deleteAt: %s\n",
				tid, table.createAt.ToString(), table.deleteAt.ToString()))
		}
	}
	return buf.String()
}

func (sm *SnapshotMeta) GetSnapshotList(SnapshotList map[uint32][]types.TS, tid uint64) []types.TS {
	sm.RLock()
	defer sm.RUnlock()
	if sm.acctIndexes[tid] == nil {
		return nil
	}
	accID := sm.acctIndexes[tid].accID
	return SnapshotList[accID]
}

func (sm *SnapshotMeta) GetSnapshotListLocked(SnapshotList map[uint32][]types.TS, tid uint64) []types.TS {
	if sm.acctIndexes[tid] == nil {
		return nil
	}
	accID := sm.acctIndexes[tid].accID
	return SnapshotList[accID]
}

func (sm *SnapshotMeta) MergeTableInfo(SnapshotList map[uint32][]types.TS) error {
	sm.Lock()
	defer sm.Unlock()
	if len(sm.tables) == 0 {
		return nil
	}
	for accID, tables := range sm.tables {
		if SnapshotList[accID] == nil {
			for _, table := range tables {
				if !table.deleteAt.IsEmpty() {
					logutil.Infof("MergeTableInfo delete table %d", table.tid)
					delete(sm.tables[accID], table.tid)
					delete(sm.acctIndexes, table.tid)
					if sm.objects[table.tid] != nil {
						delete(sm.objects, table.tid)
					}
				}
			}
			continue
		}
		for _, table := range tables {
			if !table.deleteAt.IsEmpty() && !isSnapshotRefers(table, SnapshotList[accID]) {
				logutil.Infof("MergeTableInfo delete table %d", table.tid)
				delete(sm.tables[accID], table.tid)
				delete(sm.acctIndexes, table.tid)
				if sm.objects[table.tid] != nil {
					delete(sm.objects, table.tid)
				}
			}
		}
	}
	return nil
}

func (sm *SnapshotMeta) String() string {
	sm.RLock()
	defer sm.RUnlock()
	return fmt.Sprintf("account count: %d, table count: %d, object count: %d",
		len(sm.tables), len(sm.acctIndexes), len(sm.objects))
}

func isSnapshotRefers(table *TableInfo, snapVec []types.TS) bool {
	if len(snapVec) == 0 {
		return false
	}
	left, right := 0, len(snapVec)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapVec[mid]
		if snapTS.GreaterEq(&table.createAt) && snapTS.Less(&table.deleteAt) {
			logutil.Infof("isSnapshotRefers: %s, create %v, drop %v, tid %d",
				snapTS.ToString(), table.createAt.ToString(), table.deleteAt.ToString(), table.tid)
			return true
		} else if snapTS.Less(&table.createAt) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}

func CloseSnapshotList(snapshots map[uint32]containers.Vector) {
	for _, snapshot := range snapshots {
		snapshot.Close()
	}
}
