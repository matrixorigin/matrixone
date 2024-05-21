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
	"go.uber.org/zap"
	"sync"
	"time"
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
	}

	objectDeltaSchemaTypes = []types.Type{
		types.New(types.T_Blockid, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
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

type objectInfo struct {
	stats         objectio.ObjectStats
	deltaLocation map[uint32]*objectio.Location
	createAt      types.TS
	deleteAt      types.TS
}

type SnapshotMeta struct {
	sync.RWMutex
	objects     map[objectio.Segmentid]*objectInfo
	tid         uint64
	tables      map[uint32]map[uint64]*TableInfo
	acctIndexes map[uint64]*TableInfo
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
		objects:     make(map[objectio.Segmentid]*objectInfo),
		tables:      make(map[uint32]map[uint64]*TableInfo),
		acctIndexes: make(map[uint64]*TableInfo),
	}
}

func (sm *SnapshotMeta) updateTableInfo(data *CheckpointData) {
	insTable, _, _, _, delTableTxn := data.GetTblBatchs()
	insAccIDs := vector.MustFixedCol[uint32](insTable.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector())
	insTIDs := vector.MustFixedCol[uint64](insTable.GetVectorByName(catalog2.SystemRelAttr_ID).GetDownstreamVector())
	insDBIDs := vector.MustFixedCol[uint64](insTable.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector())
	insCreateAts := vector.MustFixedCol[types.Timestamp](insTable.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector())
	for i := 0; i < insTable.Length(); i++ {
		tid := insTIDs[i]
		if sm.tid == 0 {
			tableName := string(insTable.GetVectorByName(catalog2.SystemRelAttr_Name).Get(i).([]byte))
			if tableName == "mo_snapshots" {
				logutil.Infof("mo_snapshots: %d", tid)
				sm.SetTid(tid)
			}
		}
		accID := insAccIDs[i]
		if sm.tables[accID] == nil {
			sm.tables[accID] = make(map[uint64]*TableInfo)
		}
		dbid := insDBIDs[i]
		create := insCreateAts[i]
		createAt := types.BuildTS(create.Unix(), 0)
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
	if sm.tid == 0 {
		return sm
	}
	ins := data.GetObjectBatchs()
	insDeleteTSs := vector.MustFixedCol[types.TS](ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
	insCreateTSs := vector.MustFixedCol[types.TS](ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector())
	insTableIDs := vector.MustFixedCol[uint64](ins.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		table := insTableIDs[i]
		if table != sm.tid {
			continue
		}
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		deleteTS := insDeleteTSs[i]
		createTS := insCreateTSs[i]
		if sm.objects[objectStats.ObjectName().SegmentId()] == nil {
			if !deleteTS.IsEmpty() {
				continue
			}
			sm.objects[objectStats.ObjectName().SegmentId()] = &objectInfo{
				stats:    objectStats,
				createAt: createTS,
			}
			continue
		}
		if deleteTS.IsEmpty() {
			panic(any("deleteTS is empty"))
		}
		delete(sm.objects, objectStats.ObjectName().SegmentId())
	}
	del, _, _, _ := data.GetBlkBatchs()
	delBlockIDs := vector.MustFixedCol[types.Blockid](del.GetVectorByName(catalog2.BlockMeta_ID).GetDownstreamVector())
	for i := 0; i < del.Length(); i++ {
		blockID := delBlockIDs[i]
		deltaLoc := objectio.Location(del.GetVectorByName(catalog2.BlockMeta_DeltaLoc).Get(i).([]byte))
		if sm.objects[*blockID.Segment()] != nil {
			if sm.objects[*blockID.Segment()].deltaLocation == nil {
				sm.objects[*blockID.Segment()].deltaLocation = make(map[uint32]*objectio.Location)
			}
			sm.objects[*blockID.Segment()].deltaLocation[uint32(blockID.Sequence())] = &deltaLoc
		}
	}
	return nil
}

func (sm *SnapshotMeta) GetSnapshot(ctx context.Context, fs fileservice.FileService, mp *mpool.MPool) (map[uint32]containers.Vector, error) {
	now := time.Now()
	defer func() {
		logutil.Infof("[GetSnapshot] cost %v", time.Since(now))
	}()
	sm.RLock()
	objects := sm.objects
	sm.RUnlock()
	snapshotList := make(map[uint32]containers.Vector)
	idxes := []uint16{ColTS, ColLevel, ColObjId}
	colTypes := []types.Type{
		snapshotSchemaTypes[ColTS],
		snapshotSchemaTypes[ColLevel],
		snapshotSchemaTypes[ColObjId],
	}
	for _, object := range objects {
		location := object.stats.ObjectLocation()
		name := object.stats.ObjectName()
		for i := uint32(0); i < object.stats.BlkCnt(); i++ {
			loc := objectio.BuildLocation(name, location.Extent(), 0, uint16(i))
			blk := objectio.BlockInfo{
				BlockID:   *objectio.BuildObjectBlockid(name, uint16(i)),
				SegmentID: name.SegmentId(),
				MetaLoc:   objectio.ObjectLocation(loc),
			}
			if object.deltaLocation[i] != nil {
				logutil.Infof("deltaLoc: %v, id is %d", object.deltaLocation[i].String(), i)
				blk.DeltaLoc = objectio.ObjectLocation(*object.deltaLocation[i])
			}
			checkpointTS := types.BuildTS(time.Now().UTC().UnixNano(), 0)
			bat, err := blockio.BlockRead(ctx, &blk, nil, idxes, colTypes, checkpointTS.ToTimestamp(),
				nil, nil, blockio.ReadFilter{}, fs, mp, nil, fileservice.Policy(0))
			if err != nil {
				return nil, err
			}
			defer bat.Clean(mp)
			tsList := vector.MustFixedCol[int64](bat.Vecs[0])
			typeList := vector.MustFixedCol[types.Enum](bat.Vecs[1])
			acctList := vector.MustFixedCol[uint64](bat.Vecs[2])
			for r := 0; r < bat.Vecs[0].Length(); r++ {
				ts := tsList[r]
				snapTs := types.BuildTS(ts, 0)
				acct := acctList[r]
				snapshotType := typeList[r]
				if snapshotType == SnapshotTypeCluster {
					for account := range sm.tables {
						if snapshotList[account] == nil {
							snapshotList[account] = containers.MakeVector(types.T_TS.ToType(), mp)
						}
						err = vector.AppendFixed[types.TS](snapshotList[account].GetDownstreamVector(), snapTs, false, mp)
						if err != nil {
							return nil, err
						}
						logutil.Debug("[GetSnapshot] cluster snapshot",
							common.OperationField(snapTs.ToString()))
					}
					continue
				}
				id := uint32(acct)
				if snapshotList[id] == nil {
					snapshotList[id] = containers.MakeVector(types.T_TS.ToType(), mp)
				}
				logutil.Debug("[GetSnapshot] snapshot",
					zap.Uint32("account", id),
					zap.String("snap ts", snapTs.ToString()))
				err = vector.AppendFixed[types.TS](snapshotList[id].GetDownstreamVector(), snapTs, false, mp)
				if err != nil {
					return nil, err
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
	for _, entry := range sm.objects {
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
		}
		vector.AppendFixed[uint64](
			bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
			sm.tid, false, common.DebugAllocator)
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

			if table.tid == sm.tid {
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
	insTIDs := vector.MustFixedCol[uint64](ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	if ins.Length() != 1 {
		logutil.Warnf("RebuildTid unexpected length %d", ins.Length())
		return
	}
	logutil.Infof("RebuildTid tid %d", insTIDs[0])
	sm.SetTid(insTIDs[0])
}

func (sm *SnapshotMeta) Rebuild(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	insCreateTSs := vector.MustFixedCol[types.TS](ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		createTS := insCreateTSs[i]
		if sm.tid == 0 {
			tid := ins.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
			if tid == 0 {
				panic("tid is 0")
			}
			sm.SetTid(tid)
		}
		if sm.objects[objectStats.ObjectName().SegmentId()] == nil {
			sm.objects[objectStats.ObjectName().SegmentId()] = &objectInfo{
				stats:    objectStats,
				createAt: createTS,
			}
			continue
		}
	}
}

func (sm *SnapshotMeta) RebuildDelta(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	insBlockIDs := vector.MustFixedCol[types.Blockid](ins.GetVectorByName(catalog2.BlockMeta_ID).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		blockID := insBlockIDs[i]
		deltaLoc := objectio.Location(ins.GetVectorByName(catalog2.BlockMeta_DeltaLoc).Get(i).([]byte))
		if sm.objects[*blockID.Segment()] != nil {
			if sm.objects[*blockID.Segment()].deltaLocation == nil {
				sm.objects[*blockID.Segment()].deltaLocation = make(map[uint32]*objectio.Location)
			}
			logutil.Infof("RebuildDelta: %v, loc is %v", blockID.String(), deltaLoc.String())
			sm.objects[*blockID.Segment()].deltaLocation[uint32(blockID.Sequence())] = &deltaLoc
		} else {
			panic("blockID not found")
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
					delete(sm.tables[accID], table.tid)
					delete(sm.acctIndexes, table.tid)
				}
			}
			continue
		}
		for _, table := range tables {
			if !table.deleteAt.IsEmpty() && !isSnapshotRefers(table, SnapshotList[accID]) {
				delete(sm.tables[accID], table.tid)
				delete(sm.acctIndexes, table.tid)
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
			logutil.Infof("isSnapshotRefers: %s, create %v, drop %v",
				snapTS.ToString(), table.createAt.ToString(), table.deleteAt.ToString())
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
