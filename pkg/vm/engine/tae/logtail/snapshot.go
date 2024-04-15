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
	"context"
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
	"sync"
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
)

type objectInfo struct {
	stats         objectio.ObjectStats
	deltaLocation map[uint32]*objectio.Location
	createAt      types.TS
	deleteAt      types.TS
	checkpointTS  types.TS
}

type SnapshotMeta struct {
	sync.RWMutex
	objects     map[objectio.Segmentid]*objectInfo
	tid         uint64
	tables      map[uint32]map[uint64]*tableInfo
	acctIndexes map[uint64]*tableInfo
}

type tableInfo struct {
	accID    uint32
	dbID     uint64
	tid      uint64
	createAt types.TS
	deleteAt types.TS
}

func NewSnapshotMeta() *SnapshotMeta {
	return &SnapshotMeta{
		objects:     make(map[objectio.Segmentid]*objectInfo),
		tables:      make(map[uint32]map[uint64]*tableInfo),
		acctIndexes: make(map[uint64]*tableInfo),
	}
}

func (sm *SnapshotMeta) Update(data *CheckpointData) *SnapshotMeta {
	sm.Lock()
	defer sm.Unlock()

	insTable, _, _, _, delTableTxn := data.GetTblBatchs()
	insAccIDVec := insTable.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector()
	insTIDVec := insTable.GetVectorByName(catalog2.SystemRelAttr_ID).GetDownstreamVector()
	insDBIDVec := insTable.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector()
	insCreateAtVec := insTable.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector()
	for i := 0; i < insTable.Length(); i++ {
		tableName := string(insTable.GetVectorByName(catalog2.SystemRelAttr_Name).Get(i).([]byte))
		tid := vector.GetFixedAt[uint64](insTIDVec, i)
		if sm.tid == 0 && tableName == "mo_snapshots" {
			logutil.Infof("mo_snapshots: %d", tid)
			sm.SetTid(tid)
		}
		accID := vector.GetFixedAt[uint32](insAccIDVec, i)
		if sm.tables[accID] == nil {
			sm.tables[accID] = make(map[uint64]*tableInfo)
		}
		dbid := vector.GetFixedAt[uint64](insDBIDVec, i)
		create := vector.GetFixedAt[types.Timestamp](insCreateAtVec, i)
		createAt := types.BuildTS(create.Unix(), 0)
		if sm.tables[accID][tid] != nil {
			continue
		}
		table := &tableInfo{
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

	delTableIDVec := delTableTxn.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	delDropAtVec := delTableTxn.GetVectorByName(txnbase.SnapshotAttr_CommitTS).GetDownstreamVector()
	for i := 0; i < delTableTxn.Length(); i++ {
		tid := vector.GetFixedAt[uint64](delTableIDVec, i)
		dropAt := vector.GetFixedAt[types.TS](delDropAtVec, i)
		table := sm.acctIndexes[tid]
		table.deleteAt = dropAt
		sm.acctIndexes[tid] = table
		sm.tables[table.accID][tid] = table
	}
	if sm.tid == 0 {
		return sm
	}
	ins := data.GetObjectBatchs()
	insDeleteTSVec := ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector()
	insCreateTSVec := ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	insTableIDVec := ins.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	for i := 0; i < ins.Length(); i++ {
		table := vector.GetFixedAt[uint64](insTableIDVec, i)
		if table != sm.tid {
			continue
		}
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		deleteTS := vector.GetFixedAt[types.TS](insDeleteTSVec, i)
		createTS := vector.GetFixedAt[types.TS](insCreateTSVec, i)
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
	delBlockIDVec := del.GetVectorByName(catalog2.BlockMeta_ID).GetDownstreamVector()
	for i := 0; i < del.Length(); i++ {
		blockID := vector.GetFixedAt[types.Blockid](delBlockIDVec, i)
		deltaLoc := objectio.Location(del.GetVectorByName(catalog2.BlockMeta_DeltaLoc).Get(i).([]byte))
		if sm.objects[*blockID.Segment()] != nil {
			sm.objects[*blockID.Segment()].deltaLocation[uint32(blockID.Sequence())] = &deltaLoc
		}
	}
	return nil
}

func (sm *SnapshotMeta) GetSnapshot(ctx context.Context, fs fileservice.FileService, mp *mpool.MPool) (map[uint32]containers.Vector, error) {
	sm.RLock()
	objects := sm.objects
	sm.RUnlock()
	snapshotList := make(map[uint32]containers.Vector)
	idxes := []uint16{2, 7}
	colTypes := []types.Type{
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_uint64, 0, 0),
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
				blk.DeltaLoc = objectio.ObjectLocation(*object.deltaLocation[i])
			}
			bat, err := blockio.BlockRead(ctx, &blk, nil, idxes, colTypes, object.checkpointTS.ToTimestamp(),
				nil, nil, nil, fs, mp, nil, fileservice.Policy(0))
			if err != nil {
				return nil, err
			}
			defer bat.Clean(mp)
			for r := 0; r < bat.Vecs[0].Length(); r++ {
				ts := vector.GetFixedAt[types.Timestamp](bat.Vecs[0], r)
				snapTs := types.BuildTS(ts.Unix()*types.NanoSecsPerSec, 0)
				acct := vector.GetFixedAt[uint64](bat.Vecs[1], r)
				id := uint32(acct)
				if snapshotList[id] == nil {
					snapshotList[id] = containers.MakeVector(colTypes[0], mp)
				}
				logutil.Infof("acct: %d, snapts is %v", acct, snapTs.ToString())
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

func (sm *SnapshotMeta) SaveMeta(name string, fs fileservice.FileService) error {
	if len(sm.objects) == 0 {
		return nil
	}
	bat := containers.NewBatch()
	for i, attr := range objectInfoSchemaAttr {
		bat.AddVector(attr, containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator))
	}
	for _, entry := range sm.objects {
		bat.GetVectorByName(catalog.ObjectAttr_ObjectStats).Append(entry.stats[:], false)
		bat.GetVectorByName(catalog.EntryNode_CreateAt).Append(entry.createAt, false)
		bat.GetVectorByName(catalog.EntryNode_DeleteAt).Append(entry.deleteAt, false)
		if entry.deltaLocation[0] == nil {
			bat.GetVectorByName(catalog2.BlockMeta_DeltaLoc).Append([]byte(objectio.Location{}), false)
		} else {
			bat.GetVectorByName(catalog2.BlockMeta_DeltaLoc).Append([]byte(*entry.deltaLocation[0]), false)
		}
		bat.GetVectorByName(SnapshotAttr_TID).Append(sm.tid, false)
	}
	defer bat.Close()
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs)
	if err != nil {
		return err
	}
	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(bat)); err != nil {
		return err
	}

	_, err = writer.WriteEnd(context.Background())
	return err
}

func (sm *SnapshotMeta) SaveTableInfo(name string, fs fileservice.FileService) error {
	if len(sm.tables) == 0 {
		return nil
	}
	bat := containers.NewBatch()
	for i, attr := range tableInfoSchemaAttr {
		bat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
	}
	for _, entry := range sm.tables {
		for _, table := range entry {
			bat.GetVectorByName(catalog2.SystemColAttr_AccID).Append(table.accID, false)
			bat.GetVectorByName(catalog2.SystemRelAttr_DBID).Append(table.dbID, false)
			bat.GetVectorByName(SnapshotAttr_TID).Append(table.tid, false)
			bat.GetVectorByName(catalog2.SystemRelAttr_CreateAt).Append(table.createAt, false)
			bat.GetVectorByName(catalog.EntryNode_DeleteAt).Append(table.deleteAt, false)
		}
	}
	defer bat.Close()
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs)
	if err != nil {
		return err
	}
	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(bat)); err != nil {
		return err
	}

	_, err = writer.WriteEnd(context.Background())
	return err
}

func (sm *SnapshotMeta) RebuildTableInfo(ins *containers.Batch) {
	insTIDVec := ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector()
	insAccIDVec := ins.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector()
	insDBIDVec := ins.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector()
	insCreateTSVec := ins.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector()
	insDeleteTSVec := ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector()
	for i := 0; i < ins.Length(); i++ {
		tid := vector.GetFixedAt[uint64](insTIDVec, i)
		dbid := vector.GetFixedAt[uint64](insDBIDVec, i)
		accid := vector.GetFixedAt[uint32](insAccIDVec, i)
		createTS := vector.GetFixedAt[types.TS](insCreateTSVec, i)
		deleteTS := vector.GetFixedAt[types.TS](insDeleteTSVec, i)
		if sm.tables[accid] == nil {
			sm.tables[accid] = make(map[uint64]*tableInfo)
		}
		table := &tableInfo{
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

func (sm *SnapshotMeta) Rebuild(ins *containers.Batch) {
	insCreateTSVec := ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	for i := 0; i < ins.Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		createTS := vector.GetFixedAt[types.TS](insCreateTSVec, i)
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
	mobat, release, err := reader.LoadColumns(ctx, idxes, nil, bs[0].GetID(), common.DebugAllocator)
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
	sm.RebuildTableInfo(bat)
	return nil
}

func (sm *SnapshotMeta) GetSnapshotList(SnapshotList map[uint32][]types.TS, tid uint64) []types.TS {
	sm.RLock()
	sm.RUnlock()
	if sm.acctIndexes[tid] == nil {
		return nil
	}
	accID := sm.acctIndexes[tid].accID
	return SnapshotList[accID]
}

func (sm *SnapshotMeta) MergeTableInfo(SnapshotList map[uint32][]types.TS) error {
	sm.Lock()
	sm.Unlock()
	if len(sm.tables) == 0 {
		return nil
	}
	for accID, tables := range sm.tables {
		if SnapshotList[accID] == nil {
			for _, table := range tables {
				if !table.deleteAt.IsEmpty() {
					delete(sm.tables[accID], table.tid)
				}
			}
			continue
		}
		for _, table := range tables {
			if !table.deleteAt.IsEmpty() && !isSnapshotRefers(table, SnapshotList[accID]) {
				logutil.Infof("MergeTableInfo: %d, create %v, drop %v", table.tid, table.createAt.ToString(), table.deleteAt.ToString())
				delete(sm.tables[accID], table.tid)
			}
		}
	}
	return nil
}

func isSnapshotRefers(table *tableInfo, snapVec []types.TS) bool {
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
