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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"sync"
)

var (
	objectInfoSchemaAttr = []string{
		catalog.ObjectAttr_ObjectStats,
		catalog.EntryNode_CreateAt,
		catalog.EntryNode_DeleteAt,
		catalog2.BlockMeta_DeltaLoc,
	}
	objectInfoSchemaTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, 5000, 0),
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
	objects map[objectio.Segmentid]*objectInfo
	tid     uint64
}

func NewSnapshotMeta() *SnapshotMeta {
	return &SnapshotMeta{
		objects: make(map[objectio.Segmentid]*objectInfo),
	}
}

func (sm *SnapshotMeta) Update(data *CheckpointData) *SnapshotMeta {
	sm.Lock()
	defer sm.Unlock()
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
	delDeltaVec := del.GetVectorByName(catalog2.BlockMeta_DeltaLoc).GetDownstreamVector()
	for i := 0; i < del.Length(); i++ {
		blockID := vector.GetFixedAt[types.Blockid](delBlockIDVec, i)
		deltaLoc := vector.GetFixedAt[objectio.Location](delDeltaVec, i)
		if sm.objects[*blockID.Segment()] != nil {
			sm.objects[*blockID.Segment()].deltaLocation[uint32(blockID.Sequence())] = &deltaLoc
		}
	}
	return nil
}

func (sm *SnapshotMeta) GetSnapshot(ctx context.Context, fs fileservice.FileService, mp *mpool.MPool) (map[uint64]containers.Vector, error) {
	sm.RLock()
	objects := sm.objects
	sm.RUnlock()
	snapshotList := make(map[uint64]containers.Vector)
	idxes := []uint16{0, 1}
	colTypes := []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
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
			for r := 0; r < bat.Vecs[0].Length(); r++ {
				tid := vector.GetFixedAt[uint64](bat.Vecs[0], r)
				ts := vector.GetFixedAt[types.TS](bat.Vecs[1], r)
				if snapshotList[tid] == nil {
					snapshotList[tid] = containers.MakeVector(colTypes[1], mp)
				}
				err = vector.AppendFixed[types.TS](snapshotList[tid].GetDownstreamVector(), ts, false, mp)
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
		bat.AddVector(attr, containers.MakeVector(objectInfoSchemaTypes[i], common.DefaultAllocator))
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

func (sm *SnapshotMeta) Rebuild(ins *containers.Batch) {
	insCreateTSVec := ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	for i := 0; i < ins.Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		createTS := vector.GetFixedAt[types.TS](insCreateTSVec, i)
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
	bs, err := reader.LoadAllBlocks(ctx, common.DefaultAllocator)
	if err != nil {
		return err
	}
	idxes := make([]uint16, len(objectInfoSchemaAttr))
	for i := range objectInfoSchemaAttr {
		idxes[i] = uint16(i)
	}
	mobat, release, err := reader.LoadColumns(ctx, idxes, nil, bs[0].GetID(), common.DefaultAllocator)
	if err != nil {
		return err
	}
	defer release()
	bat := containers.NewBatch()
	for i := range objectInfoSchemaAttr {
		pkgVec := mobat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(objectInfoSchemaTypes[i], common.DefaultAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.DefaultAllocator)
		}
		bat.AddVector(objectInfoSchemaAttr[i], vec)
	}
	sm.Rebuild(bat)
	return nil
}

func CloseSnapshotList(snapshots map[uint64]containers.Vector) {
	for _, snapshot := range snapshots {
		snapshot.Close()
	}
}
