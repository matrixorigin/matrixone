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
	object map[string]*objectInfo
	tid    uint64
}

func NewSnapshotMeta() *SnapshotMeta {
	return &SnapshotMeta{
		object: make(map[string]*objectInfo),
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
		if sm.object[objectStats.ObjectName().SegmentId().ToString()] == nil {
			if !deleteTS.IsEmpty() {
				continue
			}
			sm.object[objectStats.ObjectName().SegmentId().ToString()] = &objectInfo{
				stats:    objectStats,
				createAt: createTS,
			}
			continue
		}
		if deleteTS.IsEmpty() {
			panic(any("deleteTS is empty"))
		}
		delete(sm.object, objectStats.ObjectName().SegmentId().ToString())
	}

	del, _, _, _ := data.GetBlkBatchs()
	delBlockIDVec := del.GetVectorByName(catalog2.BlockMeta_ID).GetDownstreamVector()
	delDeltaVec := del.GetVectorByName(catalog2.BlockMeta_DeltaLoc).GetDownstreamVector()
	for i := 0; i < del.Length(); i++ {
		blockID := vector.GetFixedAt[types.Blockid](delBlockIDVec, i)
		deltaLoc := vector.GetFixedAt[objectio.Location](delDeltaVec, i)
		if sm.object[blockID.Segment().ToString()] != nil {
			sm.object[blockID.Segment().ToString()].deltaLocation[uint32(blockID.Sequence())] = &deltaLoc
		}
	}
	return nil
}

func (sm *SnapshotMeta) GetSnapshot(fs fileservice.FileService) (map[uint64][]types.TS, error) {
	sm.RLock()
	defer sm.RUnlock()
	snapshotList := make(map[uint64][]types.TS)
	for _, object := range sm.object {
		location := object.stats.ObjectLocation()
		for i := uint32(0); i < object.stats.BlkCnt(); i++ {
			bat, err := blockio.LoadOneBlock(context.Background(), fs, location, objectio.SchemaData)
			if err != nil {
				return nil, err
			}
			if object.deltaLocation[i] == nil {
				for r := 0; r < bat.Vecs[0].Length(); r++ {
					tid := vector.GetFixedAt[uint64](bat.Vecs[0], r)
					ts := vector.GetFixedAt[types.TS](bat.Vecs[1], r)
					if len(snapshotList[tid]) == 0 {
						snapshotList[tid] = make([]types.TS, 0)
					}
					snapshotList[tid] = append(snapshotList[tid], ts)
				}
				continue
			}
			deletes, err := blockio.LoadOneBlock(context.Background(), fs, *object.deltaLocation[i], objectio.SchemaTombstone)
			if err != nil {
				return nil, err
			}
			blockID := objectio.BuildObjectBlockid(object.stats.ObjectName(), uint16(i))
			deleteRows := blockio.EvalDeleteRowsByTimestamp(deletes, object.checkpointTS, blockID)
			if deleteRows == nil {
				continue
			}
			for n := range bat.Vecs {
				bat.Vecs[n].Shrink(deleteRows.ToI64Arrary(), true)
				for r := 0; r < bat.Vecs[n].Length(); r++ {
					tid := vector.GetFixedAt[uint64](bat.Vecs[0], r)
					ts := vector.GetFixedAt[types.TS](bat.Vecs[1], r)
					if len(snapshotList[tid]) == 0 {
						snapshotList[tid] = make([]types.TS, 0)
					}
					snapshotList[tid] = append(snapshotList[tid], ts)
				}
			}
		}
	}
	return snapshotList, nil
}

func (sm *SnapshotMeta) SetTid(tid uint64) {
	sm.tid = tid
}

func (sm *SnapshotMeta) SaveMeta(name string, fs fileservice.FileService) error {
	if len(sm.object) == 0 {
		return nil
	}
	bat := containers.NewBatch()
	for i, attr := range objectInfoSchemaAttr {
		bat.AddVector(attr, containers.MakeVector(objectInfoSchemaTypes[i], common.DefaultAllocator))
	}
	for _, entry := range sm.object {
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
		if sm.object[objectStats.ObjectName().SegmentId().ToString()] == nil {
			sm.object[objectStats.ObjectName().SegmentId().ToString()] = &objectInfo{
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
