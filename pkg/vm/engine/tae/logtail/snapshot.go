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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"sync"
)

type snapshot struct {
	ts types.TS
	tid uint64
}

type objectInfo struct {
	stats *objectio.ObjectStats
	deltaLocation map[uint32]*objectio.Location
	createAt types.TS
	deleteAt types.TS
	checkpointTS types.TS
}

type SnapshotMeta struct {
	sync.RWMutex
	object map[string]*objectInfo
	tid uint64
}

func (sm *SnapshotMeta)Update(data *CheckpointData) *SnapshotMeta {
	sm.Lock()
	defer sm.Unlock()
	ins := data.GetObjectBatchs()
	insDeleteTSVec := ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector()
	insCreateTSVec := ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	tid := ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector()
	for i:=0; i<ins.Length(); i++ {
		var objectStats *objectio.ObjectStats
		table := vector.GetFixedAt[uint64](types.T_uint64, tid, i)
		if table != sm.tid {
			continue
		}
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		var deleteTS types.TS
		deleteTS = vector.GetFixedAt[types.TS](insDeleteTSVec, i)
		createTS := vector.GetFixedAt[types.TS](insCreateTSVec, i)
		if sm.object[objectStats.ObjectName().String()] == nil {
			if !deleteTS.IsEmpty() {
				continue
			}
			sm.object[objectStats.ObjectName().String()] = &objectInfo{
				stats: objectStats,
				createAt: createTS,
			}
			continue
		}
		if deleteTS.IsEmpty() {
			panic(any("deleteTS is empty"))
		}
		delete(sm.object,objectStats.ObjectName().String())
	}
	return nil
}

func (sm *SnapshotMeta) GetSnapshot(fs fileservice.FileService) (map[uint64][]snapshot,error){
	sm.RLock()
	defer sm.RUnlock()
	snapshotList := make(map[uint64][]snapshot)
	for _, object := range sm.object {
		location := object.stats.ObjectLocation()
		for i := uint32(0); i < object.stats.BlkCnt(); i++ {
			bat, err := blockio.LoadOneBlock(context.Background(), fs, location, objectio.SchemaData)
			if err != nil {
				return nil, err
			}
			if object.deltaLocation[i] == nil {
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
						snapshotList[tid] = make([]snapshot, 0)
					}
					snapshotList[tid] = append(snapshotList[tid], snapshot{ts, tid})
				}
			}
		}
	}
	return snapshotList, nil
}