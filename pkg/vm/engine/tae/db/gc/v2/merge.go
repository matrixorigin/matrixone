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

package v2

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type tableOffset struct {
	offset int
	end    int
}

func MergeCheckpoint(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	ckpEntries []*checkpoint.CheckpointEntry,
	gcTable *GCTable,
	pool *mpool.MPool,
) ([]string, string, error) {
	objects := gcTable.getObjects()
	tombstones := gcTable.getTombstones()
	ckpData := logtail.NewCheckpointData(sid, pool)
	datas := make([]*logtail.CheckpointData, 0)
	deleteFiles := make([]string, 0)
	for _, ckpEntry := range ckpEntries {
		logutil.Info("[MergeCheckpoint]",
			zap.String("checkpoint", ckpEntry.String()))
		_, data, err := logtail.LoadCheckpointEntriesFromKey(context.Background(), sid, fs,
			ckpEntry.GetLocation(), ckpEntry.GetVersion(), nil, &types.TS{})
		if err != nil {
			return nil, "", err
		}
		datas = append(datas, data)
		nameMeta := blockio.EncodeCheckpointMetadataFileName(
			checkpoint.CheckpointDir, checkpoint.PrefixMetadata,
			ckpEntry.GetStart(), ckpEntry.GetEnd())
		deleteFiles = append(deleteFiles, nameMeta)
	}
	defer func() {
		for _, data := range datas {
			data.Close()
		}
		ckpData.Close()
	}()
	if len(datas) == 0 {
		return nil, "", nil
	}
	objs := make(map[string]*types.TS)
	{
		ins := datas[len(datas)-1].GetObjectBatchs()
		tombstone := datas[len(datas)-1].GetTombstoneObjectBatchs()
		insDropTs := vector.MustFixedColWithTypeCheck[types.TS](
			ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
		tombstoneDropTs := vector.MustFixedColWithTypeCheck[types.TS](
			tombstone.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
		for i := 0; i < ins.Length(); i++ {
			var objectStats objectio.ObjectStats
			buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
			objectStats.UnMarshal(buf)
			if objects[objectStats.ObjectName().String()] == nil {
				continue
			}
			objs[objectStats.ObjectName().String()] = &insDropTs[i]
		}
		for i := 0; i < tombstone.Length(); i++ {
			var objectStats objectio.ObjectStats
			buf := tombstone.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
			objectStats.UnMarshal(buf)
			if tombstones[objectStats.ObjectName().String()] == nil {
				continue
			}
			objs[objectStats.ObjectName().String()] = &tombstoneDropTs[i]
		}
	}

	for n, data := range datas {
		ins := data.GetObjectBatchs()
		tombstone := data.GetTombstoneObjectBatchs()
		insDropTs := vector.MustFixedColWithTypeCheck[types.TS](
			ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
		tombstoneDropTs := vector.MustFixedColWithTypeCheck[types.TS](
			tombstone.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
		for i := 0; i < ins.Length(); i++ {
			var objectStats objectio.ObjectStats
			buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
			objectStats.UnMarshal(buf)
			if objects[objectStats.ObjectName().String()] == nil {
				continue
			}
			if n == len(datas)-1 &&
				objectStats.GetAppendable() &&
				insDropTs[i].IsEmpty() &&
				objs[objectStats.ObjectName().String()].IsEmpty() {
				ins.GetVectorByName(catalog.EntryNode_DeleteAt).Update(
					i, ckpEntries[len(ckpEntries)-1].GetEnd(), false)
			}
			appendValToBatch(ins, ckpData.GetObjectBatchs(), i)
		}
		for i := 0; i < tombstone.Length(); i++ {
			var objectStats objectio.ObjectStats
			buf := tombstone.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
			objectStats.UnMarshal(buf)
			if tombstones[objectStats.ObjectName().String()] == nil {
				continue
			}
			if n == len(datas)-1 &&
				objectStats.GetAppendable() &&
				tombstoneDropTs[i].IsEmpty() &&
				objs[objectStats.ObjectName().String()].IsEmpty() {
				tombstone.GetVectorByName(catalog.EntryNode_DeleteAt).Update(
					i, ckpEntries[len(ckpEntries)-1].GetEnd(), false)
			}
			appendValToBatch(tombstone, ckpData.GetTombstoneObjectBatchs(), i)
		}
	}

	mergePool := dbutils.MakeDefaultSmallPool("merge-checkpoint-pool")
	defer mergePool.Destory()
	_, err := mergesort.SortBlockColumns(ckpData.GetObjectBatchs().Vecs, 4, mergePool)
	if err != nil {
		return nil, "", err
	}

	_, err = mergesort.SortBlockColumns(ckpData.GetTombstoneObjectBatchs().Vecs, 4, mergePool)
	if err != nil {
		return nil, "", err
	}

	tableInsertOff := make(map[uint64]*tableOffset)
	tableTombstoneOff := make(map[uint64]*tableOffset)
	for i := 0; i < ckpData.GetObjectBatchs().Vecs[0].Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ckpData.GetObjectBatchs().GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		tid := ckpData.GetObjectBatchs().GetVectorByName(catalog.SnapshotAttr_TID).Get(i).(uint64)
		logutil.Infof("merge object %v tid is %d", objectStats.ObjectName().String(), tid)
		if tableInsertOff[tid] == nil {
			tableInsertOff[tid] = &tableOffset{
				offset: i,
				end:    i,
			}
		}
		tableInsertOff[tid].end += 1
	}
	for i := 0; i < ckpData.GetTombstoneObjectBatchs().Vecs[0].Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ckpData.GetTombstoneObjectBatchs().GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		tid := ckpData.GetTombstoneObjectBatchs().GetVectorByName(catalog.SnapshotAttr_TID).Get(i).(uint64)
		logutil.Infof("merge tombstone %v tid is %d", objectStats.ObjectName().String(), tid)
		if tableTombstoneOff[tid] == nil {
			tableTombstoneOff[tid] = &tableOffset{
				offset: i,
				end:    i,
			}
		}
		tableTombstoneOff[tid].end += 1
	}

	for tid, table := range tableInsertOff {
		ckpData.UpdateObjectInsertMeta(tid, int32(table.offset), int32(table.end))
	}
	for tid, table := range tableTombstoneOff {
		ckpData.UpdateTombstoneInsertMeta(tid, int32(table.offset), int32(table.end))
	}
	cnLocation, tnLocation, _, err := ckpData.WriteTo(
		fs, logtail.DefaultCheckpointBlockRows, logtail.DefaultCheckpointSize,
	)
	if err != nil {
		return nil, "", err
	}
	end := ckpEntries[len(ckpEntries)-1].GetEnd()
	bat := makeBatchFromSchema(checkpoint.CheckpointSchema)
	bat.GetVectorByName(checkpoint.CheckpointAttr_StartTS).Append(ckpEntries[0].GetEnd(), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_EndTS).Append(end, false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_MetaLocation).Append([]byte(cnLocation), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_EntryType).Append(false, false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Version).Append(ckpEntries[len(ckpEntries)-1].GetVersion(), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_AllLocations).Append([]byte(tnLocation), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_CheckpointLSN).Append(uint64(0), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_TruncateLSN).Append(uint64(0), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Type).Append(int8(checkpoint.ET_Global), false)
	defer bat.Close()
	name := blockio.EncodeCheckpointMetadataFileName(checkpoint.CheckpointDir, checkpoint.PrefixMetadata, ckpEntries[0].GetStart(), end)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, name, fs)
	if err != nil {
		return nil, "", err
	}
	if _, err = writer.Write(containers.ToCNBatch(bat)); err != nil {
		return nil, "", err
	}

	// TODO: checkpoint entry should maintain the location
	_, err = writer.WriteEnd(ctx)
	logutil.Infof("write checkpoint %s", name)
	return deleteFiles, name, err
}

func makeBatchFromSchema(schema *catalog.Schema) *containers.Batch {
	bat := containers.NewBatch()
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], common.CheckpointAllocator))
	}
	return bat
}

func appendValToBatch(src, dst *containers.Batch, row int) {
	for v, vec := range src.Vecs {
		val := vec.Get(row)
		if val == nil {
			dst.Vecs[v].Append(val, true)
		} else {
			dst.Vecs[v].Append(val, false)
		}
	}
}
