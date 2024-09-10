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
		logutil.Infof("merge checkpoint %v", ckpEntry.String())
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
	if len(datas) == 0 {
		logutil.Infof("no checkpoint data to merge")
		return nil, "", nil
	}
	for _, data := range datas {
		ins := data.GetObjectBatchs()
		tombstone := data.GetTombstoneObjectBatchs()

		for i := 0; i < ins.Length(); i++ {
			var objectStats objectio.ObjectStats
			buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
			objectStats.UnMarshal(buf)
			if objects[objectStats.ObjectName().String()] == nil {
				continue
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
			appendValToBatch(tombstone, ckpData.GetTombstoneObjectBatchs(), i)
		}
		for i := logtail.DBInsertIDX; i <= logtail.TBLColDeleteIDX; i++ {
			if data.GetOneBatch(i).Vecs[2].Length() > 0 {
				err := ckpData.GetOneBatch(i).Append(data.GetOneBatch(i))
				if err != nil {
					return nil, "", err
				}
			}
		}
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
	bat.GetVectorByName(checkpoint.CheckpointAttr_EndTS).Append(end.Next(), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_MetaLocation).Append([]byte(cnLocation), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_EntryType).Append(false, false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Version).Append(ckpEntries[len(ckpEntries)-1].GetVersion(), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_AllLocations).Append([]byte(tnLocation), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_CheckpointLSN).Append(uint64(0), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_TruncateLSN).Append(uint64(0), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Type).Append(int8(checkpoint.ET_Global), false)
	defer bat.Close()
	name := blockio.EncodeCheckpointMetadataFileName(checkpoint.CheckpointDir, checkpoint.PrefixMetadata, ckpEntries[0].GetStart(), end.Next())
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
