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

package gc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	bf *bloomfilter.BloomFilter,
	end *types.TS,
	pool *mpool.MPool,
) (deleteFiles []string, checkpointEntry *checkpoint.CheckpointEntry, err error) {
	ckpData := logtail.NewCheckpointData(sid, pool)
	datas := make([]*logtail.CheckpointData, 0)
	deleteFiles = make([]string, 0)
	for _, ckpEntry := range ckpEntries {
		logutil.Info("[MergeCheckpoint]",
			zap.String("checkpoint", ckpEntry.String()))
		_, data, err := logtail.LoadCheckpointEntriesFromKey(context.Background(), sid, fs,
			ckpEntry.GetLocation(), ckpEntry.GetVersion(), nil, &types.TS{})
		if err != nil {
			return nil, nil, err
		}
		datas = append(datas, data)
		var nameMeta string
		if ckpEntry.GetType() == checkpoint.ET_Compacted {
			nameMeta = blockio.EncodeCompactedMetadataFileName(
				checkpoint.CheckpointDir, checkpoint.PrefixMetadata,
				ckpEntry.GetStart(), ckpEntry.GetEnd())
		} else {
			nameMeta = blockio.EncodeCheckpointMetadataFileName(
				checkpoint.CheckpointDir, checkpoint.PrefixMetadata,
				ckpEntry.GetStart(), ckpEntry.GetEnd())
		}

		// add checkpoint metafile(ckp/mete_ts-ts.ckp...) to deleteFiles
		deleteFiles = append(deleteFiles, nameMeta)

		locations, err := logtail.LoadCheckpointLocations(
			ctx, sid, ckpEntry.GetTNLocation(), ckpEntry.GetVersion(), fs)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				deleteFiles = append(deleteFiles, nameMeta)
				continue
			}
			return nil, nil, err
		}

		for name := range locations {
			deleteFiles = append(deleteFiles, name)
		}
	}
	defer func() {
		for _, data := range datas {
			data.Close()
		}
		ckpData.Close()
	}()
	if len(datas) == 0 {
		return
	}

	// merge objects referenced by sansphot and pitr
	for _, data := range datas {
		ins := data.GetObjectBatchs()
		tombstone := data.GetTombstoneObjectBatchs()
		bf.Test(ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector(),
			func(exists bool, i int) {
				if !exists {
					return
				}
				appendValToBatch(ins, ckpData.GetObjectBatchs(), i)
			})
		bf.Test(tombstone.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector(),
			func(exists bool, i int) {
				if !exists {
					return
				}
				appendValToBatch(tombstone, ckpData.GetTombstoneObjectBatchs(), i)
			})
	}

	tidColIdx := 4
	objectBatch := containers.ToCNBatch(ckpData.GetObjectBatchs())
	err = mergesort.SortColumnsByIndex(objectBatch.Vecs, tidColIdx, pool)
	if err != nil {
		return
	}

	tombstoneBatch := containers.ToCNBatch(ckpData.GetTombstoneObjectBatchs())
	err = mergesort.SortColumnsByIndex(tombstoneBatch.Vecs, tidColIdx, pool)
	if err != nil {
		return
	}

	// Update checkpoint Dat[meta]
	tableInsertOff := make(map[uint64]*tableOffset)
	tableTombstoneOff := make(map[uint64]*tableOffset)
	tableInsertTid := vector.MustFixedColNoTypeCheck[uint64](
		ckpData.GetObjectBatchs().GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	tableTombstoneTid := vector.MustFixedColNoTypeCheck[uint64](
		ckpData.GetTombstoneObjectBatchs().GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	for i := 0; i < ckpData.GetObjectBatchs().Vecs[0].Length(); i++ {
		tid := tableInsertTid[i]
		{
			// TODO: remove debug log
			var objectStats objectio.ObjectStats
			buf := ckpData.GetObjectBatchs().GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
			objectStats.UnMarshal(buf)
			logutil.Infof("merge object %v tid is %d", objectStats.ObjectName().String(), tid)
		}
		if tableInsertOff[tid] == nil {
			tableInsertOff[tid] = &tableOffset{
				offset: i,
				end:    i,
			}
		}
		tableInsertOff[tid].end += 1
	}
	for i := 0; i < ckpData.GetTombstoneObjectBatchs().Vecs[0].Length(); i++ {
		tid := tableTombstoneTid[i]
		{
			// TODO: remove debug log
			var objectStats objectio.ObjectStats
			buf := ckpData.GetTombstoneObjectBatchs().GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
			objectStats.UnMarshal(buf)
			logutil.Infof("merge tombstone %v tid is %d", objectStats.ObjectName().String(), tid)
		}
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
		return
	}
	bat := makeBatchFromSchema(checkpoint.CheckpointSchema)
	bat.GetVectorByName(checkpoint.CheckpointAttr_StartTS).Append(ckpEntries[0].GetStart(), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_EndTS).Append(*end, false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_MetaLocation).Append([]byte(cnLocation), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_EntryType).Append(false, false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Version).Append(ckpEntries[len(ckpEntries)-1].GetVersion(), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_AllLocations).Append([]byte(tnLocation), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_CheckpointLSN).Append(uint64(0), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_TruncateLSN).Append(uint64(0), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Type).Append(int8(checkpoint.ET_Compacted), false)
	defer bat.Close()
	name := blockio.EncodeCompactedMetadataFileName(checkpoint.CheckpointDir, checkpoint.PrefixMetadata, ckpEntries[0].GetStart(), *end)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, name, fs)
	if err != nil {
		return
	}
	if _, err = writer.Write(containers.ToCNBatch(bat)); err != nil {
		return
	}

	// TODO: checkpoint entry should maintain the location
	_, err = writer.WriteEnd(ctx)
	checkpointEntry = checkpoint.NewCheckpointEntry("", ckpEntries[0].GetStart(), *end, checkpoint.ET_Compacted)
	checkpointEntry.SetLocation(cnLocation, tnLocation)
	checkpointEntry.SetLSN(ckpEntries[len(ckpEntries)-1].LSN(), ckpEntries[len(ckpEntries)-1].GetTruncateLsn())
	checkpointEntry.SetState(checkpoint.ST_Finished)
	checkpointEntry.SetVersion(logtail.CheckpointCurrentVersion)
	return
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
