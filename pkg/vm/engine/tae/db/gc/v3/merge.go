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
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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
	taskName string,
	sid string,
	ckpEntries []*checkpoint.CheckpointEntry,
	bf *bloomfilter.BloomFilter,
	end *types.TS,
	client checkpoint.Runner,
	pool *mpool.MPool,
	fs fileservice.FileService,
) (deleteFiles,
	newFiles []string,
	checkpointEntry *checkpoint.CheckpointEntry,
	ckpData *logtail.CheckpointData,
	err error) {
	if len(ckpEntries) == 0 {
		return
	}
	ckpData = logtail.NewCheckpointData(sid, pool)
	deleteSet := make(map[string]struct{})
	for _, ckpEntry := range ckpEntries {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		default:
		}
		logutil.Info(
			"GC-Merge-Checkpoint",
			zap.String("task", taskName),
			zap.String("entry", ckpEntry.String()),
		)
		err = processCheckpointEntry(ctx, taskName, ckpEntry, sid, fs, bf, ckpData, deleteSet)
		if err != nil {
			return
		}
	}

	deleteFiles = make([]string, 0, len(deleteSet))
	for k := range deleteSet {
		deleteFiles = append(deleteFiles, k)
	}
	newFiles = make([]string, 0)

	tidColIdx := 4
	objectBatch := containers.ToCNBatch(ckpData.GetObjectBatchs())
	err = mergeutil.SortColumnsByIndex(objectBatch.Vecs, tidColIdx, pool)
	if err != nil {
		return
	}

	tombstoneBatch := containers.ToCNBatch(ckpData.GetTombstoneObjectBatchs())
	err = mergeutil.SortColumnsByIndex(tombstoneBatch.Vecs, tidColIdx, pool)
	if err != nil {
		return
	}

	// Update checkpoint Dat[meta]
	tableInsertOff := make(map[uint64]*tableOffset)
	tableTombstoneOff := make(map[uint64]*tableOffset)
	processTIDs(ckpData.GetObjectBatchs(), tableInsertOff)
	processTIDs(ckpData.GetTombstoneObjectBatchs(), tableTombstoneOff)

	for tid, table := range tableInsertOff {
		ckpData.UpdateObjectInsertMeta(tid, int32(table.offset), int32(table.end))
	}
	for tid, table := range tableTombstoneOff {
		ckpData.UpdateTombstoneInsertMeta(tid, int32(table.offset), int32(table.end))
	}
	cnLocation, tnLocation, files, err := ckpData.WriteTo(
		ctx, logtail.DefaultCheckpointBlockRows, logtail.DefaultCheckpointSize, fs,
	)
	if err != nil {
		return
	}

	newFiles = append(newFiles, files...)
	bat := buildCheckpointMeta(ckpEntries, end, &cnLocation, &tnLocation)
	defer bat.Close()
	name := ioutil.EncodeCompactCKPMetadataFullName(ckpEntries[0].GetStart(), *end)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, name, fs)
	if err != nil {
		return
	}
	if _, err = writer.Write(containers.ToCNBatch(bat)); err != nil {
		return
	}

	// TODO: checkpoint entry should maintain the location
	_, err = writer.WriteEnd(ctx)
	if err != nil {
		return
	}
	_, tsFile := ioutil.TryDecodeTSRangeFile(name)
	client.AddCheckpointMetaFile(tsFile.GetName())
	checkpointEntry = checkpoint.NewCheckpointEntry("", ckpEntries[0].GetStart(), *end, checkpoint.ET_Compacted)
	checkpointEntry.SetLocation(cnLocation, tnLocation)
	checkpointEntry.SetLSN(ckpEntries[len(ckpEntries)-1].LSN(), ckpEntries[len(ckpEntries)-1].GetTruncateLsn())
	checkpointEntry.SetState(checkpoint.ST_Finished)
	checkpointEntry.SetVersion(logtail.CheckpointCurrentVersion)
	newFiles = append(newFiles, name)
	return
}

func processTIDs(batch *containers.Batch, offsetMap map[uint64]*tableOffset) {
	tidVec := vector.MustFixedColNoTypeCheck[uint64](
		batch.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	for i := 0; i < len(tidVec); i++ {
		tid := tidVec[i]
		if offsetMap[tid] == nil {
			offsetMap[tid] = &tableOffset{offset: i, end: i}
		}
		offsetMap[tid].end += 1
	}
}

func processCheckpointEntry(
	ctx context.Context,
	taskName string,
	ckpEntry *checkpoint.CheckpointEntry,
	sid string,
	fs fileservice.FileService,
	bf *bloomfilter.BloomFilter,
	ckpData *logtail.CheckpointData,
	deleteSet map[string]struct{},
) error {
	var nameMeta string
	if ckpEntry.GetType() == checkpoint.ET_Compacted {
		nameMeta = ioutil.EncodeCompactCKPMetadataFullName(
			ckpEntry.GetStart(), ckpEntry.GetEnd(),
		)
	} else {
		nameMeta = ioutil.EncodeCKPMetadataFullName(
			ckpEntry.GetStart(), ckpEntry.GetEnd(),
		)
	}
	// add checkpoint metafile(ckp/mete_ts-ts.ckp...) to deleteFiles
	deleteSet[nameMeta] = struct{}{}
	// add checkpoint idx file to deleteFiles
	deleteSet[ckpEntry.GetLocation().Name().String()] = struct{}{}
	locations, err := logtail.LoadCheckpointLocations(
		ctx, sid, ckpEntry.GetTNLocation(), ckpEntry.GetVersion(), fs,
	)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			logutil.Warn(
				"GC-MERGE-CHECKPOINT-FILE-NOTFOUND",
				zap.String("task", taskName),
				zap.String("filename", nameMeta),
			)
			return nil
		}
		return err
	}
	for name := range locations {
		deleteSet[name] = struct{}{}
	}

	var data *logtail.CheckpointData
	if _, data, err = logtail.LoadCheckpointEntriesFromKey(
		ctx,
		sid,
		fs,
		ckpEntry.GetLocation(),
		ckpEntry.GetVersion(),
		nil,
		&types.TS{},
	); err != nil {
		return err
	}
	defer data.Close()

	ins := data.GetObjectBatchs()
	tombstone := data.GetTombstoneObjectBatchs()

	bf.Test(ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector(), func(exists bool, i int) {
		appendValToBatch(ins, ckpData.GetObjectBatchs(), i)
	})
	bf.Test(tombstone.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector(), func(exists bool, i int) {
		appendValToBatch(tombstone, ckpData.GetTombstoneObjectBatchs(), i)
	})

	return nil
}

func buildCheckpointMeta(
	ckpEntries []*checkpoint.CheckpointEntry,
	end *types.TS,
	cnLocation, tnLocation *objectio.Location,
) *containers.Batch {
	bat := makeBatchFromSchema(checkpoint.CheckpointSchema)
	bat.GetVectorByName(checkpoint.CheckpointAttr_StartTS).Append(
		ckpEntries[0].GetStart(), false,
	)
	bat.GetVectorByName(checkpoint.CheckpointAttr_EndTS).Append(
		*end, false,
	)
	bat.GetVectorByName(checkpoint.CheckpointAttr_MetaLocation).Append(
		[]byte(*cnLocation), false,
	)
	bat.GetVectorByName(checkpoint.CheckpointAttr_EntryType).Append(
		false, false,
	)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Version).Append(
		ckpEntries[len(ckpEntries)-1].GetVersion(), false,
	)
	bat.GetVectorByName(checkpoint.CheckpointAttr_AllLocations).Append(
		[]byte(*tnLocation), false,
	)
	bat.GetVectorByName(checkpoint.CheckpointAttr_CheckpointLSN).Append(
		uint64(0), false,
	)
	bat.GetVectorByName(checkpoint.CheckpointAttr_TruncateLSN).Append(
		uint64(0), false,
	)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Type).Append(
		int8(checkpoint.ET_Compacted), false,
	)
	return bat
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
