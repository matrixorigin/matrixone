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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

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
) (deleteFiles, newFiles []string, checkpointEntry *checkpoint.CheckpointEntry, ckpData *batch.Batch, err error) {
	ckpData = ckputil.NewObjectListBatch()
	datas := make([]*logtail.CKPReader, 0)
	deleteFiles = make([]string, 0)
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
		var data *logtail.CKPReader
		var locations map[string]objectio.Location
		if _, data, err = logtail.LoadCheckpointEntriesFromKey(
			ctx,
			sid,
			fs,
			ckpEntry.GetLocation(),
			ckpEntry.GetVersion(),
			nil,
			&types.TS{},
		); err != nil {
			return
		}
		datas = append(datas, data)
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
		deleteFiles = append(deleteFiles, nameMeta)
		// add checkpoint idx file to deleteFiles
		deleteFiles = append(deleteFiles, ckpEntry.GetLocation().Name().String())
		locations, err = logtail.LoadCheckpointLocations(
			ctx, sid, data,
		)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				deleteFiles = append(deleteFiles, nameMeta)
				continue
			}
			return
		}

		for name := range locations {
			deleteFiles = append(deleteFiles, name)
		}
	}
	if len(datas) == 0 {
		return
	}

	newFiles = make([]string, 0)

	// merge objects referenced by sansphot and pitr
	for _, data := range datas {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		default:
		}
		var objectBatch *batch.Batch
		if objectBatch, err = data.GetCheckpointData(ctx); err != nil {
			return
		}
		defer objectBatch.Clean(common.CheckpointAllocator)
		statsVec := objectBatch.Vecs[ckputil.TableObjectsAttr_ID_Idx]
		bf.Test(statsVec,
			func(exists bool, i int) {
				if !exists {
					return
				}
				appendValToBatchForObjectListBatch(objectBatch, ckpData, i, pool)
			})
	}

	sinker := ckputil.NewDataSinker(pool, fs)
	if err = sinker.Write(ctx, ckpData); err != nil {
		return
	}
	ckpWriter := logtail.NewCheckpointDataWithSinker(sinker, pool)
	var cnLocation, tnLocation objectio.Location
	var files []string
	if cnLocation, tnLocation, files, err = ckpWriter.WriteTo(ctx, fs); err != nil {
		return
	}

	newFiles = append(newFiles, files...)
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

func appendValToBatchForObjectListBatch(src, dst *batch.Batch, row int, mp *mpool.MPool) {
	if src.Vecs[ckputil.TableObjectsAttr_Accout_Idx].IsNull(uint64(row)) {
		vector.AppendFixed(dst.Vecs[ckputil.TableObjectsAttr_Accout_Idx], uint32(0), true, mp)
	} else {
		account := vector.GetFixedAtNoTypeCheck[uint32](src.Vecs[ckputil.TableObjectsAttr_Accout_Idx], row)
		vector.AppendFixed(dst.Vecs[ckputil.TableObjectsAttr_Accout_Idx], account, false, mp)
	}
	db := vector.GetFixedAtNoTypeCheck[uint64](src.Vecs[ckputil.TableObjectsAttr_DB_Idx], row)
	vector.AppendFixed(dst.Vecs[ckputil.TableObjectsAttr_DB_Idx], db, false, mp)
	tbl := vector.GetFixedAtNoTypeCheck[uint64](src.Vecs[ckputil.TableObjectsAttr_Table_Idx], row)
	vector.AppendFixed(dst.Vecs[ckputil.TableObjectsAttr_Table_Idx], tbl, false, mp)
	objType := vector.GetFixedAtNoTypeCheck[int8](src.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx], row)
	vector.AppendFixed(dst.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx], objType, false, mp)
	stats := src.Vecs[ckputil.TableObjectsAttr_ID_Idx].GetBytesAt(row)
	vector.AppendBytes(dst.Vecs[ckputil.TableObjectsAttr_ID_Idx], stats, false, mp)
	createAt := vector.GetFixedAtNoTypeCheck[types.TS](src.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], row)
	vector.AppendFixed(dst.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], createAt, false, mp)
	deleteAt := vector.GetFixedAtNoTypeCheck[types.TS](src.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], row)
	vector.AppendFixed(dst.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], deleteAt, false, mp)
	cluster := src.Vecs[ckputil.TableObjectsAttr_Cluster_Idx].GetBytesAt(row)
	vector.AppendBytes(dst.Vecs[ckputil.TableObjectsAttr_Cluster_Idx], cluster, false, mp)
	dst.SetRowCount(dst.Vecs[0].Length())
}
