// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"context"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func generateBlockWriter(writer *s3Writer,
	proc *process.Process, idx int,
	isDelete bool) (*blockio.BlockWriter, error) {
	// Use uuid as segment id
	// TODO: multiple 64m file in one segment
	obj := colexec.Get().GenerateObject()
	s3, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	seqnums := writer.seqnums[idx]
	sortIdx := writer.sortIdxs[idx]
	if isDelete {
		seqnums = nil
		sortIdx = 0
	}
	blockWriter, err := blockio.NewBlockWriterNew(
		s3,
		obj,
		writer.schemaVersions[idx],
		seqnums,
		isDelete,
	)
	if err != nil {
		return nil, err
	}

	if sortIdx > -1 {
		blockWriter.SetSortKey(uint16(sortIdx))
	}

	if isDelete {
		blockWriter.SetPrimaryKeyWithType(
			0,
			index.HBF,
			index.ObjectPrefixFn,
			index.BlockPrefixFn,
		)
	} else {
		if writer.pkIdxs[idx] > -1 {
			blockWriter.SetPrimaryKey(uint16(writer.pkIdxs[idx]))
		}
	}

	return blockWriter, err
}

func appendCfgToWriter(writer *s3Writer, tableDef *plan.TableDef) {
	var seqnums []uint16
	sortIdx := -1
	pkIdx := -1
	for i, colDef := range tableDef.Cols {
		if colDef.Name != catalog.Row_ID {
			seqnums = append(seqnums, uint16(colDef.Seqnum))
		}

		if colDef.Name == tableDef.Pkey.PkeyColName && colDef.Name != catalog.FakePrimaryKeyColName {
			sortIdx = i
			pkIdx = i
		}

		if tableDef.ClusterBy != nil && tableDef.ClusterBy.Name == colDef.Name {
			sortIdx = i
		}
	}

	thisIdx := len(writer.sortIdxs)
	writer.seqnums = append(writer.seqnums, seqnums)
	writer.sortIdxs = append(writer.sortIdxs, sortIdx)
	writer.pkIdxs = append(writer.pkIdxs, pkIdx)
	writer.schemaVersions = append(writer.schemaVersions, tableDef.Version)
	writer.isClusterBys = append(writer.isClusterBys, tableDef.ClusterBy != nil)
	if tableDef.Partition == nil {
		writer.deleteBlockMap[thisIdx] = make([]map[types.Blockid]*deleteBlockData, 1)
		writer.deleteBlockInfo[thisIdx] = make([]*deleteBlockInfo, 1)
		writer.insertBlockInfo[thisIdx] = make([]*batch.Batch, 1)
		writer.insertBlockRowCount[thisIdx] = make([]uint64, 1)
	} else {
		partitionCount := len(tableDef.Partition.PartitionTableNames)
		writer.deleteBlockMap[thisIdx] = make([]map[types.Blockid]*deleteBlockData, partitionCount)
		writer.deleteBlockInfo[thisIdx] = make([]*deleteBlockInfo, partitionCount)
		writer.insertBlockInfo[thisIdx] = make([]*batch.Batch, partitionCount)
		writer.insertBlockRowCount[thisIdx] = make([]uint64, partitionCount)
	}
}

// cloneSomeVecFromCompactBatchs  copy some vectors to new batch
// clean these batchs after used
func cloneSomeVecFromCompactBatchs(
	proc *process.Process,
	src *batch.CompactBatchs,
	partitionIdxInBatch int,
	getPartitionIdx int,
	cols []int,
	attrs []string,
	sortIdx int) ([]*batch.Batch, error) {

	var err error
	var newBat *batch.Batch
	bats := make([]*batch.Batch, 0, src.Length())
	var sortNulls *nulls.Nulls

	defer func() {
		if err != nil {
			for _, bat := range bats {
				if bat != nil {
					bat.Clean(proc.GetMPool())
				}
			}
			if newBat != nil {
				newBat.Clean(proc.GetMPool())
			}
		}
	}()

	if partitionIdxInBatch > -1 {
		expect := int32(getPartitionIdx)
		for i := 0; i < src.Length(); i++ {
			newBat = batch.NewWithSize(len(cols))
			newBat.Attrs = attrs
			oldBat := src.Get(i)
			rid2pid := vector.MustFixedColWithTypeCheck[int32](oldBat.Vecs[partitionIdxInBatch])
			nulls := oldBat.Vecs[partitionIdxInBatch].GetNulls()
			if sortIdx > -1 && oldBat.Vecs[cols[sortIdx]].HasNull() {
				sortNulls = oldBat.Vecs[cols[sortIdx]].GetNulls()
			}

			for newColIdx, oldColIdx := range cols {
				typ := oldBat.Vecs[oldColIdx].GetType()
				newBat.Vecs[newColIdx] = vector.NewVec(*typ)
			}
			err = newBat.PreExtend(proc.GetMPool(), colexec.DefaultBatchSize)
			if err != nil {
				return nil, err
			}

			for rowIdx, partition := range rid2pid {
				if !nulls.Contains(uint64(rowIdx)) {
					if sortNulls != nil && sortNulls.Contains(uint64(rowIdx)) {
						continue
					}

					if partition == -1 {
						return nil, moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
					} else if partition == expect {
						for newColIdx, oldColIdx := range cols {
							if err = newBat.Vecs[newColIdx].UnionOne(oldBat.Vecs[oldColIdx], int64(rowIdx), proc.GetMPool()); err != nil {
								return nil, err
							}
						}
					}
				}
			}
			if newBat.Vecs[0].Length() > 0 {
				newBat.SetRowCount(newBat.Vecs[0].Length())
				bats = append(bats, newBat)
			} else {
				newBat.Clean(proc.GetMPool())
			}
			newBat = nil
		}
	} else {
		for i := 0; i < src.Length(); i++ {
			newBat = batch.NewWithSize(len(cols))
			newBat.Attrs = attrs
			oldBat := src.Get(i)

			if sortIdx > -1 && oldBat.Vecs[cols[sortIdx]].HasNull() {
				sortNulls := oldBat.Vecs[cols[sortIdx]].GetNulls()
				for newColIdx, oldColIdx := range cols {
					typ := oldBat.Vecs[oldColIdx].GetType()
					newBat.Vecs[newColIdx] = vector.NewVec(*typ)
				}

				for j := 0; j < oldBat.RowCount(); j++ {
					if !sortNulls.Contains(uint64(j)) {
						for newColIdx, oldColIdx := range cols {
							if err = newBat.Vecs[newColIdx].UnionOne(oldBat.Vecs[oldColIdx], int64(j), proc.GetMPool()); err != nil {
								return nil, err
							}
						}
					}
				}
			} else {
				for newColIdx, oldColIdx := range cols {
					newBat.Vecs[newColIdx], err = oldBat.Vecs[oldColIdx].Dup(proc.GetMPool())
					if err != nil {
						return nil, err
					}
				}
			}

			if newBat.Vecs[0].Length() > 0 {
				newBat.SetRowCount(newBat.Vecs[0].Length())
				bats = append(bats, newBat)
			} else {
				newBat.Clean(proc.GetMPool())
			}
			newBat = nil
		}
	}

	return bats, nil
}

// fetchSomeVecFromCompactBatchs fetch some vectors from CompactBatchs
// do not clean these batchs
func fetchSomeVecFromCompactBatchs(
	proc *process.Process,
	src *batch.CompactBatchs,
	cols []int,
	attrs []string) ([]*batch.Batch, error) {
	mp := proc.GetMPool()
	var newBat *batch.Batch
	retBats := make([]*batch.Batch, src.Length())
	for i := 0; i < src.Length(); i++ {
		oldBat := src.Get(i)
		newBat = batch.NewWithSize(len(cols))
		newBat.Attrs = attrs
		for j, idx := range cols {
			oldVec := oldBat.Vecs[idx]
			//expand constant vector
			if oldVec.IsConst() {
				newVec := vector.NewVec(*oldVec.GetType())
				err := vector.GetUnionAllFunction(*oldVec.GetType(), mp)(newVec, oldVec)
				if err != nil {
					return nil, err
				}
				oldBat.ReplaceVector(oldVec, newVec, 0)
				newBat.Vecs[j] = newVec
			} else {
				newBat.Vecs[j] = oldVec
			}
		}
		newBat.SetRowCount(newBat.Vecs[0].Length())
		retBats[i] = newBat
	}
	return retBats, nil
}

func syncThenGetBlockInfoAndStats(ctx context.Context, blockWriter *blockio.BlockWriter, sortIdx int) ([]objectio.BlockInfo, objectio.ObjectStats, error) {
	blocks, _, err := blockWriter.Sync(ctx)
	if err != nil {
		return nil, objectio.ObjectStats{}, err
	}
	blkInfos := make([]objectio.BlockInfo, 0, len(blocks))
	for j := range blocks {
		blkInfos = append(blkInfos,
			blocks[j].GenerateBlockInfo(blockWriter.GetName(), sortIdx != -1),
		)
	}

	var stats objectio.ObjectStats
	if sortIdx != -1 {
		stats = blockWriter.GetObjectStats(objectio.WithCNCreated(), objectio.WithSorted())
	} else {
		stats = blockWriter.GetObjectStats(objectio.WithCNCreated())
	}
	return blkInfos, stats, err
}

func resetMergeBlockForOldCN(proc *process.Process, bat *batch.Batch) error {
	if bat.Attrs[len(bat.Attrs)-1] != catalog.ObjectMeta_ObjectStats {
		// bat comes from old CN, no object stats vec in it
		bat.Attrs = append(bat.Attrs, catalog.ObjectMeta_ObjectStats)
		bat.Vecs = append(bat.Vecs, vector.NewVec(types.T_binary.ToType()))

		blkVec := bat.Vecs[0]
		destVec := bat.Vecs[1]
		fs, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
		if err != nil {
			logutil.Error("get fs failed when split object stats. ", zap.Error(err))
			return err
		}
		// var objDataMeta objectio.ObjectDataMeta
		var objStats objectio.ObjectStats
		for idx := 0; idx < bat.RowCount(); idx++ {
			blkInfo := objectio.DecodeBlockInfo(blkVec.GetBytesAt(idx))
			objStats, _, err = disttae.ConstructObjStatsByLoadObjMeta(proc.Ctx, blkInfo.MetaLocation(), fs)
			if err != nil {
				return err
			}
			vector.AppendBytes(destVec, objStats.Marshal(), false, proc.GetMPool())
		}

		vector.AppendBytes(destVec, objStats.Marshal(), false, proc.GetMPool())
	}
	return nil
}
