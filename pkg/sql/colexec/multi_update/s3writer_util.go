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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
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
	if isDelete {
		seqnums = nil
	}
	blockWriter, err := blockio.NewBlockWriterNew(s3, obj, writer.schemaVersions[idx], seqnums)
	if err != nil {
		return nil, err
	}

	if writer.sortIdxs[idx] > -1 {
		blockWriter.SetSortKey(uint16(writer.sortIdxs[idx]))
	}

	if isDelete {
		blockWriter.SetDataType(objectio.SchemaTombstone)
		if writer.pkIdxs[idx] > -1 {
			blockWriter.SetPrimaryKeyWithType(
				uint16(writer.pkIdxs[idx]),
				index.HBF,
				index.ObjectPrefixFn,
				index.BlockPrefixFn,
			)
		}
	} else {
		blockWriter.SetDataType(objectio.SchemaData)
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
	writer.pkIdxs = append(writer.sortIdxs, pkIdx)
	writer.schemaVersions = append(writer.schemaVersions, tableDef.Version)
	writer.isClusterBys = append(writer.isClusterBys, tableDef.ClusterBy != nil)
	if tableDef.Partition == nil {
		writer.deleteBlockInfo[thisIdx] = make([]*batch.Batch, 1)
		writer.insertBlockInfo[thisIdx] = make([]*batch.Batch, 1)
	} else {
		partitionCount := len(tableDef.Partition.PartitionTableNames)
		writer.deleteBlockInfo[thisIdx] = make([]*batch.Batch, partitionCount)
		writer.insertBlockInfo[thisIdx] = make([]*batch.Batch, partitionCount)
	}
}

// cloneSomeVecFromCompactBatchs  for hidden table. copy vec to new batch
func cloneSomeVecFromCompactBatchs(
	proc *process.Process,
	src *batch.CompactBatchs,
	partitionIdxInBatch int,
	getPartitionIdx int,
	cols []int) ([]*batch.Batch, error) {

	var err error
	var newBat *batch.Batch
	bats := make([]*batch.Batch, src.Length())
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
			oldBat := src.Get(i)
			rid2pid := vector.MustFixedColWithTypeCheck[int32](oldBat.Vecs[partitionIdxInBatch])
			nulls := oldBat.Vecs[partitionIdxInBatch].GetNulls()

			for newColIdx, oldColIdx := range cols {
				typ := oldBat.Vecs[oldColIdx].GetType()
				newBat.Vecs[newColIdx] = vector.NewVec(*typ)
			}

			for rowIdx, partition := range rid2pid {
				if !nulls.Contains(uint64(i)) {
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
			newBat.SetRowCount(newBat.Vecs[0].Length())
			bats[i] = newBat
			newBat = nil
		}
	} else {
		for i := 0; i < src.Length(); i++ {
			newBat = batch.NewWithSize(len(cols))
			oldBat := src.Get(i)
			for newColIdx, oldColIdx := range cols {
				newBat.Vecs[newColIdx], err = oldBat.Vecs[oldColIdx].Dup(proc.GetMPool())
				if err != nil {
					return nil, err
				}
			}
			newBat.SetRowCount(newBat.Vecs[0].Length())
			bats[i] = newBat
			newBat = nil
		}
	}

	return bats, nil
}

// fetchMainTableBatchs for main table. move vec to new batch
func fetchMainTableBatchs(
	proc *process.Process,
	src *batch.CompactBatchs,
	partitionIdxInBatch int,
	getPartitionIdx int,
	cols []int) ([]*batch.Batch, error) {
	var err error
	var newBat *batch.Batch
	mp := proc.GetMPool()
	retBats := make([]*batch.Batch, src.Length())
	srcBats := src.TakeBatchs()
	defer func() {
		for i, bat := range srcBats {
			for _, vec := range bat.Vecs {
				if vec != nil {
					vec.Free(mp)
				}
			}
			srcBats[i] = nil
		}
		srcBats = nil

		if err != nil {
			for _, bat := range retBats {
				bat.Clean(mp)
			}
			retBats = nil

			if newBat != nil {
				newBat.Clean(mp)
			}
			newBat = nil
		}
	}()

	if partitionIdxInBatch > -1 {
		expect := int32(getPartitionIdx)
		for i, oldBat := range srcBats {
			newBat = batch.NewWithSize(len(cols))
			rid2pid := vector.MustFixedColWithTypeCheck[int32](oldBat.Vecs[partitionIdxInBatch])
			nulls := oldBat.Vecs[partitionIdxInBatch].GetNulls()

			for newColIdx, oldColIdx := range cols {
				typ := oldBat.Vecs[oldColIdx].GetType()
				newBat.Vecs[newColIdx] = vector.NewVec(*typ)
			}

			for rowIdx, partition := range rid2pid {
				if !nulls.Contains(uint64(i)) {
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
			newBat.SetRowCount(newBat.Vecs[0].Length())
			retBats[i] = newBat
			newBat = nil
		}
	} else {
		for i, oldBat := range srcBats {
			newBat = batch.NewWithSize(len(cols))
			for j, idx := range cols {
				oldVec := oldBat.Vecs[idx]
				srcBats[i].ReplaceVector(oldVec, nil)
				newBat.Vecs[j] = oldVec
			}
			newBat.SetRowCount(newBat.Vecs[0].Length())
			retBats[i] = newBat
		}
	}

	return retBats, nil
}

func syncThenGetBlockInfoAndStats(proc *process.Process, blockWriter *blockio.BlockWriter, sortIdx int) ([]objectio.BlockInfo, objectio.ObjectStats, error) {
	blocks, _, err := blockWriter.Sync(proc.Ctx)
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
