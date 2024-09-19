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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	InsertWriteS3Threshold uint64 = 128 * mpool.MB
	DeleteWriteS3Threshold uint64 = 16 * mpool.MB

	TagS3SizeForMOLogger uint64 = 1 * mpool.MB
)

type s3Writer struct {
	cacheBatchs *batch.CompactBatchs

	action actionType

	updateCtxs     []*MultiUpdateCtx
	seqnums        [][]uint16
	sortIdxs       []int
	pkIdxs         []int
	schemaVersions []uint32
	isClusterBys   []bool

	deleteBlockInfo [][]*batch.Batch
	insertBlockInfo [][]*batch.Batch

	deleteBuf []*batch.Batch
	insertBuf []*batch.Batch

	batchSize      uint64
	flushThreshold uint64

	checkSizeCols []int
}

func newS3Writer(update *MultiUpdate) (*s3Writer, error) {
	tableCount := len(update.MultiUpdateCtx)
	writer := &s3Writer{
		cacheBatchs:    batch.NewCompactBatchs(),
		seqnums:        make([][]uint16, 0, tableCount),
		sortIdxs:       make([]int, 0, tableCount),
		pkIdxs:         make([]int, 0, tableCount),
		schemaVersions: make([]uint32, 0, tableCount),
		isClusterBys:   make([]bool, 0, tableCount),

		deleteBuf:       make([]*batch.Batch, tableCount),
		insertBuf:       make([]*batch.Batch, tableCount),
		deleteBlockInfo: make([][]*batch.Batch, tableCount),
		insertBlockInfo: make([][]*batch.Batch, tableCount),
	}

	var thisUpdateCtxs []*MultiUpdateCtx
	var mainUpdateCtx *MultiUpdateCtx
	for _, updateCtx := range update.MultiUpdateCtx {
		if updateCtx.tableType == updateMainTable {
			mainUpdateCtx = updateCtx
			break
		}
	}
	for _, updateCtx := range update.MultiUpdateCtx {
		if updateCtx.tableType != updateMainTable {
			thisUpdateCtxs = append(thisUpdateCtxs, updateCtx)
			appendCfgToWriter(writer, updateCtx.tableDef)
		}
	}
	// main table allways at the end for s3writer.updateCtxs
	// because main table will write to s3 at last
	thisUpdateCtxs = append(thisUpdateCtxs, mainUpdateCtx)
	appendCfgToWriter(writer, mainUpdateCtx.tableDef)
	writer.updateCtxs = thisUpdateCtxs

	if len(mainUpdateCtx.deleteCols) > 0 && len(mainUpdateCtx.insertCols) > 0 {
		//update
		writer.action = actionUpdate
		writer.flushThreshold = InsertWriteS3Threshold
		writer.checkSizeCols = append(writer.checkSizeCols, mainUpdateCtx.insertCols...)
	} else if len(mainUpdateCtx.insertCols) > 0 {
		//insert
		writer.action = actionInsert
		writer.flushThreshold = InsertWriteS3Threshold
		writer.checkSizeCols = append(writer.checkSizeCols, mainUpdateCtx.insertCols...)
	} else {
		//delete
		writer.action = actionDelete
		writer.flushThreshold = DeleteWriteS3Threshold
		writer.checkSizeCols = append(writer.checkSizeCols, mainUpdateCtx.deleteCols...)
	}

	return writer, nil
}

func (writer *s3Writer) append(proc *process.Process, inBatch *batch.Batch) (err error) {
	err = writer.cacheBatchs.Push(proc.Mp(), inBatch)
	if err != nil {
		return
	}
	for _, idx := range writer.checkSizeCols {
		writer.batchSize += uint64(inBatch.Vecs[idx].Size())
	}

	if writer.batchSize >= writer.flushThreshold {
		err = writer.sortAndSync(proc)
	}
	return
}

func (writer *s3Writer) sortAndSync(proc *process.Process) (err error) {
	var bats []*batch.Batch
	onlyDelete := writer.action == actionDelete
	for i, updateCtx := range writer.updateCtxs {
		parititionCount := len(updateCtx.partitionTableIDs)

		// delete s3
		if len(updateCtx.deleteCols) > 0 {
			if parititionCount == 0 {
				// normal table
				if onlyDelete && updateCtx.tableType == updateMainTable {
					bats, err = fetchMainTableBatchs(proc, writer.cacheBatchs, -1, 0, updateCtx.deleteCols)
				} else {
					bats, err = cloneSomeVecFromCompactBatchs(proc, writer.cacheBatchs, -1, 0, updateCtx.deleteCols)
				}
				if err != nil {
					return
				}
				err = writer.sortAndSyncOneTable(proc, i, 0, true, bats)
				if err != nil {
					return
				}
			} else {
				// partition table
				lastIdx := parititionCount - 1
				for getPartitionIdx := range parititionCount {
					if onlyDelete && updateCtx.tableType == updateMainTable && getPartitionIdx == lastIdx {
						bats, err = fetchMainTableBatchs(proc, writer.cacheBatchs, updateCtx.partitionIdx, getPartitionIdx, updateCtx.deleteCols)
					} else {
						bats, err = cloneSomeVecFromCompactBatchs(proc, writer.cacheBatchs, updateCtx.partitionIdx, getPartitionIdx, updateCtx.deleteCols)
					}
					if err != nil {
						return
					}
					err = writer.sortAndSyncOneTable(proc, i, int16(getPartitionIdx), true, bats)
					if err != nil {
						return
					}
				}
			}
		}

		// insert s3
		if len(updateCtx.insertCols) > 0 {
			if parititionCount == 0 {
				// normal table
				if updateCtx.tableType == updateMainTable {
					bats, err = fetchMainTableBatchs(proc, writer.cacheBatchs, -1, 0, updateCtx.insertCols)
				} else {
					bats, err = cloneSomeVecFromCompactBatchs(proc, writer.cacheBatchs, -1, 0, updateCtx.insertCols)
				}
				if err != nil {
					return
				}
				err = writer.sortAndSyncOneTable(proc, i, 0, false, bats)
				if err != nil {
					return
				}
			} else {
				// partition table
				lastIdx := parititionCount - 1
				for getPartitionIdx := range parititionCount {
					if updateCtx.tableType == updateMainTable && getPartitionIdx == lastIdx {
						bats, err = fetchMainTableBatchs(proc, writer.cacheBatchs, updateCtx.partitionIdx, getPartitionIdx, updateCtx.insertCols)
					} else {
						bats, err = cloneSomeVecFromCompactBatchs(proc, writer.cacheBatchs, updateCtx.partitionIdx, getPartitionIdx, updateCtx.insertCols)
					}
					if err != nil {
						return
					}
					err = writer.sortAndSyncOneTable(proc, i, int16(getPartitionIdx), false, bats)
					if err != nil {
						return
					}
				}
			}
		}
	}

	writer.batchSize = 0
	return
}

func (writer *s3Writer) sortAndSyncOneTable(
	proc *process.Process,
	idx int,
	partitionIdx int16,
	isDelete bool,
	bats []*batch.Batch) (err error) {
	var blockWriter *blockio.BlockWriter
	var blockInfos []objectio.BlockInfo
	var objStats objectio.ObjectStats

	sortIndx := writer.sortIdxs[idx]
	if isDelete {
		sortIndx = 0
	}
	if sortIndx == -1 {
		blockWriter, err = generateBlockWriter(writer, proc, idx, isDelete)
		if err != nil {
			return
		}

		for i := range bats {
			_, err = blockWriter.WriteBatch(bats[i])
			if err != nil {
				return
			}
			bats[i].Clean(proc.GetMPool())
			bats[i] = nil
		}
		blockInfos, objStats, err = syncThenGetBlockInfoAndStats(proc, blockWriter, sortIndx)
		if err != nil {
			return
		}
		return writer.fillBlockInfoBat(proc, idx, partitionIdx, blockInfos, objStats, isDelete)
	}

	// need sort
	isClusterBy := writer.isClusterBys[idx]
	nulls := make([]*nulls.Nulls, len(bats))

	for i := range bats {
		err = colexec.SortByKey(proc, bats[i], sortIndx, isClusterBy, proc.GetMPool())
		if err != nil {
			return
		}
		nulls[i] = bats[i].Vecs[sortIndx].GetNulls()
	}

	merge := colexec.GetNewMergeFromBatchs(bats, sortIndx, nulls)
	blockWriter, err = generateBlockWriter(writer, proc, idx, isDelete)
	if err != nil {
		return
	}

	var buf *batch.Batch
	if isDelete {
		if writer.deleteBuf[idx] == nil {
			writer.deleteBuf[idx], err = proc.NewBatchFromSrc(bats[0], colexec.DefaultBatchSize)
			if err != nil {
				return
			}
		}
		buf = writer.deleteBuf[idx]
	} else {
		if writer.insertBuf[idx] == nil {
			writer.insertBuf[idx], err = proc.NewBatchFromSrc(bats[0], colexec.DefaultBatchSize)
			if err != nil {
				return
			}
		}
		buf = writer.insertBuf[idx]
	}
	lens := 0
	size := len(bats)
	buf.CleanOnlyData()
	var batchIndex int
	var rowIndex int
	for size > 0 {
		batchIndex, rowIndex, size = merge.GetNextPos()
		for i := range buf.Vecs {
			err = buf.Vecs[i].UnionOne(bats[batchIndex].Vecs[i], int64(rowIndex), proc.GetMPool())
			if err != nil {
				return
			}
		}
		// all data in bats[batchIndex] are used. Clean it.
		if rowIndex+1 == bats[batchIndex].RowCount() {
			bats[batchIndex].Clean(proc.GetMPool())
		}
		lens++
		if lens == colexec.DefaultBatchSize {
			lens = 0
			buf.SetRowCount(colexec.DefaultBatchSize)
			if _, err = blockWriter.WriteBatch(buf); err != nil {
				return
			}
			// force clean
			buf.CleanOnlyData()
		}
	}
	if lens > 0 {
		buf.SetRowCount(lens)
		if _, err = blockWriter.WriteBatch(buf); err != nil {
			return
		}
		buf.CleanOnlyData()
	}

	blockInfos, objStats, err = syncThenGetBlockInfoAndStats(proc, blockWriter, sortIndx)
	if err != nil {
		return
	}
	return writer.fillBlockInfoBat(proc, idx, partitionIdx, blockInfos, objStats, isDelete)
}

func (writer *s3Writer) fillBlockInfoBat(
	proc *process.Process,
	idx int,
	partitionIdx int16,
	blockInfos []objectio.BlockInfo,
	objStats objectio.ObjectStats,
	isDelete bool) (err error) {

	// init buf
	var targetBat *batch.Batch
	if isDelete {
		if writer.deleteBlockInfo[idx][partitionIdx] == nil {
			attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
			blockInfoBat := batch.NewWithSize(len(attrs))
			blockInfoBat.Attrs = attrs
			blockInfoBat.Vecs[0] = vector.NewVec(types.T_int16.ToType())
			blockInfoBat.Vecs[1] = vector.NewVec(types.T_text.ToType())
			blockInfoBat.Vecs[2] = vector.NewVec(types.T_binary.ToType())
			writer.deleteBlockInfo[idx][partitionIdx] = blockInfoBat
		}
		targetBat = writer.deleteBlockInfo[idx][partitionIdx]
	} else {
		if writer.insertBlockInfo[idx][partitionIdx] == nil {
			attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
			blockInfoBat := batch.NewWithSize(len(attrs))
			blockInfoBat.Attrs = attrs
			blockInfoBat.Vecs[0] = vector.NewVec(types.T_int16.ToType())
			blockInfoBat.Vecs[1] = vector.NewVec(types.T_text.ToType())
			blockInfoBat.Vecs[2] = vector.NewVec(types.T_binary.ToType())
			writer.insertBlockInfo[idx][partitionIdx] = blockInfoBat
		}
		targetBat = writer.insertBlockInfo[idx][partitionIdx]

	}

	for _, blkInfo := range blockInfos {
		if err = vector.AppendFixed(
			targetBat.Vecs[0],
			partitionIdx,
			false,
			proc.GetMPool()); err != nil {
			return
		}

		if err = vector.AppendBytes(
			targetBat.Vecs[1],
			objectio.EncodeBlockInfo(blkInfo),
			false,
			proc.GetMPool()); err != nil {
			return
		}
	}

	if err = vector.AppendBytes(targetBat.Vecs[2],
		objStats.Marshal(), false, proc.GetMPool()); err != nil {
		return
	}

	targetBat.SetRowCount(targetBat.Vecs[0].Length())

	return
}

func (writer *s3Writer) flushTailAndWriteToWorkspace(proc *process.Process, update *MultiUpdate) (err error) {

	if writer.batchSize > TagS3SizeForMOLogger {
		//write tail batch to s3
		err = writer.sortAndSync(proc)
		if err != nil {
			return
		}
	}

	//write block info to workspace
	for i, bats := range writer.deleteBlockInfo {
		if len(bats) == 1 {
			// normal table
			if bats[0] != nil && bats[0].RowCount() > 0 {
				err = writer.updateCtxs[i].source.Write(proc.Ctx, bats[0])
				if err != nil {
					return
				}
			}
		} else {
			// partition table
			for partIdx, bat := range bats {
				if bat != nil && bat.RowCount() > 0 {
					err = writer.updateCtxs[i].partitionSources[partIdx].Write(proc.Ctx, bat)
					if err != nil {
						return
					}
				}
			}
		}
	}
	for i, bats := range writer.insertBlockInfo {
		if len(bats) == 1 {
			// normal table
			if bats[0] != nil && bats[0].RowCount() > 0 {
				err = writer.updateCtxs[i].source.Write(proc.Ctx, bats[0])
				if err != nil {
					return
				}
			}
		} else {
			// partition table
			for partIdx, bat := range bats {
				if bat != nil && bat.RowCount() > 0 {
					err = writer.updateCtxs[i].partitionSources[partIdx].Write(proc.Ctx, bat)
					if err != nil {
						return
					}
				}
			}
		}
	}

	//write tail batch to workspace
	bats := writer.cacheBatchs.TakeBatchs()
	for _, bat := range bats {
		err = update.updateOneBatch(proc, bat)
		if err != nil {
			return
		}
	}

	return nil
}

func (writer *s3Writer) reset(proc *process.Process) (err error) {
	bats := writer.cacheBatchs.TakeBatchs()
	for _, bat := range bats {
		bat.Clean(proc.Mp())
	}
	for _, bats := range writer.insertBlockInfo {
		for _, bat := range bats {
			if bat != nil {
				bat.CleanOnlyData()
			}
		}
	}
	for _, bats := range writer.deleteBlockInfo {
		for _, bat := range bats {
			if bat != nil {
				bat.CleanOnlyData()
			}
		}
	}
	for i := range writer.insertBuf {
		if writer.insertBuf[i] != nil {
			writer.insertBuf[i].CleanOnlyData()
		}
	}
	for i := range writer.deleteBuf {
		if writer.deleteBuf[i] != nil {
			writer.deleteBuf[i].CleanOnlyData()
		}
	}
	writer.batchSize = 0
	return
}

func (writer *s3Writer) free(proc *process.Process) (err error) {
	bats := writer.cacheBatchs.TakeBatchs()
	for _, bat := range bats {
		bat.Clean(proc.Mp())
	}
	for _, bats := range writer.insertBlockInfo {
		for _, bat := range bats {
			if bat != nil {
				bat.Clean(proc.Mp())
			}
		}
	}
	writer.insertBlockInfo = nil
	for _, bats := range writer.deleteBlockInfo {
		for _, bat := range bats {
			if bat != nil {
				bat.Clean(proc.Mp())
			}
		}
	}
	writer.deleteBlockInfo = nil

	for i := range writer.insertBuf {
		if writer.insertBuf[i] != nil {
			writer.insertBuf[i].Clean(proc.Mp())
		}
	}
	writer.insertBuf = nil

	for i := range writer.deleteBuf {
		if writer.deleteBuf[i] != nil {
			writer.deleteBuf[i].Clean(proc.Mp())
		}
	}
	writer.deleteBuf = nil

	return
}
