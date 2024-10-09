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
	"fmt"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	InsertWriteS3Threshold uint64 = 128 * mpool.MB
	DeleteWriteS3Threshold uint64 = 16 * mpool.MB

	TagS3SizeForMOLogger uint64 = 1 * mpool.MB

	RowIDIdx = 0
	PkIdx    = 1
)

type deleteBlockData struct {
	bitmap *nulls.Nulls
	typ    int8
	bat    *batch.Batch
}

type deleteBlockInfo struct {
	name string
	bat  *batch.Batch
}

func newDeleteBlockData(inputBatch *batch.Batch, pkIdx int) *deleteBlockData {
	data := &deleteBlockData{
		bitmap: nulls.NewWithSize(int(options.DefaultBlockMaxRows)),
		typ:    deletion.RawRowIdBatch,
		bat:    newDeleteBatch(inputBatch, pkIdx),
	}
	return data
}

type s3Writer struct {
	cacheBatchs *batch.CompactBatchs
	segmentMap  map[string]int32

	action actionType

	updateCtxs     []*MultiUpdateCtx
	seqnums        [][]uint16
	sortIdxs       []int
	pkIdxs         []int
	schemaVersions []uint32
	isClusterBys   []bool

	deleteBlockMap  [][]map[types.Blockid]*deleteBlockData
	deleteBlockInfo [][]*deleteBlockInfo
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
		segmentMap:     update.SegmentMap,
		seqnums:        make([][]uint16, 0, tableCount),
		sortIdxs:       make([]int, 0, tableCount),
		pkIdxs:         make([]int, 0, tableCount),
		schemaVersions: make([]uint32, 0, tableCount),
		isClusterBys:   make([]bool, 0, tableCount),

		deleteBuf:       make([]*batch.Batch, tableCount),
		insertBuf:       make([]*batch.Batch, tableCount),
		deleteBlockInfo: make([][]*deleteBlockInfo, tableCount),
		insertBlockInfo: make([][]*batch.Batch, tableCount),
		deleteBlockMap:  make([][]map[types.Blockid]*deleteBlockData, tableCount),
	}

	var thisUpdateCtxs []*MultiUpdateCtx
	var mainUpdateCtx *MultiUpdateCtx
	for _, updateCtx := range update.MultiUpdateCtx {
		if updateCtx.TableType == UpdateMainTable {
			mainUpdateCtx = updateCtx
			break
		}
	}
	for _, updateCtx := range update.MultiUpdateCtx {
		if updateCtx.TableType != UpdateMainTable {
			thisUpdateCtxs = append(thisUpdateCtxs, updateCtx)
			appendCfgToWriter(writer, updateCtx.TableDef)
		}
	}
	// main table allways at the end for s3writer.updateCtxs
	// because main table will write to s3 at last
	if mainUpdateCtx != nil {
		// only insert into hidden table
		thisUpdateCtxs = append(thisUpdateCtxs, mainUpdateCtx)
		appendCfgToWriter(writer, mainUpdateCtx.TableDef)
	}
	writer.updateCtxs = thisUpdateCtxs

	upCtx := thisUpdateCtxs[len(thisUpdateCtxs)-1]
	if len(upCtx.DeleteCols) > 0 && len(upCtx.InsertCols) > 0 {
		//update
		writer.action = actionUpdate
		writer.flushThreshold = InsertWriteS3Threshold
		writer.checkSizeCols = append(writer.checkSizeCols, upCtx.InsertCols...)
	} else if len(upCtx.InsertCols) > 0 {
		//insert
		writer.action = actionInsert
		writer.flushThreshold = InsertWriteS3Threshold
		writer.checkSizeCols = append(writer.checkSizeCols, upCtx.InsertCols...)
	} else {
		//delete
		writer.action = actionDelete
		writer.flushThreshold = DeleteWriteS3Threshold
		writer.checkSizeCols = append(writer.checkSizeCols, upCtx.DeleteCols...)
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

func (writer *s3Writer) prepareDeleteBatchs(proc *process.Process, idx int, partIdx int, src []*batch.Batch) ([]*batch.Batch, error) {
	// split delete batchs by BlockID
	if writer.deleteBlockMap[idx][partIdx] == nil {
		writer.deleteBlockMap[idx][partIdx] = make(map[types.Blockid]*deleteBlockData)
	}
	blockMap := writer.deleteBlockMap[idx][partIdx]

	for _, bat := range src {
		rowIDVec := bat.GetVector(RowIDIdx)
		if rowIDVec.IsConstNull() {
			continue
		}
		nulls := rowIDVec.GetNulls()
		if nulls.Count() == bat.RowCount() {
			continue
		}

		rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](rowIDVec)
		for i, rowID := range rowIDs {
			if nulls.Contains(uint64(i)) {
				continue
			}

			blkid := rowID.CloneBlockID()
			segid := rowID.CloneSegmentID()
			// blkOffset := rowID.GetBlockOffset()
			rowOffset := rowID.GetRowOffset()

			if blockMap[blkid] == nil {
				blockMap[blkid] = newDeleteBlockData(bat, 1)
				strSegid := string(segid[:])
				if writer.segmentMap[strSegid] == colexec.TxnWorkSpaceIdType {
					blockMap[blkid].typ = deletion.RawBatchOffset
				} else if writer.segmentMap[strSegid] == colexec.CnBlockIdType {
					blockMap[blkid].typ = deletion.CNBlockOffset
				} else {
					blockMap[blkid].typ = deletion.RawRowIdBatch
				}
			}

			block := blockMap[blkid]

			bitmap := block.bitmap
			if bitmap.Contains(uint64(rowOffset)) {
				continue
			} else {
				bitmap.Add(uint64(rowOffset))
			}

			vector.AppendFixed(block.bat.GetVector(RowIDIdx), rowID, false, proc.GetMPool())
			block.bat.GetVector(PkIdx).UnionOne(bat.GetVector(PkIdx), int64(i), proc.GetMPool())
			block.bat.SetRowCount(block.bat.Vecs[0].Length())
		}
	}

	//collect batchs that can be flush
	blkids := make([]types.Blockid, 0, len(blockMap))
	for blkid, data := range blockMap {
		//Don't flush rowids belong to uncommitted cn block and raw data batch in txn's workspace.
		if data.typ != deletion.RawRowIdBatch {
			continue
		}
		blkids = append(blkids, blkid)
	}
	slices.SortFunc(blkids, func(a, b types.Blockid) int {
		return a.Compare(&b)
	})
	deleteBats := batch.NewCompactBatchs()
	for _, blkid := range blkids {
		bat := blockMap[blkid].bat
		//todo deleteBats can take bat's ownership
		err := deleteBats.Push(proc.GetMPool(), bat)
		if err != nil {
			return nil, err
		}
		bat.Clean(proc.GetMPool())
		delete(blockMap, blkid)
	}
	return deleteBats.TakeBatchs(), nil
}

func (writer *s3Writer) sortAndSync(proc *process.Process) (err error) {
	var bats []*batch.Batch
	onlyDelete := writer.action == actionDelete
	for i, updateCtx := range writer.updateCtxs {
		parititionCount := len(updateCtx.PartitionTableIDs)

		// delete s3
		if len(updateCtx.DeleteCols) > 0 {
			if parititionCount == 0 {
				// normal table
				if onlyDelete && updateCtx.TableType == UpdateMainTable {
					bats, err = fetchMainTableBatchs(proc, writer.cacheBatchs, -1, 0, updateCtx.DeleteCols)
				} else {
					bats, err = cloneSomeVecFromCompactBatchs(proc, writer.cacheBatchs, -1, 0, updateCtx.DeleteCols)
				}
				if err != nil {
					return
				}

				var delBatchs []*batch.Batch
				delBatchs, err = writer.prepareDeleteBatchs(proc, i, 0, bats)
				if err != nil {
					return
				}

				err = writer.sortAndSyncOneTable(proc, i, 0, true, delBatchs)
				if err != nil {
					return
				}
			} else {
				// partition table
				lastIdx := parititionCount - 1
				for getPartitionIdx := range parititionCount {
					if onlyDelete && updateCtx.TableType == UpdateMainTable && getPartitionIdx == lastIdx {
						bats, err = fetchMainTableBatchs(proc, writer.cacheBatchs, updateCtx.PartitionIdx, getPartitionIdx, updateCtx.DeleteCols)
					} else {
						bats, err = cloneSomeVecFromCompactBatchs(proc, writer.cacheBatchs, updateCtx.PartitionIdx, getPartitionIdx, updateCtx.DeleteCols)
					}
					if err != nil {
						return
					}

					var delBatchs []*batch.Batch
					delBatchs, err = writer.prepareDeleteBatchs(proc, i, getPartitionIdx, bats)
					if err != nil {
						return
					}
					err = writer.sortAndSyncOneTable(proc, i, int16(getPartitionIdx), true, delBatchs)
					if err != nil {
						return
					}
				}
			}
		}

		// insert s3
		if len(updateCtx.InsertCols) > 0 {
			if parititionCount == 0 {
				// normal table
				if updateCtx.TableType == UpdateMainTable {
					bats, err = fetchMainTableBatchs(proc, writer.cacheBatchs, -1, 0, updateCtx.InsertCols)
				} else {
					bats, err = cloneSomeVecFromCompactBatchs(proc, writer.cacheBatchs, -1, 0, updateCtx.InsertCols)
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
					if updateCtx.TableType == UpdateMainTable && getPartitionIdx == lastIdx {
						bats, err = fetchMainTableBatchs(proc, writer.cacheBatchs, updateCtx.PartitionIdx, getPartitionIdx, updateCtx.InsertCols)
					} else {
						bats, err = cloneSomeVecFromCompactBatchs(proc, writer.cacheBatchs, updateCtx.PartitionIdx, getPartitionIdx, updateCtx.InsertCols)
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
	for _, bat := range writer.cacheBatchs.TakeBatchs() {
		bat.Clean(proc.GetMPool())
	}
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

		if isDelete {
			return writer.fillDeleteBlockInfo(proc, idx, partitionIdx, objStats)
		} else {
			return writer.fillInsertBlockInfo(proc, idx, partitionIdx, blockInfos, objStats)
		}
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
	sinker := func(bat *batch.Batch) error {
		_, err := blockWriter.WriteBatch(bat)
		return err
	}

	err = colexec.MergeSortBatches(bats, sortIndx, buf, sinker, proc.GetMPool())
	if err != nil {
		return
	}
	blockInfos, objStats, err = syncThenGetBlockInfoAndStats(proc, blockWriter, sortIndx)
	if err != nil {
		return
	}

	if isDelete {
		return writer.fillDeleteBlockInfo(proc, idx, partitionIdx, objStats)
	} else {
		return writer.fillInsertBlockInfo(proc, idx, partitionIdx, blockInfos, objStats)
	}
}

func (writer *s3Writer) fillDeleteBlockInfo(
	proc *process.Process,
	idx int,
	partitionIdx int16,
	objStats objectio.ObjectStats) (err error) {

	// init buf
	if writer.deleteBlockInfo[idx][partitionIdx] == nil {
		attrs := []string{catalog.ObjectMeta_ObjectStats}
		blockInfoBat := batch.NewWithSize(len(attrs))
		blockInfoBat.Attrs = attrs
		blockInfoBat.Vecs[0] = vector.NewVec(types.T_text.ToType())
		writer.deleteBlockInfo[idx][partitionIdx] = &deleteBlockInfo{
			name: "",
			bat:  blockInfoBat,
		}
	}

	targetBloInfo := writer.deleteBlockInfo[idx][partitionIdx]

	objId := objStats.ObjectName().ObjectId()[:]
	targetBloInfo.name = fmt.Sprintf("%s|%d", objId, deletion.FlushDeltaLoc)

	if err = vector.AppendBytes(
		targetBloInfo.bat.GetVector(0), objStats.Marshal(), false, proc.GetMPool()); err != nil {
		return
	}
	targetBloInfo.bat.SetRowCount(targetBloInfo.bat.Vecs[0].Length())
	return
}

func (writer *s3Writer) fillInsertBlockInfo(
	proc *process.Process,
	idx int,
	partitionIdx int16,
	blockInfos []objectio.BlockInfo,
	objStats objectio.ObjectStats) (err error) {

	// init buf
	if writer.insertBlockInfo[idx][partitionIdx] == nil {
		attrs := []string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
		blockInfoBat := batch.NewWithSize(len(attrs))
		blockInfoBat.Attrs = attrs
		blockInfoBat.Vecs[0] = vector.NewVec(types.T_text.ToType())
		blockInfoBat.Vecs[1] = vector.NewVec(types.T_binary.ToType())
		writer.insertBlockInfo[idx][partitionIdx] = blockInfoBat
	}

	targetBat := writer.insertBlockInfo[idx][partitionIdx]
	for _, blkInfo := range blockInfos {
		if err = vector.AppendBytes(
			targetBat.Vecs[0],
			objectio.EncodeBlockInfo(&blkInfo),
			false,
			proc.GetMPool()); err != nil {
			return
		}
	}

	if err = vector.AppendBytes(targetBat.Vecs[1],
		objStats.Marshal(), false, proc.GetMPool()); err != nil {
		return
	}
	targetBat.SetRowCount(targetBat.Vecs[0].Length())

	return
}

func (writer *s3Writer) flushTailAndWriteToWorkspace(proc *process.Process, update *MultiUpdate) (err error) {
	onlyDelete := writer.action == actionDelete
	if !onlyDelete && writer.batchSize > TagS3SizeForMOLogger {
		//write tail batch to s3
		err = writer.sortAndSync(proc)
		if err != nil {
			return
		}
	}

	//write delete block info to workspace
	for i, partBlockInfos := range writer.deleteBlockInfo {
		if len(partBlockInfos) == 1 {
			// normal table
			if partBlockInfos[0] != nil && partBlockInfos[0].bat.RowCount() > 0 {
				err = writer.updateCtxs[i].Source.Delete(proc.Ctx, partBlockInfos[0].bat, partBlockInfos[0].name)
				if err != nil {
					return
				}
			}
		} else {
			// partition table
			for partIdx, blockInfo := range partBlockInfos {
				if blockInfo != nil && blockInfo.bat.RowCount() > 0 {
					err = writer.updateCtxs[i].PartitionSources[partIdx].Delete(proc.Ctx, blockInfo.bat, blockInfo.name)
					if err != nil {
						return
					}
				}
			}
		}
	}
	//write delete batch (which not flush to s3) to workspace
	for i, blocks := range writer.deleteBlockMap {
		if len(blocks) == 1 {
			// normal table
			for blockID, blockData := range blocks[0] {
				name := fmt.Sprintf("%s|%d", blockID, blockData.typ)
				err = writer.updateCtxs[i].Source.Delete(proc.Ctx, blockData.bat, name)
				if err != nil {
					return
				}
			}
		} else {
			// partition table
			for partIdx, blockDatas := range blocks {
				for blockID, blockData := range blockDatas {
					name := fmt.Sprintf("%s|%d", blockID, blockData.typ)
					err = writer.updateCtxs[i].PartitionSources[partIdx].Delete(proc.Ctx, blockData.bat, name)
					if err != nil {
						return
					}
				}
			}
		}
	}

	//write insert block info to workspace
	for i, bats := range writer.insertBlockInfo {
		if len(bats) == 1 {
			// normal table
			if bats[0] != nil && bats[0].RowCount() > 0 {
				resetMergeBlockForOldCN(proc, bats[0])
				err = writer.updateCtxs[i].Source.Write(proc.Ctx, bats[0])
				if err != nil {
					return
				}
			}
		} else {
			// partition table
			for partIdx, bat := range bats {
				if bat != nil && bat.RowCount() > 0 {
					resetMergeBlockForOldCN(proc, bat)
					err = writer.updateCtxs[i].PartitionSources[partIdx].Write(proc.Ctx, bat)
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
	for _, partBlockInfos := range writer.deleteBlockInfo {
		for _, blockInfo := range partBlockInfos {
			if blockInfo != nil {
				blockInfo.bat.CleanOnlyData()
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
	for _, partBlockInfos := range writer.deleteBlockInfo {
		for _, blockInfo := range partBlockInfos {
			if blockInfo != nil {
				blockInfo.bat.Clean(proc.Mp())
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
