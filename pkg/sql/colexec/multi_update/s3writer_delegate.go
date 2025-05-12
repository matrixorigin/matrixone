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
	"bytes"
	"fmt"
	"slices"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	InsertWriteS3Threshold uint64 = 128 * mpool.MB
	DeleteWriteS3Threshold uint64 = 64 * mpool.MB

	TagS3SizeForMOLogger uint64 = 8 * mpool.MB

	RowIDIdx = 0
	PkIdx    = 1
)

var (
	DeleteBatchAttrs = []string{catalog.Row_ID, "pk"}
)

type deleteBlockData struct {
	bitmap *nulls.Nulls
	typ    int8
	bat    *batch.Batch
}

type deleteBlockInfo struct {
	name        string
	bat         *batch.Batch
	rawRowCount uint64
}

func newDeleteBlockData(inputBatch *batch.Batch, pkIdx int) *deleteBlockData {
	data := &deleteBlockData{
		bitmap: nulls.NewWithSize(int(options.DefaultBlockMaxRows)),
		typ:    deletion.DeletionOnCommitted,
		bat:    newDeleteBatch(inputBatch, pkIdx),
	}
	return data
}

type s3WriterDelegate struct {
	cacheBatchs *batch.CompactBatchs
	segmentMap  map[string]int32

	action   actionType
	isRemote bool

	updateCtxs     []*MultiUpdateCtx
	updateCtxInfos map[string]*updateCtxInfo
	seqnums        [][]uint16
	sortIdxs       []int
	pkIdxs         []int
	schemaVersions []uint32
	isClusterBys   []bool

	deleteBlockMap      [][]map[types.Blockid]*deleteBlockData
	deleteBlockInfo     [][]*deleteBlockInfo
	insertBlockInfo     [][]*batch.Batch
	insertBlockRowCount [][]uint64

	deleteBuf []*batch.Batch
	insertBuf []*batch.Batch

	outputBat *batch.Batch

	batchSize      uint64
	flushThreshold uint64

	checkSizeCols []int
	buf           bytes.Buffer
}

func newS3Writer(update *MultiUpdate) (*s3WriterDelegate, error) {
	tableCount := len(update.MultiUpdateCtx)
	writer := &s3WriterDelegate{
		cacheBatchs:    batch.NewCompactBatchs(objectio.BlockMaxRows),
		updateCtxInfos: update.ctr.updateCtxInfos,
		seqnums:        make([][]uint16, 0, tableCount),
		sortIdxs:       make([]int, 0, tableCount),
		pkIdxs:         make([]int, 0, tableCount),
		schemaVersions: make([]uint32, 0, tableCount),
		isClusterBys:   make([]bool, 0, tableCount),

		deleteBuf:           make([]*batch.Batch, tableCount),
		insertBuf:           make([]*batch.Batch, tableCount),
		deleteBlockInfo:     make([][]*deleteBlockInfo, tableCount),
		insertBlockInfo:     make([][]*batch.Batch, tableCount),
		insertBlockRowCount: make([][]uint64, tableCount),
		deleteBlockMap:      make([][]map[types.Blockid]*deleteBlockData, tableCount),
		isRemote:            update.IsRemote,
	}

	mainIdx := 0
	for i, updateCtx := range update.MultiUpdateCtx {
		if update.ctr.updateCtxInfos[updateCtx.TableDef.Name].tableType == UpdateMainTable {
			mainIdx = i
		}
		appendCfgToWriter(writer, updateCtx.TableDef)
	}
	writer.updateCtxs = update.MultiUpdateCtx

	upCtx := writer.updateCtxs[mainIdx]
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

func (writer *s3WriterDelegate) append(proc *process.Process, analyzer process.Analyzer, inBatch *batch.Batch) (err error) {
	err = writer.cacheBatchs.Extend(proc.Mp(), inBatch)
	if err != nil {
		return
	}
	for _, idx := range writer.checkSizeCols {
		writer.batchSize += uint64(inBatch.Vecs[idx].Size())
	}

	if writer.batchSize >= writer.flushThreshold {
		err = writer.sortAndSync(proc, analyzer)
	}
	return
}

func (writer *s3WriterDelegate) prepareDeleteBatchs(
	proc *process.Process,
	idx int,
	partIdx int,
	src []*batch.Batch,
	needClean bool) ([]*batch.Batch, error) {

	defer func() {
		if needClean {
			for _, bat := range src {
				bat.Clean(proc.GetMPool())
			}
		}
	}()

	// split delete batchs by BlockID
	if writer.deleteBlockMap[idx][partIdx] == nil {
		writer.deleteBlockMap[idx][partIdx] = make(map[types.Blockid]*deleteBlockData)
	}
	blockMap := writer.deleteBlockMap[idx][partIdx]

	for _, bat := range src {
		rowIDVec := bat.GetVector(RowIDIdx)
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
				err := blockMap[blkid].bat.PreExtend(proc.GetMPool(), colexec.DefaultBatchSize)
				if err != nil {
					return nil, err
				}

				if colexec.IsDeletionOnTxnUnCommit(writer.segmentMap, &segid) {
					blockMap[blkid].typ = deletion.DeletionOnTxnUnCommit
				} else {
					blockMap[blkid].typ = deletion.DeletionOnCommitted
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
		if data.typ == deletion.DeletionOnTxnUnCommit {
			continue
		}
		blkids = append(blkids, blkid)
	}
	slices.SortFunc(blkids, func(a, b types.Blockid) int {
		return a.Compare(&b)
	})
	deleteBats := batch.NewCompactBatchs(objectio.BlockMaxRows)
	for _, blkid := range blkids {
		bat := blockMap[blkid].bat
		delete(blockMap, blkid)
		err := deleteBats.Push(proc.GetMPool(), bat)
		if err != nil {
			return nil, err
		}
	}
	retBatchs := deleteBats.TakeBatchs()
	return retBatchs, nil
}

func (writer *s3WriterDelegate) sortAndSync(proc *process.Process, analyzer process.Analyzer) (err error) {
	var bats []*batch.Batch
	for i, updateCtx := range writer.updateCtxs {
		// delete s3
		if len(updateCtx.DeleteCols) > 0 {
			var delBatchs []*batch.Batch
			bats, err = fetchSomeVecFromCompactBatchs(writer.cacheBatchs, updateCtx.DeleteCols, DeleteBatchAttrs)
			if err != nil {
				return
			}
			delBatchs, err = writer.prepareDeleteBatchs(proc, i, 0, bats, false)
			if err != nil {
				return
			}
			err = writer.sortAndSyncOneTable(proc, updateCtx.TableDef, analyzer, i, 0, true, delBatchs, true, true)
			if err != nil {
				return
			}
		}

		// insert s3
		if len(updateCtx.InsertCols) > 0 {
			insertAttrs := writer.updateCtxInfos[updateCtx.TableDef.Name].insertAttrs
			isClusterBy := writer.isClusterBys[i]
			needClone := writer.updateCtxInfos[updateCtx.TableDef.Name].tableType != UpdateMainTable //uk&sk need clone
			if !needClone && writer.sortIdxs[i] > -1 {
				sortIdx := updateCtx.InsertCols[writer.sortIdxs[i]]
				for j := 0; j < writer.cacheBatchs.Length(); j++ {
					needSortBat := writer.cacheBatchs.Get(j)
					if needSortBat.GetVector(int32(sortIdx)).HasNull() {
						needClone = true
						break
					}
				}
			}

			var needCleanBatch, needSortBatch bool
			if needClone {
				// cluster by do not check if sort vector is null
				sortIdx := writer.sortIdxs[i]
				if isClusterBy {
					sortIdx = -1
				}
				bats, err = cloneSomeVecFromCompactBatchs(proc, writer.cacheBatchs, updateCtx.InsertCols, insertAttrs, sortIdx)
				needSortBatch = true
				needCleanBatch = true
			} else {
				if writer.sortIdxs[i] > -1 {
					sortIdx := updateCtx.InsertCols[writer.sortIdxs[i]]
					for j := 0; j < writer.cacheBatchs.Length(); j++ {
						needSortBat := writer.cacheBatchs.Get(j)
						err = colexec.SortByKey(proc, needSortBat, sortIdx, isClusterBy, proc.GetMPool())
						if err != nil {
							return
						}
					}
				}
				bats, err = fetchSomeVecFromCompactBatchs(writer.cacheBatchs, updateCtx.InsertCols, insertAttrs)
				needSortBatch = false
				needCleanBatch = false
			}

			if err != nil {
				return
			}
			err = writer.sortAndSyncOneTable(
				proc, updateCtx.TableDef, analyzer, i, 0, false, bats, needSortBatch, needCleanBatch)
			if err != nil {
				return
			}
		}
	}

	writer.batchSize = 0
	for _, bat := range writer.cacheBatchs.TakeBatchs() {
		bat.Clean(proc.GetMPool())
	}
	return
}

func (writer *s3WriterDelegate) sortAndSyncOneTable(
	proc *process.Process,
	tblDef *plan.TableDef,
	analyzer process.Analyzer,
	idx int,
	partitionIdx int16,
	isDelete bool,
	bats []*batch.Batch,
	needSortBatch bool,
	needCleanBatch bool) (err error) {

	if len(bats) == 0 {
		return nil
	}

	sortIndex := writer.sortIdxs[idx]
	rowCount := 0
	if isDelete {
		sortIndex = 0
	}

	var (
		fs       fileservice.FileService
		bat      *batch.Batch
		s3Writer *colexec.CNS3Writer
	)

	if fs, err = colexec.GetSharedFSFromProc(proc); err != nil {
		return
	}

	if isDelete {
		pkCol := plan2.PkColByTableDef(tblDef)
		s3Writer = colexec.NewCNS3TombstoneWriter(proc.Mp(), fs, plan2.ExprType2Type(&pkCol.Typ))
	} else {
		s3Writer = colexec.NewCNS3DataWriter(proc.Mp(), fs, tblDef, false)
	}

	defer func() {
		s3Writer.Close(proc.Mp())
	}()

	if sortIndex == -1 {
		for i := range bats {
			rowCount += bats[i].RowCount()
			if err = s3Writer.Write(proc.Ctx, proc.Mp(), bats[i]); err != nil {
				return err
			}

			if needCleanBatch {
				bats[i].Clean(proc.GetMPool())
			}
			bats[i] = nil
		}

		crs := analyzer.GetOpCounterSet()
		newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

		if _, err = s3Writer.Sync(newCtx, proc.Mp()); err != nil {
			return err
		}

		analyzer.AddS3RequestCount(crs)
		analyzer.AddFileServiceCacheInfo(crs)
		analyzer.AddDiskIO(crs)

		if bat, err = s3Writer.FillBlockInfoBat(proc.Mp()); err != nil {
			return err
		}

		return writer.fillInsertBlockInfo(proc, idx, partitionIdx, bat, rowCount)
	}

	// need sort
	isClusterBy := writer.isClusterBys[idx]
	nulls := make([]*nulls.Nulls, len(bats))
	if needSortBatch {
		for i := range bats {
			rowCount += bats[i].RowCount()
			err = colexec.SortByKey(proc, bats[i], sortIndex, isClusterBy, proc.GetMPool())
			if err != nil {
				return
			}
			nulls[i] = bats[i].Vecs[sortIndex].GetNulls()
		}
	} else {
		for i := range bats {
			rowCount += bats[i].RowCount()
			nulls[i] = bats[i].Vecs[sortIndex].GetNulls()
		}
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
		return s3Writer.Write(proc.Ctx, proc.Mp(), bat)
	}

	err = mergeutil.MergeSortBatches(bats, sortIndex, buf, sinker, proc.GetMPool(), needCleanBatch)
	if err != nil {
		return
	}

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

	if _, err = s3Writer.Sync(newCtx, proc.Mp()); err != nil {
		return err
	}

	analyzer.AddS3RequestCount(crs)
	analyzer.AddFileServiceCacheInfo(crs)
	analyzer.AddDiskIO(crs)

	if bat, err = s3Writer.FillBlockInfoBat(proc.Mp()); err != nil {
		return err
	}

	if isDelete {
		return writer.fillDeleteBlockInfo(proc, idx, partitionIdx, bat, rowCount)
	}

	return writer.fillInsertBlockInfo(proc, idx, partitionIdx, bat, rowCount)
}

func (writer *s3WriterDelegate) fillDeleteBlockInfo(
	proc *process.Process,
	idx int,
	partitionIdx int16,
	bat *batch.Batch,
	rowCount int) (err error) {

	// init buf
	if writer.deleteBlockInfo[idx][partitionIdx] == nil {
		blockInfoBat := batch.NewWithSize(bat.VectorCount())
		blockInfoBat.Attrs = bat.Attrs
		for i := 0; i < bat.VectorCount(); i++ {
			blockInfoBat.Vecs[i] = vector.NewVec(*bat.Vecs[i].GetType())
		}
		writer.deleteBlockInfo[idx][partitionIdx] = &deleteBlockInfo{
			name: "",
			bat:  blockInfoBat,
		}
	}

	targetBloInfo := writer.deleteBlockInfo[idx][partitionIdx]

	stats := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(0))
	objId := stats.ObjectName().ObjectId()[:]
	targetBloInfo.name = fmt.Sprintf("%s|%d", objId, deletion.FlushDeltaLoc)
	targetBloInfo.rawRowCount += uint64(rowCount)

	for i := range bat.Vecs {
		row, col := vector.MustVarlenaRawData(bat.Vecs[i])
		for j := range bat.Vecs[i].Length() {
			if err = vector.AppendBytes(
				targetBloInfo.bat.Vecs[i], row[j].GetByteSlice(col), false, proc.GetMPool()); err != nil {
				return err
			}
		}
	}

	targetBloInfo.bat.SetRowCount(targetBloInfo.bat.Vecs[0].Length())

	return
}

func (writer *s3WriterDelegate) fillInsertBlockInfo(
	proc *process.Process,
	idx int,
	partitionIdx int16,
	bat *batch.Batch,
	rowCount int) (err error) {

	// init buf
	if writer.insertBlockInfo[idx][partitionIdx] == nil {
		blockInfoBat := batch.NewWithSize(bat.VectorCount())
		blockInfoBat.Attrs = bat.Attrs
		for i := 0; i < bat.VectorCount(); i++ {
			blockInfoBat.Vecs[i] = vector.NewVec(*bat.Vecs[i].GetType())
		}
		writer.insertBlockInfo[idx][partitionIdx] = blockInfoBat
		writer.insertBlockRowCount[idx][partitionIdx] = 0
	}

	writer.insertBlockRowCount[idx][partitionIdx] += uint64(rowCount)
	targetBat := writer.insertBlockInfo[idx][partitionIdx]

	for i := range bat.Vecs {
		row, col := vector.MustVarlenaRawData(bat.Vecs[i])
		for j := range bat.Vecs[i].Length() {
			if err = vector.AppendBytes(
				targetBat.Vecs[i], row[j].GetByteSlice(col), false, proc.GetMPool()); err != nil {
				return err
			}
		}
	}

	targetBat.SetRowCount(targetBat.Vecs[0].Length())

	return
}

func (writer *s3WriterDelegate) flushTailAndWriteToOutput(proc *process.Process, analyzer process.Analyzer) (err error) {
	if writer.batchSize > TagS3SizeForMOLogger {
		//write tail batch to s3
		err = writer.sortAndSync(proc, analyzer)
		if err != nil {
			return
		}
	}

	if writer.outputBat == nil {
		writer.outputBat = makeS3OutputBatch()
	}
	mp := proc.GetMPool()

	//write delete block info to workspace
	for i, partBlockInfos := range writer.deleteBlockInfo {
		if len(partBlockInfos) == 1 {
			// normal table
			if partBlockInfos[0] != nil && partBlockInfos[0].bat.RowCount() > 0 {
				err = writer.addBatchToOutput(mp, actionDelete, i, 0, partBlockInfos[0].rawRowCount, partBlockInfos[0].name, partBlockInfos[0].bat)
				if err != nil {
					return
				}
			}
		} else {
			// partition table
			for partIdx, blockInfo := range partBlockInfos {
				if blockInfo != nil && blockInfo.bat.RowCount() > 0 {
					err = writer.addBatchToOutput(mp, actionDelete, i, partIdx, blockInfo.rawRowCount, blockInfo.name, blockInfo.bat)
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
			for _, blockData := range blocks[0] {
				name := objectio.PhysicalAddr_Attr
				err = writer.addBatchToOutput(mp, actionDelete, i, 0, uint64(blockData.bat.RowCount()), name, blockData.bat)
			}
		} else {
			// partition table
			for partIdx, blockDatas := range blocks {
				for blockID, blockData := range blockDatas {
					name := fmt.Sprintf("%s|%d", blockID, blockData.typ)
					err = writer.addBatchToOutput(mp, actionDelete, i, partIdx, uint64(blockData.bat.RowCount()), name, blockData.bat)
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
				err = writer.addBatchToOutput(mp, actionInsert, i, 0, writer.insertBlockRowCount[i][0], "", bats[0])
				if err != nil {
					return
				}
			}
		} else {
			// partition table
			for partIdx, bat := range bats {
				if bat != nil && bat.RowCount() > 0 {
					resetMergeBlockForOldCN(proc, bat)
					err = writer.addBatchToOutput(mp, actionInsert, i, partIdx, writer.insertBlockRowCount[i][partIdx], "", bat)
					if err != nil {
						return
					}
				}
			}
		}
	}

	//write tail batch to workspace
	bats := writer.cacheBatchs.TakeBatchs()
	defer func() {
		for i := range bats {
			if bats[i] != nil {
				bats[i].Clean(proc.GetMPool())
				bats[i] = nil
			}
		}
	}()

	for i, bat := range bats {
		err = writer.addBatchToOutput(mp, actionUpdate, 0, 0, uint64(bat.RowCount()), "", bat)
		if err != nil {
			return
		}
		bat.Clean(proc.Mp())
		bats[i] = nil
	}

	return nil
}

func (writer *s3WriterDelegate) reset(proc *process.Process) (err error) {
	bats := writer.cacheBatchs.TakeBatchs()
	for _, bat := range bats {
		bat.Clean(proc.Mp())
	}

	for _, bats = range writer.insertBlockInfo {
		for _, bat := range bats {
			if bat != nil {
				bat.CleanOnlyData()
			}
		}
	}
	for _, rowCounts := range writer.insertBlockRowCount {
		for i := range rowCounts {
			rowCounts[i] = 0
		}
	}
	for _, partBlockInfos := range writer.deleteBlockInfo {
		for _, blockInfo := range partBlockInfos {
			if blockInfo != nil {
				blockInfo.bat.CleanOnlyData()
				blockInfo.rawRowCount = 0
			}
		}
	}
	for _, datas := range writer.deleteBlockMap {
		for _, data := range datas {
			for k, block := range data {
				if block != nil && block.bat != nil {
					block.bat.Clean(proc.Mp())
				}
				delete(data, k)
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
	if writer.outputBat != nil {
		writer.outputBat.CleanOnlyData()
	}
	writer.batchSize = 0
	writer.buf.Reset()
	return
}

func (writer *s3WriterDelegate) free(proc *process.Process) (err error) {
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
	writer.insertBlockRowCount = nil
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

	for _, datas := range writer.deleteBlockMap {
		for _, data := range datas {
			for _, block := range data {
				if block != nil && block.bat != nil {
					block.bat.Clean(proc.Mp())
				}
			}
		}
	}
	writer.deleteBlockMap = nil

	if writer.outputBat != nil {
		writer.outputBat.Clean(proc.Mp())
		writer.outputBat = nil
	}
	writer.buf.Reset()

	return
}

func (writer *s3WriterDelegate) addBatchToOutput(
	mp *mpool.MPool,
	action actionType,
	idx int,
	partIdx int,
	rowCount uint64,
	name string,
	bat *batch.Batch) (err error) {
	output := writer.outputBat

	err = vector.AppendFixed(output.Vecs[0], uint8(action), false, mp)
	if err != nil {
		return
	}

	err = vector.AppendFixed(output.Vecs[1], uint16(idx), false, mp)
	if err != nil {
		return
	}

	err = vector.AppendFixed(output.Vecs[2], uint16(partIdx), false, mp)
	if err != nil {
		return
	}

	err = vector.AppendFixed(output.Vecs[3], rowCount, false, mp)
	if err != nil {
		return
	}

	err = vector.AppendBytes(output.Vecs[4], []byte(name), false, mp)
	if err != nil {
		return
	}

	var val []byte
	val, err = bat.MarshalBinaryWithBuffer(&writer.buf)
	if err != nil {
		return
	}

	err = vector.AppendBytes(output.Vecs[5], val, false, mp)
	if err != nil {
		return
	}
	output.SetRowCount(output.Vecs[0].Length())
	return
}

func makeS3OutputBatch() *batch.Batch {
	bat := batch.NewWithSize(6)
	bat.Vecs[0] = vector.NewVec(types.T_uint8.ToType())   // action type  0=actionInsert, 1=actionDelete
	bat.Vecs[1] = vector.NewVec(types.T_uint16.ToType())  // index of update.UpdateCtxs
	bat.Vecs[2] = vector.NewVec(types.T_uint16.ToType())  // index of partitions
	bat.Vecs[3] = vector.NewVec(types.T_uint64.ToType())  // rowCount of s3 blocks
	bat.Vecs[4] = vector.NewVec(types.T_varchar.ToType()) // name for delete. empty for insert
	bat.Vecs[5] = vector.NewVec(types.T_text.ToType())    // originBatch.MarshalBinary()
	return bat
}

func appendCfgToWriter(
	writer *s3WriterDelegate,
	tableDef *plan.TableDef,
) {

	seqnums, _, _, sortIdx, isPrimaryKey := colexec.GetSequmsAttrsSortKeyIdxFromTableDef(tableDef)

	thisIdx := len(writer.sortIdxs)
	writer.seqnums = append(writer.seqnums, seqnums)
	writer.sortIdxs = append(writer.sortIdxs, sortIdx)

	if isPrimaryKey {
		writer.pkIdxs = append(writer.pkIdxs, sortIdx)
	} else {
		writer.pkIdxs = append(writer.pkIdxs, -1)
	}

	writer.schemaVersions = append(writer.schemaVersions, tableDef.Version)
	writer.isClusterBys = append(writer.isClusterBys, tableDef.ClusterBy != nil)
	writer.deleteBlockMap[thisIdx] = make([]map[types.Blockid]*deleteBlockData, 1)
	writer.deleteBlockInfo[thisIdx] = make([]*deleteBlockInfo, 1)
	writer.insertBlockInfo[thisIdx] = make([]*batch.Batch, 1)
	writer.insertBlockRowCount[thisIdx] = make([]uint64, 1)
}

// cloneSomeVecFromCompactBatchs  copy some vectors to new batch
// clean these batchs after used
func cloneSomeVecFromCompactBatchs(
	proc *process.Process,
	src *batch.CompactBatchs,
	cols []int,
	attrs []string,
	sortIdx int) ([]*batch.Batch, error) {

	var err error
	var newBat *batch.Batch
	bats := make([]*batch.Batch, 0, src.Length())

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

	return bats, nil
}

// fetchSomeVecFromCompactBatchs fetch some vectors from CompactBatchs
// do not clean these batchs
func fetchSomeVecFromCompactBatchs(
	src *batch.CompactBatchs,
	cols []int,
	attrs []string) ([]*batch.Batch, error) {
	var newBat *batch.Batch
	retBats := make([]*batch.Batch, src.Length())
	for i := 0; i < src.Length(); i++ {
		oldBat := src.Get(i)
		newBat = batch.NewWithSize(len(cols))
		newBat.Attrs = attrs
		for j, idx := range cols {
			oldVec := oldBat.Vecs[idx]
			newBat.Vecs[j] = oldVec
		}
		newBat.SetRowCount(newBat.Vecs[0].Length())
		retBats[i] = newBat
	}
	return retBats, nil
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
