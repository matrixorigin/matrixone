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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

const (
	InsertWriteS3Threshold uint64 = 64 * mpool.MB
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
	cacheBatches *batch.CompactBatchs
	segmentMap   map[string]int32

	action   actionType
	isRemote bool

	updateCtxs     []*MultiUpdateCtx
	updateCtxInfos map[string]*updateCtxInfo
	seqnums        [][]uint16
	sortIndexes    []int
	pkIndexes      []int
	schemaVersions []uint32
	isClusterBys   []bool

	deleteBlockMap      []map[types.Blockid]*deleteBlockData
	deleteBlockInfo     []*deleteBlockInfo
	insertBlockInfo     []*batch.Batch
	insertBlockRowCount []uint64

	outputBat *batch.Batch

	batchSize      uint64
	flushThreshold uint64

	checkSizeCols    []int
	buf              bytes.Buffer
	enforceFlushToS3 bool

	memController struct {
		grantedSize int64
		throttler   rscthrottler.RSCThrottler
	}
}

func newS3Writer(
	sid string,
	update *MultiUpdate,
) (*s3WriterDelegate, error) {

	tableCount := len(update.MultiUpdateCtx)
	writer := &s3WriterDelegate{
		cacheBatches:   batch.NewCompactBatchs(objectio.BlockMaxRows),
		updateCtxInfos: update.ctr.updateCtxInfos,
		seqnums:        make([][]uint16, 0, tableCount),
		sortIndexes:    make([]int, 0, tableCount),
		pkIndexes:      make([]int, 0, tableCount),
		schemaVersions: make([]uint32, 0, tableCount),
		isClusterBys:   make([]bool, 0, tableCount),

		deleteBlockInfo:     make([]*deleteBlockInfo, tableCount),
		insertBlockInfo:     make([]*batch.Batch, tableCount),
		insertBlockRowCount: make([]uint64, tableCount),
		deleteBlockMap:      make([]map[types.Blockid]*deleteBlockData, tableCount),
		isRemote:            update.IsRemote,
		enforceFlushToS3:    update.delegated,
	}

	if throttler, ok := runtime.ServiceRuntime(sid).GetGlobalVariables(runtime.CNMemoryThrottler); !ok {
		return nil, moerr.NewInternalErrorNoCtxf("can not get global variable %s", runtime.CNMemoryThrottler)
	} else {
		writer.memController.throttler = throttler.(rscthrottler.RSCThrottler)
	}

	faultInjected := false

	mainIdx := 0
	for i, updateCtx := range update.MultiUpdateCtx {
		if update.ctr.updateCtxInfos[updateCtx.TableDef.Name].tableType == UpdateMainTable {
			mainIdx = i
		}

		if !faultInjected {
			faultInjected, _ = objectio.LogCNFlushSmallObjsInjected(
				updateCtx.TableDef.DbName, updateCtx.TableDef.Name,
			)
		}

		appendCfgToWriter(writer, updateCtx.TableDef)
	}

	writer.updateCtxs = update.MultiUpdateCtx

	threshold := InsertWriteS3Threshold
	if faultInjected {
		threshold = colexec.FaultInjectedS3Threshold
	}

	upCtx := writer.updateCtxs[mainIdx]
	if len(upCtx.DeleteCols) > 0 && len(upCtx.InsertCols) > 0 {
		//update
		writer.action = actionUpdate
		writer.flushThreshold = threshold
		writer.checkSizeCols = append(writer.checkSizeCols, upCtx.InsertCols...)
	} else if len(upCtx.InsertCols) > 0 {
		//insert
		writer.action = actionInsert
		writer.flushThreshold = threshold
		writer.checkSizeCols = append(writer.checkSizeCols, upCtx.InsertCols...)
	} else {
		//delete
		writer.action = actionDelete
		writer.flushThreshold = DeleteWriteS3Threshold
		writer.checkSizeCols = append(writer.checkSizeCols, upCtx.DeleteCols...)
	}

	return writer, nil
}

func (writer *s3WriterDelegate) cleanCachedBatches(mp *mpool.MPool) {
	bats := writer.cacheBatches.TakeBatchs()
	for _, bat := range bats {
		bat.Clean(mp)
	}

	writer.batchSize = 0

	if writer.memController.grantedSize > 0 {
		writer.memController.throttler.Release(writer.memController.grantedSize)
		writer.memController.grantedSize = 0
	}
}

func (writer *s3WriterDelegate) append(
	proc *process.Process,
	analyzer process.Analyzer,
	inBatch *batch.Batch,
) (err error) {

	var (
		increment      uint64
		acquireGranted bool
	)

	err = writer.cacheBatches.Extend(proc.Mp(), inBatch)
	if err != nil {
		return
	}

	for _, idx := range writer.checkSizeCols {
		increment += uint64(inBatch.Vecs[idx].Size())
	}

	writer.batchSize += increment
	if writer.batchSize >= writer.flushThreshold {
		err = writer.sortAndSync(proc, analyzer)
		return err
	}

	// acquire memory failed, should flush as soon as possible, and then
	// release the pinned batches to avoid oom.
	if _, acquireGranted =
		writer.memController.throttler.Acquire(int64(increment)); !acquireGranted {
		err = writer.sortAndSync(proc, analyzer)
		return err
	}

	writer.memController.grantedSize += int64(increment)

	return
}

func (writer *s3WriterDelegate) prepareDeleteBatches(
	proc *process.Process,
	idx int,
	src []*batch.Batch,
	needClean bool,
) ([]*batch.Batch, error) {
	defer func() {
		if needClean {
			for _, bat := range src {
				bat.Clean(proc.GetMPool())
			}
		}
	}()

	// split delete batches by BlockID
	if writer.deleteBlockMap[idx] == nil {
		writer.deleteBlockMap[idx] = make(map[types.Blockid]*deleteBlockData)
	}
	blockMap := writer.deleteBlockMap[idx]

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

				tableId := uint64(0)
				txnID := []byte(nil)
				if idx < len(writer.updateCtxs) && writer.updateCtxs[idx] != nil && writer.updateCtxs[idx].TableDef != nil {
					tableId = writer.updateCtxs[idx].TableDef.TblId
				}
				if txnOp := proc.GetTxnOperator(); txnOp != nil {
					txnID = txnOp.Txn().ID
				}
				if colexec.IsDeletionOnTxnUnCommit(writer.segmentMap, &segid, tableId, txnID) {
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

	var (
		bats        []*batch.Batch
		batchBuffer = containers.NewGeneralBatchBuffer(
			0,
			true,
			0,
			0,
		)
	)

	defer func() {
		writer.cleanCachedBatches(proc.Mp())
		batchBuffer.Close(proc.GetMPool())
	}()

	for i, updateCtx := range writer.updateCtxs {
		// delete s3
		if len(updateCtx.DeleteCols) > 0 {
			var delBatches []*batch.Batch
			if bats, err = fetchSomeVecFromCompactBatches(
				writer.cacheBatches, updateCtx.DeleteCols, DeleteBatchAttrs,
			); err != nil {
				return
			}
			if delBatches, err = writer.prepareDeleteBatches(proc, i, bats, false); err != nil {
				return
			}
			if err = writer.sortAndSyncOneTable(
				proc, updateCtx.TableDef, analyzer, i, true, delBatches, true,
			); err != nil {
				return
			}
		}

		// insert s3
		if len(updateCtx.InsertCols) > 0 {
			insertAttrs := writer.updateCtxInfos[updateCtx.TableDef.Name].insertAttrs
			isClusterBy := writer.isClusterBys[i]

			// unique index table and secondary index table need clone
			// main table do not need clone
			needClone := writer.updateCtxInfos[updateCtx.TableDef.Name].tableType != UpdateMainTable //uk&sk need clone

			if isClusterBy {
				needClone = false
			}

			// for unique index table and secondary index table, if the sort key has no null, do not clone
			if needClone && writer.sortIndexes[i] > -1 {
				hasNull := false
				sortIdx := updateCtx.InsertCols[writer.sortIndexes[i]]
				for j := 0; j < writer.cacheBatches.Length(); j++ {
					cachedBat := writer.cacheBatches.Get(j)
					// if the sort key has null, need clone
					if cachedBat.GetVector(int32(sortIdx)).HasNull() {
						hasNull = true
						break
					}
				}
				if !hasNull {
					needClone = false
				}
			}

			// needClone = true:
			// unique index table and secondary index table with null values
			// why: the index table should not have null values.
			// Ex.
			// create table t1 (a int, b int unique key);
			// insert into t values (1, 101), (2, null), (3, 303)
			// The unique index table should be (101, 1), (303, 3)
			// Since it will shrink the batch, we need to clone the batch.
			// sortIdx != -1(has sort key) && isClusterBy = false(index table's isClusterBy is always false):

			// needClone = false:
			// other scenarios

			if needClone {
				sortIdx := writer.sortIndexes[i]
				if bats, err = cloneSelectedVecsFromCompactBatches(
					writer.cacheBatches, updateCtx.InsertCols, insertAttrs, sortIdx, proc.Mp(),
				); err != nil {
					return
				}
			} else {
				if bats, err = fetchSomeVecFromCompactBatches(
					writer.cacheBatches, updateCtx.InsertCols, insertAttrs,
				); err != nil {
					return
				}
			}

			// needClone = true means all the batches in `bats` was cloned from `writer.cacheBatches`
			// so we can clean the batches after use
			// needClone = false means all the batches in `bats` was sorted from `writer.cacheBatches`
			// so we can not clean the batches after use. All the batches in `writer.cacheBatches` will
			// be used in next round and will be cleaned in other place.
			cleanAfterUse := needClone
			if err = writer.sortAndSyncOneTable(
				proc,
				updateCtx.TableDef,
				analyzer,
				i,
				false,
				bats,
				cleanAfterUse,
				ioutil.WithBuffer(batchBuffer, true),
			); err != nil {
				return
			}
		}
	}

	return
}

func (writer *s3WriterDelegate) sortAndSyncOneTable(
	proc *process.Process,
	tblDef *plan.TableDef,
	analyzer process.Analyzer,
	idx int,
	isTombstone bool,
	bats []*batch.Batch,
	cleanBatchAfterUse bool,
	opts ...ioutil.SinkerOption,
) (err error) {

	if len(bats) == 0 {
		return
	}

	defer func() {
		if cleanBatchAfterUse {
			for i, bat := range bats {
				if bat != nil {
					bat.Clean(proc.GetMPool())
					bats[i] = nil
				}
			}
		}
	}()

	var (
		fs           fileservice.FileService
		blockInfoBat *batch.Batch
		s3Writer     *colexec.CNS3Writer
		rowCount     = 0
	)

	if fs, err = colexec.GetSharedFSFromProc(proc); err != nil {
		return
	}

	if isTombstone {
		pkCol := plan2.PkColByTableDef(tblDef)
		s3Writer = colexec.NewCNS3TombstoneWriter(
			proc.Mp(), fs, plan2.ExprType2Type(&pkCol.Typ), -1, opts...,
		)
	} else {
		s3Writer = colexec.NewCNS3DataWriter(proc.Mp(), fs, tblDef, -1, false, opts...)
	}

	defer s3Writer.Close()

	counterSet := analyzer.GetOpCounterSet()
	writeCtx := perfcounter.AttachS3RequestKey(proc.Ctx, counterSet)

	for i := range bats {
		rowCount += bats[i].RowCount()
		if err = s3Writer.Write(writeCtx, bats[i]); err != nil {
			return
		}

		if cleanBatchAfterUse {
			bats[i].Clean(proc.GetMPool())
		}
		bats[i] = nil
	}

	if _, err = s3Writer.Sync(writeCtx); err != nil {
		return
	}

	analyzer.AddS3RequestCount(counterSet)
	analyzer.AddFileServiceCacheInfo(counterSet)
	analyzer.AddDiskIO(counterSet)

	if blockInfoBat, err = s3Writer.FillBlockInfoBat(); err != nil {
		return
	}

	if isTombstone {
		return writer.fillDeleteBlockInfo(proc, idx, blockInfoBat, rowCount)
	}

	return writer.fillInsertBlockInfo(proc, idx, blockInfoBat, rowCount)

}

func (writer *s3WriterDelegate) fillDeleteBlockInfo(
	proc *process.Process,
	idx int,
	bat *batch.Batch,
	rowCount int,
) (err error) {

	// init buf
	if writer.deleteBlockInfo[idx] == nil {
		blockInfoBat := batch.NewWithSize(bat.VectorCount())
		blockInfoBat.Attrs = bat.Attrs
		for i := 0; i < bat.VectorCount(); i++ {
			blockInfoBat.Vecs[i] = vector.NewVec(*bat.Vecs[i].GetType())
		}
		writer.deleteBlockInfo[idx] = &deleteBlockInfo{
			name: "",
			bat:  blockInfoBat,
		}
	}

	targetBloInfo := writer.deleteBlockInfo[idx]

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
	bat *batch.Batch,
	rowCount int,
) (err error) {

	// init buf
	if writer.insertBlockInfo[idx] == nil {
		blockInfoBat := batch.NewWithSize(bat.VectorCount())
		blockInfoBat.Attrs = bat.Attrs
		for i := 0; i < bat.VectorCount(); i++ {
			blockInfoBat.Vecs[i] = vector.NewVec(*bat.Vecs[i].GetType())
		}
		writer.insertBlockInfo[idx] = blockInfoBat
		writer.insertBlockRowCount[idx] = 0
	}

	writer.insertBlockRowCount[idx] += uint64(rowCount)
	targetBat := writer.insertBlockInfo[idx]

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
	if writer.enforceFlushToS3 ||
		writer.batchSize > TagS3SizeForMOLogger {
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
	for i, block := range writer.deleteBlockInfo {
		// normal table
		if block != nil && block.bat.RowCount() > 0 {
			err = writer.addBatchToOutput(mp, actionDelete, i, block.rawRowCount, block.name, block.bat)
			if err != nil {
				return
			}
		}
	}

	//write delete batch (which not flush to s3) to workspace
	for i, block := range writer.deleteBlockMap {
		// normal table
		for _, blockData := range block {
			name := objectio.PhysicalAddr_Attr
			err = writer.addBatchToOutput(mp, actionDelete, i, uint64(blockData.bat.RowCount()), name, blockData.bat)
		}
	}

	//write insert block info to workspace
	for i, bat := range writer.insertBlockInfo {
		// normal table
		if bat != nil && bat.RowCount() > 0 {
			resetMergeBlockForOldCN(proc, bat)
			err = writer.addBatchToOutput(mp, actionInsert, i, writer.insertBlockRowCount[i], "", bat)
			if err != nil {
				return
			}
		}
	}

	//write tail batch to workspace
	bats := writer.cacheBatches.TakeBatchs()
	defer func() {
		for i := range bats {
			if bats[i] != nil {
				bats[i].Clean(proc.GetMPool())
				bats[i] = nil
			}
		}
	}()

	for i, bat := range bats {
		err = writer.addBatchToOutput(mp, actionUpdate, 0, uint64(bat.RowCount()), "", bat)
		if err != nil {
			return
		}
		bat.Clean(proc.Mp())
		bats[i] = nil
	}

	return nil
}

func (writer *s3WriterDelegate) reset(proc *process.Process) (err error) {

	writer.cleanCachedBatches(proc.Mp())

	for _, bat := range writer.insertBlockInfo {
		if bat != nil {
			bat.CleanOnlyData()
		}
	}
	for i := range writer.insertBlockRowCount {
		writer.insertBlockRowCount[i] = 0
	}
	for _, block := range writer.deleteBlockInfo {
		if block != nil {
			block.bat.CleanOnlyData()
			block.rawRowCount = 0
		}
	}
	for _, data := range writer.deleteBlockMap {
		for k, block := range data {
			if block != nil && block.bat != nil {
				block.bat.Clean(proc.Mp())
			}
			delete(data, k)
		}
	}

	if writer.outputBat != nil {
		writer.outputBat.CleanOnlyData()
	}

	writer.buf.Reset()
	return
}

func (writer *s3WriterDelegate) free(proc *process.Process) (err error) {

	writer.cleanCachedBatches(proc.Mp())

	for _, bat := range writer.insertBlockInfo {
		if bat != nil {
			bat.Clean(proc.Mp())
		}
	}
	writer.insertBlockInfo = nil
	writer.insertBlockRowCount = nil
	for _, blockInfo := range writer.deleteBlockInfo {
		if blockInfo != nil {
			blockInfo.bat.Clean(proc.Mp())
		}
	}
	writer.deleteBlockInfo = nil

	for _, data := range writer.deleteBlockMap {
		for _, block := range data {
			if block != nil && block.bat != nil {
				block.bat.Clean(proc.Mp())
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
	rowCount uint64,
	name string,
	bat *batch.Batch,
) (err error) {
	output := writer.outputBat

	err = vector.AppendFixed(output.Vecs[0], uint8(action), false, mp)
	if err != nil {
		return
	}

	err = vector.AppendFixed(output.Vecs[1], writer.updateCtxs[idx].TableDef.TblId, false, mp)
	if err != nil {
		return
	}

	err = vector.AppendFixed(output.Vecs[2], rowCount, false, mp)
	if err != nil {
		return
	}

	err = vector.AppendBytes(output.Vecs[3], []byte(name), false, mp)
	if err != nil {
		return
	}

	var val []byte
	val, err = bat.MarshalBinaryWithBuffer(&writer.buf, true)
	if err != nil {
		return
	}

	err = vector.AppendBytes(output.Vecs[4], val, false, mp)
	if err != nil {
		return
	}
	output.SetRowCount(output.Vecs[0].Length())
	return
}

func makeS3OutputBatch() *batch.Batch {
	bat := batch.NewWithSize(5)
	bat.Vecs[0] = vector.NewVec(types.T_uint8.ToType())   // action type  0=actionInsert, 1=actionDelete
	bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())  // tableID
	bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())  // rowCount of s3 blocks
	bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType()) // name for delete. empty for insert
	bat.Vecs[4] = vector.NewVec(types.T_text.ToType())    // originBatch.MarshalBinary()
	return bat
}

func appendCfgToWriter(
	writer *s3WriterDelegate,
	tableDef *plan.TableDef,
) {

	seqnums, _, _, sortIdx, isPrimaryKey := colexec.GetSequmsAttrsSortKeyIdxFromTableDef(tableDef)

	thisIdx := len(writer.sortIndexes)
	writer.seqnums = append(writer.seqnums, seqnums)
	writer.sortIndexes = append(writer.sortIndexes, sortIdx)

	if isPrimaryKey {
		writer.pkIndexes = append(writer.pkIndexes, sortIdx)
	} else {
		writer.pkIndexes = append(writer.pkIndexes, -1)
	}

	writer.schemaVersions = append(writer.schemaVersions, tableDef.Version)
	writer.isClusterBys = append(writer.isClusterBys, tableDef.ClusterBy != nil)
	writer.deleteBlockMap[thisIdx] = make(map[types.Blockid]*deleteBlockData, 1)
	writer.deleteBlockInfo[thisIdx] = nil
	writer.insertBlockInfo[thisIdx] = nil
	writer.insertBlockRowCount[thisIdx] = 0
}

// the columns of `sourceBats` include `selectCols`
// `selectColsCheckNullColIdx` is the sort key index of in `selectCols`. -1 means no sort key
// Ex.
// columns of `sourceBats` are [a, b, c, d, e]
// `selectCols` is [0, 2, 4]
// `selectColsCheckNullColIdx` is 1
// then the columns of the new batch are [a, c, e]
// the new batch is sorted by b
func cloneSelectedVecsFromCompactBatches(
	sourceBats *batch.CompactBatchs,
	selectCols []int,
	selectAttrs []string,
	selectColsCheckNullColIdx int,
	mp *mpool.MPool,
) (cloned []*batch.Batch, err error) {

	var (
		tmpBat *batch.Batch
	)
	cloned = make([]*batch.Batch, 0, sourceBats.Length())

	defer func() {
		if err != nil {
			for _, bat := range cloned {
				if bat != nil {
					bat.Clean(mp)
				}
			}
			if tmpBat != nil {
				tmpBat.Clean(mp)
			}
		}
	}()

	for i, length := 0, sourceBats.Length(); i < length; i++ {
		sourceBat := sourceBats.Get(i)
		tmpBat, err = sourceBat.CloneSelectedColumns(selectCols, selectAttrs, mp)
		if err != nil {
			return
		}

		if selectColsCheckNullColIdx > -1 && tmpBat.Vecs[selectColsCheckNullColIdx].HasNull() {
			sortKeyNulls := tmpBat.Vecs[selectColsCheckNullColIdx].GetNulls().GetBitmap().Clone()
			tmpBat.ShrinkByMask(sortKeyNulls, true, 0)
		}
		if tmpBat.RowCount() == 0 {
			tmpBat.Clean(mp)
			tmpBat = nil
			continue
		}

		cloned = append(cloned, tmpBat)
	}

	return cloned, nil
}

// fetchSomeVecFromCompactBatches fetch some vectors from CompactBatchs
// do not clean these batchs
func fetchSomeVecFromCompactBatches(
	src *batch.CompactBatchs,
	cols []int,
	attrs []string,
) (retBats []*batch.Batch, err error) {

	for i, length := 0, src.Length(); i < length; i++ {
		srcBat := src.Get(i)
		retBats = append(retBats, srcBat.SelectColumns(cols, attrs))
	}
	return
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
