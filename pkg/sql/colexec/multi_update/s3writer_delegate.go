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
	cacheBatches *batch.BatchSet
	batchPool    []*batch.Batch // reusable buffers for cacheBatches.Extend
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

	// per-table batch free lists that persist across sortAndSync calls,
	// so pre-allocated vector capacity is reused instead of re-allocated.
	insertFreeLists []*containers.BatchFreeList

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
		cacheBatches:   batch.NewBatchSet(objectio.BlockMaxRows),
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
		insertFreeLists:     make([]*containers.BatchFreeList, tableCount),
		isRemote:            update.IsRemote,
		enforceFlushToS3:    update.delegated,
	}
	for i := range writer.insertFreeLists {
		writer.insertFreeLists[i] = containers.NewBatchFreeList(nil, nil, true)
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

func (writer *s3WriterDelegate) popReuseBuf() *batch.Batch {
	n := len(writer.batchPool)
	if n == 0 {
		return nil
	}
	buf := writer.batchPool[n-1]
	writer.batchPool = writer.batchPool[:n-1]
	return buf
}

func (writer *s3WriterDelegate) cleanCachedBatches(mp *mpool.MPool) {
	bats := writer.cacheBatches.TakeBatches()
	for _, bat := range bats {
		bat.CleanOnlyData()
		writer.batchPool = append(writer.batchPool, bat)
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
		consumed       bool
	)

	reuseBuf := writer.popReuseBuf()
	consumed, err = writer.cacheBatches.Extend(proc.Mp(), inBatch, reuseBuf)
	if err != nil {
		if reuseBuf != nil && !consumed {
			reuseBuf.Clean(proc.Mp())
		}
		return
	}
	if !consumed && reuseBuf != nil {
		writer.batchPool = append(writer.batchPool, reuseBuf)
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
	deleteBats := batch.NewBatchSet(objectio.BlockMaxRows)
	for _, blkid := range blkids {
		bat := blockMap[blkid].bat
		delete(blockMap, blkid)
		err := deleteBats.Push(proc.GetMPool(), bat)
		if err != nil {
			return nil, err
		}
	}
	retBatchs := deleteBats.TakeBatches()
	return retBatchs, nil
}

func (writer *s3WriterDelegate) sortAndSync(proc *process.Process, analyzer process.Analyzer) (err error) {

	var bats []*batch.Batch

	defer func() {
		writer.cleanCachedBatches(proc.Mp())
	}()

	for i, updateCtx := range writer.updateCtxs {
		// delete s3
		if len(updateCtx.DeleteCols) > 0 {
			var delBatches []*batch.Batch
			if bats, err = fetchSomeVecFromBatchSet(
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

			// Index tables need clone; main table does not.
			needClone := writer.updateCtxInfos[updateCtx.TableDef.Name].tableType != UpdateMainTable

			if isClusterBy {
				needClone = false
			}

			// If sort key has no nulls, cloning is unnecessary.
			if needClone && writer.sortIndexes[i] > -1 {
				hasNull := false
				sortIdx := updateCtx.InsertCols[writer.sortIndexes[i]]
				for j := 0; j < writer.cacheBatches.Length(); j++ {
					if writer.cacheBatches.Get(j).GetVector(int32(sortIdx)).HasNull() {
						hasNull = true
						break
					}
				}
				if !hasNull {
					needClone = false
				}
			}

			// Clone when index table has null sort keys (nulls must be stripped).
			if needClone {
				sortIdx := writer.sortIndexes[i]
				if bats, err = cloneSelectedVecsFromBatchSet(
					writer.cacheBatches, updateCtx.InsertCols, insertAttrs, sortIdx, proc.Mp(),
				); err != nil {
					return
				}
			} else {
				if bats, err = fetchSomeVecFromBatchSet(
					writer.cacheBatches, updateCtx.InsertCols, insertAttrs,
				); err != nil {
					return
				}
			}

			// Cloned batches are owned by us and safe to clean after use.
			cleanAfterUse := needClone
			if err = writer.sortAndSyncOneTable(
				proc,
				updateCtx.TableDef,
				analyzer,
				i,
				false,
				bats,
				cleanAfterUse,
				ioutil.WithBuffer(writer.insertFreeLists[i], false),
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
		// When cleanBatchAfterUse is true, the batch is exclusively owned
		// (cloned), so we can transfer it to the sinker without copying.
		if cleanBatchAfterUse {
			owned, writeErr := s3Writer.WriteOwned(writeCtx, bats[i])
			if writeErr != nil {
				err = writeErr
				return
			}
			if owned {
				bats[i] = nil
				continue
			}
		}
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

// initBlockInfoBat creates a new off-heap batch matching the schema of src.
func initBlockInfoBat(src *batch.Batch) *batch.Batch {
	dst := batch.NewOffHeapWithSize(src.VectorCount())
	dst.Attrs = src.Attrs
	for i := 0; i < src.VectorCount(); i++ {
		dst.Vecs[i] = vector.NewOffHeapVecWithType(*src.Vecs[i].GetType())
	}
	return dst
}

// appendVarlenaBatch appends all varlena data from src vecs into dst vecs.
func appendVarlenaBatch(dst, src *batch.Batch, mp *mpool.MPool) error {
	for i := range src.Vecs {
		row, col := vector.MustVarlenaRawData(src.Vecs[i])
		for j := range src.Vecs[i].Length() {
			if err := vector.AppendBytes(
				dst.Vecs[i], row[j].GetByteSlice(col), false, mp); err != nil {
				return err
			}
		}
	}
	dst.SetRowCount(dst.Vecs[0].Length())
	return nil
}

func (writer *s3WriterDelegate) fillDeleteBlockInfo(
	proc *process.Process,
	idx int,
	bat *batch.Batch,
	rowCount int,
) (err error) {

	if writer.deleteBlockInfo[idx] == nil {
		writer.deleteBlockInfo[idx] = &deleteBlockInfo{
			bat: initBlockInfoBat(bat),
		}
	}

	info := writer.deleteBlockInfo[idx]

	stats := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(0))
	objId := stats.ObjectName().ObjectId()[:]
	info.name = fmt.Sprintf("%s|%d", objId, deletion.FlushDeltaLoc)
	info.rawRowCount += uint64(rowCount)

	return appendVarlenaBatch(info.bat, bat, proc.GetMPool())
}

func (writer *s3WriterDelegate) fillInsertBlockInfo(
	proc *process.Process,
	idx int,
	bat *batch.Batch,
	rowCount int,
) (err error) {

	if writer.insertBlockInfo[idx] == nil {
		writer.insertBlockInfo[idx] = initBlockInfoBat(bat)
		writer.insertBlockRowCount[idx] = 0
	}

	writer.insertBlockRowCount[idx] += uint64(rowCount)

	return appendVarlenaBatch(writer.insertBlockInfo[idx], bat, proc.GetMPool())
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
			if err != nil {
				return
			}
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
	bats := writer.cacheBatches.TakeBatches()
	for i, bat := range bats {
		if err = writer.addBatchToOutput(mp, actionUpdate, 0, uint64(bat.RowCount()), "", bat); err != nil {
			// Clean remaining batches on error.
			for _, b := range bats[i:] {
				if b != nil {
					b.Clean(proc.GetMPool())
				}
			}
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
	mp := proc.Mp()

	for _, bat := range writer.batchPool {
		bat.Clean(mp)
	}
	writer.batchPool = nil

	for _, fl := range writer.insertFreeLists {
		if fl != nil {
			fl.Close(mp)
		}
	}
	writer.insertFreeLists = nil

	for _, bat := range writer.insertBlockInfo {
		if bat != nil {
			bat.Clean(mp)
		}
	}
	writer.insertBlockInfo = nil
	writer.insertBlockRowCount = nil

	for _, info := range writer.deleteBlockInfo {
		if info != nil {
			info.bat.Clean(mp)
		}
	}
	writer.deleteBlockInfo = nil

	for _, data := range writer.deleteBlockMap {
		for _, block := range data {
			if block != nil && block.bat != nil {
				block.bat.Clean(mp)
			}
		}
	}
	writer.deleteBlockMap = nil

	if writer.outputBat != nil {
		writer.outputBat.Clean(mp)
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

	if err = vector.AppendFixed(output.Vecs[0], uint8(action), false, mp); err != nil {
		return
	}
	if err = vector.AppendFixed(output.Vecs[1], writer.updateCtxs[idx].TableDef.TblId, false, mp); err != nil {
		return
	}
	if err = vector.AppendFixed(output.Vecs[2], rowCount, false, mp); err != nil {
		return
	}
	if err = vector.AppendBytes(output.Vecs[3], []byte(name), false, mp); err != nil {
		return
	}

	var val []byte
	if val, err = bat.MarshalBinaryWithBuffer(&writer.buf, true); err != nil {
		return
	}
	if err = vector.AppendBytes(output.Vecs[4], val, false, mp); err != nil {
		return
	}

	output.SetRowCount(output.Vecs[0].Length())
	return
}

func makeS3OutputBatch() *batch.Batch {
	bat := batch.NewOffHeapWithSize(5)
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_uint8.ToType())   // action type  0=actionInsert, 1=actionDelete
	bat.Vecs[1] = vector.NewOffHeapVecWithType(types.T_uint64.ToType())  // tableID
	bat.Vecs[2] = vector.NewOffHeapVecWithType(types.T_uint64.ToType())  // rowCount of s3 blocks
	bat.Vecs[3] = vector.NewOffHeapVecWithType(types.T_varchar.ToType()) // name for delete. empty for insert
	bat.Vecs[4] = vector.NewOffHeapVecWithType(types.T_text.ToType())    // originBatch.MarshalBinary()
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
func cloneSelectedVecsFromBatchSet(
	sourceBats *batch.BatchSet,
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

// fetchSomeVecFromBatchSet fetch some vectors from BatchSet
// do not clean these batchs
func fetchSomeVecFromBatchSet(
	src *batch.BatchSet,
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
		bat.Vecs = append(bat.Vecs, vector.NewOffHeapVecWithType(types.T_binary.ToType()))

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
