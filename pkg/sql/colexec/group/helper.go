// Copyright 2024 Matrix Origin
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

package group

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/util/list"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ResHashRelated struct {
	mp       *mpool.MPool
	Hash     hashmap.HashMap
	Itr      hashmap.Iterator
	inserted []uint8
}

func (hr *ResHashRelated) IsEmpty() bool {
	return hr.Hash == nil || hr.Itr == nil
}

func (hr *ResHashRelated) BuildHashTable(
	ctx context.Context, mp *mpool.MPool,
	rebuild bool,
	isStrHash bool, keyNullable bool, preAllocated uint64) error {

	if hr.mp == nil {
		hr.mp = mp
	}

	if hr.mp != mp {
		return moerr.NewInternalError(ctx, "hr.map mpool reset to different mpool")
	}

	if rebuild {
		if hr.Hash != nil {
			hr.Hash.Free()
			hr.Hash = nil
		}
	}

	if hr.Hash != nil {
		return nil
	}

	if isStrHash {
		h, err := hashmap.NewStrHashMap(keyNullable, hr.mp)
		if err != nil {
			return err
		}
		hr.Hash = h

		if hr.Itr == nil {
			hr.Itr = h.NewIterator()
		} else {
			hashmap.IteratorChangeOwner(hr.Itr, hr.Hash)
		}
		if preAllocated > 0 {
			if err = h.PreAlloc(preAllocated); err != nil {
				return err
			}
		}
		return nil
	}

	h, err := hashmap.NewIntHashMap(keyNullable, hr.mp)
	if err != nil {
		return err
	}
	hr.Hash = h

	if hr.Itr == nil {
		hr.Itr = h.NewIterator()
	} else {
		hashmap.IteratorChangeOwner(hr.Itr, hr.Hash)
	}
	if preAllocated > 0 {
		if err = h.PreAlloc(preAllocated); err != nil {
			return err
		}
	}
	return nil
}

func (hr *ResHashRelated) GetBinaryInsertList(vals []uint64, before uint64) (insertList []uint8, insertCount uint64) {
	if cap(hr.inserted) < len(vals) {
		hr.inserted = make([]uint8, len(vals))
	} else {
		hr.inserted = hr.inserted[:len(vals)]
	}

	insertCount = hr.Hash.GroupCount() - before

	last := before
	for k, val := range vals {
		if val > last {
			hr.inserted[k] = 1
			last++
		} else {
			hr.inserted[k] = 0
		}
	}
	return hr.inserted, insertCount
}

func (hr *ResHashRelated) Free0() {
	if hr.Hash != nil {
		hr.Hash.Free()
		hr.Hash = nil
	}
	hr.mp = nil
}

// countNonZeroAndFindKth is a helper function to count the number of non-zero values
// and find index of values, that is the kth non-zero, -1 if there are less than k
// non-zero values.
func countNonZeroAndFindKth(values []uint8, k int) (count int, kth int) {
	count = 0
	kth = -1
	if len(values) < k {
		for _, v := range values {
			if v == 0 {
				continue
			}
			count++
		}
		return count, kth
	}

	for i, v := range values {
		if v == 0 {
			continue
		}

		count++
		if count == k {
			kth = i
			break
		}
	}

	if kth != -1 {
		for i := kth + 1; i < len(values); i++ {
			if values[i] == 0 {
				continue
			}
			count++
		}
	}
	return count, kth
}

func (ctr *container) computeBucketIndex(hashCodes []uint64, myLv uint64) {
	for i := range hashCodes {
		x := hashCodes[i] + myLv
		hashCodes[i] = xxhash.Sum64(types.EncodeUint64(&x)) & (spillNumBuckets - 1)
	}
}

func (ctr *container) spillDataToDisk(proc *process.Process, parentBkt *spillBucket) error {
	var parentLv int
	if parentBkt != nil {
		parentLv = parentBkt.lv
	}
	myLv := parentLv + 1

	// if current spill bucket is not created, create a new one.
	if ctr.currentSpillBkt == nil {
		// check parent level, if it is too deep, return error.
		// we only allow to spill up to spillMaxPass passes.
		// each pass we take spillMaskBits bits from the hashCode, and use them as the index
		// to select the spill bucket.  Default params, 32^3 = 32768 spill buckets -- if this
		// is still not enough, probably we cannot do much anyway, just fail the query.
		if parentLv >= spillMaxPass {
			return moerr.NewInternalError(proc.Ctx, "spill level too deep")
		}

		var parentName string
		if parentBkt != nil {
			parentName = parentBkt.name
		} else {
			uuid, _ := uuid.NewV7()
			parentName = fmt.Sprintf("spill_%s", uuid.String())
		}

		spillfs, err := proc.GetSpillFileService()
		if err != nil {
			return err
		}
		logutil.Infof("spilling data to disk, level %d, parent file %s", myLv, parentName)
		// now create the current spill bucket.
		ctr.currentSpillBkt = make([]*spillBucket, spillNumBuckets)
		for i := range ctr.currentSpillBkt {
			ctr.currentSpillBkt[i] = &spillBucket{
				lv:   myLv,
				name: fmt.Sprintf("%s_%d", parentName, i),
			}

			// it is OK to fail here, as all the opened files are tracked by
			// current spill bucket, and we will close them all when we clean
			// up the operator.
			if ctr.currentSpillBkt[i].file, err = spillfs.CreateAndRemoveFile(
				proc.Ctx, ctr.currentSpillBkt[i].name); err != nil {
				return err
			}
		}
	}

	// nothing to spill,
	if ctr.hr.IsEmpty() {
		return nil
	}

	// compute spill bucket.
	n := int(ctr.hr.Hash.GroupCount())
	if cap(ctr.spillHashCodes) < n {
		ctr.spillHashCodes = make([]uint64, n)
	}
	hashCodes := ctr.hr.Hash.FillGroupHashes(ctr.spillHashCodes[:n])
	// our hash code from Hash is NOT random, esp, int32/uint32 will hash to a 32 bit value,
	// bummer.
	ctr.computeBucketIndex(hashCodes, uint64(myLv))

	// tmp batch and buffer to write.   it is OK to pass in a nil vec, as
	// ctr.groupByTypes is already initialized.
	if ctr.spillGbBatch == nil {
		ctr.spillGbBatch = ctr.createNewGroupByBatch(nil, aggBatchSize)
	}
	gbBatch := ctr.spillGbBatch
	if ctr.spillBuf == nil {
		ctr.spillBuf = bytes.NewBuffer(make([]byte, 0, common.MiB))
	}
	buf := ctr.spillBuf

	// reusable per-bucket flag slice for a single batch's rows
	var batchFlags []uint8

	// Process one groupByBatch at a time to avoid holding all batches in memory.
	// For each batch, compute bucket assignments in a single pass, then write one
	// record per bucket that has matching rows.
	//
	// fullFlags is a [][]uint8 of length len(groupByBatches) passed to SaveIntermediateResult.
	// All entries are nil (→ cnt=0, skipped by reader) except the current batch's index.
	nBatches := len(ctr.groupByBatches)
	if cap(ctr.spillChunkFlags) < nBatches {
		ctr.spillChunkFlags = make([][]uint8, nBatches)
	}
	fullFlags := ctr.spillChunkFlags[:nBatches]
	for i := range fullFlags {
		fullFlags[i] = []uint8{} // empty (not nil) so UnionBatch skips it
	}

	hcOffset := 0
	for nthBatch, gb := range ctr.groupByBatches {
		rc := gb.RowCount()
		if rc == 0 {
			continue
		}
		batchHC := hashCodes[hcOffset : hcOffset+rc]
		hcOffset += rc

		// single pass: compute per-bucket flags and counts for this batch
		var bktCounts [spillNumBuckets]int64
		if cap(batchFlags) < rc {
			batchFlags = make([]uint8, rc)
		}
		flags := batchFlags[:rc]

		for j, h := range batchHC {
			b := h & (spillNumBuckets - 1)
			flags[j] = uint8(b)
			bktCounts[b]++
		}

		// reuse spillFlagFlat as the per-bucket 0/1 flag array
		if cap(ctr.spillFlagFlat) < rc {
			ctr.spillFlagFlat = make([]uint8, rc)
		}
		bktFlags := ctr.spillFlagFlat[:rc]

		for i := 0; i < spillNumBuckets; i++ {
			cnt := bktCounts[i]
			if cnt == 0 {
				continue
			}

			// fill bktFlags: 1 if row belongs to bucket i
			for j := range bktFlags {
				if flags[j] == uint8(i) {
					bktFlags[j] = 1
				} else {
					bktFlags[j] = 0
				}
			}

			buf.Reset()
			buf.Write(types.EncodeInt64(&cnt))

			// build gbBatch for this bucket's rows from this batch
			gbBatch.CleanOnlyData()
			if err := gbBatch.PreExtend(ctr.mp, int(cnt)); err != nil {
				return err
			}
			for j := range gb.Vecs {
				if err := gbBatch.Vecs[j].UnionBatch(gb.Vecs[j], 0, rc, bktFlags, ctr.mp); err != nil {
					return err
				}
			}
			gbBatch.SetRowCount(int(cnt))
			gbBatch.MarshalBinaryWithBuffer(buf, false)

			// write marker
			var magic uint64 = 0x12345678DEADBEEF
			buf.Write(types.EncodeInt64(&cnt))
			buf.Write(types.EncodeUint64(&magic))

			// save aggs: pass full-length flags with only nthBatch populated.
			// SaveIntermediateResult writes cnt=0 for nil entries (skipped on read).
			nAggs := int32(len(ctr.aggList))
			buf.Write(types.EncodeInt32(&nAggs))
			fullFlags[nthBatch] = bktFlags
			for _, ag := range ctr.aggList {
				if err := ag.SaveIntermediateResult(cnt, fullFlags, buf); err != nil {
					return err
				}
			}
			fullFlags[nthBatch] = nil

			magic = 0xdeadbeef12345678
			buf.Write(types.EncodeInt64(&cnt))
			buf.Write(types.EncodeUint64(&magic))

			ctr.currentSpillBkt[i].cnt += cnt
			if _, err := ctr.currentSpillBkt[i].file.Write(buf.Bytes()); err != nil {
				return err
			}
		}
	}

	// reset ctr for next spill
	ctr.resetForSpill()
	return nil
}

// load spilled data from the spill bucket queue.
func (ctr *container) loadSpilledData(proc *process.Process, opAnalyzer process.Analyzer, aggExprs []aggexec.AggFuncExecExpression) (bool, error) {
	// first, if there is current spill bucket, transfer it to the spill bucket queue.
	if ctr.currentSpillBkt != nil {
		if ctr.spillBkts == nil {
			ctr.spillBkts = list.New[*spillBucket]()
		}
		for _, bkt := range ctr.currentSpillBkt {
			ctr.spillBkts.PushBack(bkt)
		}
		ctr.currentSpillBkt = nil
	}

	// then, if there is no spill bucket in the queue, done.
	if ctr.spillBkts == nil || ctr.spillBkts.Len() == 0 {
		// done
		return false, nil
	}

	// popped bkt must be defer freed.
	bkt := ctr.spillBkts.PopBack().Value
	defer bkt.free()

	// reposition to the start of the file.
	bkt.file.Seek(0, io.SeekStart)

	// we reset ctr state, and create a new group by batch.
	ctr.resetForSpill()
	if ctr.spillGbBatch == nil {
		ctr.spillGbBatch = ctr.createNewGroupByBatch(nil, aggBatchSize)
	}
	gbBatch := ctr.spillGbBatch

	if ctr.spillReader == nil {
		ctr.spillReader = bufio.NewReaderSize(bkt.file, 8192)
	} else {
		ctr.spillReader.Reset(bkt.file)
	}
	bufferedFile := ctr.spillReader

	for {
		// load next batch from the spill bucket.
		cnt, err := types.ReadInt64(bufferedFile)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return false, err
			}
		}
		if cnt == 0 {
			continue
		}

		if len(ctr.aggList) != len(aggExprs) {
			ctr.aggList, err = ctr.makeAggList(aggExprs)
			if err != nil {
				return false, err
			}
		}
		if len(ctr.spillAggList) != len(aggExprs) {
			ctr.spillAggList, err = ctr.makeAggList(aggExprs)
			if err != nil {
				return false, err
			}
		}

		// load group by batch from the spill bucket.
		gbBatch.CleanOnlyData()
		if err = gbBatch.PreExtend(ctr.mp, int(cnt)); err != nil {
			return false, err
		}
		if err = gbBatch.UnmarshalFromReader(bufferedFile, ctr.mp); err != nil {
			return false, err
		}

		checkMagic, err := types.ReadUint64(bufferedFile)
		if err != nil {
			return false, err
		}
		if checkMagic != uint64(cnt) {
			return false, moerr.NewInternalError(proc.Ctx, "spill groupby cnt mismatch")
		}

		checkMagic, err = types.ReadUint64(bufferedFile)
		if err != nil {
			return false, err
		}
		if checkMagic != 0x12345678DEADBEEF {
			return false, moerr.NewInternalError(proc.Ctx, "spill groupby magic number mismatch")
		}

		nAggs, err := types.ReadInt32(bufferedFile)
		if err != nil {
			return false, err
		}
		if nAggs != int32(len(ctr.spillAggList)) {
			return false, moerr.NewInternalError(proc.Ctx, "spill agg cnt mismatch")
		}

		// load aggs from the spill bucket.
		for _, ag := range ctr.spillAggList {
			ag.UnmarshalFromReader(bufferedFile, ctr.mp)
		}

		checkMagic, err = types.ReadUint64(bufferedFile)
		if err != nil {
			return false, err
		}
		if checkMagic != uint64(cnt) {
			return false, moerr.NewInternalError(proc.Ctx, "spill agg cnt mismatch")
		}

		checkMagic, err = types.ReadUint64(bufferedFile)
		if err != nil {
			return false, err
		}
		if checkMagic != 0xDEADBEEF12345678 {
			return false, moerr.NewInternalError(proc.Ctx, "spill agg magic number mismatch")
		}

		if ctr.hr.IsEmpty() {
			if err = ctr.buildHashTable(proc.Ctx); err != nil {
				return false, err
			}
		}

		// insert group by batch into the hash table.
		rowCount := gbBatch.RowCount()
		for i := 0; i < rowCount; i += hashmap.UnitLimit {
			n := min(rowCount-i, hashmap.UnitLimit)
			originGroupCount := ctr.hr.Hash.GroupCount()
			vals, _, err := ctr.hr.Itr.Insert(i, n, gbBatch.Vecs)
			if err != nil {
				return false, err
			}
			insertList, _ := ctr.hr.GetBinaryInsertList(vals, originGroupCount)
			more, err := ctr.appendGroupByBatch(gbBatch.Vecs, i, insertList)
			if err != nil {
				return false, err
			}

			if len(ctr.aggList) == 0 {
				continue
			}
			if more > 0 {
				for j := range ctr.aggList {
					if err := ctr.aggList[j].GroupGrow(more); err != nil {
						return false, err
					}
				}
			}

			for j, ag := range ctr.aggList {
				if err := ag.BatchMerge(ctr.spillAggList[j], i, vals[:len(insertList)]); err != nil {
					return false, err
				}
			}
		}

		// free spill agg list after merging.
		ctr.freeSpillAggList()

		if ctr.needSpill(opAnalyzer) {
			if err := ctr.spillDataToDisk(proc, bkt); err != nil {
				return false, err
			}
		}
	}

	// respilling happened, so we finish the last batch and recursive down
	if ctr.isSpilling() {
		if err := ctr.spillDataToDisk(proc, bkt); err != nil {
			return false, err
		}
		return ctr.loadSpilledData(proc, opAnalyzer, aggExprs)
	}

	return true, nil
}

func (ctr *container) getNextFinalResult(proc *process.Process) (vm.CallResult, error) {
	// the groupby batches are now in groupbybatches, partial agg result is in agglist.
	// now we need to flush the final result of agg to output batches.
	if ctr.currBatchIdx >= len(ctr.groupByBatches) ||
		(ctr.currBatchIdx == len(ctr.groupByBatches)-1 &&
			ctr.groupByBatches[ctr.currBatchIdx].RowCount() == 0) {
		// exhauseed all batches, or, last group by batch has no data,
		// done.
		return vm.CancelResult, nil
	}

	curr := ctr.currBatchIdx
	ctr.currBatchIdx += 1

	if curr == 0 {
		// flush aggs final result to vectors, all aggs follow groupby columns.
		for _, ag := range ctr.aggList {
			vecs, err := ag.Flush()
			if err != nil {
				return vm.CancelResult, err
			}
			for j := range vecs {
				ctr.groupByBatches[j].Vecs = append(
					ctr.groupByBatches[j].Vecs, vecs[j])
			}
		}

		ctr.freeAggList()
	}

	// get the groupby batch
	batch := ctr.groupByBatches[curr]
	res := vm.NewCallResult()
	res.Batch = batch
	return res, nil
}

func (ctr *container) outputOneBatchFinal(proc *process.Process, opAnalyzer process.Analyzer, aggExprs []aggexec.AggFuncExecExpression) (vm.CallResult, error) {
	// read next result batch
	res, err := ctr.getNextFinalResult(proc)
	if err != nil {
		return vm.CancelResult, err
	}

	// or should we check res.Status == vm.ExecStop
	if res.Batch != nil {
		return res, nil
	}

	loaded, err := ctr.loadSpilledData(proc, opAnalyzer, aggExprs)
	if err != nil {
		return vm.CancelResult, err
	}
	if loaded {
		return ctr.outputOneBatchFinal(proc, opAnalyzer, aggExprs)
	}
	return res, nil
}

func (ctr *container) memUsed() int64 {
	sz := ctr.mp.CurrNB()
	return sz
}

func (ctr *container) needSpill(opAnalyzer process.Analyzer) bool {

	memUsed := ctr.memUsed()
	opAnalyzer.SetMemUsed(memUsed)

	// spill less than 10K, used only for debug.
	// in this case, we spill when there are more than
	// this many groups
	var needSpill bool
	if ctr.spillMem < 10000 {
		needSpill = ctr.hr.Hash.GroupCount() >= uint64(ctr.spillMem)
	} else {
		needSpill = memUsed > ctr.spillMem
	}

	if needSpill {
		opAnalyzer.Spill(memUsed)
	}
	return needSpill
}

func (ctr *container) makeAggList(aggExprs []aggexec.AggFuncExecExpression) ([]aggexec.AggFuncExec, error) {
	var err error
	aggList := make([]aggexec.AggFuncExec, len(aggExprs))
	for i, agExpr := range aggExprs {
		typs := make([]types.Type, len(agExpr.GetArgExpressions()))
		for j, arg := range agExpr.GetArgExpressions() {
			typs[j] = types.New(types.T(arg.Typ.Id), arg.Typ.Width, arg.Typ.Scale)
		}
		aggList[i], err = aggexec.MakeAgg(ctr.mp, agExpr.GetAggID(), agExpr.IsDistinct(), typs...)
		if err != nil {
			return nil, err
		}
		if config := agExpr.GetExtraConfig(); config != nil {
			if err := aggList[i].SetExtraInformation(config, 0); err != nil {
				return nil, err
			}
		}
	}

	if ctr.mtyp != H0 {
		aggexec.SyncAggregatorsToChunkSize(aggList, aggBatchSize)
	} else {
		aggexec.SyncAggregatorsToChunkSize(aggList, 1)
		for _, ag := range aggList {
			if err := ag.GroupGrow(1); err != nil {
				return nil, err
			}
		}
	}
	return aggList, nil
}

func (ctr *container) sanityCheck() {
	if util.Debug {
		originGroupCount := ctr.hr.Hash.GroupCount()
		batchRowCount := 0
		for _, batch := range ctr.groupByBatches {
			batchRowCount += batch.RowCount()
		}
		if batchRowCount != int(originGroupCount) {
			panic(moerr.NewInternalErrorNoCtx("group count mismatch"))
		}
	}
}
