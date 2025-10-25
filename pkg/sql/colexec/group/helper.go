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
	"bytes"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ResHashRelated struct {
	Hash     hashmap.HashMap
	Itr      hashmap.Iterator
	inserted []uint8
}

func (hr *ResHashRelated) IsEmpty() bool {
	return hr.Hash == nil || hr.Itr == nil
}

func (hr *ResHashRelated) BuildHashTable(
	rebuild bool,
	isStrHash bool, keyNullable bool, preAllocated uint64) error {

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
		h, err := hashmap.NewStrHashMap(keyNullable)
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

	h, err := hashmap.NewIntHashMap(keyNullable)
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

func (ctr *container) spillDataToDisk(proc *process.Process, parentBkt *spillBucket) error {
	// we only allow to spill up to spillMaxPass passes.
	// each pass we take spillMaskBits bits from the hashCode, and use them as the index
	// to select the spill bucket.  Default params, 32^3 = 32768 spill buckets -- if this
	// is still not enough, probably we cannot do much anyway, just fail the query.
	if parentBkt.lv >= spillMaxPass {
		return moerr.NewInternalError(proc.Ctx, "spill level too deep")
	}

	lv := parentBkt.lv + 1

	// initialing spill structure.
	if len(parentBkt.again) == 0 {
		parentBkt.again = make([]spillBucket, spillNumBuckets)
		fnuuid, _ := uuid.NewV7()

		// log the spill file name.
		logutil.Infof("spilling data to disk, level %d, file %s", parentBkt.lv, fnuuid.String())

		spillfs, err := proc.GetSpillFileService()
		if err != nil {
			return err
		}

		for i := range parentBkt.again {
			parentBkt.again[i].lv = lv
			ctr.createNewGroupByBatch(proc, ctr.groupByBatches[0].Vecs, aggBatchSize)
			fn := fnuuid.String() + fmt.Sprintf("_%d.spill", i)
			if parentBkt.again[i].file, err = spillfs.CreateAndRemoveFile(proc.Ctx, fn); err != nil {
				return err
			}
		}
	}

	// compute spill bucket.
	hashCodes := ctr.hr.Hash.AllGroupHash()
	for i, hashCode := range hashCodes {
		hashCodes[i] = (hashCode >> (64 - spillMaskBits*lv)) & (spillMaskBits - 1)
	}

	if parentBkt.gbBatch == nil {
		parentBkt.gbBatch = ctr.createNewGroupByBatch(
			proc, ctr.groupByBatches[0].Vecs, aggBatchSize)
	}

	// each bucket,
	buf := bytes.NewBuffer(make([]byte, 0, common.MiB))
	for i := 0; i < spillNumBuckets; i++ {
		buf.Reset()
		cnt, flags := computeChunkFlags(hashCodes, uint64(i), aggBatchSize)
		buf.Write(types.EncodeInt64(&cnt))
		if cnt == 0 {
			continue
		}

		// extend the group by batch to the new size, set row count to 0, then we union
		// group by batches to the parent batch.
		parentBkt.gbBatch.PreExtend(proc.Mp(), int(cnt))
		parentBkt.gbBatch.SetRowCount(0)
		for nthBatch, gb := range ctr.groupByBatches {
			for j := range gb.Vecs {
				err := parentBkt.gbBatch.Vecs[j].UnionBatch(
					gb.Vecs[j], 0, len(flags[nthBatch]), flags[nthBatch], proc.Mp())
				if err != nil {
					return err
				}
			}
		}
		// write batch to buf
		parentBkt.gbBatch.MarshalBinaryWithBuffer(buf)

		// save aggs to buf
		for _, ag := range ctr.aggList {
			ag.SaveIntermediateResult(cnt, flags, buf)
		}

		parentBkt.again[i].file.Write(buf.Bytes())
	}

	return nil
}

func (ctr *container) getNextFinalResult(proc *process.Process) (vm.CallResult, bool, error) {
	// the groupby batches are now in groupbybatches, partial agg result is in agglist.
	// now we need to flush the final result of agg to output batches.
	if ctr.currBatchIdx >= len(ctr.groupByBatches) {
		// exhaust batches, done.
		return vm.CancelResult, false, nil
	}
	curr := ctr.currBatchIdx
	ctr.currBatchIdx += 1
	hasMore := ctr.currBatchIdx < len(ctr.groupByBatches)

	if curr == 0 {
		// flush aggs.   this api is insane
		ctr.flushed = nil
		for _, ag := range ctr.aggList {
			vecs, err := ag.Flush()
			if err != nil {
				return vm.CancelResult, false, err
			}
			for j := range vecs {
				ctr.groupByBatches[j].Vecs = append(
					ctr.groupByBatches[j].Vecs, vecs[j])
			}
		}
	}

	// get the groupby batch
	batch := ctr.groupByBatches[curr]
	res := vm.NewCallResult()
	res.Batch = batch
	return res, hasMore, nil
}

func (ctr *container) outputOneBatchFinal(proc *process.Process) (vm.CallResult, error) {
	// read next result batch
	res, hasMore, err := ctr.getNextFinalResult(proc)
	if err != nil {
		return vm.CancelResult, err
	}

	// no more data, but we are spilling so we need to load next spilled part.
	if !hasMore && ctr.isSpilling() {
		hasMore, err = ctr.loadSpilledData(proc)
		if err != nil {
			return vm.CancelResult, err
		}
	}

	// really no more data
	if !hasMore {
		ctr.state = vm.End
	}
	return res, nil

}

// nextToLoadBkt returns the next spill bucket to load.
// if the current bucket has a file, return it.
// otherwise, check the children buckets, return the next bucket that needs to be loaded.
func (bkt *spillBucket) nextToLoadBkt() *spillBucket {
	if bkt.file != nil {
		return bkt
	}

	var next int
	var nextBkt *spillBucket

	for next = 0; next < len(bkt.again); next++ {
		nextBkt = bkt.again[next].nextToLoadBkt()
		if nextBkt != nil {
			break
		}
	}

	if nextBkt == nil {
		bkt.again = nil
		return nil
	}

	bkt.again = bkt.again[next:]
	return nextBkt
}

func (bkt *spillBucket) free(proc *process.Process) {
	if bkt.file != nil {
		bkt.file.Close()
		bkt.file = nil
	}

	if bkt.gbBatch != nil {
		bkt.gbBatch.Clean(proc.Mp())
		bkt.gbBatch = nil
	}

	for _, b := range bkt.again {
		b.free(proc)
	}
	bkt.again = nil
}

func (ctr *container) loadSpilledData(proc *process.Process) (bool, error) {
	bkt := ctr.spillBkt.nextToLoadBkt()
	if bkt == nil {
		// done
		return false, nil
	}

	// reposition to the start of the file.
	bkt.file.Seek(0, io.SeekStart)

	// reset data structures.
	ctr.hr.Free0()
	for i := range ctr.groupByBatches {
		ctr.groupByBatches[i].SetRowCount(0)
	}

	ctr.groupByBatches = nil
	ctr.currBatchIdx = 0
	for _, ag := range ctr.aggList {
		ag.Free()
		ag = nil
	}
	for _, ag := range ctr.spillAggList {
		ag.Free()
		ag = nil
	}

	for {
		// load next batch from the spill bucket.
		cnt, err := types.ReadInt64(bkt.file)
		if err != nil {
			// here should be EOF.  Check
			break
		}

		// load the group by batch.
		bkt.gbBatch.PreExtend(proc.Mp(), int(cnt))
		bkt.gbBatch.SetRowCount(0)
		bkt.gbBatch.UnmarshalFromReader(bkt.file, proc.Mp())

		// load aggs from the spill bucket.
		for _, ag := range ctr.spillAggList {
			ag.UnmarshalFromReader(bkt.file, proc.Mp())
		}

		rowCount := bkt.gbBatch.RowCount()
		for i := 0; i < rowCount; i += hashmap.UnitLimit {
			n := min(rowCount-i, hashmap.UnitLimit)
			originGroupCount := ctr.hr.Hash.GroupCount()
			vals, _, err := ctr.hr.Itr.Insert(i, n, bkt.gbBatch.Vecs)
			if err != nil {
				return false, err
			}
			insertList, _ := ctr.hr.GetBinaryInsertList(vals, originGroupCount)
			more, err := ctr.appendGroupByBatch(proc, bkt.gbBatch.Vecs, i, insertList)
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
				for j, ag := range ctr.aggList {
					if err := ag.BatchMerge(ctr.spillAggList[j], i, vals[:len(insertList)]); err != nil {
						return false, err
					}
				}
			}
		}
		// TODO: respill
		//; if ctr.memUsed() > ctr.SpillMem {
	}
	return true, nil
}
