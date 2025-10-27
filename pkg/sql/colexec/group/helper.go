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
	"github.com/matrixorigin/matrixone/pkg/util/list"
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

func (ctr *container) spillDataToDisk(proc *process.Process, parentBkt *spillBucket, last bool) error {
	var parentLv int
	var parentName string

	if parentBkt != nil {
		parentName = parentBkt.name
		parentLv = parentBkt.lv
	} else {
		uuid, _ := uuid.NewV7()
		parentName = fmt.Sprintf("spill_%s", uuid.String())
	}

	if ctr.currentSpillBkt == nil {
		// no current spill bucket, create a new one.   first check parent level.
		// we only allow to spill up to spillMaxPass passes.
		// each pass we take spillMaskBits bits from the hashCode, and use them as the index
		// to select the spill bucket.  Default params, 32^3 = 32768 spill buckets -- if this
		// is still not enough, probably we cannot do much anyway, just fail the query.
		if parentBkt != nil && parentBkt.lv >= spillMaxPass {
			return moerr.NewInternalError(proc.Ctx, "spill level too deep")
		}
		spillfs, err := proc.GetSpillFileService()
		if err != nil {
			return err
		}

		logutil.Infof("spilling data to disk, level %d, parent file %s", parentLv+1, parentName)
		// now create the current spill bucket.
		ctr.currentSpillBkt = make([]*spillBucket, spillNumBuckets)
		for i := range ctr.currentSpillBkt {
			ctr.currentSpillBkt[i] = &spillBucket{
				lv:   parentLv + 1,
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

	lv := parentLv + 1
	// compute spill bucket.
	hashCodes := ctr.hr.Hash.AllGroupHash()
	for i, hashCode := range hashCodes {
		hashCodes[i] = (hashCode >> (64 - spillMaskBits*uint64(lv))) & (spillNumBuckets - 1)
	}

	// tmp batch and bufferto write
	gbBatch := ctr.createNewGroupByBatch(proc, ctr.groupByBatches[0].Vecs, aggBatchSize)
	defer gbBatch.Clean(proc.Mp())
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
		gbBatch.PreExtend(proc.Mp(), int(cnt))
		gbBatch.SetRowCount(0)

		for nthBatch, gb := range ctr.groupByBatches {
			for j := range gb.Vecs {
				err := gbBatch.Vecs[j].UnionBatch(
					gb.Vecs[j], 0, len(flags[nthBatch]), flags[nthBatch], proc.Mp())
				if err != nil {
					return err
				}
			}
		}
		// write batch to buf
		gbBatch.MarshalBinaryWithBuffer(buf)

		// save aggs to buf
		for _, ag := range ctr.aggList {
			ag.SaveIntermediateResult(cnt, flags, buf)
		}

		ctr.currentSpillBkt[i].file.Write(buf.Bytes())
	}

	if last {
		if ctr.spillBkts == nil {
			ctr.spillBkts = list.New[*spillBucket]()
		}
		// transfer the current spill bucket to the spill bucket queue.
		// bkt.file is kept open.
		for _, bkt := range ctr.currentSpillBkt {
			ctr.spillBkts.PushBack(bkt)
		}
		ctr.currentSpillBkt = nil
	}

	return nil
}

// load spilled data from the spill bucket queue.
func (ctr *container) loadSpilledData(proc *process.Process) (bool, error) {
	if ctr.spillBkts == nil || ctr.spillBkts.Len() == 0 {
		// done
		return false, nil
	}

	bkt := ctr.spillBkts.PopBack().Value
	// reposition to the start of the file.
	bkt.file.Seek(0, io.SeekStart)

	// reset data structures,
	ctr.hr.Free0()

	// reset rowcount group by batches.
	for _, gb := range ctr.groupByBatches {
		gb.SetRowCount(0)
	}
	ctr.currBatchIdx = 0

	// Free, will clean the resource and reuse the aggregation.
	for _, ag := range ctr.aggList {
		ag.Free()
	}
	for _, ag := range ctr.spillAggList {
		ag.Free()
	}

	gbBatch := ctr.createNewGroupByBatch(proc, ctr.groupByBatches[0].Vecs, aggBatchSize)

	for {
		// load next batch from the spill bucket.
		cnt, err := types.ReadInt64(bkt.file)
		if err != nil {
			// here should be EOF.  Check
			break
		}

		// load group by batch from the spill bucket.
		gbBatch.SetRowCount(0)
		gbBatch.PreExtend(proc.Mp(), int(cnt))
		gbBatch.UnmarshalFromReader(bkt.file, proc.Mp())

		// load aggs from the spill bucket.
		for _, ag := range ctr.spillAggList {
			ag.UnmarshalFromReader(bkt.file, proc.Mp())
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
			more, err := ctr.appendGroupByBatch(proc, gbBatch.Vecs, i, insertList)
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

		if ctr.needSpill() {
			if err := ctr.spillDataToDisk(proc, bkt, false); err != nil {
				return false, err
			}
		}
	}

	if ctr.isSpilling() {
		if err := ctr.spillDataToDisk(proc, bkt, true); err != nil {
			return false, err
		}
		return ctr.loadSpilledData(proc)
	}

	return true, nil
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

	if !hasMore {
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

func (ctr *container) memUsed() int64 {
	var memUsed int64

	// group by
	for _, b := range ctr.groupByBatches {
		memUsed += int64(b.Size())
	}
	// times 2, so that roughly we have the hashtable size.
	memUsed *= 2

	// aggs
	for _, ag := range ctr.aggList {
		memUsed += ag.Size()
	}
	return memUsed
}

func (ctr *container) needSpill() bool {
	return ctr.memUsed() > ctr.spillMem
}
