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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var makeAggExec = aggexec.MakeAgg

const (
	// we use this size as preferred output batch size, which is typical
	// in MO.
	aggBatchSize = 8192

	// we use this size as pre-allocated size for hash table.
	aggHtPreAllocSize = 1024

	// spill parameters.
	spillNumBuckets = 32
	spillMaskBits   = 5
	spillMaxPass    = 3
)

func (group *Group) Prepare(proc *process.Process) (err error) {
	group.ctr.state = vm.Build

	if group.OpAnalyzer != nil {
		group.OpAnalyzer.Reset()
	}
	group.OpAnalyzer = process.NewAnalyzer(group.GetIdx(), group.IsFirst, group.IsLast, "group")

	if err = group.prepareGroupAndAggArg(proc); err != nil {
		return err
	}

	if err = group.PrepareProjection(proc); err != nil {
		return err
	}

	// setup spill parameters.
	if group.SpillMem == 0 {
		// TODO: should auto tune this number with available memory.
		group.SpillMem = common.GiB
	}

	// streaming mode usually need less memory -- we would rather give
	// the memory to later merge node.
	if !group.NeedEval {
		group.SpillMem /= 8
	}

	// but be sane, at least use 1MB, and at most 16GB.
	group.SpillMem = min(max(group.SpillMem, common.MiB), common.GiB*16)
	return nil
}

func (group *Group) prepareGroupAndAggArg(proc *process.Process) (err error) {
	if len(group.ctr.groupByEvaluate.Executor) == len(group.Exprs) {
		group.ctr.groupByEvaluate.ResetForNextQuery()
	} else {
		// calculate the key width and key nullable, and hash table type.
		group.ctr.keyWidth, group.ctr.keyNullable = 0, false
		for _, expr := range group.Exprs {
			group.ctr.keyNullable = group.ctr.keyNullable || (!expr.Typ.NotNullable)
			width := GetKeyWidth(types.T(expr.Typ.Id), expr.Typ.Width, group.ctr.keyNullable)
			group.ctr.keyWidth += width
		}

		if group.ctr.keyWidth == 0 {
			group.ctr.mtyp = H0
		} else if group.ctr.keyWidth <= 8 {
			group.ctr.mtyp = H8
		} else {
			group.ctr.mtyp = HStr
		}

		for _, flag := range group.GroupingFlag {
			if !flag {
				group.ctr.mtyp = HStr
				break
			}
		}

		// create group by evaluate
		group.ctr.groupByEvaluate.Free()
		group.ctr.groupByEvaluate, err = colexec.MakeEvalVector(proc, group.Exprs)
		if err != nil {
			return err
		}
	}

	needMakeAggArg := true
	if len(group.ctr.aggArgEvaluate) == len(group.Aggs) {
		needMakeAggArg = false
		for i := range group.ctr.aggArgEvaluate {
			if len(group.ctr.aggArgEvaluate[i].Vec) != len(group.Aggs[i].GetArgExpressions()) {
				needMakeAggArg = true
				break
			} else {
				group.ctr.aggArgEvaluate[i].ResetForNextQuery()
			}
		}
	}

	if needMakeAggArg {
		for i := range group.ctr.aggArgEvaluate {
			group.ctr.aggArgEvaluate[i].Free()
		}
		group.ctr.aggArgEvaluate = make([]colexec.ExprEvalVector, 0, len(group.Aggs))
		for _, ag := range group.Aggs {
			e, err := colexec.MakeEvalVector(proc, ag.GetArgExpressions())
			if err != nil {
				return err
			}
			group.ctr.aggArgEvaluate = append(group.ctr.aggArgEvaluate, e)
		}
	}

	// have not generated aggList agg exec yet, lets do it.
	if len(group.Aggs) > 0 {
		if len(group.ctr.aggList) == len(group.Aggs) {
			for _, ag := range group.ctr.aggList {
				ag.Free()
				if group.ctr.mtyp == H0 {
					ag.GroupGrow(1)
				}
			}
		} else {
			group.ctr.aggList = make([]aggexec.AggFuncExec, len(group.Aggs))
			for i, ag := range group.Aggs {
				group.ctr.aggList[i], err = makeAggExec(proc, ag.GetAggID(), ag.IsDistinct(), group.ctr.aggArgEvaluate[i].Typ...)
				if err != nil {
					return err
				}

				if config := ag.GetExtraConfig(); config != nil {
					if err = group.ctr.aggList[i].SetExtraInformation(config, 0); err != nil {
						return err
					}
				}
				if group.ctr.mtyp == H0 {
					group.ctr.aggList[i].GroupGrow(1)
				} else {
					aggexec.SyncAggregatorsToChunkSize(group.ctr.aggList, aggBatchSize)
				}
			}
		}
	}

	return nil
}

func GetKeyWidth(id types.T, width0 int32, nullable bool) (width int) {
	if id.FixedLength() < 0 {
		width = 128
		if width0 > 0 {
			width = int(width0)
		}

		if id == types.T_array_float32 {
			width *= 4
		}
		if id == types.T_array_float64 {
			width *= 8
		}
	} else {
		width = id.TypeLen()
	}

	if nullable {
		width++
	}
	return width
}

// main entry of the group operator.
func (group *Group) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	switch group.ctr.state {
	case vm.Build:
		// receive all data, loop till exhuasted.
		for {
			r, err := vm.ChildrenCall(group.GetChildren(0), proc, group.OpAnalyzer)
			if err != nil {
				return vm.CancelResult, err
			}

			// all handled, going to eval mode.
			if r.Status == vm.ExecStop {
				group.ctr.state = vm.Eval
				group.ctr.inputDone = true
				break
			}

			// empty batch, skip.
			bat := r.Batch
			if bat == nil || bat.IsEmpty() {
				continue
			}

			// build one batch.
			needSpill, err := group.buildOneBatch(proc, r.Batch)
			if err != nil {
				return vm.CancelResult, err
			}

			if needSpill {
				// we need to spill the data to disk.
				if group.NeedEval {
					group.spillDataToDisk(proc, &group.ctr.spillBkt)
					// continue the loop, to receive more data.
				} else {
					// break the loop, output the intermediate result.
					break
				}
			}
		}

		// spilling -- spill whatever left in memory, and load first spilled bucket.
		if group.ctr.isSpilling() {
			if err := group.spillDataToDisk(proc, &group.ctr.spillBkt); err != nil {
				return vm.CancelResult, err
			}
			if _, err := group.loadSpilledData(proc); err != nil {
				return vm.CancelResult, err
			}
		}

		return group.outputOneBatch(proc)

	case vm.Eval:
		return group.outputOneBatch(proc)

	case vm.End:
		return vm.CancelResult, nil
	}
	return vm.CancelResult, moerr.NewInternalError(proc.Ctx, "bug: unknown group state")
}

func (group *Group) memUsed() int64 {
	var memUsed int64

	// group by
	for _, b := range group.ctr.groupByBatches {
		memUsed += int64(b.Size())
	}
	// times 2, so that roughly we have the hashtable size.
	memUsed *= 2

	// aggs
	for _, ag := range group.ctr.aggList {
		memUsed += ag.Size()
	}
	return memUsed
}

func (group *Group) buildOneBatch(proc *process.Process, bat *batch.Batch) (bool, error) {
	var err error
	// evaluate the group by and agg args, no matter what mtyp,
	// we need to do this first.
	if err = group.evaluateGroupByAndAggArgs(proc, bat); err != nil {
		return false, err
	}

	// without group by, there is only one group.
	if group.ctr.mtyp == H0 {
		// note that in prepare we already called GroupGrow(1) for each agg.
		// just fill the result.
		for i, ag := range group.ctr.aggList {
			if err = ag.BulkFill(0, group.ctr.aggArgEvaluate[i].Vec); err != nil {
				return false, err
			}
		}
		return false, nil
	} else {
		if group.ctr.hr.IsEmpty() {
			if err = group.ctr.buildHashTable(proc); err != nil {
				return false, err
			}
		}

		// here is a strange loop.   our hash table exposed something called
		// hashmap.UnitLimit -- which limits per iteration insert mini batch size.
		count := bat.RowCount()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := min(count-i, hashmap.UnitLimit)
			// we will put rows of mini batch,starting from [i: i+n) into the hash table.
			originGroupCount := group.ctr.hr.Hash.GroupCount()

			// insert the mini batch into the hash table.
			vals, _, err := group.ctr.hr.Itr.Insert(i, n, bat.Vecs)
			if err != nil {
				return false, err
			}
			// find out which rows are really inserted, which are grouped with existing.
			insertList, _ := group.ctr.hr.GetBinaryInsertList(vals, originGroupCount)

			// append the mini batch to the group by batches, return the number of added
			// groups.
			more, err := group.ctr.appendGroupByBatch(proc, bat.Vecs, i, insertList)
			if err != nil {
				return false, err
			}

			// if more groups were added, grow the aggregators.
			if more > 0 {
				for _, agg := range group.ctr.aggList {
					if err = agg.GroupGrow(more); err != nil {
						return false, err
					}
				}
			}

			// do the aggregation
			for j, ag := range group.ctr.aggList {
				err = ag.BatchFill(i, vals[:n], group.ctr.aggArgEvaluate[j].Vec)
				if err != nil {
					return false, err
				}
			}
		} // end of mini batch for loop

		// check size
		return group.memUsed() > group.SpillMem, nil
	}
}

func (ctr *container) buildHashTable(proc *process.Process) error {
	// build hash table
	if err := ctr.hr.BuildHashTable(false,
		ctr.mtyp == HStr,
		ctr.keyNullable,
		aggHtPreAllocSize); err != nil {
		return err
	}

	// pre-allocate groups for each agg.
	for _, ag := range ctr.aggList {
		if err := ag.PreAllocateGroups(aggHtPreAllocSize); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) createNewGroupByBatch(proc *process.Process, vs []*vector.Vector, size int) *batch.Batch {
	// what is so special about off heap?
	b := batch.NewOffHeapWithSize(len(vs))
	for i, vec := range vs {
		b.Vecs[i] = vector.NewOffHeapVecWithType(*vec.GetType())
	}
	b.PreExtend(proc.Mp(), size)
	b.SetRowCount(0)
	return b
}

func (ctr *container) appendGroupByBatch(
	proc *process.Process,
	vs []*vector.Vector,
	offset int,
	insertList []uint8) (int, error) {

	// first find the target batch.
	if len(ctr.groupByBatches) == 0 ||
		ctr.groupByBatches[len(ctr.groupByBatches)-1].RowCount() >= aggBatchSize {
		ctr.groupByBatches = append(ctr.groupByBatches, ctr.createNewGroupByBatch(proc, vs, aggBatchSize))
	}
	currBatch := ctr.groupByBatches[len(ctr.groupByBatches)-1]
	spaceLeft := aggBatchSize - currBatch.RowCount()

	// the countNonZeroAndFindKth is so fucked up,
	toIncrease, kth := countNonZeroAndFindKth(insertList, spaceLeft)
	if toIncrease == 0 {
		// there is nothing in the insertList
		return 0, nil
	}

	thisTime := insertList
	if toIncrease > spaceLeft {
		thisTime = insertList[:kth+1]
	}

	// there is enough space in the current batch to insert thisTime.
	for i, vec := range currBatch.Vecs {
		err := vec.UnionBatch(vs[i], int64(offset), len(thisTime), thisTime, proc.Mp())
		if err != nil {
			return 0, err
		}
	}

	if toIncrease > spaceLeft {
		// there is not enough space in the current batch to insert thisTime.
		// so we need to append the rest of the insertList to the next batch.
		_, err := ctr.appendGroupByBatch(proc, vs, offset+kth+1, insertList[kth+1:])
		if err != nil {
			return 0, err
		}
	}
	return toIncrease, nil
}

func (group *Group) spillDataToDisk(proc *process.Process, parentBkt *spillBucket) error {
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
			group.ctr.createNewGroupByBatch(proc, group.ctr.groupByBatches[0].Vecs, aggBatchSize)
			fn := fnuuid.String() + fmt.Sprintf("_%d.spill", i)
			if parentBkt.again[i].file, err = spillfs.CreateAndRemoveFile(proc.Ctx, fn); err != nil {
				return err
			}
		}
	}

	// compute spill bucket.
	hashCodes := group.ctr.hr.Hash.AllGroupHash()
	for i, hashCode := range hashCodes {
		hashCodes[i] = (hashCode >> (64 - spillMaskBits*lv)) & (spillMaskBits - 1)
	}

	if parentBkt.gbBatch == nil {
		parentBkt.gbBatch = group.ctr.createNewGroupByBatch(
			proc, group.ctr.groupByBatches[0].Vecs, aggBatchSize)
	}

	// each bucket,
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
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
		for nthBatch, gb := range group.ctr.groupByBatches {
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
		for _, ag := range group.ctr.aggList {
			ag.SaveIntermediateResult(cnt, flags, buf)
		}

		parentBkt.again[i].file.Write(buf.Bytes())
	}

	return nil
}

func (group *Group) loadSpilledData(proc *process.Process) (bool, error) {
	return false, moerr.NewInternalError(proc.Ctx, "not implemented")
}

func (ctr *container) isSpilling() bool {
	return len(ctr.spillBkt.again) != 0
}

func (group *Group) outputOneBatch(proc *process.Process) (vm.CallResult, error) {
	if group.NeedEval {
		// read next result batch
		res, hasMore, err := group.getNextFinalResult(proc)
		if err != nil {
			return vm.CancelResult, err
		}

		// no more data, but we are spilling so we need to load next spilled part.
		if !hasMore && group.ctr.isSpilling() {
			hasMore, err = group.loadSpilledData(proc)
			if err != nil {
				return vm.CancelResult, err
			}
		}

		// really no more data
		if !hasMore {
			group.ctr.state = vm.End
		}
		return res, nil
	} else {
		// no need to eval, we are in streaming mode.  spill never happen
		// here.
		res, hasMore, err := group.getNextIntermediateResult(proc)
		if err != nil {
			return vm.CancelResult, err
		}
		if !hasMore {
			if group.ctr.inputDone {
				group.ctr.state = vm.End
			} else {
				// switch back to build to receive more data.
				// reset will set state to vm.Build, which will let us
				// process more by Call child.
				group.ctr.reset(proc)
			}
		}
		return res, nil
	}
}

func (group *Group) getNextFinalResult(proc *process.Process) (vm.CallResult, bool, error) {
	// the groupby batches are now in groupbybatches, partial agg result is in agglist.
	// now we need to flush the final result of agg to output batches.
	if group.ctr.currBatchIdx >= len(group.ctr.groupByBatches) {
		// exhaust batches, done.
		return vm.CancelResult, false, nil
	}
	curr := group.ctr.currBatchIdx
	group.ctr.currBatchIdx += 1
	hasMore := group.ctr.currBatchIdx < len(group.ctr.groupByBatches)

	if curr == 0 {
		// flush aggs.   this api is insane
		group.ctr.flushed = nil
		for _, ag := range group.ctr.aggList {
			vecs, err := ag.Flush()
			if err != nil {
				return vm.CancelResult, false, err
			}
			for j := range vecs {
				group.ctr.groupByBatches[j].Vecs = append(
					group.ctr.groupByBatches[j].Vecs, vecs[j])
			}
		}
	}

	// get the groupby batch
	batch := group.ctr.groupByBatches[curr]
	res := vm.NewCallResult()
	res.Batch = batch
	return res, hasMore, nil
}

func (group *Group) getNextIntermediateResult(proc *process.Process) (vm.CallResult, bool, error) {
	// the groupby batches are now in groupbybatches, partial agg result is in agglist.
	// now, we need to stream the partial results in the group by batch as aggs.
	if group.ctr.currBatchIdx >= len(group.ctr.groupByBatches) {
		// done.
		return vm.CancelResult, false, nil
	}
	curr := group.ctr.currBatchIdx
	group.ctr.currBatchIdx += 1
	hasMore := group.ctr.currBatchIdx < len(group.ctr.groupByBatches)

	batch := group.ctr.groupByBatches[curr]
	// serialize curr chunk of aggList entries to batch
	var buf bytes.Buffer
	for _, ag := range group.ctr.aggList {
		ag.SaveIntermediateResultOfChunk(curr, &buf)
	}
	batch.ExtraBuf2 = buf.Bytes()

	res := vm.NewCallResult()
	res.Batch = batch
	return res, hasMore, nil
}

// given buckets, and a specific bucket, compute the flags for vector union.
func computeChunkFlags(bucketIdx []uint64, bucket uint64, chunkSize int) (int64, [][]uint8) {
	// compute the number of chunks,
	nChunks := (len(bucketIdx) + chunkSize - 1) / chunkSize

	// return values
	cnt := int64(0)
	flags := make([][]uint8, nChunks)
	for i := range flags {
		flags[i] = make([]uint8, chunkSize)
	}

	nextX := 0
	nextY := 0

	for _, idx := range bucketIdx {
		nextY += 1
		if nextY == chunkSize {
			nextX += 1
			nextY = 0
		}

		if idx == bucket {
			flags[nextX][nextY] = 1
			cnt += 1
		}
	}
	return cnt, flags
}
