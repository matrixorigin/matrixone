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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// we use this size as preferred output batch size, which is typical
	// in MO.
	aggBatchSize = aggexec.AggBatchSize

	// we use this size as pre-allocated size for hash table.
	aggHtPreAllocSize = 1024

	// spill parameters.
	spillNumBuckets = 32
	spillMaxPass    = 3
)

func (group *Group) Prepare(proc *process.Process) (err error) {
	group.ctr.state = vm.Build
	group.ctr.mp = mpool.MustNewNoLock("group_mpool")

	// debug,
	// group.ctr.mp.EnableDetailRecording()

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

	group.ctr.setSpillMem(group.SpillMem, group.Aggs)
	return nil
}

func (group *Group) prepareGroupAndAggArg(proc *process.Process) (err error) {
	if len(group.ctr.groupByEvaluate.Executor) == len(group.GroupBy) {
		group.ctr.groupByEvaluate.ResetForNextQuery()
	} else {
		// calculate the key width and key nullable, and hash table type.
		group.ctr.keyWidth, group.ctr.keyNullable = 0, false
		for _, expr := range group.GroupBy {
			group.ctr.keyNullable = group.ctr.keyNullable || (!expr.Typ.NotNullable)
			if expr.Typ.Id == int32(types.T_tuple) {
				return moerr.NewInternalErrorNoCtx("tuple is not supported as group by column")
			}
			width := GetKeyWidth(types.T(expr.Typ.Id), expr.Typ.Width, group.ctr.keyNullable)
			group.ctr.keyWidth += int32(width)
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
		group.ctr.groupByEvaluate, err = colexec.MakeEvalVector(proc, group.GroupBy)
		if err != nil {
			return err
		}
	}

	if group.ctr.mtyp == H0 {
		// no group by, only one group, always create the dummy group by batch.
		if len(group.ctr.groupByBatches) == 0 {
			group.ctr.groupByBatches = append(group.ctr.groupByBatches,
				group.ctr.createNewGroupByBatch(group.ctr.groupByEvaluate.Vec, 1))
			group.ctr.groupByBatches[0].SetRowCount(1)
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
			group.ctr.aggList, err = group.ctr.makeAggList(group.Aggs)
			if err != nil {
				return err
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
	var err error

	var isCancel bool
	if err, isCancel = vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	group.OpAnalyzer.Start()
	defer group.OpAnalyzer.Stop()

	switch group.ctr.state {
	case vm.Build, vm.EvalReset:
		if group.ctr.state == vm.EvalReset {
			group.ctr.resetForSpill()
			group.ctr.state = vm.Build
		}

		// receive all data, loop till exhuasted.
		for !group.ctr.inputDone {
			var r vm.CallResult
			r, err = vm.ChildrenCall(group.GetChildren(0), proc, group.OpAnalyzer)
			if err != nil {
				return vm.CancelResult, err
			}

			// all handled, going to eval mode.
			//
			// XXX: Note that this test, r.Batch == nil is treated as ExecStop.
			// I am not sure this is correct, but our code depends on this.
			// Esp, some table function will produce ExecNext result with nil
			// batch as end of data.   Shuffle, on the otherhand may product
			// more batches after sending a ExecStop result.
			//
			// if r.Status == vm.ExecStop || r.Batch == nil {
			if r.Batch == nil {
				group.ctr.state = vm.Eval
				group.ctr.inputDone = true
			}

			// empty batch, skip.
			if r.Batch == nil || r.Batch.IsEmpty() {
				continue
			}

			if len(group.ctr.aggList) != len(group.Aggs) {
				group.ctr.aggList, err = group.ctr.makeAggList(group.Aggs)
				if err != nil {
					return vm.CancelResult, err
				}
			}

			// build one batch.
			var needSpill bool
			needSpill, err = group.buildOneBatch(proc, r.Batch)
			if err != nil {
				return vm.CancelResult, err
			}

			if needSpill {
				// we need to spill the data to disk.
				if group.NeedEval {
					if err := group.ctr.spillDataToDisk(proc, nil); err != nil {
						return vm.CancelResult, err
					}
					// continue the loop, to receive more data.
				} else {
					// break the loop, output the intermediate result.
					// set state to Eval, so that we can output ALL
					// the intermediate result.
					group.ctr.state = vm.Eval
					break
				}
			}
		}

		// spilling -- spill whatever left in memory, and load first spilled bucket.
		if group.ctr.isSpilling() {
			if err = group.ctr.spillDataToDisk(proc, nil); err != nil {
				return vm.CancelResult, err
			}
			if _, err = group.ctr.loadSpilledData(proc, group.OpAnalyzer, group.Aggs); err != nil {
				return vm.CancelResult, err
			}
		}

		return group.outputOneBatch(proc)

	case vm.Eval:
		return group.outputOneBatch(proc)

	case vm.End:
		return vm.CancelResult, nil
	}

	err = moerr.NewInternalError(proc.Ctx, "bug: unknown group state")
	return vm.CancelResult, err
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
			if err = group.ctr.buildHashTable(proc.Ctx); err != nil {
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
			vals, _, err := group.ctr.hr.Itr.Insert(i, n, group.ctr.groupByEvaluate.Vec)
			if err != nil {
				return false, err
			}
			// find out which rows are really inserted, which are grouped with existing.
			insertList, _ := group.ctr.hr.GetBinaryInsertList(vals, originGroupCount)

			// append the mini batch to the group by batches, return the number of added
			// groups.
			more, err := group.ctr.appendGroupByBatch(group.ctr.groupByEvaluate.Vec, i, insertList)
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
		return group.ctr.needSpill(group.OpAnalyzer), nil
	}
}

func (ctr *container) buildHashTable(ctx context.Context) error {
	// build hash table
	if err := ctr.hr.BuildHashTable(
		ctx, ctr.mp,
		false,
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

func (ctr *container) createNewGroupByBatch(vs []*vector.Vector, size int) *batch.Batch {
	// initialize the groupByTypes.   this is again very bad design.
	// types should be resolved at plan time.
	if len(ctr.groupByTypes) == 0 {
		for _, vec := range vs {
			ctr.groupByTypes = append(ctr.groupByTypes, *vec.GetType())
		}
	}

	b := batch.NewOffHeapWithSize(len(ctr.groupByTypes))
	for i, typ := range ctr.groupByTypes {
		b.Vecs[i] = vector.NewOffHeapVecWithType(typ)
	}
	b.PreExtend(ctr.mp, size)
	b.SetRowCount(0)
	return b
}

func (ctr *container) appendGroupByBatch(
	vs []*vector.Vector,
	offset int,
	insertList []uint8) (int, error) {

	// first find the target batch.
	if len(ctr.groupByBatches) == 0 ||
		ctr.groupByBatches[len(ctr.groupByBatches)-1].RowCount() >= aggBatchSize {
		ctr.groupByBatches = append(ctr.groupByBatches, ctr.createNewGroupByBatch(vs, aggBatchSize))
	}
	currBatch := ctr.groupByBatches[len(ctr.groupByBatches)-1]
	spaceLeft := aggBatchSize - currBatch.RowCount()

	toIncrease, kth := countNonZeroAndFindKth(insertList, spaceLeft)
	if toIncrease == 0 {
		// there is nothing in the insertList
		return 0, nil
	}

	thisTime := insertList
	addedRows := toIncrease
	if toIncrease > spaceLeft {
		thisTime = insertList[:kth+1]
		addedRows = spaceLeft
	}

	// there is enough space in the current batch to insert thisTime.
	for i, vec := range currBatch.Vecs {
		err := vec.UnionBatch(vs[i], int64(offset), len(thisTime), thisTime, ctr.mp)
		if err != nil {
			return 0, err
		}
	}
	currBatch.AddRowCount(addedRows)

	if toIncrease > spaceLeft {
		// there is not enough space in the current batch to insert thisTime.
		// so we need to append the rest of the insertList to the next batch.
		_, err := ctr.appendGroupByBatch(vs, offset+kth+1, insertList[kth+1:])
		if err != nil {
			return 0, err
		}
	}
	return toIncrease, nil
}

func (group *Group) outputOneBatch(proc *process.Process) (vm.CallResult, error) {
	if group.NeedEval {
		return group.ctr.outputOneBatchFinal(proc, group.OpAnalyzer, group.Aggs)
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
				group.ctr.state = vm.EvalReset
			}
		}
		return res, nil
	}
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

	// XXX: Serialize chunk of aggList entries to batch.
	// This is also a pretty bad design, we would really like to
	// dump group state to a vector and put the vector into the batch.
	// But well,
	var buf bytes.Buffer
	buf.Write(types.EncodeInt32(&group.ctr.mtyp))
	nAggs := int32(len(group.ctr.aggList))
	buf.Write(types.EncodeInt32(&nAggs))
	for _, ag := range group.ctr.aggList {
		ag.SaveIntermediateResultOfChunk(curr, &buf)
	}
	batch.ExtraBuf = buf.Bytes()

	res := vm.NewCallResult()
	res.Batch = batch
	return res, hasMore, nil
}

// given buckets, and a specific bucket, compute the flags for vector union.
func computeChunkFlags(bucketIdx []uint64, bucket uint64, chunkSize int) (int64, [][]uint8) {
	// compute the number of chunks, and last chunk size
	nChunks := (len(bucketIdx) + chunkSize - 1) / chunkSize
	lastChunkSize := len(bucketIdx) - (chunkSize * (nChunks - 1))

	cnt := int64(0)
	flags := make([][]uint8, nChunks)
	for i := range flags {
		if i+1 == nChunks {
			flags[i] = make([]uint8, lastChunkSize)
		} else {
			flags[i] = make([]uint8, chunkSize)
		}
	}

	nextX := 0
	nextY := 0

	for _, idx := range bucketIdx {
		if idx == bucket {
			flags[nextX][nextY] = 1
			cnt += 1
		}
		nextY += 1
		if nextY == chunkSize {
			nextX += 1
			nextY = 0
		}
	}
	return cnt, flags
}
