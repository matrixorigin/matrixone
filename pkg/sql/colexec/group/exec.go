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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var makeAggExec = aggexec.MakeAgg

// intermediateResultSendActionTrigger is the row number to trigger an action
// to send the intermediate result
// if the result row is not less than this number.
var intermediateResultSendActionTrigger = aggexec.BlockCapacityForStrType

func (group *Group) String(buf *bytes.Buffer) {
	buf.WriteString(thisOperatorName + ": group([")
	for i, expr := range group.Exprs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v", expr))
	}
	buf.WriteString("], [")

	for i, ag := range group.Aggs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v(%v)", function.GetAggFunctionNameByID(ag.GetAggID()), ag.GetArgExpressions()))
	}
	buf.WriteString("])")
}

func (group *Group) Prepare(proc *process.Process) (err error) {
	group.ctr.state = vm.Build
	group.ctr.dataSourceIsEmpty = true
	group.prepareAnalyzer()
	if err = group.prepareAgg(proc); err != nil {
		return err
	}
	if err = group.prepareGroup(proc); err != nil {
		return err
	}
	if err := group.initSpiller(proc); err != nil {
		return err
	}
	return group.PrepareProjection(proc)
}

func (group *Group) prepareAnalyzer() {
	if group.OpAnalyzer != nil {
		group.OpAnalyzer.Reset()
		return
	}
	group.OpAnalyzer = process.NewAnalyzer(group.GetIdx(), group.IsFirst, group.IsLast, "group")
}

func (group *Group) prepareAgg(proc *process.Process) error {
	if len(group.ctr.aggregateEvaluate) == len(group.Aggs) {
		return nil
	}

	group.ctr.freeAggEvaluate()
	group.ctr.aggregateEvaluate = make([]ExprEvalVector, 0, len(group.Aggs))
	for _, ag := range group.Aggs {
		e, err := MakeEvalVector(proc, ag.GetArgExpressions())
		if err != nil {
			return err
		}
		group.ctr.aggregateEvaluate = append(group.ctr.aggregateEvaluate, e)
	}
	return nil
}

func (group *Group) prepareGroup(proc *process.Process) (err error) {
	if len(group.ctr.groupByEvaluate.Executor) == len(group.Exprs) {
		return nil
	}

	group.ctr.freeGroupEvaluate()
	// calculate the key width and key nullable.
	group.ctr.keyWidth, group.ctr.keyNullable = 0, false
	for _, expr := range group.Exprs {
		group.ctr.keyNullable = group.ctr.keyNullable || (!expr.Typ.NotNullable)
	}
	for _, expr := range group.Exprs {
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
	group.ctr.groupByEvaluate, err = MakeEvalVector(proc, group.Exprs)
	return err
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

func (group *Group) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	var b *batch.Batch
	var err error
	if group.NeedEval {
		b, err = group.callToGetFinalResult(proc)
	} else {
		b, err = group.callToGetIntermediateResult(proc)
	}
	if err != nil {
		return vm.CancelResult, err
	}

	res := vm.NewCallResult()
	res.Batch = b
	return res, nil
}

func (group *Group) getInputBatch(proc *process.Process) (*batch.Batch, error) {
	r, err := vm.ChildrenCall(group.GetChildren(0), proc, group.OpAnalyzer)
	return r.Batch, err
}

// callToGetFinalResult
// if this operator need to eval the final result for `agg(y) group by x`,
// we are looping to receive all the data to fill the aggregate functions with every group,
// flush the final result as vectors once data was over.
//
// To avoid a single batch being too large,
// we split the result as many part of vector, and send them in order.
func (group *Group) callToGetFinalResult(proc *process.Process) (*batch.Batch, error) {
	group.ctr.finalResults.CleanLastPopped(proc.Mp())
	if group.ctr.state == vm.End {
		return nil, nil
	}

	// If spilling occurred, recall and merge all spilled data first.
	if group.ctr.spilled {
		if err := group.recallAndMergeSpilledData(proc); err != nil {
			return nil, err
		}
		group.ctr.spilled = false
	}

	for {
		if group.ctr.state == vm.Eval {
			if group.ctr.finalResults.IsEmpty() {
				group.ctr.state = vm.End
				return nil, nil
			}
			return group.ctr.finalResults.PopResult(proc.Mp())
		}

		res, err := group.getInputBatch(proc)
		if err != nil {
			return nil, err
		}
		if res == nil {
			group.ctr.state = vm.Eval

			if group.ctr.isDataSourceEmpty() && len(group.Exprs) == 0 {
				if err = group.generateInitialResult1WithoutGroupBy(proc); err != nil {
					return nil, err
				}
				group.ctr.finalResults.ToPopped[0].SetRowCount(1)
			}
			continue
		}
		if res.IsEmpty() {
			continue
		}

		group.ctr.dataSourceIsEmpty = false
		if err = group.consumeBatchToGetFinalResult(proc, res); err != nil {
			return nil, err
		}

		// check for spill
		currentSize := group.getSize()
		if currentSize > group.ctr.spillThreshold {
			if err := group.spillCurrentState(proc); err != nil {
				return nil, err
			}
		}

	}
}

func (group *Group) generateInitialResult1WithoutGroupBy(proc *process.Process) error {
	aggs, err := group.generateAggExec(proc)
	if err != nil {
		return err
	}

	group.ctr.finalResults.InitOnlyAgg(aggexec.GetMinAggregatorsChunkSize(nil, aggs), aggs)
	for i := range group.ctr.finalResults.AggList {
		if err = group.ctr.finalResults.AggList[i].GroupGrow(1); err != nil {
			return err
		}
	}
	return nil
}

func (group *Group) consumeBatchToGetFinalResult(
	proc *process.Process, bat *batch.Batch) error {

	if err := group.evaluateGroupByAndAgg(proc, bat); err != nil {
		return err
	}

	switch group.ctr.mtyp {
	case H0:
		// without group by.
		if group.ctr.finalResults.IsEmpty() {
			if err := group.generateInitialResult1WithoutGroupBy(proc); err != nil {
				return err
			}
		}

		group.ctr.finalResults.ToPopped[0].SetRowCount(1)
		for i := range group.ctr.finalResults.AggList {
			if err := group.ctr.finalResults.AggList[i].BulkFill(0, group.ctr.aggregateEvaluate[i].Vec); err != nil {
				return err
			}
		}

	default:
		// with group by.
		if group.ctr.finalResults.IsEmpty() {
			err := group.ctr.hashMap.BuildHashTable(false, group.ctr.mtyp == HStr, group.ctr.keyNullable, group.PreAllocSize)
			if err != nil {
				return err
			}

			aggs, err := group.generateAggExec(proc)
			if err != nil {
				return err
			}
			if err = group.ctr.finalResults.InitWithGroupBy(
				proc.Mp(),
				aggexec.GetMinAggregatorsChunkSize(group.ctr.groupByEvaluate.Vec, aggs), aggs, group.ctr.groupByEvaluate.Vec, group.PreAllocSize); err != nil {
				return err
			}
		}

		count := bat.RowCount()
		more := 0
		aggList := group.ctr.finalResults.GetAggList()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			originGroupCount := group.ctr.hashMap.Hash.GroupCount()
			vals, _, err := group.ctr.hashMap.Iter.Insert(i, n, group.ctr.groupByEvaluate.Vec)
			if err != nil {
				return err
			}
			insertList, _ := group.ctr.hashMap.GetBinaryInsertList(vals, originGroupCount)

			more, err = group.ctr.finalResults.AppendBatch(proc.Mp(), group.ctr.groupByEvaluate.Vec, i, insertList)
			if err != nil {
				return err
			}

			if more > 0 {
				for _, agg := range aggList {
					if err = agg.GroupGrow(more); err != nil {
						return err
					}
				}
			}

			for j := range aggList {
				if err = aggList[j].BatchFill(i, vals[:n], group.ctr.aggregateEvaluate[j].Vec); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (group *Group) generateAggExec(proc *process.Process) ([]aggexec.AggFuncExec, error) {
	var err error
	execs := make([]aggexec.AggFuncExec, 0, len(group.Aggs))
	defer func() {
		if err != nil {
			for _, exec := range execs {
				exec.Free()
			}
		}
	}()

	for i, ag := range group.Aggs {
		exec, err := makeAggExec(proc, ag.GetAggID(), ag.IsDistinct(), group.ctr.aggregateEvaluate[i].Typ...)
		if err != nil {
			return nil, err
		}
		execs = append(execs, exec)

		if config := ag.GetExtraConfig(); config != nil {
			if err = exec.SetExtraInformation(config, 0); err != nil {
				return nil, err
			}
		}
	}
	return execs, nil
}

func preExtendAggExecs(execs []aggexec.AggFuncExec, preAllocated uint64) (err error) {
	if allocate := int(preAllocated); allocate > 0 {
		for _, exec := range execs {
			if err = exec.PreAllocateGroups(allocate); err != nil {
				return err
			}
		}
	}
	return nil
}

// callToGetIntermediateResult return the intermediate result of grouped data aggregation,
// sending out intermediate results once the group count exceeds intermediateResultSendActionTrigger.
//
// this function will be only called when there is one MergeGroup operator was behind.
func (group *Group) callToGetIntermediateResult(proc *process.Process) (*batch.Batch, error) {
	group.ctr.intermediateResults.resetLastPopped()
	if group.ctr.state == vm.End {
		return nil, nil
	}

	r, err := group.initCtxToGetIntermediateResult(proc)
	if err != nil {
		return nil, err
	}

	// If spilling occurred, recall and merge all spilled data first.
	if group.ctr.spilled {
		if err := group.recallAndMergeSpilledData(proc); err != nil {
			return nil, err
		}
		group.ctr.spilled = false // All spilled data merged
	}

	var input *batch.Batch
	for {
		if group.ctr.state == vm.End && !group.ctr.recalling {
			return nil, nil
		}

		input, err = group.getInputBatch(proc)
		if err != nil {
			return nil, err
		}
		if input == nil {
			group.ctr.state = vm.End

			if group.ctr.isDataSourceEmpty() && len(group.Exprs) == 0 {
				r.SetRowCount(1)
				return r, nil
			} else if r.RowCount() > 0 {
				return r, nil
			}
			continue
		}
		if input.IsEmpty() {
			continue
		}
		group.ctr.dataSourceIsEmpty = false

		if next, er := group.consumeBatchToRes(proc, input, r); er != nil {
			return nil, er
		} else {
			if !next {
				// intermediate result is ready
				return r, nil
			}
		}
	}
}

func (group *Group) initCtxToGetIntermediateResult(
	proc *process.Process) (*batch.Batch, error) {

	r, err := group.ctr.intermediateResults.getResultBatch(
		proc, &group.ctr.groupByEvaluate, group.ctr.aggregateEvaluate, group.Aggs)
	if err != nil {
		return nil, err
	}

	if group.ctr.mtyp == H0 {
		for i := range r.Aggs {
			if err = r.Aggs[i].GroupGrow(1); err != nil {
				return nil, err
			}
		}
	} else {
		allocated := max(min(group.PreAllocSize, uint64(intermediateResultSendActionTrigger)), 0)
		if err = group.ctr.hashMap.BuildHashTable(true, group.ctr.mtyp == HStr, group.ctr.keyNullable, allocated); err != nil {
			return nil, err
		}
		err = preExtendAggExecs(r.Aggs, allocated)
	}

	return r, err
}

func (group *Group) consumeBatchToRes(
	proc *process.Process, bat *batch.Batch, res *batch.Batch) (receiveNext bool, err error) {

	if err = group.evaluateGroupByAndAgg(proc, bat); err != nil {
		return false, err
	}

	switch group.ctr.mtyp {
	case H0:
		// without group by.
		for i := range res.Aggs {
			if err = res.Aggs[i].BulkFill(0, group.ctr.aggregateEvaluate[i].Vec); err != nil {
				return false, err
			}
		}
		res.SetRowCount(1)
		return false, nil

	default:
		// with group by.
		count := bat.RowCount()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			originGroupCount := group.ctr.hashMap.Hash.GroupCount()
			vals, _, err1 := group.ctr.hashMap.Iter.Insert(i, n, group.ctr.groupByEvaluate.Vec)
			if err1 != nil {
				return false, err1
			}
			insertList, more := group.ctr.hashMap.GetBinaryInsertList(vals, originGroupCount)

			cnt := int(more)
			if cnt > 0 {
				for j, vec := range res.Vecs {
					if err = vec.UnionBatch(group.ctr.groupByEvaluate.Vec[j], int64(i), n, insertList, proc.Mp()); err != nil {
						return false, err
					}
				}

				for _, ag := range res.Aggs {
					if err = ag.GroupGrow(cnt); err != nil {
						return false, err
					}
				}
				res.AddRowCount(cnt)
			}

			for j, ag := range res.Aggs {
				if err = ag.BatchFill(i, vals[:n], group.ctr.aggregateEvaluate[j].Vec); err != nil {
					return false, err
				}
			}
		}

		// check for spill
		currentSize := group.getSize()
		if currentSize > group.ctr.spillThreshold {
			if err := group.spillCurrentState(proc); err != nil {
				return false, err
			}
			// need to re-initialize it for the next iteration or final flush
			_, err := group.initCtxToGetIntermediateResult(proc)
			if err != nil {
				return false, err
			}
		}

		return res.RowCount() < intermediateResultSendActionTrigger, nil
	}
}

func (group *Group) spillCurrentState(proc *process.Process) (err error) {
	// hashmap
	var hashmapData []byte = nil
	if group.ctr.hashMap.Hash != nil && group.ctr.hashMap.Hash.GroupCount() > 0 {
		hashmapData, err = group.ctr.hashMap.Hash.MarshalBinary()
		if err != nil {
			return err
		}
	}

	// aggregation states
	var aggStatesData [][]byte
	if group.NeedEval {
		aggStatesData = make([][]byte, len(group.ctr.finalResults.AggList))
		for i, agg := range group.ctr.finalResults.AggList {
			aggStatesData[i], err = aggexec.MarshalAggFuncExec(agg)
			if err != nil {
				return err
			}
		}
	} else {
		aggStatesData = make([][]byte, len(group.ctr.intermediateResults.res.Aggs))
		for i, agg := range group.ctr.intermediateResults.res.Aggs {
			aggStatesData[i], err = aggexec.MarshalAggFuncExec(agg)
			if err != nil {
				return err
			}
		}
	}

	// group-by batches (for NeedEval=true, multiple batches in ToPopped)
	var groupByBatchesData [][]byte
	if group.NeedEval {
		if len(group.ctr.finalResults.ToPopped) > 0 {
			for _, b := range group.ctr.finalResults.ToPopped {
				if b.IsEmpty() {
					continue // Skip empty batches
				}
				batData, marshalErr := b.MarshalBinary()
				if marshalErr != nil {
					return marshalErr
				}
				groupByBatchesData = append(groupByBatchesData, batData)
			}
			if len(groupByBatchesData) == 0 { // All batches were empty, nothing to spill for group-by keys
				return err
			}
		}

	} else {
		groupByBatchData, err := group.ctr.intermediateResults.res.MarshalBinary()
		if err != nil { // If intermediateResults.res is nil or empty, this will be empty.
			return err
		}
		groupByBatchesData = [][]byte{groupByBatchData}
	}

	if err := group.ctr.spiller.spillState(hashmapData, aggStatesData, groupByBatchesData); err != nil {
		return err
	}

	// clear in-memory state
	group.ctr.hashMap.Free0()
	if group.NeedEval {
		group.ctr.finalResults.Free0(proc.Mp())
		group.ctr.finalResults = GroupResultBuffer{}
	} else {
		group.ctr.intermediateResults.Free0(proc.Mp())
		group.ctr.intermediateResults = GroupResultNoneBlock{}
	}
	return nil
}

func (group *Group) recallAndMergeSpilledData(proc *process.Process) error {
	group.ctr.recalling = true
	defer func() {
		group.ctr.recalling = false
		group.ctr.spilled = false
	}()

	if group.ctr.hashMap.Hash == nil {
		if err := group.ctr.hashMap.BuildHashTable(false, group.ctr.mtyp == HStr, group.ctr.keyNullable, 0); err != nil {
			return err
		}
	}

	var currentAggs []aggexec.AggFuncExec
	var currentGroupByBatch *batch.Batch

	if group.NeedEval {
		if group.ctr.finalResults.IsEmpty() {
			aggs, err := group.generateAggExec(proc)
			if err != nil {
				return err
			}
			if err = group.ctr.finalResults.InitWithGroupBy(
				proc.Mp(),
				aggexec.GetMinAggregatorsChunkSize(group.ctr.groupByEvaluate.Vec, aggs), aggs, group.ctr.groupByEvaluate.Vec, 0); err != nil {
				return err
			}
		}
		currentAggs = group.ctr.finalResults.AggList

	} else {
		if group.ctr.intermediateResults.res == nil {
			r, err := group.ctr.intermediateResults.getResultBatch(
				proc, &group.ctr.groupByEvaluate, group.ctr.aggregateEvaluate, group.Aggs)
			if err != nil {
				return err
			}
			currentGroupByBatch = r
		} else {
			currentGroupByBatch = group.ctr.intermediateResults.res
		}
		currentAggs = currentGroupByBatch.Aggs
	}

	spillFiles := slices.Clone(group.ctr.spiller.getSpillFiles())
	for _, filePath := range spillFiles {
		hashmapData, aggStatesData, groupByBatchData, err := group.ctr.spiller.recallState(filePath)
		if err != nil {
			return err
		}

		var recalledHM hashmap.HashMap
		if group.ctr.mtyp == HStr {
			recalledHM, err = hashmap.NewStrMap(group.ctr.keyNullable)
		} else {
			recalledHM, err = hashmap.NewIntHashMap(group.ctr.keyNullable)
		}
		if err != nil {
			return err
		}
		// Pass the default hashmap allocator for deserialization.
		if err := recalledHM.UnmarshalBinary(hashmapData, hashtable.DefaultAllocator()); err != nil {
			recalledHM.Free()
			return err
		}

		recalledAggs := make([]aggexec.AggFuncExec, len(aggStatesData))
		aggMemoryManager := aggexec.NewSimpleAggMemoryManager(proc.Mp())
		for i, aggData := range aggStatesData {
			recalledAggs[i], err = aggexec.UnmarshalAggFuncExec(aggMemoryManager, aggData)
			if err != nil {
				for j := 0; j < i; j++ {
					recalledAggs[j].Free()
				}
				recalledHM.Free()
				return err
			}
		}

		recalledGroupByBatches := make([]*batch.Batch, len(groupByBatchData))
		for i, batData := range groupByBatchData {
			recalledBat := batch.NewOffHeapWithSize(len(group.ctr.groupByEvaluate.Vec))
			// Attributes are consistent, no need to unmarshal them.
			// recalledBat.Attrs = group.ctr.groupByEvaluate.Vec.Attrs
			if err := recalledBat.UnmarshalBinaryWithAnyMp(batData, proc.Mp()); err != nil {
				for _, agg := range recalledAggs {
					agg.Free()
				}
				recalledHM.Free()
				for j := 0; j < i; j++ { // Clean up already unmarshaled batches if error occurs
					recalledGroupByBatches[j].Clean(proc.Mp())
				}
				recalledBat.Clean(proc.Mp())
				return err
			}
			recalledGroupByBatches[i] = recalledBat
		}

		// Process each recalled group-by batch
		for _, recalledBat := range recalledGroupByBatches {
			if recalledBat.IsEmpty() {
				recalledBat.Clean(proc.Mp())
				continue
			}

			// Bulk insert recalled group-by keys into the current hashmap
			oldCurrentGroupCount := group.ctr.hashMap.Hash.GroupCount()
			vals, _, err := group.ctr.hashMap.Iter.Insert(0, recalledBat.RowCount(), recalledBat.Vecs)
			if err != nil {
				for _, agg := range recalledAggs {
					agg.Free()
				}
				recalledHM.Free()
				recalledBat.Clean(proc.Mp()) // Clean current batch
				// Clean remaining batches
				for _, bat := range recalledGroupByBatches {
					if bat != recalledBat {
						bat.Clean(proc.Mp())
					}
				}
				return err
			}

			// Identify newly added groups and map recalled group IDs to current group IDs
			// This map is specific to the current recalled batch's original group IDs
			recalledToCurrentGroupMap := make([]uint64, recalledBat.RowCount()+1) // +1 for 1-based indexing
			currentBatchNewGroupsSels := make([]int32, 0)

			for i := 0; i < recalledBat.RowCount(); i++ {
				newID := vals[i]
				recalledToCurrentGroupMap[uint64(i+1)] = newID

				if newID > oldCurrentGroupCount {
					currentBatchNewGroupsSels = append(currentBatchNewGroupsSels, int32(i))
				}
			}

			if len(currentBatchNewGroupsSels) > 0 {
				// If there are new groups, append their data to the current group-by batch and grow aggregators
				if group.NeedEval {
					// Create a new batch containing only the newly added group-by keys from this recalled batch
					newGroupByKeysBatch := batch.NewOffHeapWithSize(len(recalledBat.Vecs))
					for i, vec := range recalledBat.Vecs {
						newVec, err := vec.CloneWindow(0, recalledBat.RowCount(), proc.Mp())
						if err != nil {
							newGroupByKeysBatch.Clean(proc.Mp())
							recalledHM.Free()
							for _, agg := range recalledAggs {
								agg.Free()
							}
							recalledBat.Clean(proc.Mp())
							return err
						}
						newVec.Shrink(int32SliceToInt64(currentBatchNewGroupsSels), false)
						newGroupByKeysBatch.SetVector(int32(i), newVec)
					}
					newGroupByKeysBatch.SetRowCount(len(currentBatchNewGroupsSels))

					// Push the batch containing only new group-by keys to finalResults.ToPopped
					if err := group.ctr.finalResults.AppendRecalledBatches(proc.Mp(), []*batch.Batch{newGroupByKeysBatch}); err != nil {
						recalledHM.Free()
						for _, agg := range recalledAggs {
							agg.Free()
						}
						newGroupByKeysBatch.Clean(proc.Mp())
						recalledBat.Clean(proc.Mp())
						return err
					}

					// Grow aggregators for newly added groups
					for _, agg := range currentAggs {
						if err := agg.GroupGrow(len(currentBatchNewGroupsSels)); err != nil {
							recalledHM.Free()
							for _, agg := range recalledAggs {
								agg.Free()
							}
							newGroupByKeysBatch.Clean(proc.Mp())
							recalledBat.Clean(proc.Mp())
							return err
						}
					}
				} else { // !group.NeedEval
					// For intermediate results, we need to append the new group-by keys to the current batch.
					// The `Union` method on batch appends selected rows.
					// We need to ensure the vectors in currentGroupByBatch are extensible.
					if err := currentGroupByBatch.Union(recalledBat, int32SliceToInt64(currentBatchNewGroupsSels), proc.Mp()); err != nil { // This union will append new rows to currentGroupByBatch.Vecs
						recalledHM.Free()
						for _, agg := range recalledAggs {
							agg.Free()
						}
						recalledBat.Clean(proc.Mp())
						return err
					}
					for _, agg := range currentAggs {
						if err := agg.GroupGrow(len(currentBatchNewGroupsSels)); err != nil {
							recalledHM.Free()
							for _, agg := range recalledAggs {
								agg.Free()
							}
							recalledBat.Clean(proc.Mp())
							return err
						}

					}
				}
			}

			// Merge aggregation states for the current recalled batch
			// The BatchMerge function expects the `groups` parameter to be the *new* group IDs.
			// recalledToCurrentGroupMap[1:] provides this mapping for the original recalled group IDs.
			for aggIdx := range currentAggs {
				recalledAgg := recalledAggs[aggIdx]
				if err := currentAggs[aggIdx].BatchMerge(recalledAgg, 0, recalledToCurrentGroupMap[1:]); err != nil {
					for j := aggIdx; j < len(currentAggs); j++ {
						recalledAggs[j].Free()
					}
					recalledHM.Free()
					recalledBat.Clean(proc.Mp())
					return err
				}
			}
			recalledBat.Clean(proc.Mp()) // Clean the current recalled batch after processing
		}

		recalledHM.Free()
		for _, agg := range recalledAggs {
			agg.Free()
		}
		if err := group.ctr.spiller.DeleteFile(filePath); err != nil {
			return err
		}

	}

	return nil
}

func (group *Group) getSize() int64 {
	var size int64

	// Hash table size
	if group.ctr.hashMap.Hash != nil {
		size += group.ctr.hashMap.Hash.Size()
	}

	// Aggregation results size
	if group.NeedEval {
		for _, bat := range group.ctr.finalResults.ToPopped {
			if bat != nil {
				size += int64(bat.Allocated())
			}
		}
		for _, agg := range group.ctr.finalResults.AggList {
			if agg != nil {
				size += agg.Size()
			}
		}

	} else {
		if group.ctr.intermediateResults.res != nil {
			size += int64(group.ctr.intermediateResults.res.Allocated())
			for _, agg := range group.ctr.intermediateResults.res.Aggs {
				if agg != nil {
					size += agg.Size()
				}
			}
		}
	}

	return size
}
