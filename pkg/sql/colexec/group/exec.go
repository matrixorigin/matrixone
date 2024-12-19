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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var makeAggExec = aggexec.MakeAgg

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
	group.ctr.result1.CleanLastPopped(proc.Mp())
	if group.ctr.state == vm.End {
		return nil, nil
	}

	for {
		if group.ctr.state == vm.Eval {
			if group.ctr.result1.IsEmpty() {
				group.ctr.state = vm.End
				return nil, nil
			}
			return group.ctr.result1.PopResult(proc.Mp())
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
				group.ctr.result1.ToPopped[0].SetRowCount(1)
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
	}
}

func (group *Group) generateInitialResult1WithoutGroupBy(proc *process.Process) error {
	aggs, err := group.generateAggExec(proc)
	if err != nil {
		return err
	}

	group.ctr.result1.InitOnlyAgg(aggexec.GetMinAggregatorsChunkSize(nil, aggs), aggs)
	for i := range group.ctr.result1.AggList {
		if err = group.ctr.result1.AggList[i].GroupGrow(1); err != nil {
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
		if group.ctr.result1.IsEmpty() {
			if err := group.generateInitialResult1WithoutGroupBy(proc); err != nil {
				return err
			}
		}

		group.ctr.result1.ToPopped[0].SetRowCount(1)
		for i := range group.ctr.result1.AggList {
			if err := group.ctr.result1.AggList[i].BulkFill(0, group.ctr.aggregateEvaluate[i].Vec); err != nil {
				return err
			}
		}

	default:
		// with group by.
		if group.ctr.result1.IsEmpty() {
			err := group.ctr.hr.BuildHashTable(false, group.ctr.mtyp == HStr, group.ctr.keyNullable, group.PreAllocSize)
			if err != nil {
				return err
			}

			aggs, err := group.generateAggExec(proc)
			if err != nil {
				return err
			}
			group.ctr.result1.InitWithGroupBy(aggexec.GetMinAggregatorsChunkSize(group.ctr.groupByEvaluate.Vec, aggs), aggs, group.ctr.groupByEvaluate.Vec)
			if err = preExtendAggExecs(aggs, group.PreAllocSize); err != nil {
				return err
			}
		}

		count := bat.RowCount()
		more := 0
		aggList := group.ctr.result1.GetAggList()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			originGroupCount := group.ctr.hr.Hash.GroupCount()
			vals, _, err := group.ctr.hr.Itr.Insert(i, n, group.ctr.groupByEvaluate.Vec)
			if err != nil {
				return err
			}
			insertList, _ := group.ctr.hr.GetBinaryInsertList(vals, originGroupCount)

			more, err = group.ctr.result1.AppendBatch(proc.Mp(), group.ctr.groupByEvaluate.Vec, i, insertList)
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
		exec := makeAggExec(proc, ag.GetAggID(), ag.IsDistinct(), group.ctr.aggregateEvaluate[i].Typ...)
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

// callToGetIntermediateResult
// if this operator should only return aggregations' intermediate results because one MergeGroup operator was behind.
// we do not engage in any waiting actions at this operator,
// once a batch is received, a batch with the corresponding intermediate result will be returned.
func (group *Group) callToGetIntermediateResult(proc *process.Process) (*batch.Batch, error) {
	group.ctr.result2.resetLastPopped()
	if group.ctr.state == vm.End {
		return nil, nil
	}

	for {
		res, err := group.getInputBatch(proc)
		if err != nil {
			return nil, err
		}
		if res == nil {
			group.ctr.state = vm.End

			if group.ctr.isDataSourceEmpty() && len(group.Exprs) == 0 {
				r, er := group.ctr.result2.getResultBatch(
					proc, &group.ctr.groupByEvaluate, group.ctr.aggregateEvaluate, group.Aggs)
				if er != nil {
					return nil, er
				}
				for i := range r.Aggs {
					if err = r.Aggs[i].GroupGrow(1); err != nil {
						return nil, err
					}
				}
				r.SetRowCount(1)
				return r, nil
			}

			return nil, nil
		}
		if res.IsEmpty() {
			continue
		}
		group.ctr.dataSourceIsEmpty = false
		return group.consumeBatchToGetIntermediateResult(proc, res)
	}
}

func (group *Group) consumeBatchToGetIntermediateResult(
	proc *process.Process, bat *batch.Batch) (*batch.Batch, error) {

	if err := group.evaluateGroupByAndAgg(proc, bat); err != nil {
		return nil, err
	}

	res, err := group.ctr.result2.getResultBatch(
		proc, &group.ctr.groupByEvaluate, group.ctr.aggregateEvaluate, group.Aggs)
	if err != nil {
		return nil, err
	}

	switch group.ctr.mtyp {
	case H0:
		// without group by.
		for i := range res.Aggs {
			if err = res.Aggs[i].GroupGrow(1); err != nil {
				return nil, err
			}
		}
		res.SetRowCount(1)
		for i := range res.Aggs {
			if err = res.Aggs[i].BulkFill(0, group.ctr.aggregateEvaluate[i].Vec); err != nil {
				return nil, err
			}
		}

	default:
		// with group by.
		if err = group.ctr.hr.BuildHashTable(true, group.ctr.mtyp == HStr, group.ctr.keyNullable, 0); err != nil {
			return nil, err
		}

		count := bat.RowCount()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			originGroupCount := group.ctr.hr.Hash.GroupCount()
			vals, _, err1 := group.ctr.hr.Itr.Insert(i, n, group.ctr.groupByEvaluate.Vec)
			if err1 != nil {
				return nil, err1
			}
			insertList, more := group.ctr.hr.GetBinaryInsertList(vals, originGroupCount)

			cnt := int(more)
			if cnt > 0 {
				for j, vec := range res.Vecs {
					if err = vec.UnionBatch(group.ctr.groupByEvaluate.Vec[j], int64(i), n, insertList, proc.Mp()); err != nil {
						return nil, err
					}
				}

				for _, ag := range res.Aggs {
					if err = ag.GroupGrow(cnt); err != nil {
						return nil, err
					}
				}
				res.AddRowCount(cnt)
			}

			for j, ag := range res.Aggs {
				if err = ag.BatchFill(i, vals[:n], group.ctr.aggregateEvaluate[j].Vec); err != nil {
					return nil, err
				}
			}
		}

	}

	return res, nil
}
