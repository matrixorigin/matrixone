// Copyright 2021 Matrix Origin
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

package sample

import (
	"bytes"
	"fmt"
	"math/rand"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "sample"

func (sample *Sample) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": ")
	switch sample.Type {
	case mergeSampleByRow:
		buf.WriteString(fmt.Sprintf("merge sample %d rows ", sample.Rows))
	case sampleByRow:
		buf.WriteString(fmt.Sprintf(" sample %d rows ", sample.Rows))
		if sample.UsingBlock {
			buf.WriteString("using blocks ")
		} else {
			buf.WriteString("using rows ")
		}
	case sampleByPercent:
		buf.WriteString(fmt.Sprintf(" sample %.2f percent ", sample.Percents))
		if sample.UsingBlock {
			buf.WriteString("using blocks ")
		} else {
			buf.WriteString("using rows ")
		}
	default:
		buf.WriteString("unknown sample type")
	}
}

func (sample *Sample) OpType() vm.OpType {
	return vm.Sample
}

func (sample *Sample) Prepare(proc *process.Process) (err error) {
	sample.ctr = &container{
		isGroupBy:     len(sample.GroupExprs) != 0,
		isMultiSample: len(sample.SampleExprs) > 1,
		tempBatch1:    make([]*batch.Batch, 1),
		sampleVectors: make([]*vector.Vector, len(sample.SampleExprs)),
	}

	switch sample.Type {
	case sampleByRow:
		sample.ctr.samplePool = newSamplePoolByRows(proc, sample.Rows, len(sample.SampleExprs), sample.NeedOutputRowSeen)
	case sampleByPercent:
		sample.ctr.samplePool = newSamplePoolByPercent(proc, sample.Percents, len(sample.SampleExprs))
	case mergeSampleByRow:
		sample.ctr.samplePool = newSamplePoolByRowsForMerge(proc, sample.Rows, len(sample.SampleExprs), sample.NeedOutputRowSeen)
	default:
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("unknown sample type %d", sample.Type))
	}
	sample.ctr.samplePool.setPerfFields(sample.ctr.isGroupBy)

	// sample column related.
	sample.ctr.sampleExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, sample.SampleExprs)
	if err != nil {
		return err
	}

	// group by columns related.
	sample.ctr.groupVectorsNullable = false
	if sample.ctr.isGroupBy {
		sample.ctr.groupExecutors = make([]colexec.ExpressionExecutor, len(sample.GroupExprs))
		for i := range sample.GroupExprs {
			sample.ctr.groupExecutors[i], err = colexec.NewExpressionExecutor(proc, sample.GroupExprs[i])
			if err != nil {
				return err
			}
		}
		sample.ctr.groupVectors = make([]*vector.Vector, len(sample.GroupExprs))

		keyWidth, groupKeyNullable := getGroupKeyWidth(sample.GroupExprs)
		sample.ctr.useIntHashMap = keyWidth <= 8
		sample.ctr.groupVectorsNullable = groupKeyNullable
	}

	return nil
}

func (sample *Sample) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(sample.GetIdx(), sample.GetParallelIdx(), sample.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	// duplicate code from other operators.
	result, lastErr := vm.ChildrenCall(sample.Children[0], proc, anal)
	if lastErr != nil {
		return result, lastErr
	}

	if sample.ctr.buf != nil {
		sample.ctr.buf.Clean(proc.GetMPool())
		sample.ctr.buf = nil
	}

	// real work starts here.
	bat := result.Batch

	ctr := sample.ctr
	if ctr.workDone {
		result.Batch = nil
		return result, nil
	}

	if bat == nil {
		result.Batch, lastErr = ctr.samplePool.Result(true)
		anal.Output(result.Batch, sample.GetIsLast())
		sample.ctr.buf = result.Batch
		result.Status = vm.ExecStop
		ctr.workDone = true
		return result, lastErr
	}

	var err error
	if !bat.IsEmpty() {
		anal.Input(bat, sample.GetIsFirst())

		if err = ctr.evaluateSampleAndGroupByColumns(proc, bat); err != nil {
			return result, err
		}

		if ctr.isGroupBy {
			err = ctr.hashAndSample(bat)
		} else {
			err = ctr.samplePool.Sample(1, ctr.sampleVectors, nil, bat)
		}
		if err != nil {
			return result, err
		}
	}

	if sample.UsingBlock && ctr.samplePool.IsFull() && rand.Intn(2) == 0 {
		result.Batch, err = ctr.samplePool.Result(true)
		result.Status = vm.ExecStop
		ctr.workDone = true

	} else {
		result.Batch, err = ctr.samplePool.Result(false)
	}
	anal.Output(result.Batch, sample.GetIsLast())
	sample.ctr.buf = result.Batch
	return result, err
}

func getGroupKeyWidth(exprList []*plan.Expr) (keyWidth int, groupKeyNullable bool) {
	keyWidth = 0
	groupKeyNullable = false

	for _, expr := range exprList {
		width := types.T(expr.Typ.Id).TypeLen()
		groupKeyNullable = groupKeyNullable || (!expr.Typ.NotNullable)
		if types.T(expr.Typ.Id).FixedLength() < 0 {
			width = 128
			if expr.Typ.Width != 0 {
				width = int(expr.Typ.Width)
			}
		}
		if groupKeyNullable {
			width++
		}
		keyWidth += width
	}
	return keyWidth, groupKeyNullable
}

func (ctr *container) evaluateSampleAndGroupByColumns(proc *process.Process, bat *batch.Batch) (err error) {
	ctr.tempBatch1[0] = bat
	// evaluate the sample columns.
	for i, executor := range ctr.sampleExecutors {
		ctr.sampleVectors[i], err = executor.Eval(proc, ctr.tempBatch1, nil)
		if err != nil {
			return err
		}
	}

	// evaluate the group by columns.
	for i, executor := range ctr.groupExecutors {
		ctr.groupVectors[i], err = executor.Eval(proc, ctr.tempBatch1, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) hashAndSample(bat *batch.Batch) (err error) {
	var iterator hashmap.Iterator
	var groupList []uint64
	count := bat.RowCount()

	if ctr.useIntHashMap {
		if ctr.intHashMap == nil {
			ctr.intHashMap, err = hashmap.NewIntHashMap(ctr.groupVectorsNullable)
			if err != nil {
				return err
			}
		}
		iterator = ctr.intHashMap.NewIterator()
	} else {
		if ctr.strHashMap == nil {
			ctr.strHashMap, err = hashmap.NewStrMap(ctr.groupVectorsNullable)
			if err != nil {
				return err
			}
		}
		iterator = ctr.strHashMap.NewIterator()
	}

	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		groupList, _, err = iterator.Insert(i, n, ctr.groupVectors)
		if err != nil {
			return err
		}
		err = ctr.samplePool.BatchSample(i, n, groupList, ctr.sampleVectors, ctr.groupVectors, bat)
		if err != nil {
			return err
		}
	}
	return
}
