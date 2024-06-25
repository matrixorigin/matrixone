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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "sample"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": ")
	switch arg.Type {
	case mergeSampleByRow:
		buf.WriteString(fmt.Sprintf("merge sample %d rows ", arg.Rows))
	case sampleByRow:
		buf.WriteString(fmt.Sprintf(" sample %d rows ", arg.Rows))
		if arg.UsingBlock {
			buf.WriteString("using blocks ")
		} else {
			buf.WriteString("using rows ")
		}
	case sampleByPercent:
		buf.WriteString(fmt.Sprintf(" sample %.2f percent ", arg.Percents))
		if arg.UsingBlock {
			buf.WriteString("using blocks ")
		} else {
			buf.WriteString("using rows ")
		}
	default:
		buf.WriteString("unknown sample type")
	}
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	arg.ctr = &container{
		isGroupBy:     len(arg.GroupExprs) != 0,
		isMultiSample: len(arg.SampleExprs) > 1,
		tempBatch1:    make([]*batch.Batch, 1),
		sampleVectors: make([]*vector.Vector, len(arg.SampleExprs)),
	}

	switch arg.Type {
	case sampleByRow:
		arg.ctr.samplePool = newSamplePoolByRows(proc, arg.Rows, len(arg.SampleExprs), arg.NeedOutputRowSeen)
	case sampleByPercent:
		arg.ctr.samplePool = newSamplePoolByPercent(proc, arg.Percents, len(arg.SampleExprs))
	case mergeSampleByRow:
		arg.ctr.samplePool = newSamplePoolByRowsForMerge(proc, arg.Rows, len(arg.SampleExprs), arg.NeedOutputRowSeen)
	default:
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("unknown sample type %d", arg.Type))
	}
	arg.ctr.samplePool.setPerfFields(arg.ctr.isGroupBy)

	// sample column related.
	arg.ctr.sampleExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.SampleExprs)
	if err != nil {
		return err
	}

	// group by columns related.
	arg.ctr.groupVectorsNullable = false
	if arg.ctr.isGroupBy {
		arg.ctr.groupExecutors = make([]colexec.ExpressionExecutor, len(arg.GroupExprs))
		for i := range arg.GroupExprs {
			arg.ctr.groupExecutors[i], err = colexec.NewExpressionExecutor(proc, arg.GroupExprs[i])
			if err != nil {
				return err
			}
		}
		arg.ctr.groupVectors = make([]*vector.Vector, len(arg.GroupExprs))

		keyWidth, groupKeyNullable := getGroupKeyWidth(arg.GroupExprs)
		arg.ctr.useIntHashMap = keyWidth <= 8
		arg.ctr.groupVectorsNullable = groupKeyNullable
	}

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	// duplicate code from other operators.
	result, lastErr := arg.GetChildren(0).Call(proc)
	if lastErr != nil {
		return result, lastErr
	}
	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	if arg.ctr.buf != nil {
		proc.PutBatch(arg.ctr.buf)
		arg.ctr.buf = nil
	}

	// real work starts here.
	bat := result.Batch

	ctr := arg.ctr
	if ctr.workDone {
		result.Batch = nil
		return result, nil
	}

	if bat == nil {
		result.Batch, lastErr = ctr.samplePool.Result(true)
		anal.Output(result.Batch, arg.GetIsLast())
		arg.ctr.buf = result.Batch
		result.Status = vm.ExecStop
		ctr.workDone = true
		return result, lastErr
	}

	var err error
	if !bat.IsEmpty() {
		anal.Input(bat, arg.GetIsFirst())

		if err = ctr.evaluateSampleAndGroupByColumns(proc, bat); err != nil {
			return result, err
		}

		if ctr.isGroupBy {
			err = ctr.hashAndSample(bat, proc.Mp())
		} else {
			err = ctr.samplePool.Sample(1, ctr.sampleVectors, nil, bat)
		}
		if err != nil {
			return result, err
		}
	}

	if arg.UsingBlock && ctr.samplePool.IsFull() && rand.Intn(2) == 0 {
		result.Batch, err = ctr.samplePool.Result(true)
		result.Status = vm.ExecStop
		ctr.workDone = true

	} else {
		result.Batch, err = ctr.samplePool.Result(false)
	}
	anal.Output(result.Batch, arg.GetIsLast())
	arg.ctr.buf = result.Batch
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

func (ctr *container) hashAndSample(bat *batch.Batch, mp *mpool.MPool) (err error) {
	var iterator hashmap.Iterator
	var groupList []uint64
	count := bat.RowCount()

	if ctr.useIntHashMap {
		if ctr.intHashMap == nil {
			ctr.intHashMap, err = hashmap.NewIntHashMap(ctr.groupVectorsNullable, mp)
			if err != nil {
				return err
			}
		}
		iterator = ctr.intHashMap.NewIterator()
	} else {
		if ctr.strHashMap == nil {
			ctr.strHashMap, err = hashmap.NewStrMap(ctr.groupVectorsNullable, mp)
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
