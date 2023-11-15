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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	switch arg.Type {
	case sampleByRow:
		buf.WriteString(fmt.Sprintf(" sample %d rows ", arg.Rows))
	case sampleByPercent:
		buf.WriteString(fmt.Sprintf(" sample %.2f percent ", arg.Percents))
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
		arg.ctr.samplePool = newSamplePoolByRows(proc, arg.Rows, len(arg.SampleExprs))
	case sampleByPercent:
		arg.ctr.samplePool = newSamplePoolByPercent(proc, arg.Percents, len(arg.SampleExprs))
	default:
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("unknown sample type %d", arg.Type))
	}

	// sample column related.
	arg.ctr.sampleExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.SampleExprs)
	if err != nil {
		return err
	}

	// group by columns related.
	arg.ctr.keyWidth = 0
	arg.ctr.groupVectorsNullable = false
	if arg.ctr.isGroupBy {
		arg.ctr.groupExecutors = make([]colexec.ExpressionExecutor, len(arg.GroupExprs))
		for i, expr := range arg.GroupExprs {
			arg.ctr.groupExecutors[i], err = colexec.NewExpressionExecutor(proc, arg.GroupExprs[i])
			if err != nil {
				return err
			}

			width := types.T(expr.Typ.Id).TypeLen()
			arg.ctr.groupVectorsNullable = arg.ctr.groupVectorsNullable || (!expr.Typ.NotNullable)
			if types.T(expr.Typ.Id).FixedLength() < 0 {
				width = 128
				if expr.Typ.Width != 0 {
					width = int(expr.Typ.Width)
				}
			}
			if arg.ctr.groupVectorsNullable {
				width++
			}
			arg.ctr.keyWidth += width
		}
		arg.ctr.groupVectors = make([]*vector.Vector, len(arg.GroupExprs))
	}

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	result, lastErr := arg.children[0].Call(proc)
	if lastErr != nil {
		return result, lastErr
	}

	if arg.buf != nil {
		proc.PutBatch(arg.buf)
		arg.buf = nil
	}
	arg.buf = result.Batch
	bat := result.Batch

	ctr := arg.ctr
	if bat == nil {
		result.Batch, lastErr = ctr.samplePool.Output(true)
		return result, lastErr
	}

	var err error
	if !bat.IsEmpty() {
		ctr.tempBatch1[0] = bat
		// evaluate the sample columns.
		for i, executor := range ctr.sampleExecutors {
			ctr.sampleVectors[i], err = executor.Eval(proc, ctr.tempBatch1)
			if err != nil {
				return result, err
			}
		}

		if ctr.isGroupBy {
			// evaluate the group by columns.
			for i, executor := range ctr.groupExecutors {
				ctr.groupVectors[i], err = executor.Eval(proc, ctr.tempBatch1)
				if err != nil {
					return result, err
				}
			}

			err = ctr.hashAndSample(bat, arg.IBucket, arg.NBucket, proc.Mp())
		} else {
			err = ctr.samplePool.Sample(1, ctr.sampleVectors, nil, bat)
		}

		if err != nil {
			return result, err
		}
	}

	result.Batch, err = ctr.samplePool.Output(false)
	return result, err
}

func (ctr *container) hashAndSample(bat *batch.Batch, ib, nb int, mp *mpool.MPool) (err error) {
	var iterator hashmap.Iterator
	var groupList []uint64
	count := bat.RowCount()

	useIntHashMap := ctr.keyWidth <= 8
	if useIntHashMap {
		if ctr.intHashMap == nil {
			ctr.intHashMap, err = hashmap.NewIntHashMap(ctr.groupVectorsNullable, uint64(ib), uint64(nb), mp)
			if err != nil {
				return err
			}
		}
		iterator = ctr.intHashMap.NewIterator()
	} else {
		if ctr.strHashMap == nil {
			ctr.strHashMap, err = hashmap.NewStrMap(ctr.groupVectorsNullable, uint64(ib), uint64(nb), mp)
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
		err = ctr.samplePool.BatchSample(n, groupList, ctr.sampleVectors, ctr.groupVectors, bat)
		if err != nil {
			return err
		}
	}
	return
}
