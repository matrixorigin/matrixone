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
	if sample.OpAnalyzer == nil {
		sample.OpAnalyzer = process.NewAnalyzer(sample.GetIdx(), sample.IsFirst, sample.IsLast, "sample")
	} else {
		sample.OpAnalyzer.Reset()
	}

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
	analyzer := sample.OpAnalyzer

	// duplicate code from other operators.
	result, lastErr := vm.ChildrenCall(sample.GetChildren(0), proc, analyzer)
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
		sample.ctr.buf = result.Batch
		result.Status = vm.ExecStop
		ctr.workDone = true
		return result, lastErr
	}

	var err error
	if !bat.IsEmpty() {
		if err = ctr.evaluateSampleAndGroupByColumns(proc, bat); err != nil {
			return result, err
		}

		if ctr.isGroupBy {
			err = ctr.hashAndSample(bat, proc)
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

func (ctr *container) hashAndSample(bat *batch.Batch, proc *process.Process) (err error) {
	count := bat.RowCount()
	if !hasGroupingRows(ctr.groupVectors) {
		return ctr.hashNormalRows(bat, proc, 0, count)
	}
	if err = ctr.enableGroupingDomain(proc); err != nil {
		return err
	}
	groupingIterator := ctr.groupingHashMap.NewIterator()
	var normalIterator hashmap.Iterator

	for offset := 0; offset < count; {
		grouping := rowHasGrouping(ctr.groupVectors, offset)
		end := offset + 1
		for end < count && rowHasGrouping(ctr.groupVectors, end) == grouping {
			end++
		}
		if grouping {
			err = ctr.hashRows(
				bat,
				groupingIterator,
				&ctr.groupingGroupIDs,
				offset,
				end-offset,
				true,
			)
		} else {
			if normalIterator == nil {
				normalIterator, err = ctr.normalIterator(proc)
				if err != nil {
					return err
				}
			}
			err = ctr.hashRows(
				bat,
				normalIterator,
				&ctr.normalGroupIDs,
				offset,
				end-offset,
				true,
			)
		}
		if err != nil {
			return err
		}
		offset = end
	}
	return nil
}

func hasGroupingRows(vecs []*vector.Vector) bool {
	for _, vec := range vecs {
		if vec != nil && vec.HasGrouping() {
			return true
		}
	}
	return false
}

func rowHasGrouping(vecs []*vector.Vector, row int) bool {
	for _, vec := range vecs {
		if vec != nil && vec.GetGrouping().Contains(uint64(row)) {
			return true
		}
	}
	return false
}

func (ctr *container) normalIterator(proc *process.Process) (hashmap.Iterator, error) {
	var err error

	if ctr.useIntHashMap {
		if ctr.intHashMap == nil {
			ctr.intHashMap, err = hashmap.NewIntHashMap(ctr.groupVectorsNullable, proc.Mp())
			if err != nil {
				return nil, err
			}
		}
		return ctr.intHashMap.NewIterator(), nil
	} else {
		if ctr.strHashMap == nil {
			ctr.strHashMap, err = hashmap.NewStrHashMap(ctr.groupVectorsNullable, proc.Mp())
			if err != nil {
				return nil, err
			}
		}
		return ctr.strHashMap.NewIterator(), nil
	}
}

func (ctr *container) normalGroupCount() uint64 {
	if ctr.useIntHashMap && ctr.intHashMap != nil {
		return ctr.intHashMap.GroupCount()
	}
	if !ctr.useIntHashMap && ctr.strHashMap != nil {
		return ctr.strHashMap.GroupCount()
	}
	return 0
}

func (ctr *container) enableGroupingDomain(proc *process.Process) error {
	if ctr.groupingHashMap != nil {
		return nil
	}
	groupingMap, err := hashmap.NewStrHashMap(
		ctr.groupVectorsNullable,
		proc.Mp(),
	)
	if err != nil {
		return err
	}
	if err = groupingMap.SetGroupingAware(); err != nil {
		groupingMap.Free()
		return err
	}

	normalGroups := ctr.normalGroupCount()
	ctr.normalGroupIDs = make([]uint64, normalGroups+1)
	for i := uint64(1); i <= normalGroups; i++ {
		ctr.normalGroupIDs[i] = i
	}
	ctr.groupingGroupIDs = []uint64{0}
	ctr.nextGroupID = normalGroups
	ctr.groupingHashMap = groupingMap
	return nil
}

func (ctr *container) globalGroupIDs(
	local []uint64,
	translation *[]uint64,
) []uint64 {
	ids := *translation
	for i, localID := range local {
		if localID == 0 {
			continue
		}
		for uint64(len(ids)) <= localID {
			ctr.nextGroupID++
			ids = append(ids, ctr.nextGroupID)
		}
		local[i] = ids[localID]
	}
	*translation = ids
	return local
}

func (ctr *container) hashNormalRows(
	bat *batch.Batch,
	proc *process.Process,
	offset int,
	count int,
) error {
	iterator, err := ctr.normalIterator(proc)
	if err != nil {
		return err
	}
	return ctr.hashRows(
		bat,
		iterator,
		&ctr.normalGroupIDs,
		offset,
		count,
		ctr.groupingHashMap != nil,
	)
}

func (ctr *container) hashRows(
	bat *batch.Batch,
	iterator hashmap.Iterator,
	translation *[]uint64,
	offset int,
	count int,
	translate bool,
) error {
	end := offset + count
	for offset < end {
		n := end - offset
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		groupList, _, err := iterator.Insert(offset, n, ctr.groupVectors)
		if err != nil {
			return err
		}
		if translate {
			groupList = ctr.globalGroupIDs(groupList[:n], translation)
		}
		if err = ctr.samplePool.BatchSample(
			offset,
			n,
			groupList,
			ctr.sampleVectors,
			ctr.groupVectors,
			bat,
		); err != nil {
			return err
		}
		offset += n
	}
	return nil
}
